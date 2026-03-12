"""Tests for Historical Replay Mode."""

import os
from datetime import UTC, datetime, timedelta

import pytest
from fastapi import WebSocketDisconnect

import gateway.api.replay as replay_api
import gateway.core.replay as replay_module
from gateway.core.replay import (
    ReplayConfig,
    ReplayMessage,
    ReplaySession,
    ReplaySessionManager,
    ReplayState,
    get_replay_manager,
)


class TestReplayConfig:
    """Test ReplayConfig validation."""

    def test_valid_config(self):
        """Valid config should pass validation."""
        config = ReplayConfig(
            name="test-replay",
            symbols=["AAPL", "MSFT"],
            feeds=["bars"],
            start=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
            end=datetime(2024, 1, 15, 16, 0, tzinfo=UTC),
            speed=10.0,
        )
        assert config.validate() == []

    def test_empty_symbols_fails(self):
        """Empty symbols should fail validation."""
        config = ReplayConfig(
            name="test",
            symbols=[],
            feeds=["bars"],
            start=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
            end=datetime(2024, 1, 15, 16, 0, tzinfo=UTC),
        )
        errors = config.validate()
        assert any("symbols" in e for e in errors)

    def test_start_after_end_fails(self):
        """Start after end should fail validation."""
        config = ReplayConfig(
            name="test",
            symbols=["AAPL"],
            feeds=["bars"],
            start=datetime(2024, 1, 15, 16, 0, tzinfo=UTC),
            end=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
        )
        errors = config.validate()
        assert any("start must be before end" in e for e in errors)

    def test_speed_too_high_fails(self):
        """Speed > 100 should fail validation."""
        config = ReplayConfig(
            name="test",
            symbols=["AAPL"],
            feeds=["bars"],
            start=datetime(2024, 1, 15, 9, 30, tzinfo=UTC),
            end=datetime(2024, 1, 15, 16, 0, tzinfo=UTC),
            speed=150.0,
        )
        errors = config.validate()
        assert any("speed" in e for e in errors)


class TestReplaySession:
    """Test ReplaySession functionality."""

    def test_progress_calculation(self):
        """Progress should be calculated correctly."""
        config = ReplayConfig(
            name="test",
            symbols=["AAPL"],
            feeds=["bars"],
            start=datetime(2024, 1, 15, 9, 0, tzinfo=UTC),
            end=datetime(2024, 1, 15, 10, 0, tzinfo=UTC),
        )
        session = ReplaySession(session_id="test-123", config=config, client_id="test-client")

        # Initial progress should be 0
        assert session.progress == 0.0

        # Halfway through
        session.current_timestamp = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)
        assert session.progress == 0.5

    def test_estimated_duration(self):
        """Estimated duration should account for speed."""
        config = ReplayConfig(
            name="test",
            symbols=["AAPL"],
            feeds=["bars"],
            start=datetime(2024, 1, 15, 9, 0, tzinfo=UTC),
            end=datetime(2024, 1, 15, 10, 0, tzinfo=UTC),
            speed=10.0,
        )
        session = ReplaySession(session_id="test-123", config=config, client_id="test-client")

        # 1 hour / 10x speed = 360 seconds
        assert session.estimated_duration_seconds == 360.0

    def test_pause_resume(self):
        """Session should pause and resume correctly."""
        config = ReplayConfig(
            name="test",
            symbols=["AAPL"],
            feeds=["bars"],
            start=datetime.now(UTC),
            end=datetime.now(UTC) + timedelta(hours=1),
        )
        session = ReplaySession(session_id="test-123", config=config, client_id="test-client")

        session.pause()
        assert session.state == ReplayState.PAUSED

        session.resume(speed=5.0)
        assert session.state == ReplayState.RUNNING
        assert session.speed == 5.0

    def test_stop(self):
        """Session should stop correctly."""
        config = ReplayConfig(
            name="test",
            symbols=["AAPL"],
            feeds=["bars"],
            start=datetime.now(UTC),
            end=datetime.now(UTC) + timedelta(hours=1),
        )
        session = ReplaySession(session_id="test-123", config=config, client_id="test-client")

        session.stop()
        assert session.state == ReplayState.STOPPED
        assert session.ended_at is not None


class TestReplayMessage:
    """Test ReplayMessage formatting."""

    def test_ws_message_format(self):
        """WebSocket message should have correct format."""
        msg = ReplayMessage(
            feed="stock_bars",
            symbol="AAPL",
            data={"open": 150.0, "high": 151.0, "low": 149.5, "close": 150.5},
            market_timestamp=datetime(2024, 1, 15, 10, 30, tzinfo=UTC),
        )

        ws_msg = msg.to_ws_message(
            sequence=100,
            session_progress=0.5,
            replay_speed=10.0,
        )

        assert ws_msg["type"] == "data"
        assert ws_msg["feed"] == "stock_bars"
        assert ws_msg["symbol"] == "AAPL"
        assert ws_msg["data"]["open"] == 150.0
        assert ws_msg["meta"]["sequence"] == 100
        assert ws_msg["meta"]["replay_speed"] == 10.0
        assert ws_msg["meta"]["session_progress"] == 0.5


class TestReplaySessionManager:
    """Test ReplaySessionManager."""

    @pytest.fixture
    def manager(self):
        """Fresh manager for each test."""
        return ReplaySessionManager()

    @pytest.mark.asyncio
    async def test_create_session(self, manager):
        """Should create sessions."""
        config = ReplayConfig(
            name="test-replay",
            symbols=["AAPL"],
            feeds=["bars"],
            start=datetime.now(UTC),
            end=datetime.now(UTC) + timedelta(hours=1),
        )

        session = await manager.create_session(config, client_id="test-client")

        assert session.session_id.startswith("replay-")
        assert session.state == ReplayState.PENDING

    @pytest.mark.asyncio
    async def test_get_session(self, manager):
        """Should retrieve sessions by ID."""
        config = ReplayConfig(
            name="test",
            symbols=["AAPL"],
            feeds=["bars"],
            start=datetime.now(UTC),
            end=datetime.now(UTC) + timedelta(hours=1),
        )

        created = await manager.create_session(config, client_id="test-client")
        retrieved = manager.get_session(created.session_id)

        assert retrieved is not None
        assert retrieved.session_id == created.session_id

    @pytest.mark.asyncio
    async def test_list_sessions(self, manager):
        """Should list all sessions."""
        config = ReplayConfig(
            name="test",
            symbols=["AAPL"],
            feeds=["bars"],
            start=datetime.now(UTC),
            end=datetime.now(UTC) + timedelta(hours=1),
        )

        await manager.create_session(config, client_id="test-client")
        await manager.create_session(config, client_id="test-client")

        sessions = await manager.list_sessions()
        assert len(sessions) == 2

    @pytest.mark.asyncio
    async def test_list_sessions_page_returns_total_and_next_offset(self, manager):
        """Should paginate and report traversal metadata."""
        config = ReplayConfig(
            name="test",
            symbols=["AAPL"],
            feeds=["bars"],
            start=datetime.now(UTC),
            end=datetime.now(UTC) + timedelta(hours=1),
        )

        await manager.create_session(config, client_id="test-client")
        s2 = await manager.create_session(config, client_id="test-client")
        s3 = await manager.create_session(config, client_id="test-client")
        s2.state = ReplayState.RUNNING
        s3.state = ReplayState.RUNNING

        sessions, total, has_more, next_offset = await manager.list_sessions_page(
            client_id="test-client",
            state="running",
            limit=1,
            offset=0,
        )

        assert total == 2
        assert [s.session_id for s in sessions] == [s2.session_id]
        assert has_more is True
        assert next_offset == 1

    @pytest.mark.asyncio
    async def test_delete_session(self, manager):
        """Should delete sessions."""
        config = ReplayConfig(
            name="test",
            symbols=["AAPL"],
            feeds=["bars"],
            start=datetime.now(UTC),
            end=datetime.now(UTC) + timedelta(hours=1),
        )

        session = await manager.create_session(config, client_id="test-client")
        deleted = await manager.delete_session(session.session_id)

        assert deleted is True
        assert manager.get_session(session.session_id) is None

    @pytest.mark.asyncio
    async def test_singleton(self):
        """get_replay_manager should return singleton."""
        m1 = get_replay_manager()
        m2 = get_replay_manager()
        assert m1 is m2

    @pytest.mark.asyncio
    async def test_run_replay_sorts_out_of_order_list_loader(self, manager):
        """Out-of-order list loader data should be replayed in timestamp order."""
        start = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)
        config = ReplayConfig(
            name="sorted-replay",
            symbols=["AAPL"],
            feeds=["bars"],
            start=start,
            end=start + timedelta(minutes=1),
            speed=100.0,
        )
        session = ReplaySession(session_id="replay-sort", config=config, client_id="test-client")
        session.state = ReplayState.RUNNING

        newer = ReplayMessage(
            feed="stock_bars",
            symbol="AAPL",
            data={"close": 2},
            market_timestamp=start + timedelta(seconds=1),
        )
        older = ReplayMessage(
            feed="stock_bars",
            symbol="AAPL",
            data={"close": 1},
            market_timestamp=start,
        )

        async def _loader(_session):
            return [newer, older]

        manager.set_data_loader(_loader)

        sent: list[dict] = []

        async def _callback(message):
            sent.append(message)

        await manager._run_replay(session, _callback)

        assert session.state == ReplayState.COMPLETED
        assert [msg["data"]["close"] for msg in sent] == [1, 2]

    @pytest.mark.asyncio
    async def test_run_replay_supports_async_iterable_loader(self, manager):
        """Replay should support streaming async iterable loaders without list materialization."""
        start = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)
        config = ReplayConfig(
            name="streaming-replay",
            symbols=["AAPL"],
            feeds=["bars"],
            start=start,
            end=start + timedelta(minutes=1),
            speed=100.0,
        )
        session = ReplaySession(session_id="replay-stream", config=config, client_id="test-client")
        session.state = ReplayState.RUNNING

        async def _loader(_session):
            async def _messages():
                yield ReplayMessage(
                    feed="stock_bars",
                    symbol="AAPL",
                    data={"close": 10},
                    market_timestamp=start,
                )
                yield ReplayMessage(
                    feed="stock_bars",
                    symbol="AAPL",
                    data={"close": 11},
                    market_timestamp=start,
                )

            return _messages()

        manager.set_data_loader(_loader)

        sent: list[dict] = []

        async def _callback(message):
            sent.append(message)

        await manager._run_replay(session, _callback)

        assert session.state == ReplayState.COMPLETED
        assert session.messages_sent == 2
        assert [msg["data"]["close"] for msg in sent] == [10, 11]

    @pytest.mark.asyncio
    async def test_iter_list_messages_spools_and_cleans_temp_file(self, manager, monkeypatch):
        """Large list loaders should spool to disk and clean up temp files."""
        manager._spool_lists_to_disk = True
        manager._spool_list_threshold = 1

        start = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)
        messages = [
            ReplayMessage(
                feed="stock_bars",
                symbol="AAPL",
                data={"close": 1},
                market_timestamp=start,
            ),
            ReplayMessage(
                feed="stock_bars",
                symbol="AAPL",
                data={"close": 2},
                market_timestamp=start + timedelta(seconds=1),
            ),
            ReplayMessage(
                feed="stock_bars",
                symbol="AAPL",
                data={"close": 3},
                market_timestamp=start + timedelta(seconds=2),
            ),
        ]

        removed_paths: list[str] = []
        real_remove = os.remove

        def _tracking_remove(path: str) -> None:
            removed_paths.append(path)
            real_remove(path)

        monkeypatch.setattr(replay_module.os, "remove", _tracking_remove)

        closes: list[int] = []
        async for message in manager._iter_list_messages(messages):
            closes.append(message.data["close"])

        assert closes == [1, 2, 3]
        assert messages == []
        assert len(removed_paths) == 1


class _FakeReplayWebSocket:
    def __init__(self, messages: list[dict[str, object] | Exception]) -> None:
        self._messages = messages

    async def receive_json(self) -> dict[str, object]:
        if self._messages:
            payload = self._messages.pop(0)
            if isinstance(payload, Exception):
                raise payload
            return payload
        raise WebSocketDisconnect(code=1000)


def _make_replay_session() -> ReplaySession:
    config = ReplayConfig(
        name="ws-control",
        symbols=["AAPL"],
        feeds=["bars"],
        start=datetime(2026, 1, 1, 14, 30, tzinfo=UTC),
        end=datetime(2026, 1, 1, 15, 30, tzinfo=UTC),
    )
    session = ReplaySession(session_id="replay-ws", config=config, client_id="test-client")
    session.state = ReplayState.RUNNING
    return session


class TestReplayWebSocketControlLoop:
    @pytest.mark.asyncio
    async def test_apply_replay_ws_action_updates_session_state(self) -> None:
        session = _make_replay_session()

        should_stop = replay_api._apply_replay_ws_action(session, {"action": "pause"})
        assert should_stop is False
        assert session.state == ReplayState.PAUSED

        should_stop = replay_api._apply_replay_ws_action(session, {"action": "resume", "speed": 3.0})
        assert should_stop is False
        assert session.state == ReplayState.RUNNING
        assert session.speed == 3.0

        should_stop = replay_api._apply_replay_ws_action(
            session,
            {"action": "seek", "timestamp": "2026-01-01T14:45:00+00:00"},
        )
        assert should_stop is False
        assert session.current_timestamp == datetime(2026, 1, 1, 14, 45, tzinfo=UTC)

        should_stop = replay_api._apply_replay_ws_action(session, {"action": "stop"})
        assert should_stop is True
        assert session.state == ReplayState.STOPPED

    @pytest.mark.asyncio
    async def test_receive_replay_control_messages_processes_controls(self) -> None:
        session = _make_replay_session()
        ws = _FakeReplayWebSocket(
            [
                {"action": "pause"},
                {"action": "resume", "speed": 5.0},
                {"action": "stop"},
            ]
        )

        await replay_api._receive_replay_control_messages(ws, session)

        assert session.speed == 5.0
        assert session.state == ReplayState.STOPPED

    @pytest.mark.asyncio
    async def test_receive_replay_control_messages_stops_on_disconnect(self) -> None:
        session = _make_replay_session()
        ws = _FakeReplayWebSocket([WebSocketDisconnect(code=1001)])

        await replay_api._receive_replay_control_messages(ws, session)

        assert session.state == ReplayState.STOPPED
