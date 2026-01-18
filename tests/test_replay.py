"""Tests for Historical Replay Mode."""

from datetime import UTC, datetime, timedelta

import pytest

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
        session = ReplaySession(session_id="test-123", config=config)

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
        session = ReplaySession(session_id="test-123", config=config)

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
        session = ReplaySession(session_id="test-123", config=config)

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
        session = ReplaySession(session_id="test-123", config=config)

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

        session = await manager.create_session(config)

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

        created = await manager.create_session(config)
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

        await manager.create_session(config)
        await manager.create_session(config)

        sessions = await manager.list_sessions()
        assert len(sessions) == 2

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

        session = await manager.create_session(config)
        deleted = await manager.delete_session(session.session_id)

        assert deleted is True
        assert manager.get_session(session.session_id) is None

    @pytest.mark.asyncio
    async def test_singleton(self):
        """get_replay_manager should return singleton."""
        m1 = get_replay_manager()
        m2 = get_replay_manager()
        assert m1 is m2
