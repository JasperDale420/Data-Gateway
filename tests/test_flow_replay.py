"""Durable replay contract for UW flow WebSocket subscribers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from gateway.core.envelope import wrap_event
from gateway.core.flow_fanout import FlowFanout
from gateway.core.flow_replay import RedisFlowReplayStore, ReplayEntry


def _flow_envelope(ticker: str, premium: int = 250_000) -> dict[str, Any]:
    return wrap_event(
        event={
            "ticker": ticker,
            "option_chain": f"{ticker}260821C00100000",
            "timestamp": "2026-07-21T15:30:00Z",
            "premium": premium,
        },
        provider="unusual_whales",
        feed="flow_alerts",
        source="rest",
    )


@dataclass
class _MemoryReplayStore:
    entries: list[ReplayEntry] = field(default_factory=list)

    @property
    def available(self) -> bool:
        return True

    async def ensure_available(self) -> bool:
        return self.available

    async def append(self, envelope: dict[str, Any]) -> str:
        cursor = f"{len(self.entries) + 1}-0"
        self.entries.append(ReplayEntry(cursor=cursor, envelope=envelope))
        return cursor

    async def high_watermark(self) -> str:
        return self.entries[-1].cursor if self.entries else "0-0"

    async def read(self, after: str, through: str) -> list[ReplayEntry]:
        start = int(after.split("-", 1)[0]) if after != "$" else int(through.split("-", 1)[0])
        end = int(through.split("-", 1)[0])
        return [entry for entry in self.entries if start < int(entry.cursor.split("-", 1)[0]) <= end]

    async def close(self) -> None:
        return None


class _RecordingConnections:
    def __init__(self) -> None:
        self.calls: list[tuple[dict[str, Any], list[str]]] = []

    async def broadcast_to_connection_ids(self, message, connection_ids):
        self.calls.append((message, list(connection_ids)))
        return len(connection_ids)


async def test_deliver_persists_cursor_even_without_live_subscribers():
    store = _MemoryReplayStore()
    connections = _RecordingConnections()
    fanout = FlowFanout(connections, replay_store=store)  # type: ignore[arg-type]

    assert await fanout.deliver(_flow_envelope("AAPL")) == 0

    assert len(store.entries) == 1
    assert store.entries[0].cursor == "1-0"
    assert connections.calls == []


async def test_live_delivery_includes_durable_transport_cursor():
    store = _MemoryReplayStore()
    connections = _RecordingConnections()
    fanout = FlowFanout(connections, replay_store=store)  # type: ignore[arg-type]
    fanout.subscribe("kairos", [])

    assert await fanout.deliver(_flow_envelope("AAPL")) == 1
    await fanout.drain()

    message, targets = connections.calls[0]
    assert targets == ["kairos"]
    assert message["stream_cursor"] == "1-0"
    assert message["envelope"]["event_id"] == store.entries[0].envelope["event_id"]


async def test_replay_is_bounded_to_subscription_high_watermark():
    store = _MemoryReplayStore()
    connections = _RecordingConnections()
    first = _flow_envelope("AAPL")
    second = _flow_envelope("MSFT")
    await store.append(first)
    await store.append(second)
    fanout = FlowFanout(connections, replay_store=store)  # type: ignore[arg-type]

    replay = await fanout.prepare_replay("kairos", [], after_stream_id="1-0")
    assert replay == {"resume_supported": True, "replay_high_watermark": "2-0"}

    # Arrives after the subscription snapshot. It belongs to live delivery,
    # not to this replay batch.
    await store.append(_flow_envelope("TSLA"))
    await fanout.start_pending_replay("kairos")

    message_types = [message["type"] for message, _targets in connections.calls]
    assert message_types == ["replay_begin", "data", "replay_complete"]
    replayed = connections.calls[1][0]
    assert replayed["event_id"] == second["event_id"]
    assert replayed["stream_cursor"] == "2-0"
    assert connections.calls[-1][0]["high_watermark"] == "2-0"


async def test_reconnect_replays_before_buffered_live_delivery_without_duplicates_or_loss():
    class _PausedHighWatermarkStore(_MemoryReplayStore):
        def __init__(self) -> None:
            super().__init__()
            self.high_watermark_requested = asyncio.Event()
            self.release_high_watermark = asyncio.Event()

        async def high_watermark(self) -> str:
            self.high_watermark_requested.set()
            await self.release_high_watermark.wait()
            return await super().high_watermark()

    store = _PausedHighWatermarkStore()
    connections = _RecordingConnections()
    first = _flow_envelope("AAPL", premium=100_000)
    captured_during_setup = _flow_envelope("AAPL", premium=200_000)
    live_after_snapshot = _flow_envelope("AAPL", premium=300_000)
    await store.append(first)
    fanout = FlowFanout(connections, replay_store=store)  # type: ignore[arg-type]

    preparing = asyncio.create_task(fanout.prepare_replay("kairos", [], after_stream_id="0-0"))
    await store.high_watermark_requested.wait()

    # This event is persisted while the high-water mark is being captured. It
    # must be delivered by replay exactly once, never as an early live event.
    assert await fanout.deliver(captured_during_setup) == 1
    await fanout.drain()
    store.release_high_watermark.set()
    replay = await preparing
    assert replay == {"resume_supported": True, "replay_high_watermark": "2-0"}

    # This event is beyond the captured replay range and must wait behind the
    # replay completion barrier before returning the connection to live flow.
    assert await fanout.deliver(live_after_snapshot) == 1
    await fanout.drain()
    await fanout.start_pending_replay("kairos")

    messages = [message for message, _targets in connections.calls]
    assert [message["type"] for message in messages] == [
        "replay_begin",
        "data",
        "data",
        "replay_complete",
        "data",
    ]
    assert [message.get("stream_cursor") for message in messages if message["type"] == "data"] == [
        "1-0",
        "2-0",
        "3-0",
    ]
    assert [message["event_id"] for message in messages if message["type"] == "data"] == [
        first["event_id"],
        captured_during_setup["event_id"],
        live_after_snapshot["event_id"],
    ]


async def test_prepare_replay_fails_closed_without_durable_store():
    fanout = FlowFanout(_RecordingConnections())  # type: ignore[arg-type]

    replay = await fanout.prepare_replay("kairos", [], after_stream_id="4-0")

    assert replay == {"resume_supported": False}


async def test_redis_replay_rejects_cursor_older_than_retained_history():
    class _TrimmedRedis:
        async def xinfo_stream(self, _stream):
            return {b"first-entry": (b"5-0", {b"envelope": b"{}"})}

    store = RedisFlowReplayStore("redis://unused", stream="flow", max_len=1_000, max_replay_events=100)
    store._redis = _TrimmedRedis()
    store._healthy = True

    with pytest.raises(RuntimeError, match="older than retained history"):
        await store.read("3-0", "6-0")


class _WorkingRedis:
    def __init__(self) -> None:
        self.stream: list[tuple[bytes, bytes]] = []

    async def ping(self) -> bool:
        return True

    async def xadd(self, _stream, fields, maxlen=None, approximate=None):
        self.stream.append((b"1-0", fields[b"envelope"]))
        return b"1-0"

    async def xrevrange(self, _stream, count=1):
        return [(b"1-0", {b"envelope": b"{}"})] if self.stream else []

    async def aclose(self) -> None:
        return None


class TestReplayStoreSelfHeal:
    """An append failure latches _healthy=False; only startup initialize()
    ever cleared it, so one Redis blip disabled replay until a manual gateway
    restart (observed live: 2026-08-05 close through 2026-08-06 mid-session,
    then again after the 2026-08-06 12:18 Redis restart)."""

    @pytest.mark.asyncio
    async def test_unhealthy_store_reprobes_and_recovers(self, monkeypatch) -> None:
        store = RedisFlowReplayStore("redis://unused", stream="flow", max_len=100, max_replay_events=10)
        store._healthy = False  # latched by a prior failure

        working = _WorkingRedis()
        monkeypatch.setattr("redis.asyncio.from_url", lambda *_a, **_k: working)

        assert await store.ensure_available() is True
        assert store.available is True

    @pytest.mark.asyncio
    async def test_reprobe_is_rate_limited(self, monkeypatch) -> None:
        now = [1000.0]
        store = RedisFlowReplayStore(
            "redis://unused", stream="flow", max_len=100, max_replay_events=10, clock=lambda: now[0]
        )
        store._healthy = False
        attempts: list[int] = []

        class _DeadRedis:
            async def ping(self):
                attempts.append(1)
                raise ConnectionError("still down")

        monkeypatch.setattr("redis.asyncio.from_url", lambda *_a, **_k: _DeadRedis())

        assert await store.ensure_available() is False
        assert await store.ensure_available() is False  # within cooldown — no new probe
        assert len(attempts) == 1
        now[0] += 6.0
        assert await store.ensure_available() is False  # cooldown elapsed — probes again
        assert len(attempts) == 2

    @pytest.mark.asyncio
    async def test_append_self_heals_after_redis_returns(self, monkeypatch) -> None:
        store = RedisFlowReplayStore("redis://unused", stream="flow", max_len=100, max_replay_events=10)
        store._healthy = False

        working = _WorkingRedis()
        monkeypatch.setattr("redis.asyncio.from_url", lambda *_a, **_k: working)

        cursor = await store.append({"event_id": "e1"})
        assert cursor == "1-0"
        assert store.available is True

    @pytest.mark.asyncio
    async def test_prepare_replay_reprobes_unhealthy_store(self, monkeypatch) -> None:
        store = RedisFlowReplayStore("redis://unused", stream="flow", max_len=100, max_replay_events=10)
        store._healthy = False
        working = _WorkingRedis()
        working.stream.append((b"1-0", b"{}"))
        monkeypatch.setattr("redis.asyncio.from_url", lambda *_a, **_k: working)

        fanout = FlowFanout(_RecordingConnections(), replay_store=store)  # type: ignore[arg-type]

        replay = await fanout.prepare_replay("kairos", [], after_stream_id="0-0")

        assert replay.get("resume_supported") is True


class TestReplayStoreHardening:
    """Round-2 review requirements: idle-latch recovery, gap fail-closed,
    stale-failure identity checks, and client lifecycle."""

    @pytest.mark.asyncio
    async def test_idle_redis_restart_latches_via_high_watermark_then_recovers(self, monkeypatch) -> None:
        store = RedisFlowReplayStore("redis://unused", stream="flow", max_len=100, max_replay_events=10)

        class _DeadRedis:
            async def xrevrange(self, _stream, count=1):
                raise ConnectionError("redis restarted while idle")

        store._redis = _DeadRedis()
        store._healthy = True  # idle store never saw the restart

        with pytest.raises(ConnectionError):
            await store.high_watermark()
        assert store.available is False  # latched → subscriber retry re-probes

        working = _WorkingRedis()
        working.stream.append((b"1-0", b"{}"))
        monkeypatch.setattr("redis.asyncio.from_url", lambda *_a, **_k: working)
        assert await store.ensure_available() is True

    @pytest.mark.asyncio
    async def test_gap_marker_written_on_recovery_and_read_fails_closed_across_it(self) -> None:
        class _RecordingRedis(_WorkingRedis):
            def __init__(self) -> None:
                super().__init__()
                self.entries: list[tuple[bytes, dict[bytes, bytes]]] = []

            async def xadd(self, _stream, fields, maxlen=None, approximate=None):
                cursor = f"{len(self.entries) + 1}-0".encode()
                self.entries.append((cursor, dict(fields)))
                return cursor

            async def xinfo_stream(self, _stream):
                return {b"first-entry": (self.entries[0][0], self.entries[0][1])} if self.entries else {}

            async def xrange(self, _stream, min=None, max=None, count=None):
                return list(self.entries)

        store = RedisFlowReplayStore("redis://unused", stream="flow", max_len=100, max_replay_events=10)
        redis = _RecordingRedis()
        store._redis = redis
        store._healthy = True
        store._gap_pending = True  # a prior append failed while events published

        await store.append({"event_id": "after-gap"})

        assert redis.entries[0][1] == {b"replay_gap": b"1"}  # marker precedes the event
        assert store._gap_pending is False

        with pytest.raises(RuntimeError, match="durability gap"):
            await store.read("1-0", "9-0")

    @pytest.mark.asyncio
    async def test_stale_append_failure_does_not_clobber_new_client_health(self) -> None:
        store = RedisFlowReplayStore("redis://unused", stream="flow", max_len=100, max_replay_events=10)
        new_client = _WorkingRedis()
        store._redis = new_client
        store._healthy = True

        old_client = object()
        store._mark_unhealthy(old_client)  # stale failure from a replaced client

        assert store.available is True

    @pytest.mark.asyncio
    async def test_failed_probe_closes_candidate_client(self, monkeypatch) -> None:
        closed: list[int] = []

        class _DeadCandidate:
            async def ping(self):
                raise ConnectionError("down")

            async def aclose(self):
                closed.append(1)

        monkeypatch.setattr("redis.asyncio.from_url", lambda *_a, **_k: _DeadCandidate())
        store = RedisFlowReplayStore("redis://unused", stream="flow", max_len=100, max_replay_events=10)

        assert await store.initialize() is False
        assert closed == [1]  # no leaked pools from failed probes


class TestRedisExceptionLatching:
    @pytest.mark.asyncio
    async def test_redis_py_connection_error_in_read_latches_unhealthy(self) -> None:
        """redis-py errors do not subclass the builtins — the latch must catch
        redis.exceptions.RedisError or subscriber-driven recovery never fires."""
        from redis.exceptions import ConnectionError as RedisConnectionError

        class _DeadRedis:
            async def xinfo_stream(self, _stream):
                raise RedisConnectionError("connection lost")

        store = RedisFlowReplayStore("redis://unused", stream="flow", max_len=100, max_replay_events=10)
        store._redis = _DeadRedis()
        store._healthy = True

        with pytest.raises(RedisConnectionError):
            await store.read("1-0", "2-0")
        assert store.available is False


class TestGapSentinelDurability:
    """The pending-gap latch must survive a process restart: append fails
    (Redis down, memory-only flag) → gateway restarts → without a durable
    sentinel the recovered replay silently crosses the unmarked gap."""

    @pytest.mark.asyncio
    async def test_append_failure_writes_sentinel_and_restart_restores_gap_pending(self, tmp_path) -> None:
        sentinel = tmp_path / "flow_replay_gap_pending"

        class _DeadRedis:
            async def xadd(self, *_a, **_k):
                raise ConnectionError("redis down")

        store = RedisFlowReplayStore(
            "redis://unused", stream="flow", max_len=100, max_replay_events=10, gap_sentinel_path=sentinel
        )
        store._redis = _DeadRedis()
        store._healthy = True

        with pytest.raises(ConnectionError):
            await store.append({"event_id": "lost"})
        assert sentinel.exists()  # durable record written at failure time

        # Simulate a process restart: a fresh store instance over the same path.
        restarted = RedisFlowReplayStore(
            "redis://unused", stream="flow", max_len=100, max_replay_events=10, gap_sentinel_path=sentinel
        )
        assert restarted._gap_pending is True

    @pytest.mark.asyncio
    async def test_marker_write_clears_sentinel(self, tmp_path) -> None:
        sentinel = tmp_path / "flow_replay_gap_pending"
        sentinel.touch()

        class _RecordingRedis(_WorkingRedis):
            def __init__(self) -> None:
                super().__init__()
                self.entries: list[tuple[bytes, dict[bytes, bytes]]] = []

            async def xadd(self, _stream, fields, maxlen=None, approximate=None):
                cursor = f"{len(self.entries) + 1}-0".encode()
                self.entries.append((cursor, dict(fields)))
                return cursor

        store = RedisFlowReplayStore(
            "redis://unused", stream="flow", max_len=100, max_replay_events=10, gap_sentinel_path=sentinel
        )
        assert store._gap_pending is True  # restored from sentinel
        redis = _RecordingRedis()
        store._redis = redis
        store._healthy = True

        await store.append({"event_id": "after-gap"})

        assert redis.entries[0][1] == {b"replay_gap": b"1"}
        assert not sentinel.exists()  # cleared only after the durable marker landed
