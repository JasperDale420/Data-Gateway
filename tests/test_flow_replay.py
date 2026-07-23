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
