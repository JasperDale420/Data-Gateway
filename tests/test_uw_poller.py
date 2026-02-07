from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from gateway.core.uw_poller import HEBER_STREAM, UWPoller


class _FakeSinkRegistry:
    def __init__(self, delay_seconds: float = 0.0) -> None:
        self.delay_seconds = delay_seconds
        self.calls: list[tuple[str, dict]] = []
        self._inflight = 0
        self.max_inflight = 0

    async def publish_all(self, stream: str, envelope: dict) -> None:
        self._inflight += 1
        self.max_inflight = max(self.max_inflight, self._inflight)
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)
        self.calls.append((stream, envelope))
        self._inflight -= 1


class _FakeRedisDedupe:
    def __init__(self, duplicate_keys: set[str] | None = None) -> None:
        self.duplicate_keys = duplicate_keys or set()
        self.get_calls: list[str] = []
        self.set_calls: list[tuple[str, bool, int | None]] = []

    async def get(self, key: str):
        self.get_calls.append(key)
        return True if key in self.duplicate_keys else None

    async def set(self, key: str, value: bool, ttl: int | None = None) -> bool:
        self.set_calls.append((key, value, ttl))
        return True


@pytest.mark.asyncio
async def test_publish_envelopes_dedupes_seen_and_redis_hits() -> None:
    poller = UWPoller()
    redis = _FakeRedisDedupe(duplicate_keys={"uw:flow:e2"})
    poller._redis_dedupe = cast(Any, redis)
    poller._mark_seen("e1")
    sink = _FakeSinkRegistry()

    envelopes = [
        {"event_id": "e1", "feed": "flow_alerts"},
        {"event_id": "e2", "feed": "flow_alerts"},
        {"event_id": "e3", "feed": "flow_alerts"},
        {"feed": "flow_alerts"},  # missing event_id still publishes
    ]

    published, duplicates = await poller._publish_envelopes(
        sink_registry=sink,
        envelopes=envelopes,
        dedupe_prefix="uw:flow",
        missing_event_log="uw_flow_missing_event_id",
    )

    assert published == 2
    assert duplicates == 2
    assert len(sink.calls) == 2
    assert all(stream == HEBER_STREAM for stream, _ in sink.calls)
    assert "e3" in poller._seen_ids
    assert [key for key, _, _ in redis.set_calls] == ["uw:flow:e3"]


@pytest.mark.asyncio
async def test_publish_envelopes_respects_max_inflight_limit() -> None:
    poller = UWPoller()
    poller._redis_dedupe = None
    poller._publish_max_inflight = 2
    sink = _FakeSinkRegistry(delay_seconds=0.01)

    envelopes = [{"event_id": f"e{i}", "feed": "flow_alerts"} for i in range(8)]

    published, duplicates = await poller._publish_envelopes(
        sink_registry=sink,
        envelopes=envelopes,
        dedupe_prefix="uw:flow",
        missing_event_log="uw_flow_missing_event_id",
    )

    assert published == 8
    assert duplicates == 0
    assert sink.max_inflight <= 2
