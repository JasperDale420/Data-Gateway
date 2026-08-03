from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from gateway.core.circuit_breaker import CircuitBreakerRegistry, CircuitState
from gateway.core.data_sink import DataSink, DataSinkRegistry
from gateway.core.durable_outbox_sink import LaneRoutedSink


class _Sink:
    def __init__(self, name: str) -> None:
        self._name = name
        self.publish = AsyncMock(return_value=True)
        self.publish_batch_results = AsyncMock(side_effect=lambda messages: [True] * len(messages))
        self.health_check = AsyncMock(return_value=True)
        self.close = AsyncMock()

    @property
    def name(self) -> str:
        return self._name


class _ControlledSink(DataSink):
    def __init__(self, name: str, *, release: asyncio.Event | None = None, fail: bool = False) -> None:
        self._name = name
        self.release = release
        self.fail = fail
        self.started = asyncio.Event()
        self.published: list[tuple[str, Any]] = []
        self.buffered: list[tuple[str, Any]] = []

    @property
    def name(self) -> str:
        return self._name

    async def publish(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.fail:
            raise RuntimeError(f"{self.name} failed")
        self.published.append((topic, data))
        return True

    async def health_check(self) -> bool:
        return True

    async def publish_batch_results(
        self,
        messages: list[tuple[str, dict[str, Any]]],
    ) -> list[bool]:
        return [await self.publish(topic, data) for topic, data in messages]

    def buffer_event(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        self.buffered.append((topic, data))
        return True


class _DedupCache:
    def __init__(self) -> None:
        self.keys: set[str] = set()

    async def set_nx(self, key: str, _value: str, *, ttl: int) -> bool:
        del ttl
        if key in self.keys:
            return False
        self.keys.add(key)
        return True


@pytest.mark.asyncio
async def test_backfill_canary_routes_each_lane_to_one_sink() -> None:
    durable = _Sink("durable")
    redis = _Sink("redis")
    sink = LaneRoutedSink(durable, redis, lanes="backfill")

    await sink.publish("heber:events:backfill", {"event_id": "backfill"})
    await sink.publish("heber:events", {"event_id": "live"})

    durable.publish.assert_awaited_once_with("heber:events:backfill", {"event_id": "backfill"})
    redis.publish.assert_awaited_once_with("heber:events", {"event_id": "live"})


@pytest.mark.asyncio
async def test_both_lanes_use_durable_sink_without_redis_duplicates() -> None:
    durable = _Sink("durable")
    redis = _Sink("redis")
    sink = LaneRoutedSink(durable, redis, lanes="both")

    results = await sink.publish_batch_results(
        [
            ("heber:events", {"event_id": "live"}),
            ("heber:events:backfill", {"event_id": "backfill"}),
        ]
    )

    assert results == [True, True]
    durable.publish_batch_results.assert_awaited_once()
    redis.publish_batch_results.assert_not_awaited()


@pytest.mark.asyncio
async def test_mixed_batch_preserves_exact_per_message_results() -> None:
    durable = _Sink("durable")
    redis = _Sink("redis")
    durable.publish_batch_results.side_effect = lambda messages: [False] * len(messages)
    redis.publish_batch_results.side_effect = lambda messages: [True] * len(messages)
    sink = LaneRoutedSink(durable, redis, lanes="backfill")
    messages: list[tuple[str, dict[str, Any]]] = [
        ("heber:events", {"event_id": "live-1"}),
        ("heber:events:backfill", {"event_id": "backfill"}),
        ("heber:events", {"event_id": "live-2"}),
    ]

    results = await sink.publish_batch_results(messages)

    assert results == [True, False, True]
    assert await sink.publish_batch_indexed(messages) == {0, 2}


@pytest.mark.asyncio
async def test_one_lane_batch_failure_does_not_hide_other_lane_success() -> None:
    durable = _Sink("durable")
    redis = _Sink("redis")
    durable.publish_batch_results.side_effect = RuntimeError("broker unavailable")
    sink = LaneRoutedSink(durable, redis, lanes="backfill")

    results = await sink.publish_batch_results(
        [
            ("heber:events", {"event_id": "live"}),
            ("heber:events:backfill", {"event_id": "backfill"}),
        ]
    )

    assert results == [True, False]


@pytest.mark.asyncio
async def test_router_health_checks_only_active_delegates_and_closes_both() -> None:
    durable = _Sink("durable")
    redis = _Sink("redis")
    redis.health_check.return_value = False
    sink = LaneRoutedSink(durable, redis, lanes="both")

    assert await sink.health_check() is True
    await sink.close()

    durable.close.assert_awaited_once()
    redis.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_backfill_canary_exposes_selected_redis_live_lane_failure() -> None:
    durable = _Sink("durable")
    redis = _Sink("redis")
    redis.health_check.return_value = False
    sink = LaneRoutedSink(durable, redis, lanes="backfill")

    assert await sink.health_check() is False
    status = sink.transport_status()

    assert status["lanes"]["backfill"]["transport"] == "durable"
    assert status["lanes"]["live"] == {"transport": "redis", "health": "degraded"}


@pytest.mark.asyncio
async def test_live_durable_route_exposes_watch_as_its_own_delivery_lane() -> None:
    durable = _Sink("durable")
    redis = _Sink("redis")
    durable.transport_status = lambda: {
        "lanes": {
            "live": {"admission": "ok", "delivery": "ok"},
            "backfill": {"admission": "ok", "delivery": "ok"},
            "watch": {"admission": "ok", "delivery": "degraded"},
        }
    }
    sink = LaneRoutedSink(durable, redis, lanes="live")

    await sink.health_check()
    status = sink.transport_status()

    assert status["lanes"]["live"]["transport"] == "durable"
    assert status["lanes"]["watch"] == {
        "transport": "durable",
        "health": "ok",
        "admission": "ok",
        "delivery": "degraded",
    }


@pytest.mark.asyncio
async def test_registry_backfill_canary_queues_and_deduplicates_live_redis() -> None:
    durable = _ControlledSink("durable")
    redis = _ControlledSink("redis_streams")
    registry = DataSinkRegistry(dedup_cache=_DedupCache(), queue_size=2, worker_count=1)
    registry.register(LaneRoutedSink(durable, redis, lanes="backfill"))
    event = {"event_id": "live-1"}

    assert registry.has_durable_admission is False
    assert registry.has_durable_admission_for("heber:events") is False
    assert registry.has_durable_admission_for("heber:events:backfill") is True
    assert "redis_streams" in registry.get_backpressure_snapshot()["sinks"]

    await registry.publish_all("heber:events", event)
    await registry.publish_all("heber:events", event)
    await registry.drain_queues()

    assert redis.published == [("heber:events", event)]
    assert durable.published == []
    assert registry.get_dedup_stats() == {"checked": 2, "deduplicated": 1}


@pytest.mark.asyncio
async def test_registry_backfill_waits_for_durable_admission_without_live_failure_poisoning_it() -> None:
    release = asyncio.Event()
    durable = _ControlledSink("durable", release=release)
    redis = _ControlledSink("redis_streams", fail=True)
    registry = DataSinkRegistry(queue_size=2, worker_count=1)
    registry.register(LaneRoutedSink(durable, redis, lanes="backfill"))

    await registry.publish_all("heber:events", {"event_id": "live-failure"})
    await registry.drain_queues()

    admission = asyncio.create_task(registry.publish_all("heber:events:backfill", {"event_id": "backfill-1"}))
    await durable.started.wait()
    assert admission.done() is False
    release.set()
    await admission

    assert durable.published == [("heber:events:backfill", {"event_id": "backfill-1"})]


@pytest.mark.asyncio
async def test_registry_both_mode_keeps_all_topics_on_direct_durable_admission() -> None:
    release = asyncio.Event()
    durable = _ControlledSink("durable", release=release)
    redis = _ControlledSink("redis_streams")
    registry = DataSinkRegistry(queue_size=2, worker_count=1)
    registry.register(LaneRoutedSink(durable, redis, lanes="both"))

    admission = asyncio.create_task(registry.publish_all("heber:events", {"event_id": "live-durable"}))
    await durable.started.wait()

    assert registry.has_durable_admission is True
    assert registry.get_backpressure_snapshot()["sinks"] == {}
    assert admission.done() is False

    release.set()
    await admission
    assert redis.published == []


@pytest.mark.parametrize(
    ("method_name", "expected"),
    [
        ("publish_all_batch", 1),
        ("publish_all_batch_results", [False, True]),
        ("publish_all_batch_indexed", {1}),
    ],
)
@pytest.mark.asyncio
async def test_mixed_batch_open_live_circuit_buffers_live_but_admits_backfill(
    method_name: str,
    expected: object,
) -> None:
    durable = _ControlledSink("durable")
    redis = _ControlledSink("redis_streams")
    registry = DataSinkRegistry()
    registry.register(LaneRoutedSink(durable, redis, lanes="backfill"))
    breaker_registry = CircuitBreakerRegistry()
    breaker = await breaker_registry.get("data_sink:redis_streams")
    breaker.state = CircuitState.OPEN
    breaker.last_failure_time = 9_999_999_999.0
    messages = [
        ("heber:events", {"event_id": "live"}),
        ("heber:events:backfill", {"event_id": "backfill"}),
    ]

    with patch("gateway.core.data_sink.get_circuit_breaker", new=breaker_registry.get):
        actual = await getattr(registry, method_name)(messages)

    assert actual == expected
    assert redis.published == []
    assert redis.buffered == [("heber:events", {"event_id": "live"})]
    assert durable.published == [("heber:events:backfill", {"event_id": "backfill"})]
