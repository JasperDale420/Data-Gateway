"""Perf coverage for stream fanout and sink backpressure paths.

Run explicitly with:
    pytest -m perf tests/perf -q
"""

import asyncio
import time
from typing import Any

import pytest

from gateway.core.data_sink import DataSink, DataSinkRegistry
from gateway.core.stream import AlpacaStreamType, StreamMultiplexer

pytestmark = pytest.mark.perf


def _patch_zero_slot_wait(registry: DataSinkRegistry) -> None:
    """Override slot acquisition to drop immediately (no wait) for perf tests."""

    async def _nowait(sink_name: str) -> bool:
        sem = registry._sink_semaphores.get(sink_name)
        if sem is None:
            sem = asyncio.Semaphore(registry._max_in_flight_per_sink)
            registry._sink_semaphores[sink_name] = sem
        try:
            await asyncio.wait_for(sem.acquire(), timeout=0.0)
            return True
        except TimeoutError:
            return False

    registry._try_acquire_sink_slot = _nowait  # type: ignore[assignment]


class _BlockingSink(DataSink):
    """Sink that blocks publish until released by the test."""

    def __init__(self, release_event: asyncio.Event, sink_name: str = "blocking") -> None:
        self._release_event = release_event
        self._name = sink_name

    @property
    def name(self) -> str:
        return self._name

    async def publish(self, topic: str, data: dict[str, Any]) -> bool:
        await self._release_event.wait()
        return True

    async def health_check(self) -> bool:
        return True


class _DelayedSink(DataSink):
    """Sink that simulates slower backend publish latency."""

    def __init__(self, sink_name: str, delay_seconds: float) -> None:
        self._name = sink_name
        self._delay_seconds = delay_seconds

    @property
    def name(self) -> str:
        return self._name

    async def publish(self, topic: str, data: dict[str, Any]) -> bool:
        assert topic
        assert data
        await asyncio.sleep(self._delay_seconds)
        return True

    async def health_check(self) -> bool:
        return True


@pytest.mark.asyncio
async def test_stream_fanout_respects_inflight_semaphore_bound() -> None:
    """Ensure websocket fanout concurrency never exceeds the configured semaphore."""
    fanout_limit = 8
    total_clients = 160
    lock = asyncio.Lock()
    in_flight = 0
    peak_in_flight = 0
    delivered = 0

    async def on_data(client_id: str, data_type: str, envelope: dict[str, Any]) -> None:
        nonlocal in_flight, peak_in_flight, delivered
        assert client_id
        assert data_type == "news"
        assert "event_id" in envelope

        async with lock:
            in_flight += 1
            peak_in_flight = max(peak_in_flight, in_flight)
        await asyncio.sleep(0.002)
        async with lock:
            in_flight -= 1
            delivered += 1

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=on_data,
        lazy_connect=True,
    )
    multiplexer._fanout_semaphore = asyncio.Semaphore(fanout_limit)

    conn = multiplexer._get_connection(AlpacaStreamType.NEWS)
    assert conn is not None
    for i in range(total_clients):
        conn.subscriptions.subscribe(client_id=f"client-{i}", news=["*"])

    message = {
        "T": "n",
        "S": "AAPL",
        "symbols": ["AAPL"],
        "t": "2026-02-06T00:00:00Z",
        "headline": "perf-news",
    }

    start = time.perf_counter()
    await multiplexer._handle_message(AlpacaStreamType.NEWS, message)
    duration = time.perf_counter() - start

    assert delivered == total_clients
    assert peak_in_flight <= fanout_limit
    assert duration < 1.0


@pytest.mark.asyncio
async def test_sink_publish_backpressure_task_growth_profile() -> None:
    """Validate sink fire-and-forget backlog stays bounded under blocked sink I/O."""
    release_event = asyncio.Event()
    max_in_flight = 32
    registry = DataSinkRegistry(max_in_flight_per_sink=max_in_flight)
    _patch_zero_slot_wait(registry)
    registry.register(_BlockingSink(release_event, sink_name="blocking-single"))

    total_events = 300
    peak_background_tasks = 0

    start = time.perf_counter()
    for i in range(total_events):
        await registry.publish_all("heber:events", {"event_id": f"e{i}", "seq": i})
        peak_background_tasks = max(peak_background_tasks, len(registry._background_tasks))
    enqueue_duration = time.perf_counter() - start

    stats = registry.get_publish_stats()
    assert peak_background_tasks <= max_in_flight
    assert stats["scheduled"] <= max_in_flight
    assert stats["dropped_backpressure"] >= total_events - max_in_flight
    assert enqueue_duration < 0.5

    release_event.set()
    if registry._background_tasks:
        await asyncio.gather(*list(registry._background_tasks))

    assert len(registry._background_tasks) == 0


@pytest.mark.asyncio
async def test_sink_publish_backpressure_multi_sink_bounds() -> None:
    """Validate aggregate in-flight bounds with multiple blocked sinks."""
    release_event = asyncio.Event()
    max_in_flight = 16
    registry = DataSinkRegistry(max_in_flight_per_sink=max_in_flight)
    _patch_zero_slot_wait(registry)
    registry.register(_BlockingSink(release_event, sink_name="blocking-a"))
    registry.register(_BlockingSink(release_event, sink_name="blocking-b"))

    total_events = 120
    peak_background_tasks = 0

    start = time.perf_counter()
    for i in range(total_events):
        await registry.publish_all("heber:events", {"event_id": f"m{i}", "seq": i})
        peak_background_tasks = max(peak_background_tasks, len(registry._background_tasks))
    enqueue_duration = time.perf_counter() - start

    stats = registry.get_publish_stats()
    total_attempts = total_events * 2  # two sinks per event
    max_scheduled = max_in_flight * 2  # cap per sink

    assert peak_background_tasks <= max_scheduled
    assert stats["scheduled"] <= max_scheduled
    assert stats["dropped_backpressure"] >= total_attempts - max_scheduled
    assert enqueue_duration < 0.8

    release_event.set()
    if registry._background_tasks:
        await asyncio.gather(*list(registry._background_tasks))

    assert len(registry._background_tasks) == 0


@pytest.mark.asyncio
async def test_sink_publish_backpressure_with_slow_backend_profile() -> None:
    """Validate bounded scheduling under slower sink publish latency."""
    max_in_flight = 8
    registry = DataSinkRegistry(max_in_flight_per_sink=max_in_flight)
    _patch_zero_slot_wait(registry)
    registry.register(_DelayedSink("slow-a", delay_seconds=0.02))
    registry.register(_DelayedSink("slow-b", delay_seconds=0.02))

    total_events = 200
    peak_background_tasks = 0

    enqueue_start = time.perf_counter()
    for i in range(total_events):
        await registry.publish_all("heber:events", {"event_id": f"s{i}", "seq": i})
        peak_background_tasks = max(peak_background_tasks, len(registry._background_tasks))
    enqueue_duration = time.perf_counter() - enqueue_start

    stats = registry.get_publish_stats()
    total_attempts = total_events * 2
    max_scheduled = max_in_flight * 2

    assert peak_background_tasks <= max_scheduled
    assert stats["scheduled"] <= max_scheduled
    assert stats["dropped_backpressure"] >= total_attempts - max_scheduled
    assert enqueue_duration < 1.0

    drain_start = time.perf_counter()
    if registry._background_tasks:
        await asyncio.gather(*list(registry._background_tasks))
    drain_duration = time.perf_counter() - drain_start

    assert len(registry._background_tasks) == 0
    assert drain_duration < 0.5
