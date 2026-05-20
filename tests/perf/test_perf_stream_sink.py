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
async def test_sink_publish_queue_drops_after_block_timeout() -> None:
    """Validate the producer blocks then drops via queue-put timeout."""
    release_event = asyncio.Event()
    queue_size = 32
    registry = DataSinkRegistry(
        queue_size=queue_size,
        worker_count=4,
        # Tiny timeout so the test runs fast; production default is 100ms.
        producer_block_timeout_seconds=0.002,
    )
    registry.register(_BlockingSink(release_event, sink_name="blocking-single"))

    total_events = 300
    start = time.perf_counter()
    for i in range(total_events):
        await registry.publish_all("heber:events", {"event_id": f"e{i}", "seq": i})
    enqueue_duration = time.perf_counter() - start

    stats = registry.get_publish_stats()
    # Queue accepts queue_size events plus worker_count that are blocked
    # inside publish (one per worker) before producers start hitting the
    # block timeout. The rest are emergency-dropped.
    assert stats["queued"] >= queue_size
    assert stats["dropped_producer_timeout"] >= total_events - queue_size - 4
    assert stats["dropped_producer_timeout"] > 0
    # Producer-block budget: ≤ producer_block_timeout * total drops, plus
    # event-loop scheduling slack. 300 * 2ms = 600ms; allow 1.5s.
    assert enqueue_duration < 1.5

    release_event.set()
    await registry.drain_queues(timeout_seconds=2.0)


@pytest.mark.asyncio
async def test_sink_publish_queue_multi_sink_independent_bounds() -> None:
    """Two blocked sinks share producer time but have independent queue bounds."""
    release_event = asyncio.Event()
    queue_size = 16
    registry = DataSinkRegistry(
        queue_size=queue_size,
        worker_count=2,
        producer_block_timeout_seconds=0.002,
    )
    registry.register(_BlockingSink(release_event, sink_name="blocking-a"))
    registry.register(_BlockingSink(release_event, sink_name="blocking-b"))

    total_events = 120
    for i in range(total_events):
        await registry.publish_all("heber:events", {"event_id": f"m{i}", "seq": i})

    stats = registry.get_publish_stats()
    # Each sink absorbs queue_size + worker_count events before producer
    # drops kick in; ten events go to two sinks each.
    assert stats["queued"] >= queue_size * 2
    assert stats["dropped_producer_timeout"] > 0

    release_event.set()
    await registry.drain_queues(timeout_seconds=2.0)


@pytest.mark.asyncio
async def test_sink_publish_queue_drains_under_slow_backend() -> None:
    """Producer never silently drops when worker pool can keep up with backend."""
    queue_size = 64
    registry = DataSinkRegistry(
        queue_size=queue_size,
        worker_count=8,
        producer_block_timeout_seconds=0.5,
    )
    registry.register(_DelayedSink("slow-a", delay_seconds=0.005))
    registry.register(_DelayedSink("slow-b", delay_seconds=0.005))

    total_events = 200
    enqueue_start = time.perf_counter()
    for i in range(total_events):
        await registry.publish_all("heber:events", {"event_id": f"s{i}", "seq": i})
    enqueue_duration = time.perf_counter() - enqueue_start

    # With 8 workers per sink and a 5ms publish delay, sustained throughput
    # is ~1600 events/sec/sink — well above the producer rate, so the queue
    # never fills and no events should be dropped.
    stats = registry.get_publish_stats()
    assert stats["dropped_producer_timeout"] == 0
    assert enqueue_duration < 1.5

    drain_start = time.perf_counter()
    await registry.drain_queues(timeout_seconds=2.0)
    drain_duration = time.perf_counter() - drain_start
    assert drain_duration < 2.0
