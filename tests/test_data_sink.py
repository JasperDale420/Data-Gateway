"""Unit tests for DataSinkRegistry reliability improvements."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import pytest

from gateway.core.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerRegistry,
    CircuitState,
)
from gateway.core.data_sink import DataSink, DataSinkRegistry

# ── Mock Sinks ───────────────────────────────────────────────────────


class _TrackingSink(DataSink):
    """Sink that records all publish calls."""

    def __init__(self, sink_name: str = "tracking") -> None:
        self._name = sink_name
        self.published: list[tuple[str, Any]] = []

    @property
    def name(self) -> str:
        return self._name

    async def publish(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        self.published.append((topic, data))
        return True

    async def health_check(self) -> bool:
        return True


class _TrackingNoMetricsSink(_TrackingSink):
    """Tracking sink that doesn't record metrics internally."""

    @property
    def record_publish_metrics(self) -> bool:
        return False


# ── Dispatch-Time Circuit Check Tests ────────────────────────────────


class TestPublishAllCircuitCheck:
    """Tests for the dispatch-time circuit breaker check in publish_all."""

    @pytest.mark.asyncio
    async def test_publish_all_skips_sink_with_open_circuit(self) -> None:
        """When a sink's circuit breaker is OPEN, publish_all should skip it
        entirely — no task creation, no _safe_publish call."""
        # Create a fresh registry with known circuit breaker state
        cb_registry = CircuitBreakerRegistry()
        breaker = await cb_registry.get("data_sink:test_sink")

        # Force circuit OPEN
        breaker.state = CircuitState.OPEN
        breaker.last_failure_time = 9999999999.0  # Far future, stays open

        sink = _TrackingSink(sink_name="test_sink")
        registry = DataSinkRegistry()
        registry.register(sink)

        with patch("gateway.core.data_sink.get_circuit_breaker", new=cb_registry.get):
            await registry.publish_all("gateway.stream.bars", {"symbol": "AAPL"})

        # Give any potential background tasks time to run
        await asyncio.sleep(0.05)

        # Sink should NOT have received the event
        assert len(sink.published) == 0

    @pytest.mark.asyncio
    async def test_publish_all_publishes_when_circuit_closed(self) -> None:
        """When circuit is CLOSED, publish_all should publish normally."""
        cb_registry = CircuitBreakerRegistry()
        breaker = await cb_registry.get("data_sink:normal_sink")
        assert breaker.state == CircuitState.CLOSED

        sink = _TrackingSink(sink_name="normal_sink")
        registry = DataSinkRegistry()
        registry.register(sink)

        with patch("gateway.core.data_sink.get_circuit_breaker", new=cb_registry.get):
            await registry.publish_all("gateway.stream.bars", {"symbol": "AAPL"})

        # Give the fire-and-forget task time to complete
        await asyncio.sleep(0.1)

        assert len(sink.published) == 1
        assert sink.published[0] == ("gateway.stream.bars", {"symbol": "AAPL"})

    @pytest.mark.asyncio
    async def test_publish_all_circuit_check_does_not_block_other_sinks(self) -> None:
        """An open circuit on one sink should not prevent publishing to others."""
        cb_registry = CircuitBreakerRegistry()

        # Sink A: circuit OPEN
        breaker_a = await cb_registry.get("data_sink:sink_a")
        breaker_a.state = CircuitState.OPEN
        breaker_a.last_failure_time = 9999999999.0

        # Sink B: circuit CLOSED (default)
        await cb_registry.get("data_sink:sink_b")

        sink_a = _TrackingSink(sink_name="sink_a")
        sink_b = _TrackingSink(sink_name="sink_b")
        registry = DataSinkRegistry()
        registry.register(sink_a)
        registry.register(sink_b)

        with patch("gateway.core.data_sink.get_circuit_breaker", new=cb_registry.get):
            await registry.publish_all("gateway.stream.bars", {"symbol": "AAPL"})

        await asyncio.sleep(0.1)

        # Sink A should be skipped, Sink B should receive the event
        assert len(sink_a.published) == 0
        assert len(sink_b.published) == 1

    @pytest.mark.asyncio
    async def test_publish_all_proceeds_if_breaker_lookup_fails(self) -> None:
        """If circuit breaker lookup raises, publish should proceed (fail open)."""

        async def _broken_get(name: str) -> CircuitBreaker:
            raise RuntimeError("registry broken")

        sink = _TrackingSink(sink_name="resilient_sink")
        registry = DataSinkRegistry()
        registry.register(sink)

        with patch("gateway.core.data_sink.get_circuit_breaker", new=_broken_get):
            await registry.publish_all("gateway.stream.bars", {"symbol": "AAPL"})

        await asyncio.sleep(0.1)

        # Should still publish despite breaker lookup failure
        assert len(sink.published) == 1


class TestPublishAllDisabledAndEmpty:
    """Edge case tests for publish_all."""

    @pytest.mark.asyncio
    async def test_publish_all_no_op_when_disabled(self) -> None:
        """Disabled registry should not publish anything."""
        sink = _TrackingSink()
        registry = DataSinkRegistry()
        registry.register(sink)
        registry.disable()

        await registry.publish_all("topic", {"data": 1})

        assert len(sink.published) == 0

    @pytest.mark.asyncio
    async def test_publish_all_no_op_with_no_sinks(self) -> None:
        """Registry with no sinks should be a no-op."""
        registry = DataSinkRegistry()

        # Should not raise
        await registry.publish_all("topic", {"data": 1})


class _BufferingSink(_TrackingSink):
    """Sink that supports buffering (like RedisStreamsSink)."""

    def __init__(self, sink_name: str = "buffering") -> None:
        super().__init__(sink_name)
        self.buffered: list[tuple[str, Any]] = []

    def buffer_event(self, topic: str, data: dict[str, Any] | str | bytes) -> None:
        self.buffered.append((topic, data))


class TestCircuitOpenBuffering:
    """Tests that events are buffered (not dropped) when circuit is OPEN."""

    @pytest.mark.asyncio
    async def test_publish_all_buffers_when_circuit_open(self) -> None:
        """When circuit is OPEN and sink supports buffering, events go to buffer."""
        cb_registry = CircuitBreakerRegistry()
        breaker = await cb_registry.get("data_sink:buffering")
        breaker.state = CircuitState.OPEN
        breaker.last_failure_time = 9999999999.0

        sink = _BufferingSink(sink_name="buffering")
        registry = DataSinkRegistry()
        registry.register(sink)

        with patch("gateway.core.data_sink.get_circuit_breaker", new=cb_registry.get):
            await registry.publish_all("heber:events", {"event_id": "test1", "symbol": "AAPL"})

        await asyncio.sleep(0.05)

        assert len(sink.published) == 0
        assert len(sink.buffered) == 1
        assert sink.buffered[0] == ("heber:events", {"event_id": "test1", "symbol": "AAPL"})

    @pytest.mark.asyncio
    async def test_publish_all_drops_when_circuit_open_no_buffer(self) -> None:
        """When circuit is OPEN and sink has no buffer_event, events are dropped (existing behavior)."""
        cb_registry = CircuitBreakerRegistry()
        breaker = await cb_registry.get("data_sink:tracking")
        breaker.state = CircuitState.OPEN
        breaker.last_failure_time = 9999999999.0

        sink = _TrackingSink(sink_name="tracking")
        registry = DataSinkRegistry()
        registry.register(sink)

        with patch("gateway.core.data_sink.get_circuit_breaker", new=cb_registry.get):
            await registry.publish_all("heber:events", {"event_id": "test1"})

        await asyncio.sleep(0.05)
        assert len(sink.published) == 0

    @pytest.mark.asyncio
    async def test_publish_all_batch_buffers_when_circuit_open(self) -> None:
        """Batch publish buffers events when circuit is OPEN."""
        cb_registry = CircuitBreakerRegistry()
        breaker = await cb_registry.get("data_sink:buffering")
        breaker.state = CircuitState.OPEN
        breaker.last_failure_time = 9999999999.0

        sink = _BufferingSink(sink_name="buffering")
        registry = DataSinkRegistry()
        registry.register(sink)

        messages = [
            ("heber:events", {"event_id": "e1"}),
            ("heber:events", {"event_id": "e2"}),
        ]

        with patch("gateway.core.data_sink.get_circuit_breaker", new=cb_registry.get):
            result = await registry.publish_all_batch(messages)

        assert result == 0
        assert len(sink.buffered) == 2


# ── Bounded-Queue Dispatch Tests ─────────────────────────────────────


class _BlockingSink(DataSink):
    """Sink that blocks publish until the test releases an event."""

    def __init__(self, release_event: asyncio.Event, sink_name: str = "blocking") -> None:
        self._release_event = release_event
        self._name = sink_name
        self.published: list[tuple[str, Any]] = []

    @property
    def name(self) -> str:
        return self._name

    async def publish(self, topic: str, data: Any) -> bool:
        await self._release_event.wait()
        self.published.append((topic, data))
        return True

    async def health_check(self) -> bool:
        return True


class TestBoundedQueueDispatch:
    """Tests that verify the bounded queue + worker pool dispatch path."""

    @pytest.mark.asyncio
    async def test_producer_blocks_when_queue_full(self) -> None:
        """Producer should block once the queue + worker slots are saturated."""
        release_event = asyncio.Event()
        registry = DataSinkRegistry(
            queue_size=2,
            worker_count=1,
            producer_block_timeout_seconds=5.0,
        )
        sink = _BlockingSink(release_event, sink_name="blocking-block")
        registry.register(sink)

        # Fill queue (2) + occupy worker (1) = 3 events without producer block.
        for i in range(3):
            await registry.publish_all("heber:events", {"event_id": f"e{i}"})

        # 4th publish must block because queue is full and worker is busy.
        blocked = asyncio.create_task(registry.publish_all("heber:events", {"event_id": "e3"}))
        await asyncio.sleep(0.05)
        assert not blocked.done(), "producer should still be blocked on full queue"

        # Release worker → drains queue → 4th publish unblocks.
        release_event.set()
        await asyncio.wait_for(blocked, timeout=2.0)
        await registry.drain_queues(timeout_seconds=2.0)

        assert len(sink.published) == 4
        stats = registry.get_publish_stats()
        assert stats["queued"] == 4
        assert stats["dropped_producer_timeout"] == 0

    @pytest.mark.asyncio
    async def test_worker_pool_drains_queue_and_unblocks_producer(self) -> None:
        """Worker pool keeps draining; under steady-state, the producer never drops."""
        registry = DataSinkRegistry(
            queue_size=4,
            worker_count=4,
            producer_block_timeout_seconds=0.2,
        )
        sink = _TrackingSink(sink_name="drain-target")
        registry.register(sink)

        total = 50
        for i in range(total):
            await registry.publish_all("heber:events", {"event_id": f"d{i}"})

        await registry.drain_queues(timeout_seconds=2.0)

        assert len(sink.published) == total
        stats = registry.get_publish_stats()
        assert stats["queued"] == total
        assert stats["dropped_producer_timeout"] == 0

    @pytest.mark.asyncio
    async def test_drop_only_when_producer_block_timeout_exhausts(self) -> None:
        """Drops happen ONLY after the producer-block timeout fires, never sooner."""
        release_event = asyncio.Event()
        registry = DataSinkRegistry(
            queue_size=2,
            worker_count=1,
            producer_block_timeout_seconds=0.05,
        )
        sink = _BlockingSink(release_event, sink_name="blocking-timeout")
        registry.register(sink)

        # Fill queue (2) + occupy worker (1) = 3 absorbed.
        for i in range(3):
            await registry.publish_all("heber:events", {"event_id": f"a{i}"})

        # 4th publish blocks then drops after 50ms.
        start = asyncio.get_running_loop().time()
        await registry.publish_all("heber:events", {"event_id": "drop-me"})
        elapsed = asyncio.get_running_loop().time() - start

        # Producer waited at least the timeout before dropping.
        assert elapsed >= 0.04, f"producer dropped too quickly ({elapsed:.4f}s)"

        stats = registry.get_publish_stats()
        assert stats["dropped_producer_timeout"] == 1
        # The 3 absorbed events are still queued (worker is blocked).
        assert stats["queued"] == 3

        # Cleanup.
        release_event.set()
        await registry.drain_queues(timeout_seconds=2.0)

    @pytest.mark.asyncio
    async def test_shutdown_drains_pending_queue_items(self) -> None:
        """drain_queues + close_all flush queued events before tearing workers down."""
        registry = DataSinkRegistry(
            queue_size=64,
            worker_count=2,
            producer_block_timeout_seconds=0.1,
        )
        sink = _TrackingSink(sink_name="drain-on-shutdown")
        registry.register(sink)

        # Enqueue 20 events; they may not all be drained yet because the workers
        # are racing against the for-loop.
        for i in range(20):
            await registry.publish_all("heber:events", {"event_id": f"s{i}"})

        await registry.close_all()

        # close_all drains the queue before tearing workers down, so all events
        # must have been published.
        assert len(sink.published) == 20
        # Workers have exited.
        for workers in registry._sink_workers.values():
            for w in workers:
                assert w.done()

    @pytest.mark.asyncio
    async def test_drain_queues_timeout_cancels_stuck_workers(self) -> None:
        """drain_queues bounded wait cancels workers that won't exit."""
        release_event = asyncio.Event()
        registry = DataSinkRegistry(
            queue_size=4,
            worker_count=1,
            producer_block_timeout_seconds=0.5,
        )
        sink = _BlockingSink(release_event, sink_name="stuck-sink")
        registry.register(sink)

        # One event reserved by the blocked worker.
        await registry.publish_all("heber:events", {"event_id": "stuck"})

        # drain_queues must NOT hang past the timeout; it cancels.
        start = asyncio.get_running_loop().time()
        await registry.drain_queues(timeout_seconds=0.2)
        elapsed = asyncio.get_running_loop().time() - start

        assert elapsed < 1.0, f"drain_queues took too long: {elapsed:.4f}s"
        # Workers were cancelled.
        for workers in registry._sink_workers.values():
            for w in workers:
                assert w.done()

        release_event.set()

    @pytest.mark.asyncio
    async def test_producer_timeout_drop_increments_emergency_metric(self) -> None:
        """Emergency drop increments the new prometheus counter."""
        from gateway.core.metrics import SINK_PRODUCER_TIMEOUT_DROPS

        release_event = asyncio.Event()
        registry = DataSinkRegistry(
            queue_size=1,
            worker_count=1,
            producer_block_timeout_seconds=0.01,
        )
        sink = _BlockingSink(release_event, sink_name="metric-emit")
        registry.register(sink)

        before = SINK_PRODUCER_TIMEOUT_DROPS.labels(sink="metric-emit")._value.get()

        # Saturate (queue=1, worker=1 → 2 absorbed) then force drops.
        await registry.publish_all("heber:events", {"event_id": "absorb-1"})
        await registry.publish_all("heber:events", {"event_id": "absorb-2"})
        for i in range(3):
            await registry.publish_all("heber:events", {"event_id": f"drop-{i}"})

        after = SINK_PRODUCER_TIMEOUT_DROPS.labels(sink="metric-emit")._value.get()
        assert after - before == 3

        release_event.set()
        await registry.drain_queues(timeout_seconds=2.0)
