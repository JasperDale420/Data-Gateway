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
