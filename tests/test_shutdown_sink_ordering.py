"""Tests for the 8-step graceful shutdown ordering (BLOCKER 1).

Producers must stop *before* ``sink_registry.close_all()`` runs. Otherwise
``DataSinkRegistry.publish_all`` (and ``publish_all_batch``) early-return on
``self._enabled=False`` and any tail event emitted by a poller's final
iteration is silently dropped before reaching Redis.

These tests pin both the ordering in the lifespan source and the actual
runtime behavior of the sink registry.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from gateway.core.data_sink import DataSink, DataSinkRegistry
from gateway.main import lifespan


class _RecordingSink(DataSink):
    """Sink that records every (topic, data) it receives."""

    def __init__(self) -> None:
        self._name = "recording"
        self.published: list[tuple[str, dict]] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def record_publish_metrics(self) -> bool:
        return False

    async def publish(self, topic: str, data) -> bool:  # type: ignore[override]
        self.published.append((topic, data))
        return True

    async def health_check(self) -> bool:
        return True

    async def close(self) -> None:
        return None


class TestLifespanShutdownOrdering:
    """Static source-level ordering checks for the lifespan shutdown sequence."""

    def test_pollers_stop_before_sink_close(self) -> None:
        """Every `stop_*_poller` call must appear before `sink_registry.close_all()`.

        This is the structural fix for BLOCKER 1 — if the ordering ever
        regresses, this test fails loudly.
        """
        source = inspect.getsource(lifespan)
        close_idx = source.index("sink_registry.close_all()")

        producer_markers = (
            "stop_option_capture_service()",
            "multiplexer.stop()",
            "stop_treasury_poller()",
            "stop_quotes_poller()",
            "stop_trades_poller()",
            "stop_crypto_poller()",
            "stop_news_poller()",
            "stop_uw_poller()",
            "get_backfill_engine().shutdown()",
            "await registry.shutdown()",
        )

        for marker in producer_markers:
            marker_idx = source.index(marker)
            assert marker_idx < close_idx, (
                f"BLOCKER 1 regression: '{marker}' must execute before "
                f"'sink_registry.close_all()' in the lifespan shutdown"
            )

    def test_connections_close_after_pollers(self) -> None:
        """Client WS connections close after pollers stop but before sink close."""
        source = inspect.getsource(lifespan)
        poller_idx = source.index("stop_uw_poller()")
        close_clients_idx = source.index("connections.close_all(code=1001")
        sink_close_idx = source.index("sink_registry.close_all()")

        assert poller_idx < close_clients_idx < sink_close_idx


class TestSinkAcceptsPostProducerStopEvents:
    """Verify the sink stays open long enough to accept tail events."""

    @pytest.mark.asyncio
    async def test_late_publish_lands_when_sink_open(self) -> None:
        """A poller's final publish must reach the sink before it's closed."""
        sink = _RecordingSink()
        registry = DataSinkRegistry()
        registry.register(sink)

        async def fake_poller_stop() -> None:
            """Simulate a poller's `stop()` emitting one final envelope."""
            await registry.publish_all("heber:events", {"event_id": "tail-event"})

        # New order: stop producer (poller emits tail event), THEN close sink
        await fake_poller_stop()
        await registry.close_all()

        topics = [t for t, _ in sink.published]
        assert "heber:events" in topics, "Tail event from poller stop must reach sink before close_all()"

    @pytest.mark.asyncio
    async def test_publish_drops_silently_when_sink_already_closed(self) -> None:
        """Confirm the bug surface: publishing after close_all() is a silent no-op.

        This pins the failure mode the ordering fix prevents — without
        the reorder, this is exactly what happened to poller tail events.
        """
        sink = _RecordingSink()
        registry = DataSinkRegistry()
        registry.register(sink)

        # Old (broken) order: close first, then producer emits final event
        await registry.close_all()
        await registry.publish_all("heber:events", {"event_id": "lost"})

        # Give any potential background task a chance to fire
        await asyncio.sleep(0.05)

        assert sink.published == [], (
            "publish_all on a closed registry must be a no-op — "
            "this is the data-loss surface the shutdown reorder fixes"
        )
