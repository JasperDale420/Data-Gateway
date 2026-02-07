from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

import gateway.main as gateway_main


class _FakeConnections:
    def get(self, _client_id: str) -> None:
        return None


class _FakeSinkRegistry:
    def __init__(self, delay_seconds: float = 0.0, wait_event: asyncio.Event | None = None) -> None:
        self.delay_seconds = delay_seconds
        self.wait_event = wait_event
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def publish_all(self, topic: str, data: dict[str, Any]) -> None:
        self.calls.append((topic, data))
        if self.wait_event is not None:
            await self.wait_event.wait()
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)


@pytest.fixture(autouse=True)
def _reset_stream_sink_dispatch_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gateway_main, "_stream_sink_publish_semaphore", None)
    monkeypatch.setattr(gateway_main, "_stream_sink_publish_tasks", set())


@pytest.mark.asyncio
async def test_on_stream_data_does_not_block_on_sink_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _FakeSinkRegistry(delay_seconds=0.05)
    monkeypatch.setattr(gateway_main, "get_connection_manager", lambda: _FakeConnections())
    monkeypatch.setattr("gateway.api.deps.get_sink_registry", lambda: registry)

    started = time.perf_counter()
    await gateway_main._on_stream_data(
        client_id="c1",
        data_type="bars",
        envelope={"event_id": "e1", "payload": {"symbol": "AAPL"}},
    )
    elapsed_ms = (time.perf_counter() - started) * 1000

    assert elapsed_ms < 20.0
    await gateway_main._drain_stream_sink_publish_tasks(timeout_seconds=1.0)
    assert registry.calls == [("heber:events", {"event_id": "e1", "payload": {"symbol": "AAPL"}})]


@pytest.mark.asyncio
async def test_stream_sink_publish_backpressure_drops_when_pending_limit_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wait_event = asyncio.Event()
    registry = _FakeSinkRegistry(wait_event=wait_event)
    monkeypatch.setattr(gateway_main, "STREAM_SINK_MAX_PENDING_TASKS", 1)

    gateway_main._schedule_stream_sink_publish(registry, {"event_id": "e1"})
    gateway_main._schedule_stream_sink_publish(registry, {"event_id": "e2"})

    # Only one task should be scheduled when max pending is 1.
    assert len(gateway_main._stream_sink_publish_tasks) == 1

    wait_event.set()
    await gateway_main._drain_stream_sink_publish_tasks(timeout_seconds=1.0)

    assert len(registry.calls) == 1
    assert registry.calls[0][1]["event_id"] == "e1"
