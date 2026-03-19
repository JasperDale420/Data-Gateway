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
        self.calls: list[tuple[str, dict[str, Any] | str]] = []

    async def publish_all(self, topic: str, data: dict[str, Any] | str) -> None:
        self.calls.append((topic, data))
        if self.wait_event is not None:
            await self.wait_event.wait()
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)


@pytest.fixture(autouse=True)
def _reset_stream_sink_dispatch_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gateway_main, "_stream_sink_publish_semaphore", None)
    monkeypatch.setattr(gateway_main, "_stream_sink_publish_tasks", set())
    monkeypatch.setattr(gateway_main, "_stream_sink_registry", None)
    monkeypatch.setattr(
        gateway_main,
        "_stream_sink_max_inflight_publish",
        gateway_main.DEFAULT_STREAM_SINK_MAX_INFLIGHT_PUBLISH,
    )
    monkeypatch.setattr(
        gateway_main,
        "_stream_sink_max_pending_tasks",
        gateway_main.DEFAULT_STREAM_SINK_MAX_PENDING_TASKS,
    )


@pytest.mark.asyncio
async def test_on_stream_data_does_not_block_on_sink_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _FakeSinkRegistry(delay_seconds=0.05)
    monkeypatch.setattr(gateway_main, "get_connection_manager", lambda: _FakeConnections())
    gateway_main._set_stream_sink_registry(registry)

    started = time.perf_counter()
    await gateway_main._on_stream_data(
        client_id="c1",
        data_type="bars",
        envelope={"event_id": "e1", "payload": {"symbol": "AAPL"}},
    )
    elapsed_ms = (time.perf_counter() - started) * 1000

    assert elapsed_ms < 20.0
    await gateway_main._drain_stream_sink_publish_tasks(timeout_seconds=1.0)
    assert len(registry.calls) == 1
    assert registry.calls[0][0] == "heber:events"
    # _on_stream_data pre-serializes the envelope to a JSON string for sink efficiency
    import json

    expected_envelope = json.loads(registry.calls[0][1])
    assert expected_envelope == {"event_id": "e1", "payload": {"symbol": "AAPL"}}


@pytest.mark.asyncio
async def test_stream_sink_publish_backpressure_drops_when_pending_limit_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wait_event = asyncio.Event()
    registry = _FakeSinkRegistry(wait_event=wait_event)
    monkeypatch.setattr(gateway_main, "_stream_sink_max_pending_tasks", 1)

    gateway_main._schedule_stream_sink_publish(registry, {"event_id": "e1"})
    gateway_main._schedule_stream_sink_publish(registry, {"event_id": "e2"})

    # Only one task should be scheduled when max pending is 1.
    assert len(gateway_main._stream_sink_publish_tasks) == 1

    wait_event.set()
    await gateway_main._drain_stream_sink_publish_tasks(timeout_seconds=1.0)

    assert len(registry.calls) == 1
    assert registry.calls[0][1]["event_id"] == "e1"


def test_configure_stream_sink_dispatch_limits_clamps_and_resets_semaphore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatch_limits: list[tuple[int, int]] = []
    pending_counts: list[int] = []

    def _set_dispatch_limits(*, max_inflight_publish: int, max_pending_tasks: int) -> None:
        dispatch_limits.append((max_inflight_publish, max_pending_tasks))

    def _set_pending(count: int) -> None:
        pending_counts.append(count)

    monkeypatch.setattr(gateway_main, "set_stream_sink_dispatch_limits_metrics", _set_dispatch_limits)
    monkeypatch.setattr(gateway_main, "set_stream_sink_pending_tasks", _set_pending)
    gateway_main._configure_stream_sink_dispatch_limits(max_inflight_publish=0, max_pending_tasks=0)

    assert gateway_main._stream_sink_max_inflight_publish == 1
    assert gateway_main._stream_sink_max_pending_tasks == 1
    assert gateway_main._stream_sink_publish_semaphore is not None
    assert dispatch_limits == [(1, 1)]
    assert pending_counts[-1] == 0


@pytest.mark.asyncio
async def test_schedule_stream_sink_publish_records_scheduler_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    pending_counts: list[int] = []

    def _record_dispatch_event(status: str) -> None:
        events.append(status)

    def _set_pending(count: int) -> None:
        pending_counts.append(count)

    monkeypatch.setattr(gateway_main, "record_stream_sink_dispatch_event", _record_dispatch_event)
    monkeypatch.setattr(gateway_main, "set_stream_sink_pending_tasks", _set_pending)

    gateway_main._schedule_stream_sink_publish(_FakeSinkRegistry(), {"event_id": "e1"})
    await gateway_main._drain_stream_sink_publish_tasks(timeout_seconds=1.0)

    assert "scheduled" in events
    assert pending_counts and 1 in pending_counts
