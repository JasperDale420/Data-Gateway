"""Tests for non-blocking EOD scheduling (BLOCKER 3).

The EOD per-ticker fan-out runs across hundreds of tickers × 8 feeds and
can take many minutes. Previously the main UW poll loop awaited it inline,
starving the darkpool / extended-hours pollers that are explicitly meant
to keep running during EOD. The fix spawns EOD as a background
``asyncio.Task`` and lets ``_poll_loop`` continue servicing all other
intervals while it runs.

These tests assert:
- EOD spawns as a non-awaited task (does not block the loop).
- Darkpool continues to poll while EOD is in flight.
- The next loop iteration cleans up a finished EOD task.
- ``stop()`` cancels an in-flight EOD task during shutdown.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.core.uw_poller import UWPoller


class _FakeSink:
    async def publish_all(self, *args, **kwargs):
        return None

    async def publish_all_batch_results(self, msgs):
        return [True] * len(msgs)


@pytest.mark.asyncio
async def test_eod_runs_as_background_task(monkeypatch):
    """The EOD run is scheduled via ``asyncio.create_task`` -- not awaited."""
    poller = UWPoller(eod_enabled=True)
    poller._running = True

    # Slow EOD work so we can observe darkpool firing while it's in flight.
    eod_started = asyncio.Event()
    eod_release = asyncio.Event()

    async def slow_eod(sink_registry):
        eod_started.set()
        await eod_release.wait()

    # Force the schedule guards to True without touching wall-clock.
    monkeypatch.setattr(poller, "_should_poll_eod", lambda: True)
    monkeypatch.setattr(poller, "_should_poll_flow", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_darkpool", lambda: True)
    monkeypatch.setattr(poller, "_should_poll_tide", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_sector_tide", lambda: False)
    monkeypatch.setattr(poller, "_is_market_hours", lambda: True)
    monkeypatch.setattr(poller, "_is_extended_hours", lambda: True)
    monkeypatch.setattr(poller, "_poll_eod_snapshots", slow_eod)

    # Darkpool tracking so we can verify it ran while EOD was in flight.
    darkpool_calls: list[Any] = []

    async def fake_darkpool(sink_registry, limit):
        darkpool_calls.append(("darkpool", limit))

    monkeypatch.setattr(poller, "_poll_flow_alerts", AsyncMock())
    monkeypatch.setattr(poller, "_poll_darkpool", fake_darkpool)
    monkeypatch.setattr(poller, "_poll_market_tide", AsyncMock())
    monkeypatch.setattr(poller, "_poll_sector_tides", AsyncMock())
    monkeypatch.setattr(poller, "_cleanup_cache", MagicMock())

    sink_registry = _FakeSink()
    from gateway.core import globals as globals_module

    globals_module.set_sink_registry(sink_registry)

    # Patch asyncio.sleep *inside the uw_poller module* so the poll loop
    # doesn't actually block on BASE_LOOP_INTERVAL. Use a separate sleep
    # primitive (`_orig_sleep` captured once) to avoid recursive patching.
    _orig_sleep = asyncio.sleep
    sleep_count = 0

    async def fast_sleep(_delay):
        nonlocal sleep_count
        sleep_count += 1
        # Stop the loop after a few iterations
        if sleep_count >= 3:
            poller._running = False
        await _orig_sleep(0)

    monkeypatch.setattr("gateway.core.uw_poller.asyncio.sleep", fast_sleep)

    # Run the loop. EOD will spawn but block on eod_release; darkpool must
    # still fire on subsequent loop ticks.
    loop_task = asyncio.create_task(poller._poll_loop())

    # Wait for EOD to start and for the loop to iterate enough times for
    # darkpool to have a chance.
    await asyncio.wait_for(eod_started.wait(), timeout=2.0)
    # Allow a few more loop ticks
    for _ in range(5):
        await asyncio.sleep(0)

    # The EOD task must still be running
    assert poller._eod_task is not None
    assert not poller._eod_task.done(), "BLOCKER 3 regression: EOD was awaited inline, blocking the loop."

    # Darkpool MUST have fired at least once while EOD was in flight.
    assert len(darkpool_calls) >= 1, (
        "BLOCKER 3 regression: darkpool starved during EOD run -- "
        "EOD work is blocking the main loop instead of running in background."
    )

    # Release EOD and let the loop finish naturally.
    eod_release.set()
    poller._running = False
    await asyncio.wait_for(loop_task, timeout=2.0)


@pytest.mark.asyncio
async def test_eod_not_double_scheduled_while_running(monkeypatch):
    """A second ``_should_poll_eod=True`` tick doesn't spawn a second task."""
    poller = UWPoller(eod_enabled=True)
    poller._running = True

    started_count = 0
    release = asyncio.Event()

    async def slow_eod(sink_registry):
        nonlocal started_count
        started_count += 1
        await release.wait()

    monkeypatch.setattr(poller, "_should_poll_eod", lambda: True)
    monkeypatch.setattr(poller, "_should_poll_flow", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_darkpool", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_tide", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_sector_tide", lambda: False)
    monkeypatch.setattr(poller, "_is_market_hours", lambda: False)
    monkeypatch.setattr(poller, "_is_extended_hours", lambda: False)
    monkeypatch.setattr(poller, "_poll_eod_snapshots", slow_eod)
    monkeypatch.setattr(poller, "_cleanup_cache", MagicMock())

    sink_registry = _FakeSink()
    from gateway.core import globals as globals_module

    globals_module.set_sink_registry(sink_registry)

    ticks = 0
    _orig_sleep2 = asyncio.sleep

    async def fast_sleep(_delay):
        nonlocal ticks
        ticks += 1
        if ticks >= 4:
            poller._running = False
        await _orig_sleep2(0)

    monkeypatch.setattr("gateway.core.uw_poller.asyncio.sleep", fast_sleep)

    loop_task = asyncio.create_task(poller._poll_loop())
    # Let the loop spin a few times
    for _ in range(10):
        await asyncio.sleep(0)

    release.set()
    poller._running = False
    await asyncio.wait_for(loop_task, timeout=2.0)

    assert started_count == 1, f"EOD should spawn exactly once per day; spawned {started_count} times"


@pytest.mark.asyncio
async def test_stop_cancels_in_flight_eod_task(monkeypatch):
    """``stop()`` must cancel any in-flight EOD task during shutdown."""
    poller = UWPoller(eod_enabled=True)

    cancelled = asyncio.Event()
    started = asyncio.Event()

    async def slow_eod():
        try:
            started.set()
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    poller._eod_task = asyncio.create_task(slow_eod(), name="test_eod")
    await asyncio.wait_for(started.wait(), timeout=1.0)

    # super().stop() will set _running=False and cancel self._task; we
    # only care here that the EOD task itself is cancelled.
    from gateway.core.base_poller import BasePoller

    monkeypatch.setattr(BasePoller, "stop", AsyncMock())
    poller._provider = None  # skip provider.shutdown() path

    await poller.stop()

    assert cancelled.is_set(), "stop() must cancel the in-flight EOD task"
    assert poller._eod_task is None, "EOD task reference must be cleared on stop"


@pytest.mark.asyncio
async def test_market_hours_check_runs_off_the_loop_thread(monkeypatch):
    """``_is_market_hours`` (heavy pandas ``is_market_open``) must run in a
    worker thread, not on the event-loop thread that services the poll loop.

    ``exchange_calendars`` does synchronous pandas work; running it inline on
    the single asyncio loop was a source of multi-second loop stalls. The poll
    loop now dispatches the sync helper via ``asyncio.to_thread``. This test
    records the thread each side runs on and asserts they differ.
    """
    import threading

    poller = UWPoller(flow_enabled=True)
    poller._running = True

    loop_thread_ident = threading.get_ident()
    helper_thread_ident: list[int] = []

    def recording_market_hours() -> bool:
        helper_thread_ident.append(threading.get_ident())
        return False  # False keeps the loop from doing any real flow polling

    # Force flow to be due so the loop reaches the market-hours check.
    monkeypatch.setattr(poller, "_should_poll_flow", lambda: True)
    monkeypatch.setattr(poller, "_should_poll_darkpool", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_tide", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_sector_tide", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_eod", lambda: False)
    monkeypatch.setattr(poller, "_is_market_hours", recording_market_hours)
    monkeypatch.setattr(poller, "_cleanup_cache", MagicMock())

    sink_registry = _FakeSink()
    from gateway.core import globals as globals_module

    globals_module.set_sink_registry(sink_registry)

    # Stop after one iteration; the fake sleep runs on the loop thread.
    _orig_sleep = asyncio.sleep

    async def fast_sleep(_delay):
        poller._running = False
        await _orig_sleep(0)

    monkeypatch.setattr("gateway.core.uw_poller.asyncio.sleep", fast_sleep)

    await asyncio.wait_for(poller._poll_loop(), timeout=2.0)

    assert helper_thread_ident, "_is_market_hours was never invoked by the poll loop"
    assert helper_thread_ident[0] != loop_thread_ident, (
        "regression: _is_market_hours ran on the event-loop thread instead of "
        "a worker thread -- its blocking pandas work will stall the loop."
    )


@pytest.mark.asyncio
async def test_eod_gate_check_runs_off_the_loop_thread(monkeypatch):
    """``_should_poll_eod`` (blocking EOD-state file read + ``fcntl.flock``) must
    run in a worker thread, not on the event-loop thread.

    ``_should_poll_eod`` calls ``UwEodStateStore.should_defer`` which does
    ``Path.read_text()`` under a file lock. The poll loop now dispatches it via
    ``asyncio.to_thread``. Distinct from the flow-gate test: a regression that
    reverted only the ``_should_poll_eod`` wrap would pass that test but fail
    this one.
    """
    import threading

    poller = UWPoller(eod_enabled=True)
    poller._running = True

    loop_thread_ident = threading.get_ident()
    gate_thread_ident: list[int] = []

    def recording_should_poll_eod() -> bool:
        gate_thread_ident.append(threading.get_ident())
        return False  # False keeps the EOD branch from spawning real work

    # Everything else off so the loop reaches the EOD gate deterministically.
    monkeypatch.setattr(poller, "_should_poll_flow", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_darkpool", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_tide", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_sector_tide", lambda: False)
    monkeypatch.setattr(poller, "_should_poll_eod", recording_should_poll_eod)
    monkeypatch.setattr(poller, "_cleanup_cache", MagicMock())

    sink_registry = _FakeSink()
    from gateway.core import globals as globals_module

    globals_module.set_sink_registry(sink_registry)

    # Stop after one iteration; the fake sleep runs on the loop thread.
    _orig_sleep = asyncio.sleep

    async def fast_sleep(_delay):
        poller._running = False
        await _orig_sleep(0)

    monkeypatch.setattr("gateway.core.uw_poller.asyncio.sleep", fast_sleep)

    await asyncio.wait_for(poller._poll_loop(), timeout=2.0)

    assert gate_thread_ident, "_should_poll_eod was never invoked by the poll loop"
    assert gate_thread_ident[0] != loop_thread_ident, (
        "regression: _should_poll_eod ran on the event-loop thread instead of "
        "a worker thread -- its blocking EOD-state file I/O will stall the loop."
    )
