"""Tests for the event-loop stall watchdog."""

from __future__ import annotations

import asyncio
import time

import pytest

import gateway.core.loop_watchdog as loop_watchdog_module
from gateway.core.loop_watchdog import LoopStallWatchdog


@pytest.mark.asyncio
async def test_watchdog_detects_synchronous_loop_stall() -> None:
    stalls: list[float] = []
    recoveries: list[float] = []
    watchdog = LoopStallWatchdog(
        stall_threshold_seconds=0.2,
        heartbeat_interval_seconds=0.05,
        check_interval_seconds=0.05,
        on_stall=stalls.append,
        on_recover=recoveries.append,
    )
    await watchdog.start()
    time.sleep(0.4)  # block the event loop so the heartbeat goes stale
    await asyncio.sleep(0.3)  # let the off-loop watcher observe the stall and the recovery
    await watchdog.stop()

    assert stalls, "watchdog did not detect the synchronous stall"
    assert stalls[0] >= 0.2
    assert recoveries, "watchdog did not record the stall recovery"


@pytest.mark.asyncio
async def test_watchdog_samples_repeatedly_through_a_long_stall() -> None:
    samples: list[float] = []
    wd = LoopStallWatchdog(
        stall_threshold_seconds=0.2,
        heartbeat_interval_seconds=0.05,
        check_interval_seconds=0.05,
        on_stall=lambda s: samples.append(s),
        on_recover=lambda t: None,
    )
    wd._max_stall_samples = 3
    await wd.start()
    time.sleep(0.7)  # long synchronous block -> should yield >1 sample, capped at 3
    await asyncio.sleep(0.2)  # let the watcher observe and the loop resume
    await wd.stop()

    assert 2 <= len(samples) <= wd._max_stall_samples
    assert samples == sorted(samples)  # gap grows monotonically across the stall


@pytest.mark.asyncio
async def test_watchdog_quiet_on_healthy_loop() -> None:
    stalls: list[float] = []
    watchdog = LoopStallWatchdog(
        stall_threshold_seconds=0.3,
        heartbeat_interval_seconds=0.05,
        check_interval_seconds=0.05,
        on_stall=stalls.append,
        on_recover=lambda _total: None,
    )
    await watchdog.start()
    await asyncio.sleep(0.4)  # loop stays responsive (only non-blocking awaits)
    await watchdog.stop()

    assert stalls == [], "watchdog fired a false positive on a healthy loop"


@pytest.mark.asyncio
async def test_watchdog_stop_is_idempotent() -> None:
    watchdog = LoopStallWatchdog(stall_threshold_seconds=0.5, heartbeat_interval_seconds=0.05)
    await watchdog.start()
    await watchdog.stop()
    await watchdog.stop()  # second stop must be a no-op, not raise


def test_watchdog_rejects_heartbeat_at_or_above_threshold() -> None:
    with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
        LoopStallWatchdog(stall_threshold_seconds=1.0, heartbeat_interval_seconds=1.0)


def test_default_on_stall_captures_stacks(monkeypatch) -> None:
    # Regression: faulthandler.dump_traceback rejects an io.StringIO target (no
    # fileno()), which would make the default callback raise and capture nothing.
    # The default path must dump real thread stacks into the structured log.
    captured: dict[str, object] = {}

    class _CapturingLogger:
        def warning(self, event: str, **kwargs: object) -> None:
            captured["event"] = event
            captured.update(kwargs)

        def info(self, *args: object, **kwargs: object) -> None: ...

        def error(self, *args: object, **kwargs: object) -> None: ...

    monkeypatch.setattr(loop_watchdog_module, "logger", _CapturingLogger())
    watchdog = LoopStallWatchdog(stall_threshold_seconds=1.0, heartbeat_interval_seconds=0.1)

    watchdog._default_on_stall(7.5)

    assert captured["event"] == "event_loop_stall_detected"
    assert captured["stalled_seconds"] == 7.5
    assert captured["stacks"], "default stall callback captured no thread stacks"
