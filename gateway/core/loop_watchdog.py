"""Event-loop stall watchdog.

The gateway runs every WS stream, poller, REST handler, and the Heber sink on a
single asyncio event loop. A synchronous CPU-bound stretch or accidental blocking
call freezes the whole loop — observed as multi-second ``on_message_slow`` events,
WS ``heartbeat_timeout_disconnect`` storms, and Redis-sink reconnect churn — but the
freeze is *silent*: nothing can log while the loop is blocked, so the stall window
contains no clue about the culprit.

This watchdog captures that clue. An async heartbeat task stamps a monotonic
timestamp on the loop every ``heartbeat_interval``; a separate daemon thread (which
is NOT on the loop, so it keeps running during a freeze) watches that timestamp. When
the heartbeat goes stale by more than ``stall_threshold`` the loop is *currently*
blocked, so the thread dumps every thread's stack via ``faulthandler`` — including the
blocked event-loop thread's exact synchronous frame — plus GC counts. One dump per
stall; a recovery line records the total stalled duration.
"""

from __future__ import annotations

import asyncio
import faulthandler
import gc
import tempfile
import threading
import time
from collections.abc import Callable

from gateway.core.logger import logger


class LoopStallWatchdog:
    """Detect and stack-dump event-loop stalls from an off-loop watcher thread."""

    def __init__(
        self,
        *,
        stall_threshold_seconds: float = 5.0,
        heartbeat_interval_seconds: float = 1.0,
        check_interval_seconds: float = 1.0,
        on_stall: Callable[[float], None] | None = None,
        on_recover: Callable[[float], None] | None = None,
    ) -> None:
        if heartbeat_interval_seconds >= stall_threshold_seconds:
            raise ValueError("heartbeat_interval_seconds must be smaller than stall_threshold_seconds")
        self._stall_threshold = stall_threshold_seconds
        self._heartbeat_interval = heartbeat_interval_seconds
        self._check_interval = check_interval_seconds
        self._on_stall = on_stall or self._default_on_stall
        self._on_recover = on_recover or self._default_on_recover

        self._last_beat: float | None = None
        self._running = False
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._watcher: threading.Thread | None = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._last_beat = time.monotonic()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._watcher = threading.Thread(target=self._watch_loop, name="loop-stall-watchdog", daemon=True)
        self._watcher.start()
        logger.info(
            "loop_watchdog_started",
            stall_threshold_seconds=self._stall_threshold,
            heartbeat_interval_seconds=self._heartbeat_interval,
        )

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        if self._watcher is not None:
            # The watcher sleeps in check_interval slices; give it one to exit.
            self._watcher.join(timeout=self._check_interval * 2 + 1.0)
            self._watcher = None
        logger.info("loop_watchdog_stopped")

    async def _heartbeat_loop(self) -> None:
        try:
            while self._running:
                self._last_beat = time.monotonic()
                await asyncio.sleep(self._heartbeat_interval)
        except asyncio.CancelledError:
            raise

    def _watch_loop(self) -> None:
        """Runs on a daemon thread, off the event loop, so it survives a freeze."""
        stall_active = False
        stall_started_beat: float | None = None
        while self._running:
            time.sleep(self._check_interval)
            last_beat = self._last_beat
            if last_beat is None:
                continue
            gap = time.monotonic() - last_beat
            if gap >= self._stall_threshold:
                if not stall_active:
                    stall_active = True
                    stall_started_beat = last_beat
                    self._safe_call(self._on_stall, gap)
            elif stall_active:
                stall_active = False
                total = (time.monotonic() - stall_started_beat) if stall_started_beat is not None else gap
                self._safe_call(self._on_recover, total)
                stall_started_beat = None

    @staticmethod
    def _safe_call(callback: Callable[[float], None], value: float) -> None:
        try:
            callback(value)
        except Exception:  # noqa: BLE001 — watchdog callbacks must never crash the watcher thread
            logger.error("loop_watchdog_callback_failed", exc_info=True)

    def _default_on_stall(self, stalled_seconds: float) -> None:
        # faulthandler writes at the C fd level, so the target must expose a real
        # fileno() — an in-memory StringIO raises io.UnsupportedOperation. Dump to a
        # temp file, then read the captured stacks back into the structured log.
        with tempfile.TemporaryFile() as stack_file:
            faulthandler.dump_traceback(file=stack_file, all_threads=True)
            stack_file.seek(0)
            stacks = stack_file.read().decode("utf-8", errors="replace")
        logger.warning(
            "event_loop_stall_detected",
            stalled_seconds=round(stalled_seconds, 2),
            threshold_seconds=self._stall_threshold,
            gc_counts=gc.get_count(),
            stacks=stacks,
        )

    @staticmethod
    def _default_on_recover(total_stalled_seconds: float) -> None:
        logger.warning("event_loop_stall_recovered", total_stalled_seconds=round(total_stalled_seconds, 2))


if __name__ == "__main__":
    # ponytail: runnable self-check — a 0.4s synchronous block must be detected.
    async def _demo() -> None:
        detected: list[float] = []
        wd = LoopStallWatchdog(
            stall_threshold_seconds=0.2,
            heartbeat_interval_seconds=0.05,
            check_interval_seconds=0.05,
            on_stall=detected.append,
            on_recover=lambda _t: None,
        )
        await wd.start()
        time.sleep(0.4)  # block the event loop so the heartbeat goes stale
        await asyncio.sleep(0.2)  # let the watcher observe and the loop resume
        await wd.stop()
        assert detected, "watchdog failed to detect the synchronous loop stall"
        print(f"OK: detected stall of ~{detected[0]:.2f}s")

    asyncio.run(_demo())
