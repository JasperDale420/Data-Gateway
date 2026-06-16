"""Throttle for high-frequency log events.

A single failing dependency (e.g. a saturated Redis pool) can emit one log line
per request and bury genuine errors under hundreds of thousands of identical
lines. ``LogThrottle`` collapses repeats of the same key to one log per interval
while still reporting how many were suppressed, so the signal survives without
the flood.
"""

from __future__ import annotations

import time


class LogThrottle:
    """Allow at most one emit per ``key`` per ``interval_seconds``.

    ``should_emit(key)`` returns ``(allowed, suppressed)``: ``allowed`` is True for
    the first call for a key and again once the interval has elapsed since the last
    allowed emit; ``suppressed`` is the number of calls dropped for that key since
    the previous allowed emit (so the emitted line can report the hidden volume).
    """

    def __init__(self, interval_seconds: float) -> None:
        self._interval = interval_seconds
        self._last_emit: dict[str, float] = {}
        self._suppressed: dict[str, int] = {}

    def should_emit(self, key: str, now: float | None = None) -> tuple[bool, int]:
        if now is None:
            now = time.monotonic()
        last = self._last_emit.get(key)
        if last is None or (now - last) >= self._interval:
            suppressed = self._suppressed.pop(key, 0)
            self._last_emit[key] = now
            return True, suppressed
        self._suppressed[key] = self._suppressed.get(key, 0) + 1
        return False, 0

    def reset(self) -> None:
        """Clear all throttle state (used to isolate tests)."""
        self._last_emit.clear()
        self._suppressed.clear()
