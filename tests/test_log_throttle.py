"""Tests for LogThrottle — bounds high-frequency log events to one per interval."""

from __future__ import annotations

from gateway.core.log_throttle import LogThrottle


def test_first_emit_is_allowed_with_zero_suppressed() -> None:
    throttle = LogThrottle(interval_seconds=60.0)
    allowed, suppressed = throttle.should_emit("k", now=0.0)
    assert allowed is True
    assert suppressed == 0


def test_repeats_within_interval_are_suppressed() -> None:
    throttle = LogThrottle(interval_seconds=60.0)
    throttle.should_emit("k", now=0.0)  # first emit
    assert throttle.should_emit("k", now=10.0) == (False, 0)
    assert throttle.should_emit("k", now=20.0) == (False, 0)


def test_emits_after_interval_with_suppressed_count() -> None:
    throttle = LogThrottle(interval_seconds=60.0)
    throttle.should_emit("k", now=0.0)  # emit
    throttle.should_emit("k", now=10.0)  # suppressed 1
    throttle.should_emit("k", now=20.0)  # suppressed 2
    allowed, suppressed = throttle.should_emit("k", now=60.0)
    assert allowed is True
    assert suppressed == 2


def test_distinct_keys_throttled_independently() -> None:
    throttle = LogThrottle(interval_seconds=60.0)
    assert throttle.should_emit("a", now=0.0)[0] is True
    assert throttle.should_emit("b", now=0.0)[0] is True
    # "a" is still within its interval, "b" had its own first emit.
    assert throttle.should_emit("a", now=1.0)[0] is False


def test_suppressed_count_resets_after_emit() -> None:
    throttle = LogThrottle(interval_seconds=60.0)
    throttle.should_emit("k", now=0.0)  # emit
    throttle.should_emit("k", now=10.0)  # suppressed 1
    allowed, suppressed = throttle.should_emit("k", now=60.0)  # emit, drains suppressed
    assert (allowed, suppressed) == (True, 1)
    # New window starts clean — nothing suppressed between t=60 and t=120.
    assert throttle.should_emit("k", now=120.0) == (True, 0)
