"""Tests for provider rate limiter blocking behavior."""

from __future__ import annotations

import time
from typing import Any, cast

import pytest

from gateway.core.rate_limiter import ProviderRateLimitManager, RateLimitExceeded


class _FakeLimiter:
    def __init__(self, sequence: list[tuple[bool, int | float]]) -> None:
        self._sequence = sequence
        self._idx = 0
        self.total_throttled = 0
        self.per_minute = type("MinuteBucket", (), {"remaining": 0})()

    def try_acquire(self) -> tuple[bool, int | float]:
        if self._idx < len(self._sequence):
            result = self._sequence[self._idx]
            self._idx += 1
            return result
        return self._sequence[-1]


@pytest.mark.asyncio
async def test_blocking_acquire_uses_retry_after_hint() -> None:
    manager = ProviderRateLimitManager()
    provider = "retry-after-test"
    manager._limiters[provider] = cast(
        Any,
        _FakeLimiter(
            [
                (False, 0.2),
                (True, 0),
            ]
        ),
    )

    started = time.perf_counter()
    acquired = await manager.acquire(provider, block=True, max_wait=1.0)
    elapsed = time.perf_counter() - started

    assert acquired is True
    assert elapsed >= 0.18


@pytest.mark.asyncio
async def test_blocking_acquire_raises_timeout_after_max_wait() -> None:
    manager = ProviderRateLimitManager()
    provider = "timeout-test"
    manager._limiters[provider] = cast(
        Any,
        _FakeLimiter(
            [
                (False, 1),
            ]
        ),
    )

    with pytest.raises(RateLimitExceeded):
        await manager.acquire(provider, block=True, max_wait=0.05)

    assert manager._limiters[provider].total_throttled == 1
