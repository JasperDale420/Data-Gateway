"""Behavioural tests for RequestDeduplicator.

Complements tests/test_request_dedup.py (which covers same-key coalescing,
distinct-key isolation, and lock striping) by exercising the error path,
statistics/pending bookkeeping, sequential re-fetch after completion, and the
module singleton.
"""

from __future__ import annotations

import asyncio

import pytest

from gateway.core.dedup import RequestDeduplicator, get_deduplicator


@pytest.mark.asyncio
async def test_failed_fetch_propagates_to_all_waiters_and_clears_pending() -> None:
    """A fetcher exception is delivered to every coalesced waiter, and the
    pending slot is released so the failure is not cached."""
    dedup = RequestDeduplicator(lock_stripes=4)
    fetch_calls = 0

    async def boom() -> None:
        nonlocal fetch_calls
        fetch_calls += 1
        await asyncio.sleep(0.01)
        raise ValueError("upstream failed")

    results = await asyncio.gather(
        dedup.dedupe("bars:AAPL", boom),
        dedup.dedupe("bars:AAPL", boom),
        dedup.dedupe("bars:AAPL", boom),
        return_exceptions=True,
    )

    # Exactly one upstream call despite three concurrent callers.
    assert fetch_calls == 1
    assert all(isinstance(r, ValueError) for r in results)
    assert all(str(r) == "upstream failed" for r in results)
    # The failed future must not be cached — pending is cleaned up.
    assert dedup.get_pending_count() == 0


@pytest.mark.asyncio
async def test_failure_does_not_poison_subsequent_requests() -> None:
    """After a failed in-flight request finishes, a later call for the same
    key fetches fresh rather than re-raising the cached error."""
    dedup = RequestDeduplicator(lock_stripes=4)
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            await asyncio.sleep(0.01)
            raise RuntimeError("first attempt fails")
        return "ok"

    # Two concurrent callers on the failing attempt: both see the exception
    # (and consume the shared future, so it is not left unretrieved).
    results = await asyncio.gather(
        dedup.dedupe("k", flaky),
        dedup.dedupe("k", flaky),
        return_exceptions=True,
    )
    assert all(isinstance(r, RuntimeError) for r in results)
    assert attempts == 1  # coalesced: only one real attempt failed

    # New call after the failure fully resolved → fresh fetch, success.
    result = await dedup.dedupe("k", flaky)
    assert result == "ok"
    assert attempts == 2


@pytest.mark.asyncio
async def test_pending_count_reflects_inflight_requests() -> None:
    """get_pending_count tracks in-flight keys and drains to zero on completion."""
    dedup = RequestDeduplicator(lock_stripes=4)
    release = asyncio.Event()

    async def blocked() -> int:
        await release.wait()
        return 42

    task = asyncio.create_task(dedup.dedupe("slow", blocked))
    # Yield until the dedupe coroutine has registered the pending future.
    for _ in range(100):
        if dedup.get_pending_count() == 1:
            break
        await asyncio.sleep(0)
    assert dedup.get_pending_count() == 1

    release.set()
    assert await task == 42
    assert dedup.get_pending_count() == 0


@pytest.mark.asyncio
async def test_sequential_calls_refetch_each_time() -> None:
    """Deduplication is only for *concurrent* in-flight requests — once a
    request resolves, the next call hits the fetcher again (no result cache)."""
    dedup = RequestDeduplicator(lock_stripes=4)
    calls = 0

    async def fetcher() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await dedup.dedupe("x", fetcher) == 1
    assert await dedup.dedupe("x", fetcher) == 2
    assert await dedup.dedupe("x", fetcher) == 3
    assert calls == 3

    stats = dedup.get_stats()
    assert stats["total_requests"] == 3
    assert stats["deduplicated"] == 0


def test_get_stats_zero_division_guard() -> None:
    """dedup_rate is 0.0 with no requests rather than raising ZeroDivisionError."""
    dedup = RequestDeduplicator()
    stats = dedup.get_stats()
    assert stats["total_requests"] == 0
    assert stats["dedup_rate"] == 0.0
    assert stats["pending_requests"] == 0


@pytest.mark.asyncio
async def test_get_stats_dedup_rate_ratio() -> None:
    """dedup_rate equals deduplicated / total_requests."""
    dedup = RequestDeduplicator(lock_stripes=4)

    async def fetcher() -> int:
        await asyncio.sleep(0.01)
        return 1

    # Three concurrent same-key calls: 1 real fetch, 2 deduplicated.
    await asyncio.gather(
        dedup.dedupe("same", fetcher),
        dedup.dedupe("same", fetcher),
        dedup.dedupe("same", fetcher),
    )

    stats = dedup.get_stats()
    assert stats["total_requests"] == 3
    assert stats["deduplicated"] == 2
    assert stats["dedup_rate"] == pytest.approx(2 / 3)


def test_lock_stripes_floor_is_one() -> None:
    """A non-positive lock_stripes count is clamped to at least one stripe."""
    dedup = RequestDeduplicator(lock_stripes=0)
    assert len(dedup._key_locks) == 1
    # Every key maps onto the single stripe.
    assert dedup._lock_for_key("a") is dedup._lock_for_key("b")


def test_get_deduplicator_returns_singleton() -> None:
    """The module-level accessor returns a stable shared instance."""
    first = get_deduplicator()
    second = get_deduplicator()
    assert first is second
    assert isinstance(first, RequestDeduplicator)
