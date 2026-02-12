"""Tests for request deduplication coalescing behavior."""

from __future__ import annotations

import asyncio

import pytest

from gateway.core.dedup import RequestDeduplicator


@pytest.mark.asyncio
async def test_request_deduplicator_coalesces_same_key() -> None:
    dedup = RequestDeduplicator(lock_stripes=4)
    fetch_calls = 0

    async def fetcher() -> dict[str, int]:
        nonlocal fetch_calls
        fetch_calls += 1
        await asyncio.sleep(0.01)
        return {"value": 1}

    results = await asyncio.gather(
        dedup.dedupe("bars:AAPL", fetcher),
        dedup.dedupe("bars:AAPL", fetcher),
        dedup.dedupe("bars:AAPL", fetcher),
    )

    assert fetch_calls == 1
    assert results == [{"value": 1}, {"value": 1}, {"value": 1}]
    assert dedup.get_pending_count() == 0
    stats = dedup.get_stats()
    assert stats["total_requests"] == 3
    assert stats["deduplicated"] == 2


@pytest.mark.asyncio
async def test_request_deduplicator_different_keys_fetch_independently() -> None:
    dedup = RequestDeduplicator(lock_stripes=4)
    calls: list[str] = []

    def make_fetcher(name: str):
        async def _fetch():
            calls.append(name)
            await asyncio.sleep(0)
            return name

        return _fetch

    fetch_a = make_fetcher("AAPL")
    fetch_b = make_fetcher("MSFT")

    results = await asyncio.gather(
        dedup.dedupe("bars:AAPL", fetch_a),
        dedup.dedupe("bars:MSFT", fetch_b),
    )

    assert sorted(results) == ["AAPL", "MSFT"]
    assert sorted(calls) == ["AAPL", "MSFT"]


def test_request_deduplicator_lock_striping_is_stable_for_same_key() -> None:
    dedup = RequestDeduplicator(lock_stripes=8)
    lock_a = dedup._lock_for_key("bars:AAPL")
    lock_b = dedup._lock_for_key("bars:AAPL")
    lock_c = dedup._lock_for_key("bars:MSFT")

    assert lock_a is lock_b
    assert len(dedup._key_locks) == 8
    # Different keys may collide, but typically should spread.
    assert lock_c in dedup._key_locks
