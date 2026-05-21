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


@pytest.mark.asyncio
async def test_request_deduplicator_leader_cancel_propagates_to_followers() -> None:
    """When the leader is cancelled mid-fetch, followers must see CancelledError
    instead of hanging forever on the shared future.

    Regression — codex caught: the original `except Exception` clause did not
    catch CancelledError (a BaseException), so the leader's cancellation left
    the shared future unresolved and every follower awaiting it deadlocked.
    """
    dedup = RequestDeduplicator(lock_stripes=4)
    leader_started = asyncio.Event()
    fetcher_blocker = asyncio.Event()

    async def slow_fetcher() -> str:
        leader_started.set()
        # Block until the test cancels the leader
        await fetcher_blocker.wait()
        return "should-never-return"

    leader = asyncio.create_task(dedup.dedupe("bars:AAPL", slow_fetcher))
    # Wait for the leader to actually start the fetch (so the pending entry
    # exists and we know the follower will coalesce, not become the leader).
    await asyncio.wait_for(leader_started.wait(), timeout=1.0)

    follower = asyncio.create_task(dedup.dedupe("bars:AAPL", slow_fetcher))
    # Yield once so the follower reaches `await future`.
    await asyncio.sleep(0)

    # Cancel the leader; the follower must NOT hang forever.
    leader.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(leader, timeout=1.0)

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(follower, timeout=1.0)

    # Pending entry was cleaned up so subsequent calls aren't stuck.
    assert dedup.get_pending_count() == 0


@pytest.mark.asyncio
async def test_request_deduplicator_follower_cancel_does_not_poison_others() -> None:
    """Cancelling one follower must NOT cancel the shared future for other
    followers or break the leader's completion.

    Regression — codex flagged: followers `await future` directly, so an
    `asyncio.Task.cancel()` on a follower would propagate cancellation into
    the shared future, marking it cancelled — every other follower would
    observe `CancelledError` even though the leader could still complete.
    Followers now `await asyncio.shield(future)`.
    """
    dedup = RequestDeduplicator(lock_stripes=4)
    leader_started = asyncio.Event()
    fetcher_blocker = asyncio.Event()

    async def slow_fetcher() -> str:
        leader_started.set()
        await fetcher_blocker.wait()
        return "leader-result"

    leader = asyncio.create_task(dedup.dedupe("bars:AAPL", slow_fetcher))
    await asyncio.wait_for(leader_started.wait(), timeout=1.0)

    follower_a = asyncio.create_task(dedup.dedupe("bars:AAPL", slow_fetcher))
    follower_b = asyncio.create_task(dedup.dedupe("bars:AAPL", slow_fetcher))
    # Let both followers reach `await shield(future)`.
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    # Cancel one follower — the other follower AND the leader must still
    # be able to complete cleanly.
    follower_a.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(follower_a, timeout=1.0)

    # Release the leader's fetch.
    fetcher_blocker.set()

    leader_result = await asyncio.wait_for(leader, timeout=1.0)
    follower_b_result = await asyncio.wait_for(follower_b, timeout=1.0)

    assert leader_result == "leader-result"
    assert follower_b_result == "leader-result"


def test_request_deduplicator_lock_striping_is_stable_for_same_key() -> None:
    dedup = RequestDeduplicator(lock_stripes=8)
    lock_a = dedup._lock_for_key("bars:AAPL")
    lock_b = dedup._lock_for_key("bars:AAPL")
    lock_c = dedup._lock_for_key("bars:MSFT")

    assert lock_a is lock_b
    assert len(dedup._key_locks) == 8
    # Different keys may collide, but typically should spread.
    assert lock_c in dedup._key_locks
