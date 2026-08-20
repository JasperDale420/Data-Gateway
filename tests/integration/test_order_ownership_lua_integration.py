"""The ownership guard's Lua, run by a real Redis against a real fence lease.

Every other ownership test drives an in-memory stand-in whose scripts are the
guard's Lua re-implemented in Python by hand, so those tests prove the routes
and prove the fakes — they cannot prove the Lua, and they cannot prove a lease
that expires on its own.

What is at stake is the release that follows a broker refusal. It runs after
two broker reads, which is long enough for a 120-second fence lease to lapse
and for the next request — possibly the same client's — to take the symbol
over, claim it, and put an order on it. Deleting that successor's claim strands
a live order with no owner, which every client then reads as ambiguous. The
owner cannot distinguish the two requests; only the fence token can.

Set ``GATEWAY_TEST_REDIS_URL`` to a throwaway Redis. Tests skip when none is
reachable.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from gateway.core.order_ownership import BrokerSymbolState, OrderOwnershipGuard

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_CLIENT_ID = "test-client"
_SYMBOL = "AAPL"
_FLAT = BrokerSymbolState(has_position=False, order_owners=frozenset())

# The production script with its fence comparison removed. Asserting against
# this proves the fence check is what refuses the stale release, rather than
# some other guard clause that would refuse it anyway.
_RELEASE_WITHOUT_FENCE_CHECK = OrderOwnershipGuard._RELEASE_CLAIM_UNDER_FENCE.replace(
    "if redis.call('GET', KEYS[3]) ~= ARGV[3] then return 0 end", ""
)


async def _seed_claim(redis, guard: OrderOwnershipGuard, *, owner: str = _CLIENT_ID) -> None:
    await redis.set(
        guard.claim_key(_SYMBOL),
        json.dumps({"owner": owner, "claimed_at": "2026-08-18T13:30:00+00:00"}, separators=(",", ":"), sort_keys=True),
    )
    await redis.sadd(guard.owner_index_key(owner), _SYMBOL)


async def _claim_exists(redis, guard: OrderOwnershipGuard) -> bool:
    return await redis.get(guard.claim_key(_SYMBOL)) is not None


async def _await_fence_expiry(redis, guard: OrderOwnershipGuard) -> None:
    """Wait for Redis to actually drop the lease, however long that takes.

    Polling rather than sleeping a fixed span: the lease is real, so on a
    loaded machine it can outlive any constant a test picks, and a test that
    fails under load is worse than no test.
    """
    deadline = asyncio.get_running_loop().time() + 10.0
    while await redis.get(guard.fence_key(_SYMBOL)) is not None:
        assert asyncio.get_running_loop().time() < deadline, "the fence lease never expired"
        await asyncio.sleep(0.02)


async def test_the_real_lua_releases_a_claim_held_under_a_live_fence(redis_probe) -> None:
    guard = OrderOwnershipGuard(redis_probe)
    await _seed_claim(redis_probe, guard)
    fence_token = await guard.acquire_fence(_SYMBOL)

    await guard.release_claim_if_broker_is_clear(
        client_id=_CLIENT_ID,
        symbol=_SYMBOL,
        fence_token=fence_token,
        broker_state=_FLAT,
    )

    assert not await _claim_exists(redis_probe, guard)
    assert await redis_probe.smembers(guard.owner_index_key(_CLIENT_ID)) == set()


async def test_a_fence_that_expired_on_its_own_cannot_release(redis_probe, monkeypatch) -> None:
    """A real TTL, allowed to lapse, then a successor takes the same symbol."""
    monkeypatch.setattr(OrderOwnershipGuard, "_FENCE_TTL_MS", 60)
    guard = OrderOwnershipGuard(redis_probe)
    await _seed_claim(redis_probe, guard)
    stale_token = await guard.acquire_fence(_SYMBOL)

    await _await_fence_expiry(redis_probe, guard)
    # Full-length lease from here: the successor's lease must not be able to
    # lapse mid-test, or the scenario quietly degrades into "no lease at all".
    monkeypatch.undo()

    # The successor is the SAME client: an owner comparison cannot tell them
    # apart, which is the whole point of the fence token.
    successor_token = await guard.acquire_fence(_SYMBOL)
    await redis_probe.set(
        guard.claim_key(_SYMBOL),
        json.dumps({"owner": _CLIENT_ID, "claimed_at": "2026-08-18T13:31:00+00:00"}, separators=(",", ":")),
    )

    await guard.release_claim_if_broker_is_clear(
        client_id=_CLIENT_ID,
        symbol=_SYMBOL,
        fence_token=stale_token,
        broker_state=_FLAT,
    )

    assert await _claim_exists(redis_probe, guard), "the successor's claim must survive the stale release"
    assert await redis_probe.get(guard.fence_key(_SYMBOL)) == successor_token.encode()


async def test_the_stale_release_would_succeed_without_the_fence_comparison(redis_probe) -> None:
    """Pins the fence check as the clause doing the work.

    Without this, the test above would still pass if the script refused the
    release for some unrelated reason, and the protection could be deleted
    without a single test noticing.
    """
    guard = OrderOwnershipGuard(redis_probe)
    await _seed_claim(redis_probe, guard)
    stale_token = "a-token-that-is-no-longer-the-lease"
    await redis_probe.set(guard.fence_key(_SYMBOL), "the-successors-token")

    released = await redis_probe.eval(
        _RELEASE_WITHOUT_FENCE_CHECK,
        3,
        guard.claim_key(_SYMBOL),
        guard.owner_index_key(_CLIENT_ID),
        guard.fence_key(_SYMBOL),
        _CLIENT_ID,
        _SYMBOL,
        stale_token,
    )

    assert released == 1
    assert not await _claim_exists(redis_probe, guard)


async def test_the_real_lua_refuses_a_claim_that_is_frozen_or_mid_mutation(redis_probe) -> None:
    guard = OrderOwnershipGuard(redis_probe)
    fence_token = await guard.acquire_fence(_SYMBOL)

    for blocking_field in ("frozen_reason", "mutation_pending"):
        await redis_probe.set(
            guard.claim_key(_SYMBOL),
            json.dumps({"owner": _CLIENT_ID, "claimed_at": "2026-08-18T13:30:00+00:00", blocking_field: "x"}),
        )
        await guard.release_claim_if_broker_is_clear(
            client_id=_CLIENT_ID,
            symbol=_SYMBOL,
            fence_token=fence_token,
            broker_state=_FLAT,
        )
        assert await _claim_exists(redis_probe, guard), f"a claim carrying {blocking_field} must not be released"


async def test_the_real_lua_refuses_another_clients_claim(redis_probe) -> None:
    guard = OrderOwnershipGuard(redis_probe)
    await _seed_claim(redis_probe, guard, owner="another-client")
    fence_token = await guard.acquire_fence(_SYMBOL)

    await guard.release_claim_if_broker_is_clear(
        client_id=_CLIENT_ID,
        symbol=_SYMBOL,
        fence_token=fence_token,
        broker_state=_FLAT,
    )

    assert await _claim_exists(redis_probe, guard)


@pytest.mark.parametrize(
    "broker_state",
    [
        pytest.param(BrokerSymbolState(has_position=True, order_owners=frozenset()), id="position-open"),
        pytest.param(BrokerSymbolState(has_position=False, order_owners=frozenset({_CLIENT_ID})), id="order-resting"),
        pytest.param(BrokerSymbolState(has_position=False, order_owners=frozenset(), complete=False), id="incomplete"),
    ],
)
async def test_a_symbol_the_broker_still_holds_is_never_released(redis_probe, broker_state) -> None:
    guard = OrderOwnershipGuard(redis_probe)
    await _seed_claim(redis_probe, guard)
    fence_token = await guard.acquire_fence(_SYMBOL)

    await guard.release_claim_if_broker_is_clear(
        client_id=_CLIENT_ID,
        symbol=_SYMBOL,
        fence_token=fence_token,
        broker_state=broker_state,
    )

    assert await _claim_exists(redis_probe, guard)
