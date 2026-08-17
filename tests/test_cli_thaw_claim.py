"""Operator thaw path for a frozen shared-account ownership claim.

A frozen claim blocks every write on its symbol, including the owner's own
risk-reducing close, and nothing in the request path may clear it — that is the
point of the freeze. The operator command is the only release, and it releases
only after a fresh broker reconciliation shows the symbol is unambiguous.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from gateway.cli import thaw_claim
from gateway.core.order_ownership import BrokerSymbolState, OrderOwnershipGuard

_OWNER = "orion"
_SYMBOL = "PLTR260821C00182500"


class _MemoryRedis:
    """In-memory stand-in that re-implements the guard's Lua scripts in Python.

    Same caveat as the fakes in tests/test_order_ownership.py: the Lua never
    runs here, so this must be kept in sync with it by hand.
    """

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.members: dict[str, set[str]] = {}

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str, *, nx: bool = False, px: int | None = None) -> bool | None:
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    async def sadd(self, key: str, value: str) -> int:
        self.members.setdefault(key, set()).add(value)
        return 1

    async def srem(self, key: str, value: str) -> int:
        self.members.get(key, set()).discard(value)
        return 1

    async def eval(self, script: str, _keys: int, key: str, *args: str | int) -> int:
        extra_keys = args[: _keys - 1]
        argv = args[_keys - 1 :]
        if "-- thaw_claim" in script:
            raw = self.values.get(key)
            if raw is None or raw != str(argv[0]):
                return 0
            if str(argv[1]) == "1":
                del self.values[key]
                self.members.get(str(extra_keys[0]), set()).discard(str(argv[2]))
                return 1
            claim = json.loads(raw)
            claim.pop("frozen_reason", None)
            claim.pop("mutation_pending", None)
            self.values[key] = json.dumps(claim, separators=(",", ":"), sort_keys=True)
            return 1
        if "PEXPIRE" in script:
            return 1 if self.values.get(key) == str(argv[0]) else 0
        expected = str(argv[0])
        if self.values.get(key) != expected:
            return 0
        del self.values[key]
        return 1


def _guard(
    *, frozen: bool = True, pending: bool = True, fence: bool = False
) -> tuple[OrderOwnershipGuard, _MemoryRedis]:
    redis = _MemoryRedis()
    guard = OrderOwnershipGuard(redis)
    claim: dict[str, str] = {"owner": _OWNER, "claimed_at": "2026-08-17T14:27:34+00:00"}
    if frozen:
        claim["frozen_reason"] = "broker_mutation_unclassified"
    if pending:
        claim["mutation_pending"] = "cancel_order:abc123"
    redis.values[guard.claim_key(_SYMBOL)] = json.dumps(claim, separators=(",", ":"), sort_keys=True)
    redis.members[guard.owner_index_key(_OWNER)] = {_SYMBOL}
    if fence:
        redis.values[guard.fence_key(_SYMBOL)] = "fence-token"
    return guard, redis


def _reconciler(state: BrokerSymbolState) -> Any:
    async def _reconcile(_symbol: str) -> BrokerSymbolState:
        return state

    return _reconcile


def _claim(redis: _MemoryRedis, guard: OrderOwnershipGuard) -> dict[str, Any] | None:
    raw = redis.values.get(guard.claim_key(_SYMBOL))
    return None if raw is None else json.loads(raw)


@pytest.mark.asyncio
async def test_thaw_clears_freeze_and_pending_marker_when_state_is_unambiguous(
    capsys: pytest.CaptureFixture[str],
) -> None:
    guard, redis = _guard()
    state = BrokerSymbolState(has_position=True, order_owners=frozenset({_OWNER}))

    exit_code = await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol=_SYMBOL)

    assert exit_code == 0
    claim = _claim(redis, guard)
    assert claim is not None
    assert "frozen_reason" not in claim
    assert "mutation_pending" not in claim
    assert claim["owner"] == _OWNER
    printed = capsys.readouterr().out
    assert "before" in printed and "after" in printed
    assert "broker_mutation_unclassified" in printed


@pytest.mark.asyncio
async def test_thaw_refuses_when_another_client_holds_an_open_order() -> None:
    guard, redis = _guard()
    state = BrokerSymbolState(has_position=True, order_owners=frozenset({"cerberus"}))

    exit_code = await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol=_SYMBOL)

    assert exit_code == 1
    assert _claim(redis, guard) == {
        "owner": _OWNER,
        "claimed_at": "2026-08-17T14:27:34+00:00",
        "frozen_reason": "broker_mutation_unclassified",
        "mutation_pending": "cancel_order:abc123",
    }


@pytest.mark.asyncio
async def test_thaw_refuses_when_a_manual_order_is_open() -> None:
    guard, redis = _guard()
    state = BrokerSymbolState(has_position=False, order_owners=frozenset({None}))

    exit_code = await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol=_SYMBOL)

    assert exit_code == 1
    assert _claim(redis, guard) is not None
    assert _claim(redis, guard)["frozen_reason"] == "broker_mutation_unclassified"  # type: ignore[index]


@pytest.mark.asyncio
async def test_thaw_refuses_when_reconciliation_is_incomplete() -> None:
    guard, redis = _guard()
    state = BrokerSymbolState(has_position=True, order_owners=frozenset(), complete=False)

    exit_code = await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol=_SYMBOL)

    assert exit_code == 1
    assert _claim(redis, guard)["frozen_reason"] == "broker_mutation_unclassified"  # type: ignore[index]


@pytest.mark.asyncio
async def test_thaw_refuses_while_a_gateway_mutation_still_holds_the_fence() -> None:
    """A live fence lease means a broker write may still be travelling."""
    guard, redis = _guard(fence=True)
    state = BrokerSymbolState(has_position=True, order_owners=frozenset({_OWNER}))

    exit_code = await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol=_SYMBOL)

    assert exit_code == 1
    assert _claim(redis, guard)["frozen_reason"] == "broker_mutation_unclassified"  # type: ignore[index]


@pytest.mark.asyncio
async def test_thaw_refuses_when_there_is_no_claim() -> None:
    guard = OrderOwnershipGuard(_MemoryRedis())
    state = BrokerSymbolState(has_position=False, order_owners=frozenset())

    exit_code = await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol=_SYMBOL)

    assert exit_code == 1


@pytest.mark.asyncio
async def test_delete_drops_the_claim_and_its_owner_index_when_the_symbol_is_flat() -> None:
    guard, redis = _guard()
    state = BrokerSymbolState(has_position=False, order_owners=frozenset())

    exit_code = await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol=_SYMBOL, delete=True)

    assert exit_code == 0
    assert _claim(redis, guard) is None
    assert redis.members[guard.owner_index_key(_OWNER)] == set()


@pytest.mark.asyncio
async def test_delete_refuses_while_the_position_is_still_open() -> None:
    guard, redis = _guard()
    state = BrokerSymbolState(has_position=True, order_owners=frozenset())

    exit_code = await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol=_SYMBOL, delete=True)

    assert exit_code == 1
    assert _claim(redis, guard) is not None


@pytest.mark.asyncio
async def test_thaw_refuses_a_claim_that_is_neither_frozen_nor_mid_mutation() -> None:
    """Nothing to lift means nothing to write — a healthy claim is left alone."""
    guard, redis = _guard(frozen=False, pending=False)
    before = redis.values[guard.claim_key(_SYMBOL)]
    state = BrokerSymbolState(has_position=True, order_owners=frozenset())

    exit_code = await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol=_SYMBOL)

    assert exit_code == 1
    assert redis.values[guard.claim_key(_SYMBOL)] == before


@pytest.mark.asyncio
async def test_thaw_refuses_when_the_claim_changed_after_it_was_reviewed() -> None:
    """The write applies to the reviewed claim or to nothing at all.

    A claim that moved between the review and the write — released and re-taken
    by another client, thawed beside this run, marked for a new mutation — must
    not absorb a decision made about the old one.
    """
    guard, redis = _guard()

    async def _reconcile_then_rewrite(_symbol: str) -> BrokerSymbolState:
        redis.values[guard.claim_key(_SYMBOL)] = json.dumps(
            {"owner": "cerberus", "claimed_at": "2026-08-17T14:31:00+00:00"},
            separators=(",", ":"),
            sort_keys=True,
        )
        return BrokerSymbolState(has_position=True, order_owners=frozenset({_OWNER}))

    exit_code = await thaw_claim(guard=guard, reconcile=_reconcile_then_rewrite, symbol=_SYMBOL)

    assert exit_code == 1
    assert _claim(redis, guard) == {"owner": "cerberus", "claimed_at": "2026-08-17T14:31:00+00:00"}


@pytest.mark.asyncio
async def test_thaw_releases_the_symbol_fence_it_held() -> None:
    """The owner must be able to mutate immediately after a successful thaw."""
    guard, redis = _guard()
    state = BrokerSymbolState(has_position=True, order_owners=frozenset())

    assert await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol=_SYMBOL) == 0

    assert guard.fence_key(_SYMBOL) not in redis.values


@pytest.mark.asyncio
async def test_thaw_rejects_a_symbol_that_cannot_be_canonicalised() -> None:
    guard, _redis = _guard()
    state = BrokerSymbolState(has_position=False, order_owners=frozenset())

    exit_code = await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol="!!not-a-symbol!!")

    assert exit_code == 1


@pytest.mark.asyncio
async def test_thaw_canonicalises_the_symbol_before_loading_the_claim() -> None:
    """An operator typing a lowercase ticker must reach the same claim key."""
    redis = _MemoryRedis()
    guard = OrderOwnershipGuard(redis)
    redis.values[guard.claim_key("AAPL")] = json.dumps(
        {"owner": _OWNER, "claimed_at": "2026-08-17T14:27:34+00:00", "frozen_reason": "broker_mutation_502"},
        separators=(",", ":"),
        sort_keys=True,
    )
    state = BrokerSymbolState(has_position=True, order_owners=frozenset())

    exit_code = await thaw_claim(guard=guard, reconcile=_reconciler(state), symbol="aapl")

    assert exit_code == 0
    assert "frozen_reason" not in json.loads(redis.values[guard.claim_key("AAPL")])
