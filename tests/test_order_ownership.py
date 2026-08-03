"""Fail-closed ownership checks for the shared Alpaca account."""

from __future__ import annotations

import json

import pytest

from gateway.core.order_ownership import (
    BrokerSymbolState,
    OrderOwnershipGuard,
    OwnershipConflict,
    canonical_broker_symbol,
)


class _FakeRedis:
    """In-memory stand-in that re-implements the guard's Lua scripts in Python.

    The scripts themselves never execute here, so this fake and
    ``OrderOwnershipGuard``'s Lua must be kept in sync by hand — a divergence
    between them is invisible to this suite. Exercising the real scripts needs
    a live Redis (cjson key ordering, decode behaviour on malformed values,
    and Lua truthiness of numeric markers all differ from Python).
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
        if "-- begin_mutation" in script:
            raw = self.values.get(key)
            if raw is None:
                return 0
            claim = json.loads(raw)
            if claim.get("mutation_pending"):
                return 0
            claim["mutation_pending"] = str(argv[0])
            self.values[key] = json.dumps(claim, separators=(",", ":"), sort_keys=True)
            return 1
        if "-- complete_mutation" in script:
            raw = self.values.get(key)
            if raw is None:
                return 0
            claim = json.loads(raw)
            if claim.get("mutation_pending") != str(argv[0]):
                return 0
            del claim["mutation_pending"]
            self.values[key] = json.dumps(claim, separators=(",", ":"), sort_keys=True)
            return 1
        if "SADD" in script:
            index_key = extra_keys[0]
            value, symbol = argv
            if key in self.values:
                return 0
            self.values[key] = str(value)
            self.members.setdefault(str(index_key), set()).add(str(symbol))
            return 1
        if "PEXPIRE" in script:
            return 1 if self.values.get(key) == str(argv[0]) else 0
        if "-- release_claim" in script:
            # Mirrors the Lua: match on the decoded owner, never on raw bytes.
            raw = self.values.get(key)
            if raw is None:
                return 0
            claim = json.loads(raw)
            if claim.get("owner") != str(argv[0]):
                return 0
            if claim.get("mutation_pending") or claim.get("frozen_reason"):
                return 0
            del self.values[key]
            index_key, symbol = extra_keys[0], argv[1]
            self.members.get(str(index_key), set()).discard(str(symbol))
            return 1
        expected = str(argv[0])
        if self.values.get(key) != expected:
            return 0
        del self.values[key]
        return 1


@pytest.mark.asyncio
async def test_first_gateway_owner_claims_canonical_occ_symbol_without_ttl() -> None:
    redis = _FakeRedis()
    guard = OrderOwnershipGuard(redis)
    symbol = canonical_broker_symbol("aapl260116c00200000")

    await guard.authorize_submission(
        client_id="orion",
        symbol=symbol,
        broker_state=BrokerSymbolState(has_position=False, order_owners=frozenset()),
    )

    claim = json.loads(redis.values[guard.claim_key(symbol)])
    assert symbol == "AAPL260116C00200000"
    assert claim["owner"] == "orion"
    assert symbol in redis.members[guard.owner_index_key("orion")]


@pytest.mark.asyncio
async def test_manual_or_mixed_broker_orders_freeze_symbol_before_submission() -> None:
    guard = OrderOwnershipGuard(_FakeRedis())

    with pytest.raises(OwnershipConflict, match="manual_or_mixed_broker_orders"):
        await guard.authorize_submission(
            client_id="orion",
            symbol="AAPL",
            broker_state=BrokerSymbolState(has_position=False, order_owners=frozenset({"orion", None})),
        )


@pytest.mark.asyncio
async def test_position_without_a_recorded_owner_freezes_close_and_new_orders() -> None:
    guard = OrderOwnershipGuard(_FakeRedis())
    state = BrokerSymbolState(has_position=True, order_owners=frozenset())

    with pytest.raises(OwnershipConflict, match="unowned_broker_position"):
        await guard.authorize_submission(client_id="orion", symbol="AAPL", broker_state=state)
    with pytest.raises(OwnershipConflict, match="unowned_broker_position"):
        await guard.authorize_close(client_id="orion", symbol="AAPL", broker_state=state)


@pytest.mark.asyncio
async def test_foreign_client_cannot_adopt_an_unclaimed_open_gateway_order() -> None:
    guard = OrderOwnershipGuard(_FakeRedis())

    with pytest.raises(OwnershipConflict, match="unclaimed_broker_order"):
        await guard.authorize_submission(
            client_id="cerberus",
            symbol="AAPL",
            broker_state=BrokerSymbolState(has_position=False, order_owners=frozenset({"orion"})),
        )


@pytest.mark.asyncio
async def test_frozen_claim_blocks_another_close_until_broker_state_is_resolved() -> None:
    guard = OrderOwnershipGuard(_FakeRedis())
    state = BrokerSymbolState(has_position=False, order_owners=frozenset())
    await guard.authorize_submission(client_id="orion", symbol="AAPL", broker_state=state)
    await guard.freeze("AAPL", "broker_mutation_504")

    with pytest.raises(OwnershipConflict, match="claim_frozen_after_ambiguous_broker_mutation"):
        await guard.authorize_close(
            client_id="orion",
            symbol="AAPL",
            broker_state=BrokerSymbolState(has_position=True, order_owners=frozenset()),
        )
    with pytest.raises(OwnershipConflict, match="claim_frozen_after_ambiguous_broker_mutation"):
        await guard.authorize_submission(
            client_id="orion",
            symbol="AAPL",
            broker_state=BrokerSymbolState(has_position=False, order_owners=frozenset()),
        )


@pytest.mark.asyncio
async def test_pending_mutation_blocks_submission_until_post_write_reconciliation_completes() -> None:
    guard = OrderOwnershipGuard(_FakeRedis())
    empty_state = BrokerSymbolState(has_position=False, order_owners=frozenset())
    await guard.authorize_submission(client_id="orion", symbol="AAPL", broker_state=empty_state)
    token = await guard.begin_mutation(symbol="AAPL", operation="create_order")

    with pytest.raises(OwnershipConflict, match="mutation_pending_reconciliation"):
        await guard.begin_mutation(symbol="AAPL", operation="create_order")
    with pytest.raises(OwnershipConflict, match="mutation_pending_reconciliation"):
        await guard.authorize_submission(client_id="orion", symbol="AAPL", broker_state=empty_state)

    await guard.verify_reconciliation(client_id="orion", symbol="AAPL", broker_state=empty_state)
    with pytest.raises(OwnershipConflict, match="mutation_completion_token_lost"):
        await guard.complete_mutation(symbol="AAPL", token="wrong-token")
    await guard.complete_mutation(symbol="AAPL", token=token)
    await guard.authorize_submission(client_id="orion", symbol="AAPL", broker_state=empty_state)


@pytest.mark.asyncio
async def test_claim_releases_only_after_broker_confirms_zero_position_and_no_open_orders() -> None:
    redis = _FakeRedis()
    guard = OrderOwnershipGuard(redis)
    symbol = "AAPL"
    await guard.authorize_submission(
        client_id="orion",
        symbol=symbol,
        broker_state=BrokerSymbolState(has_position=False, order_owners=frozenset()),
    )

    await guard.authorize_submission(
        client_id="cerberus",
        symbol=symbol,
        broker_state=BrokerSymbolState(has_position=False, order_owners=frozenset()),
    )

    claim = json.loads(redis.values[guard.claim_key(symbol)])
    assert claim["owner"] == "cerberus"
    assert symbol not in redis.members[guard.owner_index_key("orion")]


@pytest.mark.asyncio
async def test_claim_still_releases_when_stored_json_key_order_differs() -> None:
    """Release must not depend on the stored claim's byte representation.

    The mutation scripts rewrite the claim with Lua's cjson.encode, whose key
    order is not the order Python's sort_keys dump produces. A release that
    compared raw bytes would stop matching after the first completed mutation
    and the symbol could never be claimed by anyone else again.
    """
    redis = _FakeRedis()
    guard = OrderOwnershipGuard(redis)
    symbol = "AAPL"
    empty_state = BrokerSymbolState(has_position=False, order_owners=frozenset())
    await guard.authorize_submission(client_id="orion", symbol=symbol, broker_state=empty_state)

    # Re-encode the claim with the keys in the opposite order, as cjson may.
    stored = json.loads(redis.values[guard.claim_key(symbol)])
    reordered = {"owner": stored["owner"], "claimed_at": stored["claimed_at"]}
    redis.values[guard.claim_key(symbol)] = json.dumps(reordered, separators=(",", ":"))
    assert not redis.values[guard.claim_key(symbol)].startswith('{"claimed_at"')

    await guard.authorize_submission(client_id="cerberus", symbol=symbol, broker_state=empty_state)

    assert json.loads(redis.values[guard.claim_key(symbol)])["owner"] == "cerberus"


@pytest.mark.parametrize("blocker", ["mutation_pending", "frozen_reason"])
@pytest.mark.asyncio
async def test_release_refuses_a_frozen_or_mid_mutation_claim(blocker: str) -> None:
    """The release script itself must refuse, not only the callers above it.

    Exercises `_release_if_clear` directly: `authorize_submission` rejects a
    frozen or mid-mutation claim before it ever reaches the release path, so
    going through the public entry point would leave the script's own guards
    untested.
    """
    redis = _FakeRedis()
    guard = OrderOwnershipGuard(redis)
    empty_state = BrokerSymbolState(has_position=False, order_owners=frozenset())
    await guard.authorize_submission(client_id="orion", symbol="AAPL", broker_state=empty_state)
    if blocker == "mutation_pending":
        await guard.begin_mutation(symbol="AAPL", operation="create_order")
    else:
        await guard.freeze("AAPL", "broker_mutation_504")

    # The broker reports the symbol flat, which is normally releasable.
    claim = json.loads(redis.values[guard.claim_key("AAPL")])
    await guard._release_if_clear(symbol="AAPL", claim=claim, broker_state=empty_state)

    assert guard.claim_key("AAPL") in redis.values
    assert json.loads(redis.values[guard.claim_key("AAPL")])["owner"] == "orion"
