"""A submission the broker refused outright must not freeze an ownership claim.

Alpaca answers an order submission it will not accept with a 4xx carrying a
structured error body — ``403 {"code": 40310000}`` for insufficient quantity,
day-trade buying power or a wash trade, ``422 {"code": 42210000}`` for
validation. Every one of those is decided before anything reaches the book, so
the symbol is exactly as it was reconciled a moment earlier.

The refusal used to reach the catch-all handler on ``POST /orders`` and
``DELETE /positions/{symbol}``, which assumed a write might still be in flight
and froze the symbol's durable claim. On 2026-08-18 that froze twelve option
symbols for one trading system: each close had already filled, its position
monitor re-sent the close before Alpaca's positions endpoint caught up, and the
"insufficient qty" refusal that came back locked the symbol.

Genuinely ambiguous outcomes still freeze — 5xx, timeouts, transport failures,
and any reply whose status or body cannot be read — because there the executor
thread may complete the write after the Gateway gave up.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast

import pytest
from alpaca.common.exceptions import APIError
from fastapi import HTTPException

from gateway.api.alpaca import trading
from gateway.core.order_ownership import (
    BrokerSymbolState,
    OrderOwnershipGuard,
    OwnershipConflict,
    OwnershipStoreUnavailable,
)
from gateway.core.registry import ProviderRegistry
from gateway.providers.alpaca.trading import AlpacaTradingMixin

_CLIENT_ID = "test-client"
_OTHER_CLIENT_ID = "other-client"
_SYMBOL = "AAPL"

_FLAT = BrokerSymbolState(has_position=False, order_owners=frozenset())
_HELD = BrokerSymbolState(has_position=True, order_owners=frozenset())
_RESTING = BrokerSymbolState(has_position=False, order_owners=frozenset({_CLIENT_ID}))
_INCOMPLETE = BrokerSymbolState(has_position=False, order_owners=frozenset(), complete=False)


def _api_error(
    *,
    code: int | None = None,
    message: str = "",
    status_code: int | None = None,
) -> APIError:
    """Build an APIError shaped like the SDK's, whose ``code``/``status_code`` read back."""
    body = json.dumps({"code": code, "message": message}) if code is not None else message
    http_error = SimpleNamespace(response=SimpleNamespace(status_code=status_code))
    return APIError(body, http_error)


def _insufficient_qty_error() -> APIError:
    """The exact reply the incident produced: a close re-sent after it filled."""
    return _api_error(code=40310000, message="insufficient qty available for order", status_code=403)


def _position_not_found_error() -> APIError:
    """What Alpaca answers a close aimed at a position that is already gone."""
    return _api_error(code=40410000, message="position not found", status_code=404)


class _MemoryRedis:
    """In-memory stand-in that re-implements the guard's Lua scripts in Python.

    Same caveat as the fakes in tests/test_order_ownership.py: the Lua never
    runs here, so this must be kept in sync with it by hand.
    """

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.index: dict[str, set[str]] = {}

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str, *, nx: bool = False, px: int | None = None) -> bool | None:
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    async def sadd(self, key: str, value: str) -> int:
        self.index.setdefault(key, set()).add(value)
        return 1

    async def srem(self, key: str, value: str) -> int:
        self.index.setdefault(key, set()).discard(value)
        return 1

    async def eval(self, script: str, _keys: int, key: str, *args: str | int) -> int:
        # ``key`` is KEYS[1]; for the multi-key scripts ``args[0]`` is KEYS[2]
        # (the owner index), ``args[1]`` is KEYS[3], and everything from
        # ``_keys - 1`` on is ARGV.
        owner_index_key = str(args[0]) if _keys > 1 else ""
        argv = args[_keys - 1 :]
        if "-- release_claim_under_fence" in script:
            fence_key = str(args[1])
            if self.values.get(fence_key) != str(argv[2]):
                return 0
            raw = self.values.get(key)
            if raw is None:
                return 0
            claim = json.loads(raw)
            if claim.get("owner") != str(argv[0]):
                return 0
            if claim.get("mutation_pending") or claim.get("frozen_reason"):
                return 0
            del self.values[key]
            self.index.setdefault(owner_index_key, set()).discard(str(argv[1]))
            return 1
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
            value, symbol = argv
            if key in self.values:
                return 0
            self.values[key] = str(value)
            self.index.setdefault(owner_index_key, set()).add(str(symbol))
            return 1
        if "PEXPIRE" in script:
            return 1 if self.values.get(key) == str(argv[0]) else 0
        if "-- release_claim" in script:
            raw = self.values.get(key)
            if raw is None:
                return 0
            claim = json.loads(raw)
            if claim.get("owner") != str(argv[0]):
                return 0
            if claim.get("mutation_pending") or claim.get("frozen_reason"):
                return 0
            del self.values[key]
            self.index.setdefault(owner_index_key, set()).discard(str(argv[1]))
            return 1
        expected = str(argv[0])
        if self.values.get(key) != expected:
            return 0
        del self.values[key]
        return 1


class _FakeRegistry:
    def __init__(self, provider: Any) -> None:
        self._provider = provider

    def get(self, name: str) -> Any:
        return self._provider if name == "alpaca" else None


class _SubmitProvider:
    """Alpaca trading provider whose submissions raise a supplied error."""

    def __init__(self, error: BaseException | None = None) -> None:
        self._error = error
        self.create_calls: list[str] = []
        self.close_calls: list[str] = []

    def create_order(self, **kwargs: Any) -> dict[str, Any]:
        self.create_calls.append(str(kwargs.get("symbol")))
        if self._error is not None:
            raise self._error
        return {"id": "o-new", "symbol": kwargs.get("symbol")}

    def close_position(self, symbol: str, _qty: Any = None, _percentage: Any = None) -> dict[str, Any]:
        self.close_calls.append(symbol)
        if self._error is not None:
            raise self._error
        return {"id": "close-1", "symbol": symbol}


@pytest.fixture(autouse=True)
def _reset_trading_inflight_sem() -> Any:
    trading._reset_trading_inflight_sem_for_tests()
    yield
    trading._reset_trading_inflight_sem_for_tests()


def _seeded_guard(
    monkeypatch: pytest.MonkeyPatch,
    *,
    owner: str = _CLIENT_ID,
) -> tuple[OrderOwnershipGuard, _MemoryRedis]:
    """Install a real guard over an in-memory store, holding a claim on the symbol."""
    redis = _MemoryRedis()
    guard = OrderOwnershipGuard(redis)
    redis.values[guard.claim_key(_SYMBOL)] = json.dumps(
        {"owner": owner, "claimed_at": "2026-08-18T13:30:00+00:00"},
        separators=(",", ":"),
        sort_keys=True,
    )
    redis.index[guard.owner_index_key(owner)] = {_SYMBOL}
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    return guard, redis


def _stub_broker_states(
    monkeypatch: pytest.MonkeyPatch,
    states: list[BrokerSymbolState | BaseException],
) -> list[int]:
    """Return successive reconciliation results; the last one repeats.

    A submission reconciles once before the write, and the refusal path
    reconciles again before it decides whether the claim still has anything
    behind it — so the ordering of this list is the story each test tells.
    """
    calls: list[int] = []

    async def _state(*_args: Any, **_kwargs: Any) -> BrokerSymbolState:
        index = min(len(calls), len(states) - 1)
        calls.append(index)
        result = states[index]
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr(trading, "_reconcile_broker_symbol_state", _state)
    return calls


def _stored_claim(redis: _MemoryRedis, guard: OrderOwnershipGuard) -> dict[str, Any] | None:
    raw = redis.values.get(guard.claim_key(_SYMBOL))
    return None if raw is None else cast(dict[str, Any], json.loads(raw))


def _live_claim(redis: _MemoryRedis, guard: OrderOwnershipGuard) -> dict[str, Any]:
    claim = _stored_claim(redis, guard)
    assert claim is not None, "claim must survive the refused submission"
    return claim


async def _submit(provider: _SubmitProvider, *, client_id: str = _CLIENT_ID) -> HTTPException:
    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol=_SYMBOL,
            side="sell",
            qty=1,
            client=cast(Any, SimpleNamespace(id=client_id)),
            registry=cast(ProviderRegistry, _FakeRegistry(provider)),
        )
    return exc.value


async def _close(provider: _SubmitProvider) -> HTTPException:
    with pytest.raises(HTTPException) as exc:
        await trading.close_position(
            symbol=_SYMBOL,
            qty=None,
            percentage=None,
            client=cast(Any, SimpleNamespace(id=_CLIENT_ID)),
            registry=cast(ProviderRegistry, _FakeRegistry(provider)),
        )
    return exc.value


# ---------------------------------------------------------------------------
# POST /orders
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_insufficient_qty_submission_does_not_freeze_and_returns_the_brokers_4xx(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The incident: a close re-sent after it had already filled.

    Alpaca refused it, so nothing about the symbol changed. Freezing here
    locked the owner out of every later write on that symbol.
    """
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_FLAT, _FLAT])
    provider = _SubmitProvider(_insufficient_qty_error())

    error = await _submit(provider)

    assert error.status_code == 403
    assert provider.create_calls == [_SYMBOL]
    # Nothing is on the book and nothing is held: the claim taken for this
    # submission has nothing left behind it.
    assert _stored_claim(redis, guard) is None
    assert redis.index.get(guard.owner_index_key(_CLIENT_ID)) == set()


@pytest.mark.asyncio
async def test_refused_submission_keeps_the_claim_when_the_position_is_still_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live position must stay claimed — releasing it strands the owner."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _HELD])

    error = await _submit(_SubmitProvider(_insufficient_qty_error()))

    assert error.status_code == 403
    claim = _live_claim(redis, guard)
    assert "frozen_reason" not in claim
    assert "mutation_pending" not in claim
    assert claim["owner"] == _CLIENT_ID


@pytest.mark.asyncio
async def test_refused_submission_keeps_the_claim_while_an_order_rests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unclaimed open order is ambiguous for EVERY client, so never release one."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_RESTING, _RESTING])

    await _submit(_SubmitProvider(_insufficient_qty_error()))

    claim = _live_claim(redis, guard)
    assert "frozen_reason" not in claim
    assert "mutation_pending" not in claim


@pytest.mark.asyncio
async def test_refused_submission_keeps_the_claim_when_reconciliation_is_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreadable broker record may BE the position — do not read it as flat."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_FLAT, _INCOMPLETE])

    await _submit(_SubmitProvider(_insufficient_qty_error()))

    claim = _live_claim(redis, guard)
    assert "frozen_reason" not in claim
    assert "mutation_pending" not in claim


@pytest.mark.asyncio
async def test_a_validation_refusal_does_not_freeze_either(monkeypatch: pytest.MonkeyPatch) -> None:
    """42210000 on a submission is a rejected request, not an in-flight write."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _HELD])

    error = await _submit(_SubmitProvider(_api_error(code=42210000, message="qty must be > 0", status_code=422)))

    assert error.status_code == 422
    assert "frozen_reason" not in _live_claim(redis, guard)


@pytest.mark.asyncio
async def test_a_broker_5xx_on_submission_still_freezes(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 5xx leaves the write possibly in flight — unchanged behaviour."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _HELD])

    await _submit(_SubmitProvider(_api_error(code=50010000, message="internal server error", status_code=500)))

    assert _live_claim(redis, guard)["frozen_reason"] == "broker_mutation_500"


@pytest.mark.asyncio
async def test_an_unknown_exception_on_submission_still_freezes(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unclassified transport failure proves nothing — unchanged behaviour."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _HELD])

    await _submit(_SubmitProvider(ConnectionError("connection reset by peer")))

    assert _live_claim(redis, guard)["frozen_reason"] == "broker_mutation_502"


@pytest.mark.asyncio
async def test_a_4xx_with_an_unreadable_error_body_still_freezes(monkeypatch: pytest.MonkeyPatch) -> None:
    """The status alone is not the proof — the structured refusal is.

    A 4xx whose body the gateway cannot read could have come from anywhere in
    front of Alpaca, so it says nothing about what the broker did with the
    order.
    """
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _HELD])

    error = await _submit(_SubmitProvider(_api_error(message="<html>forbidden</html>", status_code=403)))

    assert error.status_code == 403
    assert _live_claim(redis, guard)["frozen_reason"] == "broker_mutation_unclassified"


@pytest.mark.asyncio
async def test_the_next_submission_is_authorized_after_a_refusal(monkeypatch: pytest.MonkeyPatch) -> None:
    """The outcome that matters: the symbol is still tradable afterwards."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_FLAT, _FLAT])
    await _submit(_SubmitProvider(_insufficient_qty_error()))

    provider = _SubmitProvider()
    result = await trading.create_order(
        symbol=_SYMBOL,
        side="buy",
        qty=1,
        client=cast(Any, SimpleNamespace(id=_CLIENT_ID)),
        registry=cast(ProviderRegistry, _FakeRegistry(provider)),
    )

    assert result["success"] is True
    assert provider.create_calls == [_SYMBOL]
    assert "frozen_reason" not in _live_claim(redis, guard)


@pytest.mark.asyncio
async def test_a_close_is_authorized_after_a_refused_submission(monkeypatch: pytest.MonkeyPatch) -> None:
    """The risk-reducing exit is the write that must never be locked out."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _HELD])
    await _submit(_SubmitProvider(_insufficient_qty_error()))

    provider = _SubmitProvider()
    result = await trading.close_position(
        symbol=_SYMBOL,
        qty=None,
        percentage=None,
        client=cast(Any, SimpleNamespace(id=_CLIENT_ID)),
        registry=cast(ProviderRegistry, _FakeRegistry(provider)),
    )

    assert result["success"] is True
    assert provider.close_calls == [_SYMBOL]
    assert "frozen_reason" not in _live_claim(redis, guard)


@pytest.mark.asyncio
async def test_a_marker_that_cannot_be_cleared_is_reported_not_papered_over(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stuck marker locks the symbol as surely as a freeze would."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_FLAT, _FLAT])

    async def _refuse(**_kwargs: Any) -> None:
        raise OwnershipConflict("mutation_completion_token_lost:AAPL")

    monkeypatch.setattr(guard, "complete_mutation", _refuse)
    error = await _submit(_SubmitProvider(_insufficient_qty_error()))

    assert error.status_code == 409
    assert error.detail["code"] == "GW-E4301"
    assert "insufficient qty" in error.detail["broker_error"]
    assert "thaw-claim" in error.detail["recovery_hint"]
    # The marker is left for the operator, and a claim carrying one is never
    # released out from under them.
    assert _live_claim(redis, guard)["mutation_pending"].startswith("create_order:")


@pytest.mark.asyncio
async def test_a_marker_left_stuck_by_an_unavailable_store_returns_503_without_freezing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 5xx must not trigger the route-level freeze — it runs unfenced."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_FLAT, _FLAT])

    async def _unavailable(**_kwargs: Any) -> None:
        raise OwnershipStoreUnavailable("redis_mutation_complete_failed:AAPL")

    monkeypatch.setattr(guard, "complete_mutation", _unavailable)
    error = await _submit(_SubmitProvider(_insufficient_qty_error()))

    assert error.status_code == 503
    # The "order may have placed despite the 5xx" retry contract must not be
    # attached to a submission the broker proved it refused.
    assert "retry_hint" not in error.detail
    assert "frozen_reason" not in _live_claim(redis, guard)


@pytest.mark.asyncio
async def test_a_failed_post_refusal_reconciliation_keeps_the_claim_without_freezing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unable to prove the symbol is flat, the claim simply stays.

    It blocks nothing: the next submission reconciles the same state and
    releases it. Freezing instead would need an operator.
    """
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_FLAT, OwnershipConflict("invalid_broker_reconciliation_shape:AAPL")])

    error = await _submit(_SubmitProvider(_insufficient_qty_error()))

    assert error.status_code == 403
    claim = _live_claim(redis, guard)
    assert "frozen_reason" not in claim
    assert "mutation_pending" not in claim


@pytest.mark.asyncio
async def test_a_lost_fence_stops_the_release(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without the lease, another client may already be mutating the symbol."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_FLAT, _FLAT])
    renewals: list[str] = []
    real_renew = guard.renew_fence

    async def _renew(symbol: str, token: str) -> None:
        renewals.append(token)
        if len(renewals) > 1:
            raise OwnershipConflict(f"gateway_fence_lost:{symbol}")
        await real_renew(symbol, token)

    monkeypatch.setattr(guard, "renew_fence", _renew)
    error = await _submit(_SubmitProvider(_insufficient_qty_error()))

    assert error.status_code == 403
    claim = _live_claim(redis, guard)
    assert "frozen_reason" not in claim
    assert "mutation_pending" not in claim


@pytest.mark.asyncio
async def test_a_fence_that_expires_mid_refusal_cannot_release_the_next_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The release has to be atomic with the lease, not merely preceded by it.

    The fence carries a TTL, so it can lapse after the renewal and before the
    release — the two broker reads in between are the window. Another request
    for the SAME client can then take a fresh fence, claim the symbol and put
    an order on it, and the first request's release would be acting on a
    reconciliation that is no longer true. An owner check cannot catch that:
    the owner is identical. The fence token is what distinguishes them.
    """
    guard, redis = _seeded_guard(monkeypatch)
    reconciles: list[int] = []

    async def _state(*_args: Any, **_kwargs: Any) -> BrokerSymbolState:
        reconciles.append(len(reconciles))
        if len(reconciles) == 2:
            # The lease lapsed during this read and a concurrent request for
            # the same client took the symbol over: new fence, new claim.
            redis.values[guard.fence_key(_SYMBOL)] = "fence-token-of-the-next-request"
            redis.values[guard.claim_key(_SYMBOL)] = json.dumps(
                {"owner": _CLIENT_ID, "claimed_at": "2026-08-18T13:31:00+00:00"},
                separators=(",", ":"),
                sort_keys=True,
            )
        return _FLAT

    monkeypatch.setattr(trading, "_reconcile_broker_symbol_state", _state)
    error = await _submit(_SubmitProvider(_insufficient_qty_error()))

    assert error.status_code == 403
    # The successor's claim survives: the stale request holds no lease on it.
    assert _live_claim(redis, guard)["claimed_at"] == "2026-08-18T13:31:00+00:00"


@pytest.mark.asyncio
async def test_another_clients_claim_is_never_released_by_a_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refusal only ever clears up after its own caller."""
    guard, redis = _seeded_guard(monkeypatch, owner=_OTHER_CLIENT_ID)
    _stub_broker_states(monkeypatch, [_FLAT, _FLAT])

    async def _release_nothing(**_kwargs: Any) -> None:
        return None

    # The submission is rejected before any write: the symbol belongs to
    # someone else, so the refusal path is never even reached.
    monkeypatch.setattr(guard, "_release_if_clear", _release_nothing)
    error = await _submit(_SubmitProvider(_insufficient_qty_error()))

    assert error.status_code == 409
    assert _live_claim(redis, guard)["owner"] == _OTHER_CLIENT_ID


# ---------------------------------------------------------------------------
# DELETE /positions/{symbol}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_refused_close_does_not_freeze_the_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    """``close_position`` submits an order too, and carried the same catch-all."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _HELD])

    error = await _close(_SubmitProvider(_insufficient_qty_error()))

    assert error.status_code == 403
    claim = _live_claim(redis, guard)
    assert "frozen_reason" not in claim
    assert "mutation_pending" not in claim


@pytest.mark.asyncio
async def test_a_refused_close_releases_a_claim_the_broker_has_nothing_behind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The position closed under the retry: the claim has nothing left to hold."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _FLAT])

    await _close(_SubmitProvider(_insufficient_qty_error()))

    assert _stored_claim(redis, guard) is None


class _RealAdapter(AlpacaTradingMixin):
    """The production provider with only its Alpaca SDK client replaced.

    The route never sees the broker's own ``APIError`` for a vanished
    position — this adapter converts it — so the conversion has to be in the
    test or the route path under test is fiction.
    """

    def __init__(self, error: BaseException) -> None:
        def _raise(*_args: Any, **_kwargs: Any) -> Any:
            raise error

        self._trading_client = cast(Any, SimpleNamespace(close_position=_raise))


def test_the_adapter_converts_a_vanished_position_into_a_sub_500_http_error() -> None:
    """Pins the coupling the route's marker handling depends on."""
    with pytest.raises(HTTPException) as exc:
        _RealAdapter(_position_not_found_error()).close_position(_SYMBOL)

    assert exc.value.status_code == 404
    assert exc.value.detail["code"] == "POSITION_NOT_FOUND"


@pytest.mark.asyncio
async def test_a_close_on_a_vanished_position_does_not_strand_the_mutation_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half of the incident, and the one that never freezes.

    A duplicate close of a position that already closed is answered
    POSITION_NOT_FOUND. The route correctly declines to freeze it, but the
    pending-mutation marker used to survive anyway — which rejects every later
    write on the symbol with the same 409 a freeze would, and needs the same
    operator command to clear.
    """
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _FLAT])

    with pytest.raises(HTTPException) as exc:
        await trading.close_position(
            symbol=_SYMBOL,
            qty=None,
            percentage=None,
            client=cast(Any, SimpleNamespace(id=_CLIENT_ID)),
            registry=cast(ProviderRegistry, _FakeRegistry(_RealAdapter(_position_not_found_error()))),
        )

    assert exc.value.status_code == 404
    assert exc.value.detail["code"] == "POSITION_NOT_FOUND"
    # Nothing is held and nothing rests: the claim goes with the position.
    assert _stored_claim(redis, guard) is None


@pytest.mark.asyncio
async def test_a_vanished_position_keeps_a_claim_that_still_has_an_order_behind_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The marker still clears, but the claim stays while an order rests."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _RESTING])

    with pytest.raises(HTTPException):
        await trading.close_position(
            symbol=_SYMBOL,
            qty=None,
            percentage=None,
            client=cast(Any, SimpleNamespace(id=_CLIENT_ID)),
            registry=cast(ProviderRegistry, _FakeRegistry(_RealAdapter(_position_not_found_error()))),
        )

    claim = _live_claim(redis, guard)
    assert "mutation_pending" not in claim
    assert "frozen_reason" not in claim


@pytest.mark.asyncio
async def test_a_close_with_a_broker_5xx_still_freezes(monkeypatch: pytest.MonkeyPatch) -> None:
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _HELD])

    await _close(_SubmitProvider(_api_error(code=50010000, message="internal server error", status_code=500)))

    assert _live_claim(redis, guard)["frozen_reason"] == "broker_mutation_500"


@pytest.mark.asyncio
async def test_a_close_with_an_unknown_exception_still_freezes(monkeypatch: pytest.MonkeyPatch) -> None:
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _HELD])

    await _close(_SubmitProvider(ConnectionError("connection reset by peer")))

    assert _live_claim(redis, guard)["frozen_reason"] == "broker_mutation_502"


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(_insufficient_qty_error(), id="insufficient-qty-403"),
        pytest.param(_api_error(code=40310000, message="wash trade detected", status_code=403), id="wash-403"),
        pytest.param(_api_error(code=42210000, message="qty must be > 0", status_code=422), id="validation-422"),
        pytest.param(_position_not_found_error(), id="not-found-404"),
    ],
)
def test_the_refusal_codes_this_account_produces_are_classified_as_unapplied(exc: BaseException) -> None:
    assert trading._broker_submission_proven_unapplied(exc) is True


@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(_api_error(code=50010000, message="internal server error", status_code=500), id="broker-5xx"),
        pytest.param(_api_error(code=50310000, message="unavailable", status_code=503), id="broker-503"),
        pytest.param(APIError("not-json-garbage", None), id="unparseable-body"),
        pytest.param(_api_error(code=40310000, message="insufficient qty", status_code=None), id="unreadable-status"),
        pytest.param(ConnectionError("connection reset by peer"), id="transport-error"),
        pytest.param(TimeoutError(), id="timeout"),
    ],
)
def test_ambiguous_submission_replies_are_not_classified_as_unapplied(exc: BaseException) -> None:
    assert trading._broker_submission_proven_unapplied(exc) is False


@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(_api_error(code=42910000, message="too many requests", status_code=429), id="rate-limit-429"),
        pytest.param(_api_error(code=40110000, message="access key verification failed", status_code=401), id="auth"),
        pytest.param(_api_error(code=40010000, message="invalid symbol", status_code=400), id="unlisted-400"),
        pytest.param(_api_error(message="<html>not found</html>", status_code=404), id="unreadable-404"),
        pytest.param(_api_error(code=40499999, message="unknown", status_code=404), id="unlisted-404-code"),
    ],
)
def test_an_unlisted_4xx_code_stays_ambiguous(exc: BaseException) -> None:
    """The classifier names the refusals this account has actually produced.

    A 4xx status with any parseable body is not proof on its own: Alpaca
    publishes no universal "rejected before dispatch" guarantee, a 429 may be
    thrown by a proxy in front of the broker, and clearing ownership on a
    reply nobody has characterised is how a symbol ends up written by two
    clients. A bare 404 is the same story — the cancel path accepts one on the
    status alone, but a cancel names an order the broker could not find, while
    a submission names nothing that was ever supposed to exist. Everything
    unlisted keeps the pre-existing freeze.
    """
    assert trading._broker_submission_proven_unapplied(exc) is False


@pytest.mark.asyncio
async def test_an_uncharacterized_404_on_a_submission_still_freezes(monkeypatch: pytest.MonkeyPatch) -> None:
    """The classifier's rule has to hold at the route, not just in isolation."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_states(monkeypatch, [_HELD, _HELD])

    await _submit(_SubmitProvider(_api_error(message="<html>not found</html>", status_code=404)))

    assert _live_claim(redis, guard)["frozen_reason"] == "broker_mutation_unclassified"


def test_a_sub_500_http_error_from_the_adapter_is_classified_as_unapplied() -> None:
    """The gateway already treats sub-500 as definitive when deciding to freeze.

    ``_freeze_after_ambiguous_mutation`` skips the freeze below 500, so the
    pending-mutation marker must clear on the same test — otherwise the symbol
    the gateway deliberately did not freeze is silently locked anyway.
    """
    not_found = HTTPException(status_code=404, detail={"code": "POSITION_NOT_FOUND"})
    assert trading._broker_submission_proven_unapplied(not_found) is True
    assert trading._broker_submission_proven_unapplied(HTTPException(status_code=503, detail="upstream")) is False


def test_the_cancel_classifier_is_unchanged_by_the_submission_classifier() -> None:
    """Cancel/replace keep the narrower proof: a 4xx there is not the same claim.

    A cancel refused with "insufficient qty available" says nothing about
    whether an earlier write is still travelling, so it must keep freezing.
    """
    assert trading._broker_write_proven_unapplied(_insufficient_qty_error()) is False
