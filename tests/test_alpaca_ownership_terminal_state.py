"""A broker reply that proves no write landed must not freeze an ownership claim.

Alpaca answers a cancel/replace aimed at an order that already reached a
terminal state with ``422 {"code": 42210000, "message": 'order is already in
"filled" state'}``. That reply is proof the broker refused the mutation, so the
symbol's ownership claim is still exactly as reconciled a moment earlier and the
next mutation — typically the owner's own risk-reducing close — must remain
authorized.

Genuinely ambiguous outcomes (5xx, timeouts, unknown exception types) still
freeze: there the executor thread may complete the write after the Gateway gave
up, so a later reconciliation could race it.
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

_CLIENT_ID = "test-client"
_SYMBOL = "AAPL"


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


def _terminal_state_error(state: str = "filled") -> APIError:
    return _api_error(code=42210000, message=f'order is already in "{state}" state', status_code=422)


class _MemoryRedis:
    """In-memory stand-in that re-implements the guard's Lua scripts in Python.

    Same caveat as the fakes in tests/test_order_ownership.py: the Lua never
    runs here, so this must be kept in sync with it by hand.
    """

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str, *, nx: bool = False, px: int | None = None) -> bool | None:
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    async def sadd(self, _key: str, _value: str) -> int:
        return 1

    async def srem(self, _key: str, _value: str) -> int:
        return 1

    async def eval(self, script: str, _keys: int, key: str, *args: str | int) -> int:
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
            value, _symbol = argv
            if key in self.values:
                return 0
            self.values[key] = str(value)
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


class _OrderProvider:
    """Alpaca trading provider whose order mutations raise a supplied error."""

    def __init__(self, error: BaseException | None = None, order_ids: list[str] | None = None) -> None:
        self._error = error
        self._order_ids = order_ids or ["o-1"]
        self.cancel_calls: list[str] = []
        self.replace_calls: list[str] = []
        self.close_calls: list[str] = []

    def get_order(self, order_id: str) -> dict[str, Any]:
        return {
            "id": order_id,
            "client_order_id": f"c-{_CLIENT_ID}-{order_id}",
            "symbol": _SYMBOL,
        }

    def get_orders(self, **_kwargs: Any) -> list[dict[str, Any]]:
        return [
            {"id": order_id, "client_order_id": f"c-{_CLIENT_ID}-{order_id}", "symbol": _SYMBOL}
            for order_id in self._order_ids
        ]

    def cancel_order(self, order_id: str) -> bool:
        self.cancel_calls.append(order_id)
        if self._error is not None:
            raise self._error
        return True

    def replace_order(self, order_id: str, **_kwargs: Any) -> dict[str, Any]:
        self.replace_calls.append(order_id)
        if self._error is not None:
            raise self._error
        return {"id": order_id}

    def close_position(self, symbol: str, _qty: Any = None, _percentage: Any = None) -> dict[str, Any]:
        self.close_calls.append(symbol)
        return {"id": "close-1", "symbol": symbol}


@pytest.fixture(autouse=True)
def _reset_trading_inflight_sem() -> Any:
    trading._reset_trading_inflight_sem_for_tests()
    yield
    trading._reset_trading_inflight_sem_for_tests()


def _seeded_guard(monkeypatch: pytest.MonkeyPatch) -> tuple[OrderOwnershipGuard, _MemoryRedis]:
    """Install a real guard over an in-memory store, holding this caller's claim."""
    redis = _MemoryRedis()
    guard = OrderOwnershipGuard(redis)
    redis.values[guard.claim_key(_SYMBOL)] = json.dumps(
        {"owner": _CLIENT_ID, "claimed_at": "2026-08-17T14:00:00+00:00"},
        separators=(",", ":"),
        sort_keys=True,
    )
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    return guard, redis


def _stub_broker_state(
    monkeypatch: pytest.MonkeyPatch,
    *,
    has_position: bool = False,
    order_owners: frozenset[str | None] = frozenset({_CLIENT_ID}),
) -> None:
    async def _state(*_args: Any, **_kwargs: Any) -> BrokerSymbolState:
        return BrokerSymbolState(has_position=has_position, order_owners=order_owners)

    monkeypatch.setattr(trading, "_reconcile_broker_symbol_state", _state)


def _claim(redis: _MemoryRedis, guard: OrderOwnershipGuard) -> dict[str, Any]:
    raw = redis.values.get(guard.claim_key(_SYMBOL))
    assert raw is not None, "claim must survive the mutation"
    return cast(dict[str, Any], json.loads(raw))


async def _cancel(provider: _OrderProvider) -> HTTPException:
    with pytest.raises(HTTPException) as exc:
        await trading.cancel_order(
            order_id="o-1",
            client=cast(Any, SimpleNamespace(id=_CLIENT_ID)),
            registry=cast(ProviderRegistry, _FakeRegistry(provider)),
        )
    return exc.value


# ---------------------------------------------------------------------------
# DELETE /orders/{order_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_of_already_filled_order_does_not_freeze_the_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The incident: a stale-entry cancel that lost the race to its own fill.

    Alpaca refused the cancel, so nothing about the symbol changed — freezing
    here locked the owner out of closing its own filled position.
    """
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)
    provider = _OrderProvider(_terminal_state_error())

    error = await _cancel(provider)

    assert error.status_code == 422
    claim = _claim(redis, guard)
    assert "frozen_reason" not in claim
    assert "mutation_pending" not in claim


@pytest.mark.asyncio
async def test_close_is_authorized_after_a_terminal_state_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The outcome that matters: the owner can still exit the filled position."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)
    provider = _OrderProvider(_terminal_state_error())

    await _cancel(provider)

    # The order filled: the open order is gone and the position is now open.
    _stub_broker_state(monkeypatch, has_position=True, order_owners=frozenset())
    result = await trading.close_position(
        symbol=_SYMBOL,
        qty=None,
        percentage=None,
        client=cast(Any, SimpleNamespace(id=_CLIENT_ID)),
        registry=cast(ProviderRegistry, _FakeRegistry(provider)),
    )

    assert result["success"] is True
    assert provider.close_calls == [_SYMBOL]
    assert "frozen_reason" not in _claim(redis, guard)


@pytest.mark.asyncio
async def test_cancel_of_a_missing_order_does_not_freeze_the_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 404 means the broker never reached an order to mutate."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)
    provider = _OrderProvider(_api_error(code=40410000, message="order not found", status_code=404))

    error = await _cancel(provider)

    assert error.status_code == 404
    claim = _claim(redis, guard)
    assert "frozen_reason" not in claim
    assert "mutation_pending" not in claim


@pytest.mark.asyncio
async def test_cancel_rejected_for_another_reason_still_freezes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Alpaca reuses 42210000; only the terminal-state message is proof."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)
    provider = _OrderProvider(_api_error(code=42210000, message="insufficient qty available", status_code=422))

    await _cancel(provider)

    assert _claim(redis, guard)["frozen_reason"] == "broker_mutation_unclassified"


@pytest.mark.asyncio
async def test_cancel_with_a_broker_5xx_still_freezes(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 5xx leaves the write possibly in flight — unchanged behaviour."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)
    provider = _OrderProvider(_api_error(code=50010000, message="internal server error", status_code=500))

    await _cancel(provider)

    assert _claim(redis, guard)["frozen_reason"] == "broker_mutation_500"


@pytest.mark.asyncio
async def test_cancel_with_an_unknown_exception_still_freezes(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unclassified failure proves nothing about the broker — unchanged behaviour."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)
    provider = _OrderProvider(ConnectionError("connection reset by peer"))

    await _cancel(provider)

    # An unclassified transport failure is rewritten to 502 by the provider-call
    # boundary, which re-freezes with the status-specific reason.
    assert _claim(redis, guard)["frozen_reason"] == "broker_mutation_502"


@pytest.mark.asyncio
async def test_a_marker_that_cannot_be_cleared_is_reported_not_papered_over(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stuck marker locks the symbol as surely as a freeze would.

    Answering with the broker's "already filled" 422 would send the caller off
    to act on its fill and straight into a rejected close, so the ownership
    error is raised instead — carrying the broker's message and the command
    that lifts the lock.
    """
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)

    async def _refuse(**_kwargs: Any) -> None:
        raise OwnershipConflict("mutation_completion_token_lost:AAPL")

    monkeypatch.setattr(guard, "complete_mutation", _refuse)
    error = await _cancel(_OrderProvider(_terminal_state_error()))

    assert error.status_code == 409
    assert error.detail["code"] == "GW-E4301"
    assert "already in" in error.detail["broker_error"]
    assert "thaw-claim" in error.detail["recovery_hint"]
    # The marker is left in place for the operator rather than reported as gone.
    assert _claim(redis, guard)["mutation_pending"].startswith("cancel_order:")


@pytest.mark.asyncio
async def test_a_marker_left_stuck_by_an_unavailable_store_returns_503_without_freezing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 5xx must not trigger the route-level freeze.

    That freeze runs after the symbol's fence is released and is itself
    unfenced, so if the store had in fact applied the clear and only lost the
    reply, it would stamp a claim another client may already be mutating under.
    Nothing needs recording either way: the broker applied nothing, and a marker
    that really did survive already blocks the symbol by itself.
    """
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)

    async def _unavailable(**_kwargs: Any) -> None:
        raise OwnershipStoreUnavailable("redis_mutation_complete_failed:AAPL")

    monkeypatch.setattr(guard, "complete_mutation", _unavailable)
    error = await _cancel(_OrderProvider(_terminal_state_error()))

    assert error.status_code == 503
    assert error.detail["code"] == "GW-E5301"
    assert "frozen_reason" not in _claim(redis, guard)


# ---------------------------------------------------------------------------
# PATCH /orders/{order_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replace_marker_failure_does_not_freeze_a_later_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same post-release freeze hazard on the replace path, and the same answer."""
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)

    async def _unavailable(**_kwargs: Any) -> None:
        raise OwnershipStoreUnavailable("redis_mutation_complete_failed:AAPL")

    monkeypatch.setattr(guard, "complete_mutation", _unavailable)
    with pytest.raises(HTTPException) as exc:
        await trading.replace_order(
            order_id="o-1",
            limit_price=1.23,
            client=cast(Any, SimpleNamespace(id=_CLIENT_ID)),
            registry=cast(ProviderRegistry, _FakeRegistry(_OrderProvider(_terminal_state_error()))),
        )

    assert exc.value.status_code == 503
    # The "replacement may have applied despite the 5xx" retry contract must not
    # be attached to an outcome the broker proved it refused.
    assert "retry_hint" not in exc.value.detail
    assert "frozen_reason" not in _claim(redis, guard)


@pytest.mark.asyncio
async def test_replace_of_an_already_filled_order_does_not_freeze_the_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)
    provider = _OrderProvider(_terminal_state_error())

    with pytest.raises(HTTPException) as exc:
        await trading.replace_order(
            order_id="o-1",
            limit_price=1.23,
            client=cast(Any, SimpleNamespace(id=_CLIENT_ID)),
            registry=cast(ProviderRegistry, _FakeRegistry(provider)),
        )

    assert exc.value.status_code == 422
    claim = _claim(redis, guard)
    assert "frozen_reason" not in claim
    assert "mutation_pending" not in claim


@pytest.mark.asyncio
async def test_replace_rejected_for_another_reason_still_freezes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)
    provider = _OrderProvider(_api_error(code=42210000, message="insufficient qty available", status_code=422))

    with pytest.raises(HTTPException):
        await trading.replace_order(
            order_id="o-1",
            limit_price=1.23,
            client=cast(Any, SimpleNamespace(id=_CLIENT_ID)),
            registry=cast(ProviderRegistry, _FakeRegistry(provider)),
        )

    assert _claim(redis, guard)["frozen_reason"] == "broker_mutation_unclassified"


# ---------------------------------------------------------------------------
# DELETE /orders (batch) — parity guard for the path that already behaved
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_cancel_of_an_already_filled_order_does_not_freeze_the_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guard, redis = _seeded_guard(monkeypatch)
    _stub_broker_state(monkeypatch)
    provider = _OrderProvider(_terminal_state_error())

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id=_CLIENT_ID)),
        registry=cast(ProviderRegistry, _FakeRegistry(provider)),
    )

    errors = response["meta"]["errors"]
    assert [error["status_code"] for error in errors] == [422]
    claim = _claim(redis, guard)
    assert "frozen_reason" not in claim
    assert "mutation_pending" not in claim


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("state", ["filled", "canceled", "expired", "rejected", "done_for_day"])
def test_terminal_state_replies_are_classified_as_unapplied(state: str) -> None:
    assert trading._broker_write_proven_unapplied(_terminal_state_error(state)) is True


@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(_api_error(code=42210000, message="insufficient qty available", status_code=422), id="other-422"),
        pytest.param(
            _api_error(code=42210000, message='order is already in "pending_new" state', status_code=422),
            id="non-terminal-state",
        ),
        pytest.param(_api_error(code=40310000, message="wash trade detected", status_code=403), id="wash-403"),
        pytest.param(_api_error(code=50010000, message="internal server error", status_code=500), id="broker-5xx"),
        pytest.param(APIError("not-json-garbage", None), id="unparseable-body"),
        pytest.param(ConnectionError("connection reset by peer"), id="transport-error"),
        pytest.param(TimeoutError(), id="timeout"),
    ],
)
def test_ambiguous_replies_are_not_classified_as_unapplied(exc: BaseException) -> None:
    assert trading._broker_write_proven_unapplied(exc) is False


def test_broker_not_found_is_classified_as_unapplied() -> None:
    assert trading._broker_write_proven_unapplied(_api_error(code=40410000, message="order not found", status_code=404))
