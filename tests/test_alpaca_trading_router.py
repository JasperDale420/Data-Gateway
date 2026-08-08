from __future__ import annotations

import asyncio
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import HTTPException
from starlette.status import HTTP_502_BAD_GATEWAY, HTTP_503_SERVICE_UNAVAILABLE, HTTP_504_GATEWAY_TIMEOUT

from gateway.api.alpaca import trading
from gateway.api.alpaca.common import ALPACA_ROUTER_PREFIX
from gateway.config import Settings
from gateway.core.cache import InMemoryCache
from gateway.core.order_ownership import (
    BrokerSymbolState,
    OrderOwnershipGuard,
    OwnershipConflict,
    OwnershipStoreUnavailable,
)
from gateway.core.registry import ProviderRegistry


class _AllowAllOwnershipGuard:
    async def freeze(self, _symbol: str, _reason: str) -> None:
        return None

    async def acquire_fence(self, _symbol: str) -> str:
        return "test-fence"

    async def renew_fence(self, _symbol: str, _token: str) -> None:
        return None

    async def release_fence(self, _symbol: str, _token: str) -> None:
        return None

    async def authorize_submission(self, **_kwargs: Any) -> None:
        return None

    async def authorize_close(self, **_kwargs: Any) -> None:
        return None

    async def begin_mutation(self, **_kwargs: Any) -> str:
        return "test-mutation"

    async def complete_mutation(self, **_kwargs: Any) -> None:
        return None

    async def verify_reconciliation(self, **_kwargs: Any) -> None:
        return None


@pytest.fixture(autouse=True)
def _reset_trading_inflight_sem(monkeypatch: pytest.MonkeyPatch):
    """Each test gets a fresh event loop — reset the lazy semaphore so it
    binds to the new loop (asyncio.Semaphore in 3.10+ is loop-bound on first
    use) and so per-test settings overrides take effect."""
    trading._reset_trading_inflight_sem_for_tests()
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: _AllowAllOwnershipGuard())

    async def _empty_broker_state(*_args: Any, **_kwargs: Any) -> BrokerSymbolState:
        return BrokerSymbolState(has_position=False, order_owners=frozenset())

    monkeypatch.setattr(trading, "_reconcile_broker_symbol_state", _empty_broker_state)
    yield
    trading._reset_trading_inflight_sem_for_tests()


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


_DEFAULT_TEST_CLIENT_ID = "test-client"


class _MemoryRedis:
    """In-memory stand-in that re-implements the guard's Lua scripts in Python.

    Same caveat as ``_FakeRedis`` in tests/test_order_ownership.py: the Lua
    never runs here, so this fake must be kept in sync with it by hand.
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
            _index_key = extra_keys[0]
            value, _symbol = argv
            if key in self.values:
                return 0
            self.values[key] = str(value)
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
            return 1
        expected = str(argv[0])
        if self.values.get(key) != expected:
            return 0
        del self.values[key]
        return 1


def _owned_coid(client_id: str = _DEFAULT_TEST_CLIENT_ID, suffix: str = "fake") -> str:
    """Build an ownership-prefixed client_order_id for the given test client.

    Matches the gateway's ``c-{client.id}-`` prefix scheme so a
    ``_FakeProvider`` can return orders that pass the ownership filter on
    ``get_orders`` / ``get_order`` and the pre-check on ``replace_order``
    / ``cancel_order``.
    """
    return f"c-{client_id}-{suffix}"


class _FakeProvider:
    """Fake Alpaca trading provider for router-layer tests.

    Returns orders tagged with the default test client's ownership prefix
    so they pass the new per-client filtering / verification gates added
    by BLOCKER 1 fixes. Tests that want to exercise the foreign-prefix /
    no-prefix branches override individual methods on a per-test subclass
    (the established pattern in this file).
    """

    def __init__(self, owner_client_id: str = _DEFAULT_TEST_CLIENT_ID) -> None:
        self.orders_calls: list[dict[str, Any]] = []
        self.cancel_calls: list[str] = []
        self.calendar_calls: list[tuple[date | None, date | None]] = []
        self.assets_calls: list[dict[str, Any]] = []
        self.asset_calls: list[str] = []
        self.get_order_calls: list[str] = []
        self._owner_client_id = owner_client_id

    def get_account(self) -> dict[str, Any]:
        return {"status": "ACTIVE"}

    def create_order(self, **kwargs: Any) -> dict[str, Any]:
        return kwargs

    def get_orders(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.orders_calls.append(kwargs)
        return [
            {"id": "o-1", "client_order_id": _owned_coid(self._owner_client_id, "o-1")},
            {"id": "o-2", "client_order_id": _owned_coid(self._owner_client_id, "o-2")},
        ]

    def get_order(self, order_id: str) -> dict[str, Any]:
        # Default behaviour: pretend every order is owned by the test
        # client so the new ownership pre-check on get_order / cancel /
        # replace passes for existing tests. Override on subclasses to
        # exercise the foreign-prefix branch.
        self.get_order_calls.append(order_id)
        return {
            "id": order_id,
            "client_order_id": _owned_coid(self._owner_client_id, order_id),
            "symbol": "AAPL",
        }

    def cancel_order(self, order_id: str) -> bool:
        self.cancel_calls.append(order_id)
        return True

    def get_calendar(self, start: date | None, end: date | None) -> list[dict[str, Any]]:
        self.calendar_calls.append((start, end))
        return [{"date": "2026-01-02"}]

    def get_assets(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.assets_calls.append(kwargs)
        return [{"symbol": "AAPL"}]

    def get_asset(self, symbol: str) -> dict[str, str]:
        self.asset_calls.append(symbol)
        return {"symbol": symbol}


def _helper_monkeypatch(
    monkeypatch: pytest.MonkeyPatch,
    *,
    route_registry: _FakeRegistry,
) -> None:
    async def _execute_alpaca_call(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is False
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)

    async def _execute_alpaca_cached_call(
        *,
        registry: ProviderRegistry,
        cache: Any,
        cache_key: str,
        ttl: int,
        provider_call: Any,
        route_label: str,
        cache_mode: str = "alpaca",
        block: bool = False,
    ):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is False
        assert ttl > 0
        assert route_label.startswith("alpaca_trading_")
        assert cache_key.startswith("alpaca:trading:")
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(trading, "execute_alpaca_cached_call", _execute_alpaca_cached_call)


@pytest.mark.asyncio
async def test_get_account_uses_shared_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.get_account(
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert response["success"] is True
    assert response["data"]["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_create_order_maps_value_error_to_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ValueErrorProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            raise ValueError("invalid order")

    provider = _ValueErrorProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    assert exc.value.detail == "invalid order"


@pytest.mark.asyncio
async def test_create_order_threads_position_intent_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """position_intent (e.g. sell_to_close) must reach the provider so callers
    can force reduce-only semantics — Alpaca then never converts a close into
    an opening (naked short) position."""
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.create_order(
        symbol="AAPL260529P00315000",
        side="sell",
        qty=10,
        order_type="limit",
        limit_price=1.0,
        position_intent="sell_to_close",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert response["success"] is True
    assert response["data"]["position_intent"] == "sell_to_close"


@pytest.mark.asyncio
async def test_get_orders_splits_symbols_and_sets_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.get_orders(
        status="open",
        limit=50,
        direction="desc",
        symbols="AAPL,MSFT",
        nested=True,
        side=None,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.orders_calls[0]["symbols"] == ["AAPL", "MSFT"]
    assert response["meta"]["count"] == 2


@pytest.mark.asyncio
async def test_get_orders_threads_after_until_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """O2b: after/until date filters are passed through to the provider so Orion
    can page the submitted_at window for same-day fill coverage."""
    from datetime import UTC, datetime

    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    after = datetime(2026, 6, 10, 0, 0, tzinfo=UTC)
    until = datetime(2026, 6, 11, 0, 0, tzinfo=UTC)
    await trading.get_orders(
        status="all",
        limit=500,
        direction="asc",
        symbols=None,
        nested=True,
        side=None,
        after=after,
        until=until,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    call = provider.orders_calls[0]
    assert call["after"] == after
    assert call["until"] == until


@pytest.mark.asyncio
async def test_get_orders_after_until_passed_as_none_unaffects_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """O2b additive: after/until=None threads None through (existing callers unaffected)."""
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    await trading.get_orders(
        status="open",
        limit=50,
        direction="desc",
        symbols=None,
        nested=True,
        side=None,
        after=None,
        until=None,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    call = provider.orders_calls[0]
    assert call["after"] is None
    assert call["until"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_field", ["after", "until"])
async def test_get_orders_rejects_naive_after_until(
    monkeypatch: pytest.MonkeyPatch,
    bad_field: str,
) -> None:
    """G8: a naive (tz-unaware) after/until is rejected with 400 GW-E4007 and
    never reaches the provider — a naive value serializes as UTC and silently
    shifts the submitted_at window, missing fills."""
    from datetime import UTC, datetime

    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    naive = datetime(2026, 6, 10, 0, 0)  # noqa: DTZ001 — intentionally naive
    aware = datetime(2026, 6, 11, 0, 0, tzinfo=UTC)
    after = naive if bad_field == "after" else aware
    until = naive if bad_field == "until" else aware

    with pytest.raises(HTTPException) as exc:
        await trading.get_orders(
            status="all",
            limit=500,
            direction="asc",
            symbols=None,
            nested=True,
            side=None,
            after=after,
            until=until,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "GW-E4007"
    assert bad_field in exc.value.detail["message"]
    assert provider.orders_calls == []


@pytest.mark.asyncio
async def test_get_orders_rejects_after_later_than_until(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """G8: after > until is a degenerate window — reject with 400 GW-E4007 and
    don't call the provider."""
    from datetime import UTC, datetime

    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    after = datetime(2026, 6, 11, 0, 0, tzinfo=UTC)
    until = datetime(2026, 6, 10, 0, 0, tzinfo=UTC)

    with pytest.raises(HTTPException) as exc:
        await trading.get_orders(
            status="all",
            limit=500,
            direction="asc",
            symbols=None,
            nested=True,
            side=None,
            after=after,
            until=until,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "GW-E4007"
    assert provider.orders_calls == []


@pytest.mark.asyncio
async def test_get_orders_valid_tz_aware_pair_passes_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """G8: a valid tz-aware after<=until pair passes validation and threads
    through to the provider's GetOrdersRequest unchanged."""
    from datetime import UTC, datetime

    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    after = datetime(2026, 6, 10, 0, 0, tzinfo=UTC)
    until = datetime(2026, 6, 11, 0, 0, tzinfo=UTC)
    await trading.get_orders(
        status="all",
        limit=500,
        direction="asc",
        symbols=None,
        nested=True,
        side=None,
        after=after,
        until=until,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    call = provider.orders_calls[0]
    assert call["after"] == after
    assert call["until"] == until


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "direction", "side", "bad_field"),
    [
        ("filled", "desc", None, "status"),
        ("open", "descending", None, "direction"),
        ("open", "desc", "long", "side"),
    ],
)
async def test_get_orders_rejects_invalid_enum_params(
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    direction: str,
    side: str | None,
    bad_field: str,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.get_orders(
            status=status,
            limit=50,
            direction=direction,
            symbols=None,
            nested=True,
            side=side,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    assert bad_field in str(exc.value.detail).lower()
    assert provider.orders_calls == []


@pytest.mark.asyncio
async def test_get_orders_times_out_stuck_trading_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SlowProvider(_FakeProvider):
        def get_orders(self, **kwargs: Any) -> list[dict[str, Any]]:
            self.orders_calls.append(kwargs)
            import time

            time.sleep(0.6)
            return [{"id": "o-1"}]

    provider = _SlowProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        assert registry is cast(ProviderRegistry, route_registry)
        return await provider_call(registry.get("alpaca"))

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=0.5,
        ),
    )

    with pytest.raises(HTTPException) as exc:
        await trading.get_orders(
            status="open",
            limit=50,
            direction="desc",
            symbols="AAPL",
            nested=True,
            side=None,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == HTTP_504_GATEWAY_TIMEOUT


@pytest.mark.asyncio
async def test_get_orders_raises_502_when_provider_response_is_not_a_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed (non-list) broker response must fail loudly, not be
    silently treated as zero orders — a zero-order response here is
    indistinguishable from "you genuinely have no open orders", which is
    corrupted-data-reported-as-truth on the read path callers reconcile
    against."""

    class _NonListProvider(_FakeProvider):
        def get_orders(self, **kwargs: Any) -> Any:
            self.orders_calls.append(kwargs)
            return {"unexpected": "shape"}

    provider = _NonListProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.get_orders(
            status="open",
            limit=50,
            direction="desc",
            symbols=None,
            nested=True,
            side=None,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == HTTP_502_BAD_GATEWAY
    assert exc.value.detail["code"] == "GW-E5009"


@pytest.mark.asyncio
async def test_cancel_order_returns_success_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.cancel_order(
        order_id="ord-1",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.cancel_calls == ["ord-1"]
    assert response["success"] is True
    assert response["data"]["cancelled"] is True


@pytest.mark.asyncio
async def test_get_calendar_threads_dates_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    start = date(2026, 1, 1)
    end = date(2026, 1, 31)

    response = await trading.get_calendar(
        start=start,
        end=end,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.calendar_calls == [(start, end)]
    assert response["meta"]["count"] == 1


@pytest.mark.asyncio
async def test_get_assets_uses_cached_helper_key_and_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    cache = InMemoryCache(max_size=32, default_ttl=60)
    observed: dict[str, Any] = {}
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    async def _cached_call(**kwargs: Any):
        observed["key"] = kwargs["cache_key"]
        observed["route_label"] = kwargs["route_label"]
        provider_obj = kwargs["registry"].get("alpaca")
        return await kwargs["provider_call"](provider_obj)

    monkeypatch.setattr(trading, "execute_alpaca_cached_call", _cached_call)

    response = await trading.get_assets(
        status="active",
        asset_class="us_equity",
        exchange="NYSE",
        client=cast(Any, SimpleNamespace(id="test-client")),
        cache=cache,
        registry=cast(ProviderRegistry, route_registry),
    )

    assert observed["key"] == "alpaca:trading:assets:active:us_equity:nyse"
    assert observed["route_label"] == "alpaca_trading_assets"
    assert provider.assets_calls == [{"status": "active", "asset_class": "us_equity", "exchange": "NYSE"}]
    assert response["meta"]["count"] == 1


@pytest.mark.asyncio
async def test_get_asset_uses_cached_helper_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    cache = InMemoryCache(max_size=32, default_ttl=60)
    observed: dict[str, Any] = {}
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    async def _cached_call(**kwargs: Any):
        observed["key"] = kwargs["cache_key"]
        observed["route_label"] = kwargs["route_label"]
        provider_obj = kwargs["registry"].get("alpaca")
        return await kwargs["provider_call"](provider_obj)

    monkeypatch.setattr(trading, "execute_alpaca_cached_call", _cached_call)

    response = await trading.get_asset(
        symbol="aapl",
        client=cast(Any, SimpleNamespace(id="test-client")),
        cache=cache,
        registry=cast(ProviderRegistry, route_registry),
    )

    assert observed["key"] == "alpaca:trading:asset:AAPL"
    assert observed["route_label"] == "alpaca_trading_asset"
    assert provider.asset_calls == ["AAPL"]
    assert response["data"]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_create_order_passes_bracket_params_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bracket order params (order_class, take_profit, stop_loss) pass through to the provider."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "bracket-order-1"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.create_order(
        symbol="AAPL",
        side="buy",
        qty=10,
        order_type="limit",
        limit_price=150.0,
        order_class="bracket",
        take_profit_limit_price=160.0,
        stop_loss_stop_price=145.0,
        stop_loss_limit_price=144.0,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert response["success"] is True
    assert captured["order_class"] == "bracket"
    assert captured["take_profit_limit_price"] == 160.0
    assert captured["stop_loss_stop_price"] == 145.0
    assert captured["stop_loss_limit_price"] == 144.0


# ---------------------------------------------------------------------------
# Backpressure tests — ensure slow Alpaca calls don't pile up in the executor
# queue and starve the event loop (which causes WebSocket keepalive-ping
# timeouts for connected streaming clients).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_trading_provider_call_fast_fails_when_inflight_cap_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the in-flight cap is fully reserved, new calls get 503 immediately."""
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(alpaca_trading_max_inflight=2, alpaca_trading_call_timeout_seconds=5.0),
    )

    # Pre-saturate the semaphore to simulate 2 in-flight calls.
    sem = trading._get_trading_inflight_sem()
    await sem.acquire()
    await sem.acquire()
    assert sem.locked()

    with pytest.raises(HTTPException) as exc:
        await trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=lambda _p: "unused",
            operation="get_account",
        )
    assert exc.value.status_code == HTTP_503_SERVICE_UNAVAILABLE
    assert exc.value.detail["code"] == "GW-E5005"


@pytest.mark.asyncio
async def test_run_trading_provider_call_releases_permit_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful call must release its in-flight permit so the next call can proceed."""
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(alpaca_trading_max_inflight=2, alpaca_trading_call_timeout_seconds=5.0),
    )

    result = await trading._run_trading_provider_call(
        provider=SimpleNamespace(),
        provider_fn=lambda _p: "ok",
        operation="get_account",
    )
    assert result == "ok"

    # Semaphore should be fully released — running a second call succeeds.
    result2 = await trading._run_trading_provider_call(
        provider=SimpleNamespace(),
        provider_fn=lambda _p: "ok-again",
        operation="get_account",
    )
    assert result2 == "ok-again"

    sem = trading._get_trading_inflight_sem()
    assert not sem.locked()


@pytest.mark.asyncio
async def test_run_trading_provider_call_releases_permit_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 504 timeout must still release the permit so backpressure clears itself."""
    import time

    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(alpaca_trading_max_inflight=2, alpaca_trading_call_timeout_seconds=0.5),
    )

    def _slow(_p: Any) -> Any:
        time.sleep(1.0)
        return "should-not-return"

    with pytest.raises(HTTPException) as exc:
        await trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=_slow,
            operation="get_orders",
        )
    assert exc.value.status_code == HTTP_504_GATEWAY_TIMEOUT

    # The context manager releases the permit synchronously when the 504 is
    # raised, so the semaphore should be fully free immediately.
    sem = trading._get_trading_inflight_sem()
    assert not sem.locked(), "permit must be released after 504 timeout"


@pytest.mark.asyncio
async def test_run_trading_provider_call_timeout_log_includes_request_context(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging
    import time

    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(alpaca_trading_max_inflight=2, alpaca_trading_call_timeout_seconds=0.5),
    )
    trading._reset_trading_inflight_sem_for_tests()

    def _slow(_p: Any) -> Any:
        time.sleep(1.0)
        return "late"

    with caplog.at_level(logging.ERROR, logger="data-gateway"), pytest.raises(HTTPException):
        await trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=_slow,
            operation="get_orders",
            log_context={
                "client_id": "orion",
                "method": "GET",
                "path": "/api/v1/alpaca/orders",
            },
        )

    rendered = "\n".join(record.getMessage() for record in caplog.records)
    assert "alpaca_trading_call_timeout" in rendered
    assert "client_id" in rendered and "orion" in rendered
    assert "path" in rendered and "/api/v1/alpaca/orders" in rendered
    assert "method" in rendered and "GET" in rendered


@pytest.mark.asyncio
async def test_run_trading_provider_call_releases_permit_on_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provider-raised exceptions must still release the permit."""
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(alpaca_trading_max_inflight=2, alpaca_trading_call_timeout_seconds=5.0),
    )

    def _raiser(_p: Any) -> Any:
        raise RuntimeError("simulated provider error")

    with pytest.raises(RuntimeError, match="simulated provider error"):
        await trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=_raiser,
            operation="get_account",
        )

    sem = trading._get_trading_inflight_sem()
    assert not sem.locked(), "permit must be released after provider exception"


# ---------------------------------------------------------------------------
# create_order idempotency — gateway-generated client_order_id must flow
# through to the provider and surface in both success.meta and the 504
# timeout detail so callers can safely retry/verify after a 5xx.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_order_auto_generates_client_order_id_when_caller_omits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the caller doesn't supply a client_order_id, the gateway must
    generate one (prefixed ``dg-``), pass it to the provider, AND return
    it in meta — otherwise the caller has no key to retry idempotently
    after a 504 timeout."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "order-1", "status": "accepted"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.create_order(
        symbol="AAPL",
        side="buy",
        qty=10,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    # Provider saw a generated key prefixed with the caller's ownership tag.
    assert captured["client_order_id"] is not None
    assert captured["client_order_id"].startswith("c-test-client-dg-")
    assert len(captured["client_order_id"]) == len("c-test-client-dg-") + 32  # uuid4 hex
    # Caller sees the same FULL prefixed key in meta so they know what to
    # retry with (and so retry tooling that round-trips the meta value
    # back through GET /orders:by_client_order_id passes the ownership
    # check).
    assert response["meta"]["client_order_id"] == captured["client_order_id"]
    assert response["meta"]["client_order_id_source"] == "gateway"


@pytest.mark.asyncio
async def test_create_order_freezes_manual_broker_state_before_provider_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: OrderOwnershipGuard(_MemoryRedis()))

    async def _manual_broker_state(*_args: Any, **_kwargs: Any) -> BrokerSymbolState:
        return BrokerSymbolState(has_position=False, order_owners=frozenset({None}))

    monkeypatch.setattr(trading, "_reconcile_broker_symbol_state", _manual_broker_state)

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            qty=1,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "GW-E4301"


@pytest.mark.asyncio
async def test_post_write_reconciliation_conflict_freezes_before_releasing_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    redis = _MemoryRedis()

    class _PostWriteConflictGuard(OrderOwnershipGuard):
        async def verify_reconciliation(self, **_kwargs: Any) -> None:
            raise OwnershipConflict("post_write_drift")

    ownership_guard = _PostWriteConflictGuard(redis)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: ownership_guard)

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            qty=1,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 409
    assert "may have completed" in exc.value.detail["message"]
    claim = json.loads(redis.values[ownership_guard.claim_key("AAPL")])
    assert claim["frozen_reason"] == "post_write_reconciliation_post_write_drift"


@pytest.mark.asyncio
async def test_reconciliation_reads_open_orders_before_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ordering is a safety property, not a style choice.

    A fill moves a symbol out of the open-order list and into the position
    list. Reading positions first leaves a window where a filling order is in
    neither result, which would present a live owner's symbol as flat and let
    another client take the claim and flatten it.
    """
    calls: list[str] = []

    class _OrderTrackingProvider(_FakeProvider):
        def get_orders(self, **_kwargs: Any) -> list[dict[str, Any]]:
            calls.append("orders")
            return []

        def get_positions(self) -> list[dict[str, Any]]:
            calls.append("positions")
            return []

    # The autouse fixture stubs out _reconcile_broker_symbol_state; drop that
    # so this test exercises the real reconciliation.
    monkeypatch.undo()

    state = await trading._reconcile_broker_symbol_state(_OrderTrackingProvider(), "AAPL")

    assert calls == ["orders", "positions"]
    assert state.has_position is False
    assert state.order_owners == frozenset()


@pytest.mark.asyncio
async def test_create_order_submits_the_canonical_occ_contract_symbol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "order-1"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: OrderOwnershipGuard(_MemoryRedis()))

    response = await trading.create_order(
        symbol="aapl 2026-01-16 $200 call",
        side="buy",
        qty=1,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert captured["symbol"] == "AAPL260116C00200000"
    assert response["meta"]["symbol"] == "AAPL260116C00200000"


@pytest.mark.asyncio
async def test_create_order_passes_order_context_to_failure_logger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    captured: dict[str, Any] = {}

    async def _execute_alpaca_call(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        assert registry is cast(ProviderRegistry, route_registry)
        captured["log_context"] = log_context
        return await provider_call(registry.get("alpaca"))

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)

    await trading.create_order(
        symbol="spy",
        side="BUY",
        qty=10,
        order_type="Market",
        time_in_force="Day",
        client=cast(Any, SimpleNamespace(id="orion")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert captured["log_context"] == {
        "client_id": "orion",
        "symbol": "SPY",
        "side": "buy",
        "order_type": "market",
        "time_in_force": "day",
        "order_class": None,
        "qty_provided": True,
        "notional_provided": False,
        "client_order_id_source": "gateway",
    }


@pytest.mark.asyncio
async def test_create_order_preserves_caller_supplied_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the caller supplies a client_order_id, the gateway must NOT
    overwrite it (otherwise the caller's own idempotency contract breaks)."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "order-1"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.create_order(
        symbol="AAPL",
        side="buy",
        qty=10,
        client_order_id="caller-key-abc-123",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    # The gateway transparently prefixes the caller-supplied key with
    # ``c-{client.id}-`` so the order is unambiguously attributed to this
    # caller on later lookups against the SHARED Alpaca account.
    assert captured["client_order_id"] == "c-test-client-caller-key-abc-123"
    assert response["meta"]["client_order_id"] == "c-test-client-caller-key-abc-123"
    assert response["meta"]["client_order_id_source"] == "caller"


@pytest.mark.asyncio
async def test_create_order_504_timeout_includes_client_order_id_in_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CRITICAL retry contract: when create_order times out, the caller
    needs the client_order_id in the error body so they can retry safely.
    Without it, the caller has no idempotency key and a naive retry could
    double-place the order at Alpaca."""

    class _SlowProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            import time

            time.sleep(0.6)
            return {"id": "should-not-return"}

    provider = _SlowProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        return await provider_call(registry.get("alpaca"))

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=0.5,
        ),
    )

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            qty=10,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == HTTP_504_GATEWAY_TIMEOUT
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["code"] == "GW-E5004"
    # The idempotency key MUST be in the 504 body. This is the load-bearing
    # piece that prevents double-place on retry — DO NOT REGRESS.
    assert "client_order_id" in detail
    assert detail["client_order_id"].startswith("c-test-client-dg-")
    assert detail["client_order_id_source"] == "gateway"
    assert detail["retry_with"] == "client_order_id"
    assert "Alpaca natively dedupes" in detail["retry_hint"]


@pytest.mark.asyncio
async def test_create_order_504_timeout_preserves_caller_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the caller supplied a key and the call times out, the 504
    surfaces THE CALLER'S key (not a fresh gateway key) so the caller's
    own retry path is wired correctly."""

    class _SlowProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            import time

            time.sleep(0.6)
            return {"id": "should-not-return"}

    provider = _SlowProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        return await provider_call(registry.get("alpaca"))

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=0.5,
        ),
    )

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            qty=10,
            client_order_id="caller-retry-key-xyz",
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == HTTP_504_GATEWAY_TIMEOUT
    detail = exc.value.detail
    assert isinstance(detail, dict)
    # 504 carries the FULL prefixed key so callers' retry tooling can pass
    # it straight back to GET /orders:by_client_order_id without
    # re-deriving the prefix.
    assert detail["client_order_id"] == "c-test-client-caller-retry-key-xyz"
    assert detail["client_order_id_source"] == "caller"


@pytest.mark.asyncio
async def test_create_order_503_backpressure_includes_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 503 backpressure path runs BEFORE the call reaches Alpaca, so
    the order definitely didn't place. But the caller may be reconciling
    a logical order and needs the idempotency key surfaced for retry
    safety. (If they get a 503 and then a 504 on the retry, the 504's
    key MUST match — verified by another test.)"""
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(alpaca_trading_max_inflight=2, alpaca_trading_call_timeout_seconds=5.0),
    )

    # Pre-saturate the semaphore.
    sem = trading._get_trading_inflight_sem()
    await sem.acquire()
    await sem.acquire()
    assert sem.locked()

    with pytest.raises(HTTPException) as exc:
        await trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=lambda _p: "unused",
            operation="create_order",
            idempotency_context={
                "client_order_id": "test-key-1",
                "client_order_id_source": "gateway",
                "retry_with": "client_order_id",
                "retry_hint": "stub",
            },
        )

    assert exc.value.status_code == HTTP_503_SERVICE_UNAVAILABLE
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["client_order_id"] == "test-key-1"


@pytest.mark.asyncio
async def test_create_order_value_error_path_does_not_surface_504_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ValueError from the provider (invalid order params) must still
    raise 400 cleanly — the idempotency context plumbing must not break
    the 400 path."""

    class _ValueErrorProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            raise ValueError("bad input")

    provider = _ValueErrorProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    assert exc.value.detail == "bad input"


# ---------------------------------------------------------------------------
# close_position idempotency — Alpaca's ClosePositionRequest does not accept
# client_order_id, so a timeout must freeze and direct the caller to broker
# reconciliation instead of treating a position GET as proof that retry is safe.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_position_504_timeout_requires_broker_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Critical retry contract: when close_position times out, the caller
    needs broker reconciliation instructions. A position GET alone cannot
    distinguish a pending or partial close, so retry is deliberately frozen."""

    class _SlowProvider:
        def close_position(self, symbol: str, qty: Any = None, percentage: Any = None) -> dict[str, Any]:
            import time

            time.sleep(0.6)
            return {"id": "should-not-return"}

    provider = _SlowProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        return await provider_call(registry.get("alpaca"))

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=0.5,
        ),
    )

    with pytest.raises(HTTPException) as exc:
        await trading.close_position(
            symbol="aapl",  # intentionally lowercase to test normalization
            qty=None,
            percentage=None,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == HTTP_504_GATEWAY_TIMEOUT
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["code"] == "GW-E5004"
    # Symbol is upper-cased so the caller's follow-up GET hits the same key.
    assert detail["symbol"] == "AAPL"
    assert detail["retry_with"] == "broker_reconciliation"
    # The retry hint must point at the ACTUAL mounted prefix
    # (``/api/v1/alpaca``) — see test_retry_hint_uses_actual_router_prefix
    # for the static-vs-dynamic guard against drift.
    assert f"GET {ALPACA_ROUTER_PREFIX}/positions/AAPL" in detail["retry_hint"]
    assert f"GET {ALPACA_ROUTER_PREFIX}/orders?status=open&symbols=AAPL" in detail["retry_hint"]
    assert "do not retry" in detail["retry_hint"]


@pytest.mark.asyncio
async def test_close_position_success_meta_includes_symbol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On success, the upper-cased symbol is surfaced in meta so the
    caller's reconciliation logic has the canonical key without needing
    to re-derive it."""
    captured: dict[str, Any] = {}

    class _OkProvider:
        def close_position(self, symbol: str, qty: Any = None, percentage: Any = None) -> dict[str, Any]:
            captured["symbol"] = symbol
            return {"id": "close-1", "status": "accepted"}

    provider = _OkProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.close_position(
        symbol="msft",
        qty=None,
        percentage=None,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert response["success"] is True
    assert response["data"]["id"] == "close-1"
    assert response["meta"]["symbol"] == "MSFT"
    # Provider sees the un-uppercased value — its own normalization layer
    # handles that (see AlpacaTradingMixin.close_position which calls
    # ``symbol.upper()`` before hitting the SDK).
    assert captured["symbol"] == "MSFT"


@pytest.mark.asyncio
async def test_close_position_rejects_negative_qty_before_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Provider:
        called = False

        def close_position(self, symbol: str, qty: Any = None, percentage: Any = None) -> dict[str, Any]:
            self.called = True
            return {"id": "should-not-call"}

    provider = _Provider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.close_position(
            symbol="CRNC",
            qty=-8000.0,
            percentage=None,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    assert "qty" in str(exc.value.detail).lower()
    assert "-8000.0" in str(exc.value.detail)
    assert provider.called is False


@pytest.mark.asyncio
async def test_run_trading_provider_call_admits_concurrent_within_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With cap=3, three concurrent calls must all succeed (no spurious 503s)."""
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(alpaca_trading_max_inflight=3, alpaca_trading_call_timeout_seconds=5.0),
    )

    call_count = 0

    def _counter(_p: Any) -> Any:
        nonlocal call_count
        call_count += 1
        return call_count

    results = await asyncio.gather(
        trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=_counter,
            operation="get_account",
        ),
        trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=_counter,
            operation="get_account",
        ),
        trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=_counter,
            operation="get_account",
        ),
    )
    assert sorted(results) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Retry-hint URL drift guard — the 5xx retry_hint strings must reference the
# router's ACTUAL mounted prefix. Hard-coding a stale prefix in the docstring
# / hint silently breaks caller reconciliation logic. These tests pin the
# invariant: if anyone reorganizes the router mount-point, the hint stays in
# sync.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_order_504_retry_hint_uses_actual_router_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retry hint MUST reference the actual mounted prefix (currently
    ``/api/v1/alpaca``). A prior bug emitted ``/api/alpaca/trading/...`` —
    wrong on TWO counts (missing the ``v1`` and the extra ``trading``
    segment) — which sent caller retries to a 404 and left them flying
    blind during a 504. This test pins the hint to whatever prefix the
    parent router is mounted at, so it can't drift again."""

    class _SlowProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            import time

            time.sleep(0.6)
            return {"id": "should-not-return"}

    provider = _SlowProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        return await provider_call(registry.get("alpaca"))

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=0.5,
        ),
    )

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            qty=10,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    hint = exc.value.detail["retry_hint"]
    # The actual mounted prefix appears in the hint.
    assert ALPACA_ROUTER_PREFIX in hint
    # And the GET path uses the by_client_order_id endpoint under that prefix.
    assert f"GET {ALPACA_ROUTER_PREFIX}/orders:by_client_order_id" in hint
    assert f"POST {ALPACA_ROUTER_PREFIX}/orders" in hint
    # Belt-and-suspenders: the old, wrong shape must NOT appear.
    assert "/api/alpaca/trading/" not in hint


@pytest.mark.asyncio
async def test_close_position_504_retry_hint_uses_actual_router_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The close-position hint must use the mounted broker-reconciliation URLs."""

    class _SlowProvider:
        def close_position(self, symbol: str, qty: Any = None, percentage: Any = None) -> dict[str, Any]:
            import time

            time.sleep(0.6)
            return {"id": "should-not-return"}

    provider = _SlowProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        return await provider_call(registry.get("alpaca"))

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=0.5,
        ),
    )

    with pytest.raises(HTTPException) as exc:
        await trading.close_position(
            symbol="aapl",
            qty=None,
            percentage=None,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    hint = exc.value.detail["retry_hint"]
    assert ALPACA_ROUTER_PREFIX in hint
    assert f"GET {ALPACA_ROUTER_PREFIX}/positions/AAPL" in hint
    # Old, wrong shape MUST NOT appear.
    assert "/api/alpaca/trading/" not in hint


def test_retry_hint_prefix_matches_parent_router_mount_point() -> None:
    """Sanity: the constant the trading module uses to build retry hints
    matches the prefix the parent router is actually mounted at. Without
    this guard, somebody changing ``ALPACA_ROUTER_PREFIX`` in
    ``alpaca/__init__.py`` but forgetting to update the constant in
    ``common.py`` (or vice versa — they share the constant today, but the
    test pins the invariant for the future) would silently re-introduce
    the URL-drift bug."""
    from gateway.api.alpaca import router as parent_router

    assert parent_router.prefix == ALPACA_ROUTER_PREFIX


# ---------------------------------------------------------------------------
# client_order_id input validation — empty/whitespace must be rejected with
# 400 GW-E4006 rather than silently falling through to gateway-minted UUID
# (which would defeat Alpaca-side dedup on retry).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_key", ["", " ", "   ", "\t", "\n\n", " \t \n "])
async def test_create_order_rejects_empty_or_whitespace_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
    bad_key: str,
) -> None:
    """Empty / whitespace-only client_order_id must surface as 400 GW-E4006.

    The previous code silently auto-generated a UUID and labelled it
    ``client_order_id_source="caller"`` — so each retry minted a fresh
    UUID, defeating idempotency."""
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            qty=10,
            client_order_id=bad_key,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["code"] == "GW-E4006"
    # Provider must NOT have been called.
    assert not hasattr(provider, "_calls") or provider.assets_calls == []


@pytest.mark.asyncio
async def test_create_order_rejects_oversize_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Alpaca caps client_order_id at 128 chars; a clean 400 from the gateway
    beats Alpaca's 422 surfacing later."""
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    oversize = "x" * 129

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            qty=10,
            client_order_id=oversize,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "GW-E4006"
    assert "128" in exc.value.detail["message"]


@pytest.mark.asyncio
async def test_create_order_accepts_max_length_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller key sized to the post-prefix budget (128 - prefix_len) is the
    inclusive maximum and must pass through. Alpaca's 128-char ceiling
    applies to the FINAL prefixed string, so the per-caller budget shrinks
    by the length of ``c-{client.id}-``."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "max-len-ok"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    # client.id == "test-client" → prefix "c-test-client-" is 14 chars.
    # Per-caller budget is 128 - 14 = 114.
    prefix_len = len("c-test-client-")
    max_caller_len = 128 - prefix_len
    max_len_key = "x" * max_caller_len

    response = await trading.create_order(
        symbol="AAPL",
        side="buy",
        qty=10,
        client_order_id=max_len_key,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    # Provider sees the FULL 128-char prefixed string (the ceiling).
    assert captured["client_order_id"] == f"c-test-client-{max_len_key}"
    assert len(captured["client_order_id"]) == 128
    assert response["meta"]["client_order_id_source"] == "caller"


# ---------------------------------------------------------------------------
# Non-timeout 5xx must still surface idempotency context. The previous
# implementation only attached client_order_id / symbol on the 503/504 paths
# inside _run_trading_provider_call — but execute_alpaca_provider_call's
# error remapping (APIError → HTTPException, httpx.HTTPStatusError →
# HTTPException, bare Exception → 502) replaced the detail with a plain
# string, losing the retry contract. These tests pin that EVERY 5xx path
# carries the context.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_order_non_timeout_5xx_preserves_client_order_id_in_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Simulate a 503 ``httpx.HTTPStatusError`` from the Alpaca SDK — the
    route must merge the idempotency_context into the rewritten
    HTTPException's detail so the caller still gets the client_order_id
    back. (Choosing HTTPStatusError instead of APIError because APIError's
    status_code is a read-only property; HTTPStatusError covers the same
    common.py code branch.)"""
    import httpx

    class _Http503Provider:
        def create_order(self, **kwargs: Any) -> Any:
            request = httpx.Request("POST", "https://api.alpaca.markets/v2/orders")
            response = httpx.Response(status_code=503, request=request, text="simulated upstream 503")
            raise httpx.HTTPStatusError(
                "simulated Alpaca 503",
                request=request,
                response=response,
            )

    provider = _Http503Provider()
    route_registry = _FakeRegistry({"alpaca": provider})

    # Use the REAL execute_alpaca_provider_call so we exercise its error
    # rewrite path. Stub require_provider_rate_limit + upstream_semaphore so
    # nothing else interferes.
    from gateway.api.alpaca import common as _common

    async def _no_rate_limit(provider_name: str, block: bool = True) -> None:  # noqa: ARG001
        return None

    class _NoSem:
        def upstream_semaphore(self, name: str) -> None:  # noqa: ARG002
            return None

    monkeypatch.setattr(_common, "require_provider_rate_limit", _no_rate_limit)
    monkeypatch.setattr(_common, "get_rate_limiter", lambda: _NoSem())

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            qty=10,
            client_order_id="caller-non-timeout-key-1",
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 503
    detail = exc.value.detail
    assert isinstance(detail, dict), (
        f"5xx detail must be a dict carrying the idempotency context, got {type(detail)}: {detail!r}"
    )
    # Critical retry-contract assertion: caller's key (now ownership-
    # prefixed) is preserved on a non-timeout 5xx so they can safely
    # retry / verify.
    assert detail["client_order_id"] == "c-test-client-caller-non-timeout-key-1"
    assert detail["client_order_id_source"] == "caller"
    assert detail["retry_with"] == "client_order_id"
    assert ALPACA_ROUTER_PREFIX in detail["retry_hint"]


@pytest.mark.asyncio
async def test_close_position_non_timeout_5xx_preserves_symbol_in_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same contract for close_position: a non-timeout 5xx (here a bare
    Exception → 502) must still carry symbol + retry_with so the caller
    knows to GET /positions/<symbol>."""

    class _BoomProvider:
        def close_position(self, symbol: str, qty: Any = None, percentage: Any = None) -> Any:
            raise RuntimeError("simulated upstream blowup")

    provider = _BoomProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    redis = _MemoryRedis()

    class _ReleaseObservingGuard(OrderOwnershipGuard):
        def __init__(self) -> None:
            super().__init__(redis)
            self.release_saw_frozen_claim = False

        async def release_fence(self, symbol: str, token: str) -> None:
            claim = json.loads(redis.values[self.claim_key(symbol)])
            self.release_saw_frozen_claim = "frozen_reason" in claim
            await super().release_fence(symbol, token)

    ownership_guard = _ReleaseObservingGuard()
    await ownership_guard.authorize_submission(
        client_id="test-client",
        symbol="MSFT",
        broker_state=BrokerSymbolState(has_position=False, order_owners=frozenset()),
    )
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: ownership_guard)

    async def _open_position(*_args: Any, **_kwargs: Any) -> BrokerSymbolState:
        return BrokerSymbolState(has_position=True, order_owners=frozenset())

    monkeypatch.setattr(trading, "_reconcile_broker_symbol_state", _open_position)

    from gateway.api.alpaca import common as _common

    async def _no_rate_limit(provider_name: str, block: bool = True) -> None:  # noqa: ARG001
        return None

    class _NoSem:
        def upstream_semaphore(self, name: str) -> None:  # noqa: ARG002
            return None

    monkeypatch.setattr(_common, "require_provider_rate_limit", _no_rate_limit)
    monkeypatch.setattr(_common, "get_rate_limiter", lambda: _NoSem())

    with pytest.raises(HTTPException) as exc:
        await trading.close_position(
            symbol="msft",
            qty=None,
            percentage=None,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    # Bare Exception → 502 in execute_alpaca_provider_call.
    assert exc.value.status_code == 502
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["symbol"] == "MSFT"
    assert detail["retry_with"] == "broker_reconciliation"
    assert f"GET {ALPACA_ROUTER_PREFIX}/positions/MSFT" in detail["retry_hint"]
    assert ownership_guard.release_saw_frozen_claim is True
    with pytest.raises(OwnershipConflict, match="claim_frozen_after_ambiguous_broker_mutation"):
        await ownership_guard.authorize_close(
            client_id="test-client",
            symbol="MSFT",
            broker_state=BrokerSymbolState(has_position=True, order_owners=frozenset()),
        )


# ---------------------------------------------------------------------------
# HTTP-layer test via FastAPI TestClient — exercises the full request stack
# (auth → middleware → router → handler) instead of invoking the coroutine
# directly. The static-string URL bug slipped through unit tests because
# nothing exercised the real path-prefix. This test gives one anchor point.
# ---------------------------------------------------------------------------


def test_create_order_504_via_http_layer_contains_correct_retry_hint_url(
    client,
    monkeypatch,
    auth_headers,
    test_registry,
) -> None:
    """Through-the-wire test: a 504 from create_order must contain a
    retry_hint that references the ACTUAL mounted URL (the one the caller
    just hit). Exercises the full middleware/router stack so URL drift in
    the docstring/hint can't escape review.

    Note: status_code is 504 from the gateway timeout, NOT 500. The
    middleware passes the dict ``detail`` through to the JSON body."""
    # Trading endpoints require role=trader/admin/super_admin. The default
    # test client has no role, so override require_api_key to return a
    # trader-role client for this test only.
    from gateway.api.deps import require_api_key
    from gateway.core.auth import Client, ClientPermissions
    from gateway.main import app

    app.dependency_overrides[require_api_key] = lambda: Client(
        id="test-trader",
        permissions=ClientPermissions(
            providers=["alpaca"],
            feeds=["bars", "quotes"],
            max_symbols=100,
            rate_limit=60,
        ),
        role="trader",
    )

    # Configure the mock provider to simulate a slow create_order so the
    # gateway's asyncio.wait_for fires.
    def _slow_create(**kwargs: Any) -> dict[str, Any]:
        import time

        time.sleep(1.0)
        return {"id": "should-not-return"}

    test_registry.get.return_value.create_order = _slow_create

    # Settings enforces a 0.5s minimum on the timeout (sanity guard against
    # accidental sub-second timeouts in prod). Pin to the floor so the test
    # finishes quickly while still exercising the timeout path.
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=0.5,
            alpaca_trading_max_inflight=10,
        ),
    )

    try:
        response = client.post(
            f"{ALPACA_ROUTER_PREFIX}/orders?symbol=AAPL&side=buy&qty=10&client_order_id=http-layer-test-1",
            headers=auth_headers,
        )
    finally:
        app.dependency_overrides.pop(require_api_key, None)

    # The endpoint must EXIST at this URL — a 404 here would mean the
    # router prefix drifted away from the constant.
    assert response.status_code != 404, (
        f"Endpoint {ALPACA_ROUTER_PREFIX}/orders returned 404 — router prefix has drifted from ALPACA_ROUTER_PREFIX."
    )
    assert response.status_code == 504, response.text

    body = response.json()
    # The 504 detail must be a dict (not a plain string) so the retry
    # contract survives the JSON round-trip. The error handler may wrap it
    # in {"success": false, "error": {...}, "detail": {...}} — accept either.
    detail = body.get("detail") or body.get("error") or body
    assert isinstance(detail, dict), f"detail must be a dict, got {body!r}"
    # The gateway prefixes the caller key with ``c-{client.id}-`` for
    # per-client ownership isolation against the shared Alpaca account.
    assert detail["client_order_id"] == "c-test-trader-http-layer-test-1"
    assert detail["retry_with"] == "client_order_id"
    assert f"GET {ALPACA_ROUTER_PREFIX}/orders:by_client_order_id" in detail["retry_hint"]
    assert "/api/alpaca/trading/" not in detail["retry_hint"]


# ---------------------------------------------------------------------------
# replace_order idempotency — mirrors the create_order contract. PATCH must
# carry the same retry plumbing because Alpaca's replace_order_by_id accepts
# a client_order_id on the *replacement* and dedupes by it, AND a naive
# retry against a partially-replaced order can double-modify the position.
# The retry hint additionally points at GET /orders/{order_id} so the caller
# can observe the original order transitioning to status="replaced".
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replace_order_auto_generates_client_order_id_when_caller_omits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Caller omits client_order_id → gateway mints ``dg-<uuid4hex>`` and
    threads it to the provider so the replacement is Alpaca-side dedupable."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def replace_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
            captured["order_id"] = order_id
            captured.update(kwargs)
            return {"id": "replacement-1", "status": "accepted", "replaces": order_id}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.replace_order(
        order_id="orig-order-1",
        qty=15,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert captured["order_id"] == "orig-order-1"
    assert captured["client_order_id"] is not None
    # Prefixed for ownership isolation against the shared Alpaca account.
    assert captured["client_order_id"].startswith("c-test-client-dg-")
    assert len(captured["client_order_id"]) == len("c-test-client-dg-") + 32
    assert response["meta"]["client_order_id"] == captured["client_order_id"]
    assert response["meta"]["client_order_id_source"] == "gateway"


@pytest.mark.asyncio
async def test_replace_order_maps_value_error_to_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provider-side ValueError (e.g. ``TimeInForce(unknown_value)``) must
    surface as 400 — not as a synthetic 502 from execute_alpaca_provider_call.
    Without the local ``except ValueError`` branch the caller-input fault
    would get labeled as a retryable 5xx and the new idempotency-merge logic
    would attach retry hints, sending the caller down a phantom-Alpaca-outage
    chase. Mirrors ``test_create_order_maps_value_error_to_400``.
    """

    class _ValueErrorProvider(_FakeProvider):
        def replace_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
            raise ValueError("invalid time_in_force")

    provider = _ValueErrorProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.replace_order(
            order_id="orig-order-1",
            time_in_force="bogus",
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    assert exc.value.detail == "invalid time_in_force"


@pytest.mark.asyncio
async def test_replace_order_preserves_caller_supplied_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Caller supplies a client_order_id → gateway must use it verbatim."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def replace_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
            captured["order_id"] = order_id
            captured.update(kwargs)
            return {"id": "replacement-1"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.replace_order(
        order_id="orig-order-2",
        qty=15,
        client_order_id="caller-replace-key-1",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    # The gateway transparently prefixes caller-supplied keys with
    # ``c-{client.id}-`` so the replacement order can be uniquely
    # attributed to this caller on later lookups.
    assert captured["client_order_id"] == "c-test-client-caller-replace-key-1"
    assert response["meta"]["client_order_id"] == "c-test-client-caller-replace-key-1"
    assert response["meta"]["client_order_id_source"] == "caller"


@pytest.mark.asyncio
async def test_replace_order_504_timeout_includes_client_order_id_in_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Critical retry contract: 504 on PATCH must carry the client_order_id
    in detail. Without it, a naive retry against a replaced order would
    either no-op or double-modify."""

    class _SlowProvider(_FakeProvider):
        def replace_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
            import time

            time.sleep(0.6)
            return {"id": "should-not-return"}

    provider = _SlowProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        return await provider_call(registry.get("alpaca"))

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=0.5,
        ),
    )

    with pytest.raises(HTTPException) as exc:
        await trading.replace_order(
            order_id="orig-order-3",
            qty=20,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == HTTP_504_GATEWAY_TIMEOUT
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["code"] == "GW-E5004"
    assert "client_order_id" in detail
    assert detail["client_order_id"].startswith("c-test-client-dg-")
    assert detail["client_order_id_source"] == "gateway"
    assert detail["retry_with"] == "client_order_id"
    # The retry hint must mention BOTH lookup paths (original order +
    # by_client_order_id) — distinctive to the PATCH contract.
    assert "Alpaca natively dedupes" in detail["retry_hint"]
    assert "DO NOT retry PATCH" in detail["retry_hint"]


@pytest.mark.asyncio
async def test_replace_order_504_timeout_preserves_caller_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When caller supplies the key and it times out, the 504 surfaces THE
    CALLER'S key so the caller's retry path stays wired correctly."""

    class _SlowProvider(_FakeProvider):
        def replace_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
            import time

            time.sleep(0.6)
            return {"id": "should-not-return"}

    provider = _SlowProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        return await provider_call(registry.get("alpaca"))

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=0.5,
        ),
    )

    with pytest.raises(HTTPException) as exc:
        await trading.replace_order(
            order_id="orig-order-4",
            qty=20,
            client_order_id="caller-replace-retry-key",
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == HTTP_504_GATEWAY_TIMEOUT
    detail = exc.value.detail
    assert isinstance(detail, dict)
    # FULL prefixed key surfaces in the 504 body so caller retry tooling
    # can pass it straight back to GET /orders:by_client_order_id.
    assert detail["client_order_id"] == "c-test-client-caller-replace-retry-key"
    assert detail["client_order_id_source"] == "caller"


@pytest.mark.asyncio
async def test_replace_order_503_backpressure_includes_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """503 backpressure path runs BEFORE the call hits Alpaca — the
    replacement definitely did not apply, but the idempotency key still
    needs to surface so the caller can safely retry the same logical
    replacement."""
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(alpaca_trading_max_inflight=2, alpaca_trading_call_timeout_seconds=5.0),
    )

    sem = trading._get_trading_inflight_sem()
    await sem.acquire()
    await sem.acquire()
    assert sem.locked()

    with pytest.raises(HTTPException) as exc:
        await trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=lambda _p: "unused",
            operation="replace_order",
            idempotency_context={
                "client_order_id": "replace-test-key-1",
                "client_order_id_source": "gateway",
                "retry_with": "client_order_id",
                "retry_hint": "stub",
            },
        )

    assert exc.value.status_code == HTTP_503_SERVICE_UNAVAILABLE
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["client_order_id"] == "replace-test-key-1"


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_key", ["", " ", "   ", "\t", "\n\n", " \t \n "])
async def test_replace_order_rejects_empty_or_whitespace_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
    bad_key: str,
) -> None:
    """Empty / whitespace client_order_id must surface as 400 GW-E4006 —
    same contract as create_order. Silently auto-generating would defeat
    Alpaca-side dedup on retry."""
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.replace_order(
            order_id="orig-order-5",
            qty=10,
            client_order_id=bad_key,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["code"] == "GW-E4006"


@pytest.mark.asyncio
async def test_replace_order_rejects_oversize_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same 128-char cap as create_order — Alpaca's REST API rejects above
    that, the gateway surfaces a structured 400 first."""
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    oversize = "x" * 129

    with pytest.raises(HTTPException) as exc:
        await trading.replace_order(
            order_id="orig-order-6",
            qty=10,
            client_order_id=oversize,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "GW-E4006"
    assert "128" in exc.value.detail["message"]


@pytest.mark.asyncio
async def test_replace_order_accepts_max_length_client_order_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller key sized to the post-prefix budget (128 - prefix_len) is the
    inclusive maximum and passes through. Alpaca's 128-char ceiling
    applies to the FINAL prefixed string — the per-caller budget shrinks
    by the length of ``c-{client.id}-``."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def replace_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "max-len-replace-ok"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    prefix_len = len("c-test-client-")
    max_caller_len = 128 - prefix_len
    max_len_key = "y" * max_caller_len

    response = await trading.replace_order(
        order_id="orig-order-7",
        qty=10,
        client_order_id=max_len_key,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    # Provider sees the FULL 128-char prefixed string (the ceiling).
    assert captured["client_order_id"] == f"c-test-client-{max_len_key}"
    assert len(captured["client_order_id"]) == 128
    assert response["meta"]["client_order_id_source"] == "caller"


@pytest.mark.asyncio
async def test_replace_order_non_timeout_5xx_preserves_client_order_id_in_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-timeout 5xx (e.g. 503 from Alpaca during a deployment) must
    still carry the idempotency key — the rewrite in
    execute_alpaca_provider_call would otherwise strip it."""
    import httpx

    class _Http503Provider:
        def get_order(self, order_id: str) -> dict[str, Any]:
            # Ownership pre-check sees an order owned by ``test-client``
            # — passes through to the replace call which then 503s.
            return {"id": order_id, "client_order_id": f"c-test-client-{order_id}", "symbol": "AAPL"}

        def replace_order(self, order_id: str, **kwargs: Any) -> Any:
            request = httpx.Request("PATCH", f"https://api.alpaca.markets/v2/orders/{order_id}")
            response = httpx.Response(status_code=503, request=request, text="simulated upstream 503")
            raise httpx.HTTPStatusError(
                "simulated Alpaca 503",
                request=request,
                response=response,
            )

    provider = _Http503Provider()
    route_registry = _FakeRegistry({"alpaca": provider})

    from gateway.api.alpaca import common as _common

    async def _no_rate_limit(provider_name: str, block: bool = True) -> None:  # noqa: ARG001
        return None

    class _NoSem:
        def upstream_semaphore(self, name: str) -> None:  # noqa: ARG002
            return None

    monkeypatch.setattr(_common, "require_provider_rate_limit", _no_rate_limit)
    monkeypatch.setattr(_common, "get_rate_limiter", lambda: _NoSem())

    with pytest.raises(HTTPException) as exc:
        await trading.replace_order(
            order_id="orig-order-8",
            qty=10,
            client_order_id="caller-replace-non-timeout-1",
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 503
    detail = exc.value.detail
    assert isinstance(detail, dict), (
        f"5xx detail must be a dict carrying the idempotency context, got {type(detail)}: {detail!r}"
    )
    # FULL ownership-prefixed key is preserved on non-timeout 5xx.
    assert detail["client_order_id"] == "c-test-client-caller-replace-non-timeout-1"
    assert detail["client_order_id_source"] == "caller"
    assert detail["retry_with"] == "client_order_id"
    assert ALPACA_ROUTER_PREFIX in detail["retry_hint"]


@pytest.mark.asyncio
async def test_replace_order_504_retry_hint_uses_actual_router_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retry hint must reference the actual mounted prefix AND both
    lookup paths (the original-order GET and the by-client-order-id GET).
    Any drift here sends caller retries to a 404 just like the original
    URL-drift bug for create_order."""

    class _SlowProvider(_FakeProvider):
        def replace_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
            import time

            time.sleep(0.6)
            return {"id": "should-not-return"}

    provider = _SlowProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        return await provider_call(registry.get("alpaca"))

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=0.5,
        ),
    )

    with pytest.raises(HTTPException) as exc:
        await trading.replace_order(
            order_id="orig-9",
            qty=10,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    hint = exc.value.detail["retry_hint"]
    assert ALPACA_ROUTER_PREFIX in hint
    # BOTH retry lookup paths are present in the PATCH retry contract.
    assert f"GET {ALPACA_ROUTER_PREFIX}/orders/orig-9" in hint
    assert f"GET {ALPACA_ROUTER_PREFIX}/orders:by_client_order_id" in hint
    # Old wrong-shape strings must not appear.
    assert "/api/alpaca/trading/" not in hint


def test_replace_order_504_via_http_layer_contains_correct_retry_hint_url(
    client,
    monkeypatch,
    auth_headers,
    test_registry,
) -> None:
    """HTTP-layer through-the-wire test (mirrors the create_order variant).
    Exercises the full middleware/router stack so future URL drift in the
    docstring/hint can't escape review."""
    from gateway.api.deps import require_api_key
    from gateway.core.auth import Client, ClientPermissions
    from gateway.main import app

    app.dependency_overrides[require_api_key] = lambda: Client(
        id="test-trader",
        permissions=ClientPermissions(
            providers=["alpaca"],
            feeds=["bars", "quotes"],
            max_symbols=100,
            rate_limit=60,
        ),
        role="trader",
    )

    def _slow_replace(order_id: str, **kwargs: Any) -> dict[str, Any]:
        import time

        time.sleep(1.0)
        return {"id": "should-not-return"}

    # The replace_order endpoint now runs an ownership pre-check via
    # provider.get_order(order_id) before mutating — return an order
    # owned by ``test-trader`` so the pre-check passes and the test
    # reaches the slow-replace timeout path.
    def _owned_get_order(order_id: str) -> dict[str, Any]:
        return {"id": order_id, "client_order_id": f"c-test-trader-{order_id}", "symbol": "AAPL"}

    test_registry.get.return_value.replace_order = _slow_replace
    test_registry.get.return_value.get_order = _owned_get_order

    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=0.5,
            alpaca_trading_max_inflight=10,
        ),
    )

    try:
        response = client.patch(
            f"{ALPACA_ROUTER_PREFIX}/orders/orig-http-1?qty=10&client_order_id=http-layer-replace-1",
            headers=auth_headers,
        )
    finally:
        app.dependency_overrides.pop(require_api_key, None)

    assert response.status_code != 404, (
        f"Endpoint {ALPACA_ROUTER_PREFIX}/orders/<order_id> returned 404 — "
        "router prefix has drifted from ALPACA_ROUTER_PREFIX."
    )
    assert response.status_code == 504, response.text

    body = response.json()
    detail = body.get("detail") or body.get("error") or body
    assert isinstance(detail, dict), f"detail must be a dict, got {body!r}"
    # The gateway prefixes the caller key with ``c-{client.id}-`` for
    # per-client ownership isolation against the shared Alpaca account.
    assert detail["client_order_id"] == "c-test-trader-http-layer-replace-1"
    assert detail["retry_with"] == "client_order_id"
    # Both retry paths are present.
    assert f"GET {ALPACA_ROUTER_PREFIX}/orders/orig-http-1" in detail["retry_hint"]
    assert f"GET {ALPACA_ROUTER_PREFIX}/orders:by_client_order_id" in detail["retry_hint"]
    assert "/api/alpaca/trading/" not in detail["retry_hint"]


# ---------------------------------------------------------------------------
# Read vs write timeout split — writes get a longer ceiling
# (alpaca_trading_write_call_timeout_seconds, default 25s) than reads
# (alpaca_trading_call_timeout_seconds, default 15s). Reads are safe to
# retry after a 504; writes prefer slack-to-completion over forcing the
# caller into the idempotency-retry contract.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "write_op",
    [
        "create_order",
        "replace_order",
        "cancel_order",
        "cancel_all_orders",
        # The per-order cancel DELETE /orders actually issues — a write whose
        # 5xx freezes the symbol's ownership claim, so it must not run on the
        # tighter read budget.
        "cancel_all_orders.cancel",
        "close_position",
        "close_all_positions",
    ],
)
async def test_run_trading_provider_call_uses_write_timeout_for_write_operations(
    monkeypatch: pytest.MonkeyPatch,
    write_op: str,
) -> None:
    """Write operations must consult alpaca_trading_write_call_timeout_seconds,
    NOT the read timeout. The split keeps reads tight while letting writes
    absorb opening-bell broker slowdowns instead of surfacing a 504 that
    forces the caller through the idempotency-retry contract."""
    # Pin read timeout long, write timeout at the floor (Settings enforces
    # ge=0.5 — sanity guard against accidental sub-second timeouts in prod).
    # A write op must 504 because it's reading the write knob; if it
    # incorrectly read the read knob it would succeed.
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=5.0,
            alpaca_trading_write_call_timeout_seconds=0.5,
            alpaca_trading_max_inflight=10,
        ),
    )

    def _slow(_p: Any) -> Any:
        import time

        time.sleep(1.0)
        return "should-not-return"

    with pytest.raises(HTTPException) as exc:
        await trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=_slow,
            operation=write_op,
        )
    assert exc.value.status_code == HTTP_504_GATEWAY_TIMEOUT
    assert exc.value.detail["code"] == "GW-E5004"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "read_op",
    [
        "get_account",
        "get_orders",
        "get_order",
        "get_order_by_client_id",
        "get_positions",
        "get_position",
        "get_portfolio_history",
        "get_assets",
        "get_asset",
        "get_clock",
        "get_calendar",
    ],
)
async def test_run_trading_provider_call_uses_read_timeout_for_read_operations(
    monkeypatch: pytest.MonkeyPatch,
    read_op: str,
) -> None:
    """Read operations must consult alpaca_trading_call_timeout_seconds, NOT
    the write knob. Pinned read timeout at the floor (ge=0.5), write
    timeout long — a read op that incorrectly read the write knob would
    succeed instead of 504."""
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=5.0,
            alpaca_trading_max_inflight=10,
        ),
    )

    def _slow(_p: Any) -> Any:
        import time

        time.sleep(1.0)
        return "should-not-return"

    with pytest.raises(HTTPException) as exc:
        await trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=_slow,
            operation=read_op,
        )
    assert exc.value.status_code == HTTP_504_GATEWAY_TIMEOUT


@pytest.mark.asyncio
async def test_run_trading_provider_call_write_timeout_longer_than_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Black-box smoke test of the asymmetry: with read=0.5s and write=2.0s,
    a 1.0s-sleep write SUCCEEDS while a 1.0s-sleep read 504s. Pins the
    direction of the split — writes must get MORE budget than reads. The
    read floor (0.5s) is the Settings ge=0.5 minimum."""
    monkeypatch.setattr(
        trading,
        "get_settings",
        lambda: Settings(
            alpaca_trading_call_timeout_seconds=0.5,
            alpaca_trading_write_call_timeout_seconds=2.0,
            alpaca_trading_max_inflight=10,
        ),
    )

    def _med(_p: Any) -> Any:
        import time

        time.sleep(1.0)
        return "ok"

    # Read 504s (1.0s > 0.5s read timeout).
    with pytest.raises(HTTPException) as read_exc:
        await trading._run_trading_provider_call(
            provider=SimpleNamespace(),
            provider_fn=_med,
            operation="get_account",
        )
    assert read_exc.value.status_code == HTTP_504_GATEWAY_TIMEOUT

    # Write SUCCEEDS (1.0s < 2.0s write timeout).
    result = await trading._run_trading_provider_call(
        provider=SimpleNamespace(),
        provider_fn=_med,
        operation="create_order",
    )
    assert result == "ok"


def test_trading_call_timeout_defaults_keep_writes_slacker_than_reads() -> None:
    """Sanity check on the default configuration: writes must have AT LEAST
    as much wall-clock budget as reads, and the HTTP-level safety net must
    not be lower than the wall-clock timeout. Catches a footgun where a
    deploy-time override (env var) inverts the relationship without anyone
    noticing — the regression would silently re-introduce the 2026-05-15
    opening-bell timeout pattern."""
    settings = Settings()
    assert settings.alpaca_trading_write_call_timeout_seconds >= settings.alpaca_trading_call_timeout_seconds, (
        "writes must get at least as much wall-clock budget as reads"
    )
    # HTTP safety net releases the executor thread when the wall-clock
    # timer fires; if the HTTP timeout is lower, the thread is killed
    # BEFORE the user-facing 504 fires, which defeats the idempotency
    # contract (the SDK exception masks the wall-clock timeout).
    assert settings.alpaca_trading_http_timeout_seconds >= settings.alpaca_trading_write_call_timeout_seconds, (
        "HTTP timeout must be >= write call timeout — see alpaca_trading_http_timeout_seconds docstring"
    )
    assert settings.alpaca_trading_http_timeout_seconds >= settings.alpaca_trading_call_timeout_seconds, (
        "HTTP timeout must be >= read call timeout"
    )


# ---------------------------------------------------------------------------
# M0.4 — Trading-route authorization characterization. These pin TODAY's
# observable authz behavior at the HTTP layer (require_api_key →
# _enforce_trading_role in gateway/api/deps.py) BEFORE M1 introduces a finer
# 'trading' permission for granted clients. The role gate currently admits any
# client whose role is trader/admin/super_admin and 403s everyone else
# (GW-E2008).
#
# Robustness to the M1 permission addition:
#   - The "granted" client is configured with role=trader AND a forward-compat
#     ``trading`` permission marker (a ``trading`` feed plus a ``trading: true``
#     key — unknown keys are ignored by the loader today, see
#     gateway/core/auth.ClientAuthenticator._load_clients). So whether M1 keys
#     the new gate off the role, a feed, or a dedicated permission, this client
#     keeps its trading rights and these success assertions stay valid.
#   - The "denied" client has role=client and no trading marker, so it is
#     denied under both today's role gate and any finer M1 gate.
#
# The authz lives in require_api_key, NOT in the route handlers — so these go
# through the real FastAPI TestClient stack (auth → middleware → router) rather
# than invoking the route coroutines directly like the unit tests above.
# ---------------------------------------------------------------------------

_AUTHZ_GRANTED_KEY = "gw_m04_granted_trader_key"  # pragma: allowlist secret
_AUTHZ_DENIED_KEY = "gw_m04_denied_client_key"  # pragma: allowlist secret
# M1: a trader-role client that LACKS the new ``trading`` capability. It clears
# the coarse role gate but must be 403'd on mutating order/position routes.
_AUTHZ_TRADER_NO_CAP_KEY = "gw_m1_trader_no_trading_cap_key"  # pragma: allowlist secret


@pytest.fixture
def _authz_clients(tmp_path: Path):
    """Mount a real authenticator with two clients exercising the trading gate.

    - granted: role=trader plus a forward-compatible ``trading`` permission
      marker so it retains trading rights after M1's finer permission lands.
    - denied: role=client with no trading marker — denied today and post-M1.
    """
    from gateway.api.deps import get_authenticator
    from gateway.core.auth import ClientAuthenticator
    from gateway.main import app as _app

    clients_file = tmp_path / "authz_clients.yaml"
    clients_file.write_text(
        f"""
clients:
  - id: m04-granted-trader
    key: "{_AUTHZ_GRANTED_KEY}"
    role: trader
    permissions:
      providers: [alpaca]
      feeds: [bars, trading]
      trading: true
      max_symbols: 100
      rate_limit: 600
    enabled: true
  - id: m04-denied-client
    key: "{_AUTHZ_DENIED_KEY}"
    role: client
    permissions:
      providers: [alpaca]
      feeds: [bars]
      max_symbols: 100
      rate_limit: 600
    enabled: true
  - id: m1-trader-no-trading-cap
    key: "{_AUTHZ_TRADER_NO_CAP_KEY}"
    role: trader
    permissions:
      providers: [alpaca]
      feeds: [bars]
      max_symbols: 100
      rate_limit: 600
    enabled: true
"""
    )
    authenticator = ClientAuthenticator(clients_file)
    # override_deps (autouse) already overrode get_authenticator with the
    # default test authenticator; replace it with ours for the duration.
    previous = _app.dependency_overrides.get(get_authenticator)
    _app.dependency_overrides[get_authenticator] = lambda: authenticator
    try:
        yield
    finally:
        if previous is not None:
            _app.dependency_overrides[get_authenticator] = previous
        else:
            _app.dependency_overrides.pop(get_authenticator, None)


def _authz_headers(key: str) -> dict[str, str]:
    return {"X-Gateway-Key": key}


def _stub_trading_provider(test_registry) -> Any:
    """Wire the mock provider so write-class trading calls return cleanly."""
    from unittest.mock import MagicMock

    provider = test_registry.get.return_value
    provider.create_order = MagicMock(return_value={"id": "ord-m04", "status": "accepted"})
    provider.cancel_order = MagicMock(return_value=True)
    provider.close_position = MagicMock(return_value={"id": "close-m04", "status": "accepted"})
    # Per-client ownership pre-check (replace/cancel/get_order) fetches the order
    # and verifies its client_order_id prefix. The granted authz client is
    # ``m04-granted-trader``, so return an order it owns or the pre-check 404s
    # before the operation under test runs.
    provider.get_order = MagicMock(
        return_value={"id": "ord-m04", "client_order_id": "c-m04-granted-trader-dg-stub", "symbol": "AAPL"}
    )
    return provider


def _stub_alpaca_account_mutation_provider(test_registry) -> Any:
    """Wire account/watchlist mutation calls that must be blocked by authz."""
    from unittest.mock import MagicMock

    provider = _stub_trading_provider(test_registry)
    provider.set_account_configurations = MagicMock(return_value={"suspend_trade": True})
    provider.create_watchlist = MagicMock(return_value={"id": "wl-1", "name": "Test"})
    provider.update_watchlist = MagicMock(return_value={"id": "wl-1", "name": "Updated"})
    provider.delete_watchlist = MagicMock(return_value=True)
    provider.add_asset_to_watchlist = MagicMock(return_value={"symbol": "AAPL"})
    provider.remove_asset_from_watchlist = MagicMock(return_value={"symbol": "AAPL"})
    return provider


def test_authz_granted_trader_can_create_order(client, test_registry, _authz_clients) -> None:
    """A client that retains trading rights (role=trader + trading permission
    marker) CAN place an order. Pins the 200 success envelope shape."""
    _stub_trading_provider(test_registry)

    response = client.post(
        f"{ALPACA_ROUTER_PREFIX}/orders?symbol=AAPL&side=buy&qty=1",
        headers=_authz_headers(_AUTHZ_GRANTED_KEY),
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["success"] is True
    assert body["data"]["id"] == "ord-m04"
    assert body["data"]["status"] == "accepted"
    # Idempotency contract surfaces the effective key in meta — gateway-minted
    # here because the caller omitted client_order_id. Per-client ownership
    # isolation prefixes the auto-generated key with ``c-{client.id}-``.
    assert body["meta"]["provider"] == "alpaca"
    assert body["meta"]["client_order_id"].startswith("c-m04-granted-trader-dg-")
    assert body["meta"]["client_order_id_source"] == "gateway"


def test_authz_granted_trader_can_cancel_order(client, test_registry, _authz_clients) -> None:
    """A trading-rights client CAN cancel an order. Pins the 200 cancel shape."""
    _stub_trading_provider(test_registry)

    response = client.delete(
        f"{ALPACA_ROUTER_PREFIX}/orders/ord-m04",
        headers=_authz_headers(_AUTHZ_GRANTED_KEY),
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["success"] is True
    assert body["data"] == {"order_id": "ord-m04", "cancelled": True}
    assert body["meta"]["provider"] == "alpaca"


def test_authz_granted_trader_can_close_position(client, test_registry, _authz_clients) -> None:
    """A trading-rights client CAN close a position. Pins the 200 close shape,
    including the upper-cased symbol echoed in meta."""
    _stub_trading_provider(test_registry)

    response = client.delete(
        f"{ALPACA_ROUTER_PREFIX}/positions/aapl",
        headers=_authz_headers(_AUTHZ_GRANTED_KEY),
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["success"] is True
    assert body["data"]["id"] == "close-m04"
    assert body["meta"]["provider"] == "alpaca"
    assert body["meta"]["symbol"] == "AAPL"


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", f"{ALPACA_ROUTER_PREFIX}/orders?symbol=AAPL&side=buy&qty=1"),
        ("delete", f"{ALPACA_ROUTER_PREFIX}/orders/ord-m04"),
        ("delete", f"{ALPACA_ROUTER_PREFIX}/positions/AAPL"),
    ],
)
def test_authz_denied_client_role_gets_403(client, test_registry, _authz_clients, method: str, path: str) -> None:
    """A non-trader (role=client) key is forbidden from the write-class trading
    endpoints with 403 GW-E2008, and the provider is NEVER reached. Pins the
    denial code and the structured error envelope.

    Scoped to write endpoints (POST orders / DELETE orders / DELETE positions):
    these are non-cacheable so they reach the route-level require_api_key gate
    directly. Cacheable GET trading reads (e.g. /account) are authenticated by
    CacheMiddleware against the process-global authenticator, which the
    per-test DI override does not reach — that is a separate middleware-auth
    concern, not the route-level trading-role gate under characterization here.
    """
    provider = _stub_trading_provider(test_registry)

    response = getattr(client, method)(path, headers=_authz_headers(_AUTHZ_DENIED_KEY))

    assert response.status_code == 403, response.text
    body = response.json()
    assert body["success"] is False
    assert body["error"]["code"] == "GW-E2008"
    assert body["error"]["message"] == "Trading access required"
    # Backward-compat detail mirror.
    assert body["detail"]["code"] == "GW-E2008"
    # The role gate rejects BEFORE the handler runs — no order/cancel/close
    # ever hit the provider.
    provider.create_order.assert_not_called()
    provider.cancel_order.assert_not_called()
    provider.close_position.assert_not_called()


# ---------------------------------------------------------------------------
# M1 — fine-grained ``trading`` capability gate. Order-/position-mutating
# routes (POST orders, PATCH/DELETE order, DELETE/close positions) now require
# permissions.trading == true ON TOP of the trader role. A trader-role client
# WITHOUT the capability clears _enforce_trading_role but is 403'd by
# _enforce_trading_capability (GW-E2009) before the handler runs.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method,path",
    [
        ("post", f"{ALPACA_ROUTER_PREFIX}/orders?symbol=AAPL&side=buy&qty=1"),
        ("delete", f"{ALPACA_ROUTER_PREFIX}/orders/ord-m1"),
        ("patch", f"{ALPACA_ROUTER_PREFIX}/orders/ord-m1?qty=2"),
        ("delete", f"{ALPACA_ROUTER_PREFIX}/positions/AAPL"),
    ],
)
def test_authz_trader_without_trading_capability_gets_403(
    client, test_registry, _authz_clients, method: str, path: str
) -> None:
    """A trader-role key LACKING the ``trading`` capability is 403'd on every
    mutating order/position route with GW-E2009, and the provider is never
    reached."""
    from unittest.mock import MagicMock

    provider = _stub_trading_provider(test_registry)
    provider.replace_order = MagicMock(return_value={"id": "ord-m1", "status": "replaced"})

    response = getattr(client, method)(path, headers=_authz_headers(_AUTHZ_TRADER_NO_CAP_KEY))

    assert response.status_code == 403, response.text
    body = response.json()
    assert body["success"] is False
    assert body["error"]["code"] == "GW-E2009"
    assert body["error"]["message"] == "Trading capability required"
    assert body["detail"]["code"] == "GW-E2009"
    provider.create_order.assert_not_called()
    provider.cancel_order.assert_not_called()
    provider.replace_order.assert_not_called()
    provider.close_position.assert_not_called()


@pytest.mark.parametrize(
    "method,path",
    [
        ("patch", f"{ALPACA_ROUTER_PREFIX}/account/configurations?suspend_trade=true"),
        ("post", f"{ALPACA_ROUTER_PREFIX}/watchlists?name=blocked&symbols=AAPL"),
        ("put", f"{ALPACA_ROUTER_PREFIX}/watchlists/wl-1?name=blocked"),
        ("delete", f"{ALPACA_ROUTER_PREFIX}/watchlists/wl-1"),
        ("post", f"{ALPACA_ROUTER_PREFIX}/watchlists/wl-1/assets?symbol=AAPL"),
        ("delete", f"{ALPACA_ROUTER_PREFIX}/watchlists/wl-1/assets/AAPL"),
    ],
)
def test_authz_trader_without_trading_capability_cannot_mutate_alpaca_account_state(
    client, test_registry, _authz_clients, method: str, path: str
) -> None:
    """Trader-role clients without ``trading: true`` must not mutate Alpaca state
    through account configuration or watchlist endpoints either."""
    provider = _stub_alpaca_account_mutation_provider(test_registry)

    response = getattr(client, method)(path, headers=_authz_headers(_AUTHZ_TRADER_NO_CAP_KEY))

    assert response.status_code == 403, response.text
    body = response.json()
    assert body["error"]["code"] == "GW-E2009"
    provider.set_account_configurations.assert_not_called()
    provider.create_watchlist.assert_not_called()
    provider.update_watchlist.assert_not_called()
    provider.delete_watchlist.assert_not_called()
    provider.add_asset_to_watchlist.assert_not_called()
    provider.remove_asset_from_watchlist.assert_not_called()


def test_authz_trader_without_trading_capability_can_create_order_with_cap(
    client, test_registry, _authz_clients
) -> None:
    """The granted client carries ``trading: true`` and CAN still place an
    order under the new capability gate — proving the gate admits, not just
    denies."""
    _stub_trading_provider(test_registry)

    response = client.post(
        f"{ALPACA_ROUTER_PREFIX}/orders?symbol=AAPL&side=buy&qty=1",
        headers=_authz_headers(_AUTHZ_GRANTED_KEY),
    )

    assert response.status_code == 200, response.text
    assert response.json()["data"]["id"] == "ord-m04"


def test_real_config_grants_trading_capability_to_order_placing_clients() -> None:
    """The live clients.yaml must grant ``trading: true`` to exactly the
    order-placing systems that route orders through the gateway
    (kairos, cerberus, 3roses, orion) and withhold it from EVERY other client.

    orion was granted the capability (commit 709fe35) because it places orders
    through the gateway. drogon trades direct-to-Alpaca, not via the gateway,
    so it stays withheld here. This asserts the FULL parsed client map, so any
    future client that gains trading must be added to ``trading_granted``
    consciously — otherwise this test fails, which is the intended tripwire."""
    from gateway.core.auth import ClientAuthenticator

    config_path = Path(__file__).resolve().parents[1] / "config" / "clients.yaml"
    auth = ClientAuthenticator(config_path)

    trading_granted = {"kairos", "cerberus", "3roses", "orion"}

    actual: dict[str, bool] = {}
    for client_id in auth.list_client_ids():
        c = auth.get_client(client_id)
        assert c is not None, client_id
        actual[client_id] = c.permissions.trading

    expected = {client_id: (client_id in trading_granted) for client_id in actual}
    assert actual == expected, f"trading capability drift vs config: {actual}"


def test_real_config_restricts_plaintext_to_powerless_test_client() -> None:
    """The dev/test client stays enabled for fixtures but must be powerless, and
    every real client must use a hashed key.

    The test client is the credential the test suite authenticates with (see
    conftest), so it cannot be disabled here. The security invariant that
    matters is that it carries no trading/admin capability and that the test
    client is the *only* client permitted to use a plaintext key.
    """
    from gateway.core.auth import ClientAuthenticator

    config_path = Path(__file__).resolve().parents[1] / "config" / "clients.yaml"
    auth = ClientAuthenticator(config_path)

    test_client = auth.get_client("test")
    assert test_client is not None
    assert test_client.role == "client"  # not trader/admin/super_admin
    assert test_client.permissions.trading is False
    # Only the powerless test client may use a plaintext key; all real clients hashed.
    assert set(auth._plaintext_keys.values()) <= {"test"}


# ---------------------------------------------------------------------------
# Per-client order ownership isolation (BLOCKER 1 + BLOCKER 2).
#
# Multiple Empire trading systems (Cerberus, 3Roses, Kairos, Orion, Atlas,
# Orbit) share a SINGLE upstream Alpaca account. Without per-client
# isolation:
#   - any trading client could list, mutate, or cancel any other client's
#     open orders against the shared broker account (BLOCKER 1);
#   - any client could collide with, probe, or replay another client's
#     ``client_order_id`` (BLOCKER 2).
#
# The fix prefixes every effective ``client_order_id`` going to Alpaca with
# ``c-{client.id}-`` and uses that prefix as the ownership marker on every
# subsequent lookup / mutation. The tests below pin the contract.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_order_prefixes_auto_generated_client_order_id_with_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto-generated ``client_order_id`` must carry the caller's
    ownership prefix ``c-{client.id}-``. Without this, multiple gateway
    clients sharing an upstream Alpaca account cannot be told apart on
    list/by_client_order_id/get_order lookups."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "owned-1"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.create_order(
        symbol="AAPL",
        side="buy",
        qty=10,
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert captured["client_order_id"].startswith("c-cerberus-dg-")
    assert response["meta"]["client_order_id"].startswith("c-cerberus-dg-")
    assert response["meta"]["client_order_id_source"] == "gateway"


@pytest.mark.asyncio
async def test_create_order_prefixes_caller_supplied_client_order_id_with_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Caller-supplied ``client_order_id`` must be transparently prefixed
    with ``c-{client.id}-``. The caller sees the FULL prefixed value in
    ``meta`` so they know what to retry with."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "owned-2"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.create_order(
        symbol="AAPL",
        side="buy",
        qty=10,
        client_order_id="my-key-abc",
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert captured["client_order_id"] == "c-cerberus-my-key-abc"
    assert response["meta"]["client_order_id"] == "c-cerberus-my-key-abc"
    assert response["meta"]["client_order_id_source"] == "caller"


@pytest.mark.asyncio
async def test_create_order_rejects_caller_key_that_overflows_post_prefix_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per-caller key budget is ``128 - len(c-{client.id}-)``. A caller
    key that fits in 128 chars on its own but would push the prefixed
    string past 128 must be rejected with 400 GW-E4006, and the error
    message must reference the per-client budget so the caller knows
    how many chars they have to work with."""
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    # client.id "cerberus" → prefix "c-cerberus-" is 11 chars → budget = 117.
    over_budget_caller_key = "x" * 118

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            qty=10,
            client_order_id=over_budget_caller_key,
            client=cast(Any, SimpleNamespace(id="cerberus")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["code"] == "GW-E4006"
    # Message MUST tell the caller what their per-client budget actually
    # is, otherwise they'd see "128" and be confused why a 118-char key
    # was rejected.
    assert "per-client budget" in detail["message"]
    assert "117" in detail["message"]
    assert "'c-cerberus-'" in detail["message"]


@pytest.mark.asyncio
async def test_get_orders_filters_out_orders_not_owned_by_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``get_orders`` must filter out orders that don't carry the caller's
    ownership prefix. Without this filter, every client of the shared
    Alpaca account would see (and could enumerate) every other client's
    open orders."""

    class _MixedOwnersProvider(_FakeProvider):
        def get_orders(self, **kwargs: Any) -> list[dict[str, Any]]:
            return [
                # Caller's own order.
                {"id": "mine-1", "client_order_id": "c-cerberus-key1"},
                # Foreign client's order — must be filtered out.
                {"id": "atlas-1", "client_order_id": "c-atlas-key1"},
                # Foreign client's order with the gateway auto-prefix shape.
                {"id": "3roses-1", "client_order_id": "c-3roses-dg-abc"},
                # Order without any client_order_id (e.g. placed outside
                # the gateway) — must also be filtered out.
                {"id": "rogue-1", "client_order_id": None},
                # Empty client_order_id.
                {"id": "rogue-2", "client_order_id": ""},
                # Another of caller's own.
                {"id": "mine-2", "client_order_id": "c-cerberus-key2"},
            ]

    provider = _MixedOwnersProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.get_orders(
        status="open",
        limit=50,
        direction="desc",
        symbols=None,
        nested=True,
        side=None,
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    ids = {order["id"] for order in response["data"]}
    assert ids == {"mine-1", "mine-2"}, f"expected only cerberus-owned orders, got {ids}"
    assert response["meta"]["count"] == 2


@pytest.mark.asyncio
async def test_get_order_by_client_order_id_with_foreign_prefix_returns_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``GET /orders:by_client_order_id`` MUST return 404 (NOT 403) for a
    lookup whose supplied key doesn't carry the caller's ownership
    prefix. A 403 would confirm the key exists at Alpaca, enabling
    enumeration of foreign client keys. The provider's
    ``get_order_by_client_id`` must NOT be called."""
    provider_call_count = {"n": 0}

    class _CountingProvider(_FakeProvider):
        def get_order_by_client_id(self, client_order_id: str) -> dict[str, Any]:
            provider_call_count["n"] += 1
            return {"id": "should-not-be-returned", "client_order_id": client_order_id}

    provider = _CountingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.get_order_by_client_id(
            client_order_id="c-atlas-key1",  # foreign-prefixed key
            client=cast(Any, SimpleNamespace(id="cerberus")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 404
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["code"] == "GW-E4404"
    # Detail must NOT echo the supplied key back — that's an enumeration
    # signal we deliberately suppress.
    assert "c-atlas-key1" not in str(detail)
    # The provider was never called — ownership check rejected before
    # any upstream Alpaca round-trip.
    assert provider_call_count["n"] == 0


@pytest.mark.asyncio
async def test_get_order_for_foreign_owned_order_returns_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``GET /orders/{order_id}`` fetches the order from Alpaca, inspects
    the returned ``client_order_id`` for the caller's ownership prefix,
    and returns 404 if absent. (Option A — stateless verification.) A
    403 would confirm the order exists at Alpaca, enabling enumeration."""

    class _ForeignOrderProvider(_FakeProvider):
        def get_order(self, order_id: str) -> dict[str, Any]:
            # Order exists at Alpaca but belongs to a foreign client.
            return {"id": order_id, "client_order_id": "c-atlas-key1"}

    provider = _ForeignOrderProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.get_order(
            order_id="foreign-1",
            client=cast(Any, SimpleNamespace(id="cerberus")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 404
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["code"] == "GW-E4404"
    assert detail["order_id"] == "foreign-1"


@pytest.mark.asyncio
async def test_replace_order_for_foreign_owned_order_returns_404_and_skips_replace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``PATCH /orders/{order_id}`` MUST verify ownership BEFORE the
    Alpaca SDK ``replace_order_by_id`` call fires. A foreign-owned
    order must return 404, and the SDK's ``replace_order`` must NOT be
    invoked (no Alpaca side-effect against another client's order)."""
    replace_calls: list[str] = []

    class _ForeignOrderProvider(_FakeProvider):
        def get_order(self, order_id: str) -> dict[str, Any]:
            return {"id": order_id, "client_order_id": "c-3roses-key1"}

        def replace_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
            replace_calls.append(order_id)
            return {"id": "MUST-NOT-RETURN"}

    provider = _ForeignOrderProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.replace_order(
            order_id="foreign-2",
            qty=20,
            client=cast(Any, SimpleNamespace(id="cerberus")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 404
    assert exc.value.detail["code"] == "GW-E4404"
    # CRITICAL: no upstream replace was attempted against the foreign
    # client's order.
    assert replace_calls == [], f"replace_order MUST NOT fire for a foreign-owned order — saw calls: {replace_calls}"


@pytest.mark.asyncio
async def test_cancel_order_for_foreign_owned_order_returns_404_and_skips_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``DELETE /orders/{order_id}`` MUST verify ownership BEFORE the
    Alpaca SDK ``cancel_order_by_id`` fires. A foreign-owned order must
    return 404 and the upstream cancel must NOT be invoked."""
    cancel_calls: list[str] = []

    class _ForeignOrderProvider(_FakeProvider):
        def get_order(self, order_id: str) -> dict[str, Any]:
            return {"id": order_id, "client_order_id": "c-kairos-key1"}

        def cancel_order(self, order_id: str) -> bool:
            cancel_calls.append(order_id)
            return True

    provider = _ForeignOrderProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.cancel_order(
            order_id="foreign-3",
            client=cast(Any, SimpleNamespace(id="cerberus")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 404
    assert exc.value.detail["code"] == "GW-E4404"
    # CRITICAL: no upstream cancel was attempted against the foreign
    # client's order.
    assert cancel_calls == [], f"cancel_order MUST NOT fire for a foreign-owned order — saw calls: {cancel_calls}"


@pytest.mark.asyncio
async def test_cross_client_collision_attempt_yields_distinct_orders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Client B reusing client A's caller-key MUST result in a DIFFERENT
    wire-level ``client_order_id`` and therefore a separate order at
    Alpaca — never a silent short-circuit to A's existing order. The
    ownership prefix is the load-bearing isolation mechanism."""
    captured_for_a: dict[str, Any] = {}
    captured_for_b: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def __init__(self) -> None:
            super().__init__()
            self._call_n = 0

        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            self._call_n += 1
            if self._call_n == 1:
                captured_for_a.update(kwargs)
                return {"id": "alpaca-side-order-A", "client_order_id": kwargs["client_order_id"]}
            captured_for_b.update(kwargs)
            return {"id": "alpaca-side-order-B", "client_order_id": kwargs["client_order_id"]}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    shared_caller_key = "logical-order-42"

    # Client A places an order with key "logical-order-42".
    await trading.create_order(
        symbol="AAPL",
        side="buy",
        qty=10,
        client_order_id=shared_caller_key,
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    # Client B tries to reuse client A's key.
    await trading.create_order(
        symbol="AAPL",
        side="buy",
        qty=10,
        client_order_id=shared_caller_key,
        client=cast(Any, SimpleNamespace(id="atlas")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert captured_for_a["client_order_id"] == "c-cerberus-logical-order-42"
    assert captured_for_b["client_order_id"] == "c-atlas-logical-order-42"
    # Distinct keys → distinct orders at Alpaca (no collision / short-circuit).
    assert captured_for_a["client_order_id"] != captured_for_b["client_order_id"]


@pytest.mark.asyncio
async def test_foreign_order_access_attempt_logs_structured_audit_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Every cross-client order-access attempt must emit a structured
    WARNING with code ``GW-A4001`` carrying both ``client_id_attempted``
    (who tried) and ``order_owner_actual`` (whose order it really was).
    Operators alert on this — a non-zero rate is either misconfiguration
    or active enumeration."""
    import logging

    class _ForeignOrderProvider(_FakeProvider):
        def get_order(self, order_id: str) -> dict[str, Any]:
            return {"id": order_id, "client_order_id": "c-atlas-secret-key"}

    provider = _ForeignOrderProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with caplog.at_level(logging.WARNING, logger="data-gateway"):
        with pytest.raises(HTTPException) as exc:
            await trading.get_order(
                order_id="foreign-audit-1",
                client=cast(Any, SimpleNamespace(id="cerberus")),
                registry=cast(ProviderRegistry, route_registry),
            )
        assert exc.value.status_code == 404

    # Find the audit record. structlog renders structured fields into the
    # log message; we assert on substrings of the rendered JSON.
    audit_records = [rec for rec in caplog.records if "alpaca_trading_foreign_order_access_attempt" in rec.getMessage()]
    assert audit_records, (
        f"expected at least one alpaca_trading_foreign_order_access_attempt WARNING, "
        f"got: {[r.getMessage() for r in caplog.records]}"
    )

    msg = audit_records[0].getMessage()
    # All load-bearing fields must be present in the structured log.
    assert "GW-A4001" in msg
    assert "client_id_attempted" in msg and "cerberus" in msg
    assert "order_owner_actual" in msg and "atlas" in msg
    assert "get_order" in msg


@pytest.mark.asyncio
async def test_foreign_order_access_via_by_client_order_id_logs_audit_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The audit-log requirement extends to ``GET /orders:by_client_order_id``
    too — a foreign-prefixed key lookup must emit GW-A4001 with
    ``client_id_attempted`` and ``order_owner_actual`` parsed from the
    supplied key's prefix."""
    import logging

    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with caplog.at_level(logging.WARNING, logger="data-gateway"):
        with pytest.raises(HTTPException) as exc:
            await trading.get_order_by_client_id(
                client_order_id="c-orion-other-key",
                client=cast(Any, SimpleNamespace(id="cerberus")),
                registry=cast(ProviderRegistry, route_registry),
            )
        assert exc.value.status_code == 404

    audit_records = [rec for rec in caplog.records if "alpaca_trading_foreign_order_access_attempt" in rec.getMessage()]
    assert audit_records, "expected GW-A4001 audit warning for foreign by_client_order_id lookup"
    msg = audit_records[0].getMessage()
    assert "GW-A4001" in msg
    assert "client_id_attempted" in msg and "cerberus" in msg
    assert "order_owner_actual" in msg and "orion" in msg
    assert "get_order_by_client_id" in msg


@pytest.mark.asyncio
async def test_get_order_for_owned_order_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sanity counter-test for the foreign-owner branches above: an order
    owned by the caller must round-trip without 404 from the ownership
    check. Without this counter-test a regression that returns 404 for
    every order would also pass the foreign-owner tests above."""

    class _OwnedProvider(_FakeProvider):
        def get_order(self, order_id: str) -> dict[str, Any]:
            return {"id": order_id, "client_order_id": "c-cerberus-key1", "symbol": "AAPL"}

    provider = _OwnedProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.get_order(
        order_id="mine-1",
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert response["success"] is True
    assert response["data"]["id"] == "mine-1"
    assert response["data"]["client_order_id"] == "c-cerberus-key1"


@pytest.mark.asyncio
async def test_unsafe_client_id_raises_500_before_calling_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defense in depth: if a malformed client.id ever reaches the trading
    router (config drift / future bypass), the ownership-prefix builder
    must raise 500 GW-E5008 — NOT silently emit a malformed
    ``client_order_id`` that Alpaca rejects later in the call."""
    call_count = {"n": 0}

    class _CountingProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            call_count["n"] += 1
            return {"id": "should-not-return"}

    provider = _CountingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            qty=10,
            client=cast(Any, SimpleNamespace(id="bad:id:with:colons")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 500
    assert exc.value.detail["code"] == "GW-E5008"
    assert call_count["n"] == 0


# ---------------------------------------------------------------------------
# Codex review follow-ups (CRITICAL/HIGH/MEDIUM findings).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_order_idempotent_retry_does_not_double_prefix_already_owned_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CRITICAL retry contract: when a caller retries with the FULL
    prefixed key they received in meta.client_order_id (as documented),
    the gateway must NOT double-prefix to ``c-cerberus-c-cerberus-...``
    — that would defeat Alpaca-side dedup and double-place on retry."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "ok"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    # Caller retries with the same full key they got back in meta on the
    # first attempt.
    already_owned = "c-cerberus-original-key-abc"

    response = await trading.create_order(
        symbol="AAPL",
        side="buy",
        qty=10,
        client_order_id=already_owned,
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    # NO double-prefix — provider sees the verbatim key.
    assert captured["client_order_id"] == "c-cerberus-original-key-abc"
    assert response["meta"]["client_order_id"] == "c-cerberus-original-key-abc"


@pytest.mark.asyncio
async def test_replace_order_idempotent_retry_does_not_double_prefix_already_owned_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same CRITICAL contract for replace_order — PATCH retries must
    forward the prefixed key as-is so Alpaca-side dedup works."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def get_order(self, order_id: str) -> dict[str, Any]:
            # Order owned by cerberus so the pre-check passes.
            return {"id": order_id, "client_order_id": f"c-cerberus-{order_id}", "symbol": "AAPL"}

        def replace_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "ok"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    already_owned = "c-cerberus-replace-key-xyz"
    await trading.replace_order(
        order_id="orig-1",
        qty=5,
        client_order_id=already_owned,
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert captured["client_order_id"] == "c-cerberus-replace-key-xyz"


@pytest.mark.asyncio
async def test_create_order_rejects_foreign_prefixed_caller_key(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """CRITICAL: caller B passing a foreign ``c-A-...`` key must be
    rejected (400 GW-E4007) — silently rewriting the prefix would mask
    cross-client replay attempts. The provider's ``create_order`` must
    NOT be called."""
    import logging

    create_calls: list[dict[str, Any]] = []

    class _CountingProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            create_calls.append(kwargs)
            return {"id": "MUST-NOT-RETURN"}

    provider = _CountingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with caplog.at_level(logging.WARNING, logger="data-gateway"), pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            qty=10,
            client_order_id="c-atlas-replay-key",  # foreign prefix
            client=cast(Any, SimpleNamespace(id="cerberus")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["code"] == "GW-E4007"
    # No upstream call — defense in depth before Alpaca ever sees the key.
    assert create_calls == []
    # Audit log emitted as GW-A4001.
    audit_records = [r for r in caplog.records if "GW-A4001" in r.getMessage()]
    assert audit_records, "expected GW-A4001 audit warning for foreign-prefix POST attempt"


@pytest.mark.asyncio
async def test_replace_order_rejects_foreign_prefixed_caller_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same foreign-prefix rejection for PATCH — but the ownership
    pre-check runs FIRST so a foreign caller_order_id only matters
    if the order_id itself was owned by the caller (which the
    pre-check verifies). We exercise the OWNED-order case here so the
    foreign-key branch in _validate_client_order_id is the one that
    fires."""
    replace_calls: list[Any] = []

    class _CountingProvider(_FakeProvider):
        def get_order(self, order_id: str) -> dict[str, Any]:
            return {"id": order_id, "client_order_id": f"c-cerberus-{order_id}", "symbol": "AAPL"}

        def replace_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
            replace_calls.append((order_id, kwargs))
            return {"id": "MUST-NOT-RETURN"}

    provider = _CountingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.replace_order(
            order_id="orig-foreign-key-1",
            qty=10,
            client_order_id="c-atlas-replay",  # foreign-prefixed
            client=cast(Any, SimpleNamespace(id="cerberus")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "GW-E4007"
    # No upstream replace_order call.
    assert replace_calls == []


@pytest.mark.asyncio
async def test_cancel_all_orders_only_cancels_owned_orders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CRITICAL: ``DELETE /orders`` MUST NOT call the bulk
    ``cancel_all_orders()`` SDK method — that would mass-cancel every
    other gateway client's open orders on the shared Alpaca account.
    Instead, list open orders, filter to those owned by the caller,
    cancel each one individually."""
    bulk_cancel_calls = {"n": 0}
    per_order_cancels: list[str] = []

    class _MixedOwnersProvider(_FakeProvider):
        def get_orders(self, **kwargs: Any) -> list[dict[str, Any]]:
            return [
                {"id": "mine-1", "client_order_id": "c-cerberus-key1", "symbol": "AAPL"},
                {"id": "atlas-1", "client_order_id": "c-atlas-key1", "symbol": "AAPL"},  # foreign
                {"id": "mine-2", "client_order_id": "c-cerberus-key2", "symbol": "TSLA"},
                {"id": "rogue-1", "client_order_id": None, "symbol": "AAPL"},  # un-owned
                {"id": "3roses-1", "client_order_id": "c-3roses-key1", "symbol": "TSLA"},  # foreign
            ]

        def cancel_all_orders(self) -> list[dict[str, Any]]:
            # If this fires, the test FAILS — that's the
            # mass-cancel-everyone bug.
            bulk_cancel_calls["n"] += 1
            return []

        def cancel_order(self, order_id: str) -> bool:
            per_order_cancels.append(order_id)
            return True

    provider = _MixedOwnersProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    # The bulk SDK method MUST NOT have been called.
    assert bulk_cancel_calls["n"] == 0
    # Only the owned orders were cancelled — never atlas-1, 3roses-1, or rogue-1.
    assert sorted(per_order_cancels) == ["mine-1", "mine-2"]
    assert response["meta"]["count"] == 2
    assert response["meta"]["owned_count"] == 2
    assert response["meta"]["errors"] == []


@pytest.mark.asyncio
async def test_cancel_all_orders_with_no_owned_orders_yields_empty_cancels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When NONE of the open orders belong to the caller, the bulk
    cancel must be a no-op — neither the bulk SDK nor per-order
    cancels fire. The previous implementation would have cancelled
    every order in the shared account."""
    bulk_cancel_calls = {"n": 0}
    per_order_cancels: list[str] = []

    class _AllForeignProvider(_FakeProvider):
        def get_orders(self, **kwargs: Any) -> list[dict[str, Any]]:
            return [
                {"id": "atlas-1", "client_order_id": "c-atlas-key1", "symbol": "AAPL"},
                {"id": "3roses-1", "client_order_id": "c-3roses-key1", "symbol": "TSLA"},
            ]

        def cancel_all_orders(self) -> list[dict[str, Any]]:
            bulk_cancel_calls["n"] += 1
            return []

        def cancel_order(self, order_id: str) -> bool:
            per_order_cancels.append(order_id)
            return True

    provider = _AllForeignProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert bulk_cancel_calls["n"] == 0
    assert per_order_cancels == []
    assert response["data"] == []
    assert response["meta"]["count"] == 0
    assert response["meta"]["owned_count"] == 0


@pytest.mark.asyncio
async def test_cancel_all_orders_raises_502_when_broker_list_is_not_a_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed (non-list) broker response must fail loudly, not be
    silently treated as zero owned orders. This is a bulk-cancel
    reconciliation path — a caller reading ``{"success": true,
    "owned_count": 0}`` after a corrupted broker response would wrongly
    conclude nothing was open, when the truth is unknown."""
    bulk_cancel_calls = {"n": 0}

    class _NonListOrdersProvider(_FakeProvider):
        def get_orders(self, **_kwargs: Any) -> Any:
            return {"unexpected": "shape"}

        def cancel_all_orders(self) -> list[dict[str, Any]]:
            bulk_cancel_calls["n"] += 1
            return []

    provider = _NonListOrdersProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.cancel_all_orders(
            client=cast(Any, SimpleNamespace(id="cerberus")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == HTTP_502_BAD_GATEWAY
    assert exc.value.detail["code"] == "GW-E5009"
    # Never falls back to the account-wide bulk cancel, and never reaches
    # the per-order cancel path either (the actual write path this route
    # uses — see the route docstring: bulk `cancel_all_orders()` is NEVER
    # called from this endpoint).
    assert bulk_cancel_calls["n"] == 0
    assert provider.cancel_calls == []


# ---------------------------------------------------------------------------
# DELETE /orders must run through the SAME per-symbol fence as
# DELETE /orders/{id}. The bulk route previously called
# provider.cancel_order() straight from asyncio.gather — a reachable
# unfenced broker-write path on the shared Alpaca account.
#
# The fence is per SYMBOL, so orders on one symbol serialize under a single
# fence while different symbols still cancel concurrently.
# ---------------------------------------------------------------------------


class _FenceRecordingGuard(_AllowAllOwnershipGuard):
    """Ownership guard that records the fence lifecycle for assertions."""

    def __init__(
        self,
        *,
        unacquirable: frozenset[str] = frozenset(),
        unauthorized: frozenset[str] = frozenset(),
        freeze_failure: Exception | None = None,
    ) -> None:
        self.events: list[tuple[str, Any]] = []
        self.held: set[str] = set()
        self.acquisitions: list[str] = []
        self.max_symbols_held = 0
        self.mutations: list[str] = []
        self.completed: list[str] = []
        self.frozen: dict[str, str] = {}
        self.renewals = 0
        self._unacquirable = unacquirable
        self._unauthorized = unauthorized
        self._freeze_failure = freeze_failure

    async def acquire_fence(self, symbol: str) -> str:
        if symbol in self._unacquirable:
            self.events.append(("fence_denied", symbol))
            raise OwnershipConflict(f"concurrent_gateway_reconciliation:{symbol}")
        assert symbol not in self.held, f"fence for {symbol} held twice at once"
        self.held.add(symbol)
        self.max_symbols_held = max(self.max_symbols_held, len(self.held))
        self.acquisitions.append(symbol)
        self.events.append(("acquire", symbol))
        return f"fence-{symbol}"

    async def renew_fence(self, symbol: str, _token: str) -> None:
        if symbol not in self.held:
            raise OwnershipConflict(f"gateway_fence_lost:{symbol}")
        self.renewals += 1
        self.events.append(("renew", symbol))

    async def release_fence(self, symbol: str, _token: str) -> None:
        self.held.discard(symbol)
        self.events.append(("release", symbol))

    async def authorize_submission(self, **kwargs: Any) -> None:
        symbol = kwargs["symbol"]
        if symbol in self._unauthorized:
            raise OwnershipConflict(f"claim_frozen_after_ambiguous_broker_mutation:{symbol}")

    async def begin_mutation(self, **kwargs: Any) -> str:
        self.mutations.append(kwargs["symbol"])
        self.events.append(("begin_mutation", kwargs["symbol"]))
        return f"cancel_all_orders:{kwargs['symbol']}"

    async def complete_mutation(self, **kwargs: Any) -> None:
        self.completed.append(kwargs["symbol"])
        self.events.append(("complete_mutation", kwargs["symbol"]))

    async def freeze(self, symbol: str, reason: str) -> None:
        if self._freeze_failure is not None:
            self.events.append(("freeze_failed", symbol))
            raise self._freeze_failure
        self.frozen[symbol] = reason
        self.events.append(("freeze", symbol))


class _FencedCancelProvider(_FakeProvider):
    """Provider that flags any cancel reaching the broker without a held fence."""

    def __init__(
        self,
        orders: list[dict[str, Any]],
        guard: _FenceRecordingGuard,
        *,
        failures: dict[str, HTTPException] | None = None,
    ) -> None:
        super().__init__()
        self._orders = orders
        self._guard = guard
        self._failures = failures or {}
        self._symbol_by_id = {order["id"]: order.get("symbol") for order in orders}
        self.bulk_cancel_calls = 0
        self.cancelled: list[str] = []
        self.unfenced_cancels: list[str] = []

    def get_orders(self, **_kwargs: Any) -> list[dict[str, Any]]:
        return list(self._orders)

    def cancel_all_orders(self) -> list[dict[str, Any]]:
        self.bulk_cancel_calls += 1
        return []

    def cancel_order(self, order_id: str) -> bool:
        if self._symbol_by_id.get(order_id) not in self._guard.held:
            self.unfenced_cancels.append(order_id)
        self._guard.events.append(("cancel", order_id))
        self.cancelled.append(order_id)
        failure = self._failures.get(order_id)
        if failure is not None:
            raise failure
        return True


def _record_reconciliation(monkeypatch: pytest.MonkeyPatch, guard: _FenceRecordingGuard) -> None:
    async def _reconcile(_provider: Any, symbol: str) -> BrokerSymbolState:
        guard.events.append(("reconcile", symbol))
        return BrokerSymbolState(has_position=False, order_owners=frozenset())

    monkeypatch.setattr(trading, "_reconcile_broker_symbol_state", _reconcile)


@pytest.mark.asyncio
async def test_cancel_all_orders_fences_every_symbol_before_cancelling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each symbol in the batch runs the full fenced protocol: fence →
    uncached broker reconciliation → claim → mutation marker → broker
    write → post-write reconciliation → marker clear → fence release."""
    guard = _FenceRecordingGuard()
    provider = _FencedCancelProvider(
        [
            {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
            {"id": "t-1", "client_order_id": _owned_coid("cerberus", "t-1"), "symbol": "TSLA"},
        ],
        guard,
    )
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.bulk_cancel_calls == 0
    assert provider.unfenced_cancels == []
    assert sorted(provider.cancelled) == ["a-1", "t-1"]
    assert sorted(guard.acquisitions) == ["AAPL", "TSLA"]
    assert sorted(guard.mutations) == ["AAPL", "TSLA"]
    assert sorted(guard.completed) == ["AAPL", "TSLA"]
    assert guard.frozen == {}
    assert response["meta"]["count"] == 2
    assert response["meta"]["errors"] == []

    aapl_events = [event for event in guard.events if event[1] in {"AAPL", "a-1"}]
    assert aapl_events == [
        ("acquire", "AAPL"),
        ("reconcile", "AAPL"),
        ("renew", "AAPL"),
        ("begin_mutation", "AAPL"),
        ("renew", "AAPL"),
        ("cancel", "a-1"),
        ("reconcile", "AAPL"),
        ("complete_mutation", "AAPL"),
        ("release", "AAPL"),
    ]


@pytest.mark.asyncio
async def test_cancel_all_orders_serializes_orders_on_the_same_symbol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two orders on ONE symbol must not both hold that symbol's fence.
    They run sequentially under a single acquisition — the guard asserts
    on any concurrent second acquisition of the same symbol."""
    guard = _FenceRecordingGuard()
    provider = _FencedCancelProvider(
        [
            {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
            {"id": "a-2", "client_order_id": _owned_coid("cerberus", "a-2"), "symbol": "AAPL"},
        ],
        guard,
    )
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert guard.acquisitions == ["AAPL"]
    assert guard.max_symbols_held == 1
    assert provider.unfenced_cancels == []
    # The fence lease is renewed immediately before EVERY broker cancel: one
    # renewal up front cannot cover a group that outruns the 120s lease.
    assert guard.events == [
        ("acquire", "AAPL"),
        ("reconcile", "AAPL"),
        ("renew", "AAPL"),
        ("begin_mutation", "AAPL"),
        ("renew", "AAPL"),
        ("cancel", "a-1"),
        ("renew", "AAPL"),
        ("cancel", "a-2"),
        ("reconcile", "AAPL"),
        ("complete_mutation", "AAPL"),
        ("release", "AAPL"),
    ]
    assert response["meta"]["count"] == 2


@pytest.mark.asyncio
async def test_cancel_all_orders_runs_different_symbols_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parallelism ACROSS symbols is deliberate — a sequential loop could
    run for minutes during an opening-bell mass cancel. The guard blocks
    each acquisition until both symbols have arrived, so this only
    completes if the symbol groups are genuinely concurrent."""

    class _RendezvousGuard(_FenceRecordingGuard):
        def __init__(self, parties: int) -> None:
            super().__init__()
            self._parties = parties
            self._arrived = 0
            self._all_arrived = asyncio.Event()

        async def acquire_fence(self, symbol: str) -> str:
            token = await super().acquire_fence(symbol)
            self._arrived += 1
            if self._arrived >= self._parties:
                self._all_arrived.set()
            await asyncio.wait_for(self._all_arrived.wait(), timeout=2.0)
            return token

    guard = _RendezvousGuard(2)
    provider = _FencedCancelProvider(
        [
            {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
            {"id": "t-1", "client_order_id": _owned_coid("cerberus", "t-1"), "symbol": "TSLA"},
        ],
        guard,
    )
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert guard.max_symbols_held == 2
    assert response["meta"]["count"] == 2
    assert response["meta"]["errors"] == []


@pytest.mark.asyncio
async def test_cancel_all_orders_fails_closed_on_blocked_symbol_and_cancels_the_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A symbol whose fence cannot be acquired — or whose claim is frozen —
    yields a per-order error and NEVER an unfenced cancel, while orders on
    other symbols still cancel."""
    guard = _FenceRecordingGuard(unacquirable=frozenset({"TSLA"}), unauthorized=frozenset({"MSFT"}))
    provider = _FencedCancelProvider(
        [
            {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
            {"id": "t-1", "client_order_id": _owned_coid("cerberus", "t-1"), "symbol": "TSLA"},
            {"id": "t-2", "client_order_id": _owned_coid("cerberus", "t-2"), "symbol": "TSLA"},
            {"id": "m-1", "client_order_id": _owned_coid("cerberus", "m-1"), "symbol": "MSFT"},
        ],
        guard,
    )
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.cancelled == ["a-1"]
    assert provider.unfenced_cancels == []
    assert response["meta"]["count"] == 1
    assert response["meta"]["owned_count"] == 4
    errors = response["meta"]["errors"]
    assert {error["order_id"] for error in errors} == {"t-1", "t-2", "m-1"}
    for error in errors:
        assert error["cancelled"] is False
        assert error["status_code"] == 409
        assert error["detail"]["code"] == "GW-E4301"


@pytest.mark.asyncio
async def test_cancel_all_orders_skips_orders_whose_symbol_cannot_be_fenced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No symbol means no fence, and no fence means no broker cancel."""
    guard = _FenceRecordingGuard()
    provider = _FencedCancelProvider(
        [
            {"id": "no-symbol", "client_order_id": _owned_coid("cerberus", "no-symbol")},
            {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
        ],
        guard,
    )
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.cancelled == ["a-1"]
    assert provider.unfenced_cancels == []
    assert guard.acquisitions == ["AAPL"]
    errors = response["meta"]["errors"]
    assert [error["order_id"] for error in errors] == ["no-symbol"]
    assert errors[0]["status_code"] == 409
    assert errors[0]["detail"]["code"] == "GW-E4301"


@pytest.mark.asyncio
async def test_cancel_all_orders_freezes_symbol_after_ambiguous_broker_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 5xx from the broker leaves the cancel outcome unknown: freeze the
    symbol before releasing its fence, skip the symbol's remaining orders,
    and never clear the mutation marker — other symbols keep cancelling."""
    guard = _FenceRecordingGuard()
    provider = _FencedCancelProvider(
        [
            {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
            {"id": "a-2", "client_order_id": _owned_coid("cerberus", "a-2"), "symbol": "AAPL"},
            {"id": "t-1", "client_order_id": _owned_coid("cerberus", "t-1"), "symbol": "TSLA"},
        ],
        guard,
        failures={"a-1": HTTPException(status_code=HTTP_503_SERVICE_UNAVAILABLE, detail="broker unavailable")},
    )
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    # a-2 is never attempted: AAPL is frozen mid-batch. Symbol groups run
    # concurrently, so only the set of cancels is deterministic.
    assert sorted(provider.cancelled) == ["a-1", "t-1"]
    assert provider.unfenced_cancels == []
    assert guard.frozen["AAPL"] == "broker_mutation_503"
    assert guard.completed == ["TSLA"]
    # A dispatched write whose outcome is unknown keeps its lease: the executor
    # thread can still reach Alpaca after the asyncio call gives up, so the
    # fence is left to expire rather than handed to the next client.
    assert ("release", "AAPL") not in guard.events
    assert ("release", "TSLA") in guard.events
    assert response["meta"]["count"] == 1
    errors = {error["order_id"]: error for error in response["meta"]["errors"]}
    assert set(errors) == {"a-1", "a-2"}
    assert errors["a-1"]["status_code"] == HTTP_503_SERVICE_UNAVAILABLE
    assert errors["a-2"]["status_code"] == 409


@pytest.mark.asyncio
async def test_cancel_all_orders_stops_the_group_when_the_fence_lease_is_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the lease is gone before the next cancel, another gateway client may
    already hold the symbol. The earlier cancel is ambiguous, so freeze and
    never issue the remaining writes."""

    class _LeaseExpiringGuard(_FenceRecordingGuard):
        async def renew_fence(self, symbol: str, token: str) -> None:
            if self.renewals >= 2:
                # Simulates the Redis lease expiring mid-group.
                self.held.discard(symbol)
            await super().renew_fence(symbol, token)

    guard = _LeaseExpiringGuard()
    provider = _FencedCancelProvider(
        [
            {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
            {"id": "a-2", "client_order_id": _owned_coid("cerberus", "a-2"), "symbol": "AAPL"},
        ],
        guard,
    )
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.cancelled == ["a-1"]
    assert provider.unfenced_cancels == []
    assert guard.frozen["AAPL"] == "gateway_fence_lost"
    assert guard.completed == []
    errors = response["meta"]["errors"]
    assert [error["order_id"] for error in errors] == ["a-2"]


@pytest.mark.asyncio
async def test_cancel_all_orders_freezes_when_the_request_is_cancelled_mid_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task cancellation after a broker write is ambiguous, not clean: the
    symbol must be frozen even though ``CancelledError`` is not an ``Exception``."""

    class _StallingGuard(_FenceRecordingGuard):
        def __init__(self) -> None:
            super().__init__()
            self.stalled = asyncio.Event()

        async def renew_fence(self, symbol: str, token: str) -> None:
            await super().renew_fence(symbol, token)
            if self.renewals == 3:
                self.stalled.set()
                await asyncio.sleep(3600)

    guard = _StallingGuard()
    provider = _FencedCancelProvider(
        [
            {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
            {"id": "a-2", "client_order_id": _owned_coid("cerberus", "a-2"), "symbol": "AAPL"},
        ],
        guard,
    )
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    task = asyncio.ensure_future(
        trading.cancel_all_orders(
            client=cast(Any, SimpleNamespace(id="cerberus")),
            registry=cast(ProviderRegistry, route_registry),
        )
    )
    await asyncio.wait_for(guard.stalled.wait(), timeout=2.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert provider.cancelled == ["a-1"]
    assert guard.frozen["AAPL"] == "broker_mutation_cancelled"
    # The executor thread can still complete the SDK call after the coroutine
    # dies, so the lease is held to expiry instead of being handed on.
    assert ("release", "AAPL") not in guard.events


@pytest.mark.asyncio
async def test_cancel_all_orders_revalidates_the_lease_after_upstream_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Renewing before the wait for an upstream Alpaca permit proves nothing:
    the wait is unbounded and the lease is only 120s. The lease must be
    revalidated AFTER admission, immediately before the broker write."""
    guard = _FenceRecordingGuard()
    provider = _FencedCancelProvider(
        [
            {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
        ],
        guard,
    )
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _lease_expiring_execute(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        if provider_call.__qualname__.endswith("cancel"):
            # Stands in for a long wait on the upstream permit during which
            # the Redis lease expires.
            guard.held.discard("AAPL")
            guard.events.append(("admitted", "AAPL"))
        return await provider_call(registry.get("alpaca"))

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _lease_expiring_execute)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.cancelled == []
    assert guard.frozen["AAPL"] == "gateway_fence_lost"
    assert ("admitted", "AAPL") in guard.events
    errors = response["meta"]["errors"]
    assert [error["order_id"] for error in errors] == ["a-1"]


@pytest.mark.asyncio
async def test_cancel_all_orders_fails_closed_when_one_order_id_spans_two_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broker listing that maps one order id to two canonical symbols would
    otherwise be cancelled twice, once under the wrong symbol's fence."""
    guard = _FenceRecordingGuard()
    provider = _FencedCancelProvider(
        [
            {"id": "dup-1", "client_order_id": _owned_coid("cerberus", "dup-1"), "symbol": "AAPL"},
            {"id": "dup-1", "client_order_id": _owned_coid("cerberus", "dup-1"), "symbol": "TSLA"},
            {"id": "m-1", "client_order_id": _owned_coid("cerberus", "m-1"), "symbol": "MSFT"},
        ],
        guard,
    )
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.cancelled == ["m-1"]
    assert guard.acquisitions == ["MSFT"]
    errors = response["meta"]["errors"]
    assert [error["order_id"] for error in errors] == ["dup-1", "dup-1"]
    assert all(error["status_code"] == 409 for error in errors)


@pytest.mark.asyncio
async def test_cancel_all_orders_reports_a_freeze_that_did_not_persist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never tell the caller a symbol is frozen when the store rejected the
    freeze — and hold the lease rather than releasing an unrecorded ambiguity."""
    guard = _FenceRecordingGuard(freeze_failure=OwnershipStoreUnavailable("redis_freeze_failed:AAPL"))
    provider = _FencedCancelProvider(
        [
            {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
            {"id": "a-2", "client_order_id": _owned_coid("cerberus", "a-2"), "symbol": "AAPL"},
        ],
        guard,
        failures={"a-1": HTTPException(status_code=HTTP_503_SERVICE_UNAVAILABLE, detail="broker unavailable")},
    )
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.cancelled == ["a-1"]
    assert guard.frozen == {}
    assert ("release", "AAPL") not in guard.events
    errors = {error["order_id"]: error for error in response["meta"]["errors"]}
    assert errors["a-2"]["status_code"] == HTTP_503_SERVICE_UNAVAILABLE
    assert errors["a-2"]["detail"]["code"] == "GW-E5301"


@pytest.mark.asyncio
async def test_cancel_all_orders_takes_an_upstream_permit_per_broker_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The upstream Alpaca permit is per broker call, not per symbol group.
    Holding one permit for a whole sequential group starves every other
    trading route behind a long mass-cancel."""
    depth = {"current": 0, "max": 0, "calls": 0}
    guard = _FenceRecordingGuard()
    provider = _FencedCancelProvider(
        [
            {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
            {"id": "a-2", "client_order_id": _owned_coid("cerberus", "a-2"), "symbol": "AAPL"},
        ],
        guard,
    )
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _counting_execute(
        *,
        registry: ProviderRegistry,
        provider_call: Any,
        block: bool = False,
        log_context: dict[str, Any] | None = None,
    ):
        depth["calls"] += 1
        depth["current"] += 1
        depth["max"] = max(depth["max"], depth["current"])
        try:
            return await provider_call(registry.get("alpaca"))
        finally:
            depth["current"] -= 1

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _counting_execute)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: guard)
    _record_reconciliation(monkeypatch, guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert response["meta"]["count"] == 2
    # list + pre-reconcile + 2 cancels + post-reconcile, each with its own permit.
    assert depth["calls"] == 5
    assert depth["max"] == 1


@pytest.mark.asyncio
async def test_cancel_all_orders_runs_the_real_ownership_guard_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive the real guard (NX fence, Lua-equivalent claim/marker scripts)
    rather than an allow-all fake: the claim survives, the mutation marker is
    cleared by its own token, and the fence key is gone afterwards."""
    redis = _MemoryRedis()
    ownership_guard = OrderOwnershipGuard(redis)

    class _RealGuardProvider(_FakeProvider):
        def __init__(self) -> None:
            super().__init__()
            self.cancelled: list[str] = []

        def get_orders(self, **_kwargs: Any) -> list[dict[str, Any]]:
            return [
                {"id": "a-1", "client_order_id": _owned_coid("cerberus", "a-1"), "symbol": "AAPL"},
                {"id": "a-2", "client_order_id": _owned_coid("cerberus", "a-2"), "symbol": "AAPL"},
            ]

        def cancel_order(self, order_id: str) -> bool:
            assert redis.values.get(ownership_guard.fence_key("AAPL")) is not None, "cancel ran without a fence"
            self.cancelled.append(order_id)
            return True

    provider = _RealGuardProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    monkeypatch.setattr(trading, "get_order_ownership_guard", lambda: ownership_guard)

    response = await trading.cancel_all_orders(
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.cancelled == ["a-1", "a-2"]
    assert response["meta"]["count"] == 2
    assert ownership_guard.fence_key("AAPL") not in redis.values
    claim = json.loads(redis.values[ownership_guard.claim_key("AAPL")])
    assert claim["owner"] == "cerberus"
    assert "mutation_pending" not in claim
    assert "frozen_reason" not in claim


@pytest.mark.asyncio
async def test_get_orders_applies_limit_after_filter_not_before(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HIGH: limit MUST apply AFTER ownership filtering. Pushing the
    limit upstream means the broker could return N foreign-client orders
    that get filtered out, leaving the caller with an empty list even
    if older OWNED orders exist — a reconciliation hazard.

    Verifies (a) the upstream call always requests the 500-order ceiling
    regardless of caller-supplied limit, and (b) the caller's limit is
    applied to the FILTERED list."""
    upstream_limits: list[int] = []

    class _LargeMixedProvider(_FakeProvider):
        def get_orders(self, **kwargs: Any) -> list[dict[str, Any]]:
            upstream_limits.append(kwargs.get("limit"))
            # First 5 are foreign (atlas), next 3 are owned (cerberus).
            return [{"id": f"atlas-{i}", "client_order_id": f"c-atlas-key{i}"} for i in range(5)] + [
                {"id": f"mine-{i}", "client_order_id": f"c-cerberus-key{i}"} for i in range(3)
            ]

    provider = _LargeMixedProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    # Caller asks for at most 5 orders. Pre-fix: provider would have
    # been called with limit=5 and returned only the 5 foreign atlas
    # orders — caller would see empty list. Post-fix: provider is
    # called with limit=500, gateway filters to owned, then truncates.
    response = await trading.get_orders(
        status="open",
        limit=5,
        direction="desc",
        symbols=None,
        nested=True,
        side=None,
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    # Upstream limit must be the broker's per-call ceiling.
    assert upstream_limits == [500], (
        f"upstream get_orders MUST request the 500-order ceiling regardless of caller's limit, got {upstream_limits}"
    )
    # 3 owned orders, capped at caller-limit=5 → all 3 returned.
    assert len(response["data"]) == 3
    assert {o["id"] for o in response["data"]} == {"mine-0", "mine-1", "mine-2"}


@pytest.mark.asyncio
async def test_get_orders_caller_limit_truncates_owned_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the caller's limit is SMALLER than the owned count, truncate
    the owned list at that limit. (Order is preserved from the upstream
    response — Alpaca's direction= parameter controls that.)"""

    class _ManyOwnedProvider(_FakeProvider):
        def get_orders(self, **kwargs: Any) -> list[dict[str, Any]]:
            return [{"id": f"mine-{i}", "client_order_id": f"c-cerberus-key{i}"} for i in range(10)]

    provider = _ManyOwnedProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.get_orders(
        status="open",
        limit=3,
        direction="desc",
        symbols=None,
        nested=True,
        side=None,
        client=cast(Any, SimpleNamespace(id="cerberus")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert len(response["data"]) == 3
    assert [o["id"] for o in response["data"]] == ["mine-0", "mine-1", "mine-2"]
    assert response["meta"]["count"] == 3


@pytest.mark.asyncio
async def test_audit_log_correctly_attributes_hyphenated_client_id(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """MEDIUM: ``heber-watch`` is a real configured client id with a
    hyphen. The naive first-hyphen split in
    ``_parse_owner_from_client_order_id`` mis-attributes
    ``c-heber-watch-abc`` to ``heber``. Fix uses longest-prefix-match
    against the authenticator's known client ids."""
    import logging

    # Patch the trading module's known-client-ids lookup to a
    # deterministic set including the hyphenated id (cerberus is the
    # caller; heber-watch is the foreign owner whose attribution we
    # want correct in the audit log).
    monkeypatch.setattr(
        trading,
        "_known_client_ids",
        lambda: ["cerberus", "atlas", "heber-watch", "heber"],
    )

    class _ForeignOrderProvider(_FakeProvider):
        def get_order(self, order_id: str) -> dict[str, Any]:
            return {"id": order_id, "client_order_id": "c-heber-watch-secret-key"}

    provider = _ForeignOrderProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with caplog.at_level(logging.WARNING, logger="data-gateway"), pytest.raises(HTTPException):
        await trading.get_order(
            order_id="foreign-hyphenated-1",
            client=cast(Any, SimpleNamespace(id="cerberus")),
            registry=cast(ProviderRegistry, route_registry),
        )

    audit_records = [r for r in caplog.records if "GW-A4001" in r.getMessage()]
    assert audit_records
    msg = audit_records[0].getMessage()
    # Owner is attributed to the FULL hyphenated id, not just ``heber``.
    assert '"order_owner_actual": "heber-watch"' in msg or "order_owner_actual=heber-watch" in msg, (
        f"expected order_owner_actual to be 'heber-watch' (longest-prefix-match), got: {msg}"
    )


@pytest.mark.asyncio
async def test_get_order_with_legacy_unprefixed_client_order_id_returns_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backward-compat: orders placed BEFORE this change shipped have
    ``client_order_id`` values without the ``c-{id}-`` prefix (e.g.
    ``dg-abc123`` or anything else the previous code emitted). The
    ownership check is fail-CLOSED — any order whose
    ``client_order_id`` doesn't carry the caller's prefix returns 404,
    so legacy orders are not visible via the new endpoints. This is
    intentional and the safest backward-compat stance: callers must
    reconcile their legacy orders out-of-band (Alpaca dashboard, direct
    SDK) until they re-place with the new prefixed scheme."""

    class _LegacyOrderProvider(_FakeProvider):
        def get_order(self, order_id: str) -> dict[str, Any]:
            # Pre-prefix legacy order — was placed by ``cerberus`` but
            # before the ownership-prefix scheme shipped.
            return {"id": order_id, "client_order_id": "dg-legacy-key-abc"}

    provider = _LegacyOrderProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.get_order(
            order_id="legacy-1",
            client=cast(Any, SimpleNamespace(id="cerberus")),
            registry=cast(ProviderRegistry, route_registry),
        )

    # Fail-closed: legacy orders are NOT readable via this endpoint
    # after the change ships. This is the documented backward-compat
    # stance — see PR body for the rationale.
    assert exc.value.status_code == 404
    assert exc.value.detail["code"] == "GW-E4404"


def test_account_wide_actions_require_super_admin() -> None:
    """Account-wide flatten / config require super_admin; per-symbol close stays at trader."""
    from gateway.api.deps import _enforce_account_wide_admin
    from gateway.core.auth import Client, ClientPermissions

    trader = Client(id="kairos", permissions=ClientPermissions(trading=True), role="trader")
    super_admin = Client(id="ops", permissions=ClientPermissions(trading=True), role="super_admin")

    # Trader is BLOCKED from account-wide actions.
    for method, path in [
        ("DELETE", "/api/v1/alpaca/positions"),  # close_all_positions
        ("PATCH", "/api/v1/alpaca/account/configurations"),
    ]:
        with pytest.raises(HTTPException) as exc:
            _enforce_account_wide_admin(method, path, trader)
        assert exc.value.status_code == 403

    # Trader CAN still close its own single position (not account-wide).
    _enforce_account_wide_admin("DELETE", "/api/v1/alpaca/positions/AAPL", trader)

    # super_admin passes the account-wide gate.
    _enforce_account_wide_admin("DELETE", "/api/v1/alpaca/positions", super_admin)
    _enforce_account_wide_admin("PATCH", "/api/v1/alpaca/account/configurations", super_admin)
