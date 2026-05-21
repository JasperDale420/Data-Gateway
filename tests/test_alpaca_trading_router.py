from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import HTTPException
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE, HTTP_504_GATEWAY_TIMEOUT

from gateway.api.alpaca import trading
from gateway.api.alpaca.common import ALPACA_ROUTER_PREFIX
from gateway.config import Settings
from gateway.core.cache import InMemoryCache
from gateway.core.registry import ProviderRegistry


@pytest.fixture(autouse=True)
def _reset_trading_inflight_sem():
    """Each test gets a fresh event loop — reset the lazy semaphore so it
    binds to the new loop (asyncio.Semaphore in 3.10+ is loop-bound on first
    use) and so per-test settings overrides take effect."""
    trading._reset_trading_inflight_sem_for_tests()
    yield
    trading._reset_trading_inflight_sem_for_tests()


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


class _FakeProvider:
    def __init__(self) -> None:
        self.orders_calls: list[dict[str, Any]] = []
        self.cancel_calls: list[str] = []
        self.calendar_calls: list[tuple[date | None, date | None]] = []
        self.assets_calls: list[dict[str, Any]] = []
        self.asset_calls: list[str] = []

    def get_account(self) -> dict[str, Any]:
        return {"status": "ACTIVE"}

    def create_order(self, **kwargs: Any) -> dict[str, Any]:
        return kwargs

    def get_orders(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.orders_calls.append(kwargs)
        return [{"id": "o-1"}, {"id": "o-2"}]

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
    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
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

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
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

    # Provider saw a generated key.
    assert captured["client_order_id"] is not None
    assert captured["client_order_id"].startswith("dg-")
    assert len(captured["client_order_id"]) == 3 + 32  # "dg-" + uuid4 hex
    # Caller sees the same key in meta so they know what to retry with.
    assert response["meta"]["client_order_id"] == captured["client_order_id"]
    assert response["meta"]["client_order_id_source"] == "gateway"


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

    assert captured["client_order_id"] == "caller-key-abc-123"
    assert response["meta"]["client_order_id"] == "caller-key-abc-123"
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

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
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
    assert detail["client_order_id"].startswith("dg-")
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

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
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
    assert detail["client_order_id"] == "caller-retry-key-xyz"
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
# client_order_id, so the retry contract surfaced in the 504 body must point
# the caller at GET /positions/<symbol> instead of a Alpaca-side dedup key.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_position_504_timeout_includes_get_position_retry_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Critical retry contract: when close_position times out, the caller
    needs the symbol and a pointer to GET /positions/<symbol> so they can
    resolve "did the close actually happen?" before retrying. Without this
    the caller is flying blind — they might leave a position open thinking
    the close failed, or double-close if the position was still partial."""

    class _SlowProvider:
        def close_position(self, symbol: str, qty: Any = None, percentage: Any = None) -> dict[str, Any]:
            import time

            time.sleep(0.6)
            return {"id": "should-not-return"}

    provider = _SlowProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
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
    assert detail["retry_with"] == "get_position"
    # The retry hint must point at the ACTUAL mounted prefix
    # (``/api/v1/alpaca``) — see test_retry_hint_uses_actual_router_prefix
    # for the static-vs-dynamic guard against drift.
    assert f"GET {ALPACA_ROUTER_PREFIX}/positions/AAPL" in detail["retry_hint"]
    assert "404 means" in detail["retry_hint"]


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
    assert captured["symbol"] == "msft"


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

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
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
    """Mirror of the create_order variant — the close_position hint must
    point at GET ``{ALPACA_ROUTER_PREFIX}/positions/<symbol>``."""

    class _SlowProvider:
        def close_position(self, symbol: str, qty: Any = None, percentage: Any = None) -> dict[str, Any]:
            import time

            time.sleep(0.6)
            return {"id": "should-not-return"}

    provider = _SlowProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
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
    """128-char key (the inclusive maximum) must pass through untouched —
    only longer keys are rejected."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "max-len-ok"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    max_len_key = "x" * 128

    response = await trading.create_order(
        symbol="AAPL",
        side="buy",
        qty=10,
        client_order_id=max_len_key,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert captured["client_order_id"] == max_len_key
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
    # Critical retry-contract assertion: caller's key is preserved on a
    # non-timeout 5xx so they can safely retry / verify.
    assert detail["client_order_id"] == "caller-non-timeout-key-1"
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
    assert detail["retry_with"] == "get_position"
    assert f"GET {ALPACA_ROUTER_PREFIX}/positions/MSFT" in detail["retry_hint"]


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
    assert detail["client_order_id"] == "http-layer-test-1"
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
    assert captured["client_order_id"].startswith("dg-")
    assert len(captured["client_order_id"]) == 3 + 32
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

    assert captured["client_order_id"] == "caller-replace-key-1"
    assert response["meta"]["client_order_id"] == "caller-replace-key-1"
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

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
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
    assert detail["client_order_id"].startswith("dg-")
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

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
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
    assert detail["client_order_id"] == "caller-replace-retry-key"
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
    """128-char key (inclusive max) passes through untouched."""
    captured: dict[str, Any] = {}

    class _CapturingProvider(_FakeProvider):
        def replace_order(self, order_id: str, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"id": "max-len-replace-ok"}

    provider = _CapturingProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    max_len_key = "y" * 128

    response = await trading.replace_order(
        order_id="orig-order-7",
        qty=10,
        client_order_id=max_len_key,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert captured["client_order_id"] == max_len_key
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
    assert detail["client_order_id"] == "caller-replace-non-timeout-1"
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

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
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

    test_registry.get.return_value.replace_order = _slow_replace

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
    assert detail["client_order_id"] == "http-layer-replace-1"
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
