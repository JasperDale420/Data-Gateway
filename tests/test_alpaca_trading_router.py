from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import HTTPException
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE, HTTP_504_GATEWAY_TIMEOUT

from gateway.api.alpaca import trading
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
        lambda: Settings(alpaca_trading_call_timeout_seconds=0.5),
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
        lambda: Settings(alpaca_trading_call_timeout_seconds=0.5),
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
        lambda: Settings(alpaca_trading_call_timeout_seconds=0.5),
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
        lambda: Settings(alpaca_trading_call_timeout_seconds=0.5),
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
    assert "GET /api/alpaca/trading/positions/AAPL" in detail["retry_hint"]
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
