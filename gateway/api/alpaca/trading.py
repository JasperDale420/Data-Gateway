"""Alpaca trading endpoints - orders, positions, account, assets, clock, calendar."""

import asyncio
import concurrent.futures
import uuid
from collections.abc import Callable
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE, HTTP_504_GATEWAY_TIMEOUT

from gateway.api.alpaca.common import (
    DESC_COMMA_SYMBOLS,
    Client,
    execute_alpaca_cached_call,
    execute_alpaca_provider_call,
    get_cache,
    get_registry,
    require_api_key,
)
from gateway.config import get_settings
from gateway.core.cache import HybridCache, InMemoryCache
from gateway.core.logger import logger
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()
TRADING_ASSETS_CACHE_TTL_SECONDS = 600
TRADING_CALENDAR_CACHE_TTL_SECONDS = 3600

# ─────────────────────────────────────────────────────────────────────────────
# Idempotency-key prefix for gateway-generated client_order_id values. Used to
# distinguish auto-generated keys from caller-supplied ones in logs/dashboards.
# Alpaca's client_order_id max length is 48 chars; "dg-" + 32-char UUID hex
# leaves room for the prefix without truncation.
# ─────────────────────────────────────────────────────────────────────────────
_GATEWAY_CLIENT_ORDER_ID_PREFIX = "dg-"


def _generate_client_order_id() -> str:
    """Generate a gateway-side client_order_id for order idempotency.

    Alpaca natively dedupes `submit_order` by client_order_id — if a call
    times out mid-flight (e.g. our 15s asyncio wait_for fires while the
    underlying executor thread is still talking to Alpaca), the caller can
    safely retry with the same client_order_id: Alpaca returns the existing
    order rather than placing a second one. The gateway returns the
    auto-generated key in the 504 response detail and in successful
    response meta so callers always know which key to retry with.
    """
    return f"{_GATEWAY_CLIENT_ORDER_ID_PREFIX}{uuid.uuid4().hex}"


# Module-level dedicated executor for trading calls (created lazily)
_trading_executor: concurrent.futures.ThreadPoolExecutor | None = None

# Bounded semaphore that caps concurrent in-flight trading calls. When all
# permits are held the next call fast-fails with 503 rather than queueing in
# the executor's unbounded internal queue. Queue pile-up during Alpaca
# slowdowns holds outstanding asyncio tasks/timers that contend with the
# WebSocket keepalive task for event-loop CPU, causing clients to disconnect
# with "keepalive ping timeout".
_trading_inflight_sem: asyncio.BoundedSemaphore | None = None


def _get_trading_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _trading_executor
    if _trading_executor is None:
        _trading_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=get_settings().alpaca_trading_thread_pool_size,
            thread_name_prefix="alpaca-trading",
        )
    return _trading_executor


def _get_trading_inflight_sem() -> asyncio.BoundedSemaphore:
    global _trading_inflight_sem
    if _trading_inflight_sem is None:
        _trading_inflight_sem = asyncio.BoundedSemaphore(get_settings().alpaca_trading_max_inflight)
    return _trading_inflight_sem


def _reset_trading_inflight_sem_for_tests() -> None:
    """Reset the in-flight semaphore so test-level settings overrides apply.

    Module-level lazy singletons don't observe monkeypatched settings after
    first initialisation; tests that tweak ``alpaca_trading_max_inflight``
    should call this between cases.
    """
    global _trading_inflight_sem
    _trading_inflight_sem = None


async def _execute_trading_call(
    *,
    registry: ProviderRegistry,
    provider_fn: Callable[[Any], Any],
    operation: str,
) -> Any:
    async def call(provider: Any) -> Any:
        return await _run_trading_provider_call(
            provider=provider,
            provider_fn=provider_fn,
            operation=operation,
        )

    call.__qualname__ = f"trading.{operation}"
    return await execute_alpaca_provider_call(
        registry=registry,
        provider_call=call,
    )


async def _execute_trading_cached_call(
    *,
    registry: ProviderRegistry,
    cache: InMemoryCache | HybridCache,
    cache_key: str,
    ttl: int,
    route_label: str,
    provider_fn: Callable[[Any], Any],
    operation: str,
) -> Any:
    async def call(provider: Any) -> Any:
        return await _run_trading_provider_call(
            provider=provider,
            provider_fn=provider_fn,
            operation=operation,
        )

    call.__qualname__ = f"trading.{operation}"
    return await execute_alpaca_cached_call(
        registry=registry,
        cache=cache,
        cache_key=cache_key,
        ttl=ttl,
        route_label=route_label,
        provider_call=call,
    )


async def _run_trading_provider_call(
    *,
    provider: Any,
    provider_fn: Callable[[Any], Any],
    operation: str,
    idempotency_context: dict[str, Any] | None = None,
) -> Any:
    """Run a single trading-SDK call with timeout + backpressure protection.

    Args:
        provider: Alpaca provider instance.
        provider_fn: Synchronous callable invoked in the trading executor.
        operation: Stable name used in metrics/logs.
        idempotency_context: Optional dict of fields to surface in 503/504
            error responses so callers can safely retry. For ``create_order``
            this carries ``{"client_order_id": "...", "retry_with":
            "client_order_id"}`` — the caller reads this from the 504 body
            and either (a) GETs the order by client_order_id to see whether
            the order actually placed, or (b) retries POST with the same
            client_order_id (Alpaca natively dedupes by that key). For
            ``close_position`` it carries
            ``{"symbol": "...", "retry_with": "get_position"}`` — Alpaca's
            ClosePositionRequest does not accept client_order_id, so the
            caller checks GET /positions/<symbol> to decide whether to
            retry the close.
    """
    settings = get_settings()
    timeout_seconds = settings.alpaca_trading_call_timeout_seconds
    sem = _get_trading_inflight_sem()

    # Fast-fail when the in-flight cap is fully reserved. ``locked()`` is safe
    # to inspect without a lock: asyncio is single-threaded so no other
    # coroutine can change the permit count between this check and the
    # subsequent acquire (acquire does not suspend when a permit is free).
    if sem.locked():
        logger.warning(
            "alpaca_trading_backpressure_reject",
            operation=operation,
            max_inflight=settings.alpaca_trading_max_inflight,
            **(idempotency_context or {}),
        )
        detail_503: dict[str, Any] = {
            "code": "GW-E5005",
            "message": (
                f"Alpaca trading API backpressure during {operation}: "
                f"{settings.alpaca_trading_max_inflight} calls already in-flight. "
                "Retry shortly."
            ),
        }
        if idempotency_context:
            # Backpressure rejects BEFORE the call hits Alpaca, so the
            # order definitely did NOT place. But surfacing the
            # idempotency key here lets the caller retry with the same
            # key if they happen to retry the *same logical order*, which
            # protects against double-place if the original 503 attempt
            # is racing in the caller's reconciliation logic.
            detail_503.update(idempotency_context)
        raise HTTPException(
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
            detail=detail_503,
        )

    async with sem:
        try:
            loop = asyncio.get_running_loop()
            return await asyncio.wait_for(
                loop.run_in_executor(_get_trading_executor(), provider_fn, provider),
                timeout=timeout_seconds,
            )
        except TimeoutError as exc:
            logger.error(
                "alpaca_trading_call_timeout",
                operation=operation,
                timeout_seconds=timeout_seconds,
                **(idempotency_context or {}),
            )
            detail_504: dict[str, Any] = {
                "code": "GW-E5004",
                "message": f"Timed out waiting for Alpaca trading API during {operation}",
            }
            if idempotency_context:
                # CRITICAL: the asyncio task is cancelled, but the
                # executor thread keeps running until the Alpaca SDK call
                # completes — so the order MAY have placed at Alpaca by
                # the time the caller sees the 504. The retry contract
                # surfaced here lets callers either verify the order's
                # actual status or retry idempotently. DO NOT REMOVE.
                detail_504.update(idempotency_context)
            raise HTTPException(
                status_code=HTTP_504_GATEWAY_TIMEOUT,
                detail=detail_504,
            ) from exc


@router.get("/account", response_model=SuccessResponse)
async def get_account(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account information."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_account(),
        operation="get_account",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.post("/orders", response_model=SuccessResponse)
async def create_order(
    symbol: str,
    side: str,
    qty: float | None = None,
    notional: float | None = None,
    order_type: str = "market",
    time_in_force: str = "day",
    limit_price: float | None = None,
    stop_price: float | None = None,
    client_order_id: str | None = None,
    extended_hours: bool = False,
    order_class: str | None = None,
    take_profit_limit_price: float | None = None,
    stop_loss_stop_price: float | None = None,
    stop_loss_limit_price: float | None = None,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Create a new order.

    Idempotency contract (DO NOT REGRESS):
      - If the caller supplies ``client_order_id``, we use it verbatim.
      - If the caller does NOT supply one, the gateway auto-generates a
        ``dg-<uuid4hex>`` key and uses that.
      - The effective client_order_id is returned in ``meta.client_order_id``
        on success AND in the 504 timeout error detail. Alpaca natively
        dedupes ``submit_order`` by client_order_id — a caller that sees
        a 504 can safely retry POST with the same key (returning the
        existing order rather than placing a second) or GET
        ``/orders:by_client_order_id`` to check status.
    """
    effective_client_order_id = client_order_id or _generate_client_order_id()
    gateway_generated = client_order_id is None
    idempotency_context: dict[str, Any] = {
        "client_order_id": effective_client_order_id,
        "client_order_id_source": "gateway" if gateway_generated else "caller",
        "retry_with": "client_order_id",
        "retry_hint": (
            "Order may have placed at Alpaca despite the 5xx — Alpaca natively "
            "dedupes by client_order_id. Either GET /api/alpaca/trading/orders:"
            f"by_client_order_id?client_order_id={effective_client_order_id} to check status, or retry "
            "POST /api/alpaca/trading/orders with the same client_order_id to "
            "idempotently re-attempt."
        ),
    }

    async def _call(provider: Any) -> Any:
        try:
            return await _run_trading_provider_call(
                provider=provider,
                provider_fn=lambda provider: provider.create_order(
                    symbol=symbol,
                    qty=qty,
                    notional=notional,
                    side=side,
                    order_type=order_type,
                    time_in_force=time_in_force,
                    limit_price=limit_price,
                    stop_price=stop_price,
                    client_order_id=effective_client_order_id,
                    extended_hours=extended_hours,
                    order_class=order_class,
                    take_profit_limit_price=take_profit_limit_price,
                    stop_loss_stop_price=stop_loss_stop_price,
                    stop_loss_limit_price=stop_loss_limit_price,
                ),
                operation="create_order",
                idempotency_context=idempotency_context,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=_call,
    )
    return {
        "success": True,
        "data": data,
        "meta": {
            "provider": "alpaca",
            "client_order_id": effective_client_order_id,
            "client_order_id_source": "gateway" if gateway_generated else "caller",
        },
    }


_VALID_ORDER_QUERY_STATUSES = frozenset({"open", "closed", "all"})
_VALID_ORDER_SORT_DIRECTIONS = frozenset({"asc", "desc"})
_VALID_ORDER_SIDES = frozenset({"buy", "sell"})


@router.get("/orders", response_model=SuccessResponse)
async def get_orders(
    status: str = Query(default="open", description="Order status: open, closed, all"),
    limit: int = Query(default=50, le=500, description="Max orders to return"),
    direction: str = Query(default="desc", description="Sort direction: asc, desc"),
    symbols: str | None = Query(default=None, description=DESC_COMMA_SYMBOLS),
    nested: bool = Query(default=True, description="Include nested multi-leg orders"),
    side: str | None = Query(default=None, description="Filter by side: buy, sell"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get all orders with optional filters."""
    if status not in _VALID_ORDER_QUERY_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status '{status}'. Must be one of: {sorted(_VALID_ORDER_QUERY_STATUSES)}",
        )
    if direction not in _VALID_ORDER_SORT_DIRECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid direction '{direction}'. Must be one of: {sorted(_VALID_ORDER_SORT_DIRECTIONS)}",
        )
    if side is not None and side not in _VALID_ORDER_SIDES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid side '{side}'. Must be one of: {sorted(_VALID_ORDER_SIDES)}",
        )

    symbols_list = symbols.split(",") if symbols else None
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_orders(
            status=status,
            limit=limit,
            direction=direction,
            symbols=symbols_list,
            nested=nested,
            side=side,
        ),
        operation="get_orders",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/orders/{order_id}", response_model=SuccessResponse)
async def get_order(
    order_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get a specific order by ID."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_order(order_id),
        operation="get_order",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.get("/orders:by_client_order_id", response_model=SuccessResponse)
async def get_order_by_client_id(
    client_order_id: str = Query(..., description="Client order ID"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get order by client order ID."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_order_by_client_id(client_order_id),
        operation="get_order_by_client_id",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.patch("/orders/{order_id}", response_model=SuccessResponse)
async def replace_order(
    order_id: str,
    qty: float | None = None,
    limit_price: float | None = None,
    stop_price: float | None = None,
    time_in_force: str | None = None,
    client_order_id: str | None = None,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Replace/modify an existing order."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.replace_order(
            order_id,
            qty=qty,
            limit_price=limit_price,
            stop_price=stop_price,
            time_in_force=time_in_force,
            client_order_id=client_order_id,
        ),
        operation="replace_order",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.delete("/orders/{order_id}", response_model=SuccessResponse)
async def cancel_order(
    order_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Cancel an order by ID."""
    success = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.cancel_order(order_id),
        operation="cancel_order",
    )
    return {
        "success": success,
        "data": {"order_id": order_id, "cancelled": success},
        "meta": {"provider": "alpaca"},
    }


@router.delete("/orders", response_model=SuccessResponse)
async def cancel_all_orders(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Cancel all open orders."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.cancel_all_orders(),
        operation="cancel_all_orders",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/positions", response_model=SuccessResponse)
async def get_positions(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get all open positions."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_positions(),
        operation="get_positions",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/positions/{symbol}", response_model=SuccessResponse)
async def get_position(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get position for a specific symbol."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_position(symbol),
        operation="get_position",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.delete("/positions/{symbol}", response_model=SuccessResponse)
async def close_position(
    symbol: str,
    qty: float | None = Query(default=None, description="Quantity to close"),
    percentage: float | None = Query(default=None, description="Percentage to close (0-100)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Close a position (fully or partially)."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.close_position(symbol, qty, percentage),
        operation="close_position",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.delete("/positions", response_model=SuccessResponse)
async def close_all_positions(
    cancel_orders: bool = Query(default=True, description="Cancel open orders first"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Close all open positions."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.close_all_positions(cancel_orders),
        operation="close_all_positions",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/portfolio/history", response_model=SuccessResponse)
async def get_portfolio_history(
    period: str | None = Query(default="1M", description="Period: 1D, 1W, 1M, 3M, 6M, 1A, all"),
    timeframe: str | None = Query(default="1D", description="Timeframe: 1Min, 5Min, 15Min, 1H, 1D"),
    extended_hours: bool = Query(default=False, description="Include extended hours"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account portfolio history."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_portfolio_history(
            period=period,
            timeframe=timeframe,
            extended_hours=extended_hours,
        ),
        operation="get_portfolio_history",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.get("/assets", response_model=SuccessResponse)
async def get_assets(
    status: str | None = Query(default="active", description="Asset status: active, inactive"),
    asset_class: str | None = Query(default="us_equity", description="Asset class: us_equity, crypto"),
    exchange: str | None = Query(default=None, description="Exchange filter"),
    client: Client = Depends(require_api_key),
    cache: InMemoryCache | HybridCache = Depends(get_cache),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get available assets."""
    normalized_status = (status or "none").strip().lower()
    normalized_asset_class = (asset_class or "none").strip().lower()
    normalized_exchange = (exchange or "all").strip().lower()
    data = await _execute_trading_cached_call(
        registry=registry,
        cache=cache,
        cache_key=(f"alpaca:trading:assets:{normalized_status}:{normalized_asset_class}:{normalized_exchange}"),
        ttl=TRADING_ASSETS_CACHE_TTL_SECONDS,
        route_label="alpaca_trading_assets",
        provider_fn=lambda provider: provider.get_assets(
            status=status,
            asset_class=asset_class,
            exchange=exchange,
        ),
        operation="get_assets",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/assets/{symbol}", response_model=SuccessResponse)
async def get_asset(
    symbol: str,
    client: Client = Depends(require_api_key),
    cache: InMemoryCache | HybridCache = Depends(get_cache),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get a specific asset by symbol."""
    normalized_symbol = symbol.strip().upper()
    data = await _execute_trading_cached_call(
        registry=registry,
        cache=cache,
        cache_key=f"alpaca:trading:asset:{normalized_symbol}",
        ttl=TRADING_ASSETS_CACHE_TTL_SECONDS,
        route_label="alpaca_trading_asset",
        provider_fn=lambda provider: provider.get_asset(normalized_symbol),
        operation="get_asset",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.get("/clock", response_model=SuccessResponse)
async def get_clock(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get market clock."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_clock(),
        operation="get_clock",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.get("/calendar", response_model=SuccessResponse)
async def get_calendar(
    start: date | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: date | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    cache: InMemoryCache | HybridCache = Depends(get_cache),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get trading calendar."""
    start_key = start.isoformat() if start else "none"
    end_key = end.isoformat() if end else "none"
    data = await _execute_trading_cached_call(
        registry=registry,
        cache=cache,
        cache_key=f"alpaca:trading:calendar:{start_key}:{end_key}",
        ttl=TRADING_CALENDAR_CACHE_TTL_SECONDS,
        route_label="alpaca_trading_calendar",
        provider_fn=lambda provider: provider.get_calendar(start, end),
        operation="get_calendar",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }
