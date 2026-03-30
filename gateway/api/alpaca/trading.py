"""Alpaca trading endpoints - orders, positions, account, assets, clock, calendar."""

import asyncio
from collections.abc import Callable
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alpaca.common import (
    DESC_COMMA_SYMBOLS,
    Client,
    execute_alpaca_cached_call,
    execute_alpaca_provider_call,
    get_cache,
    get_registry,
    require_api_key,
)
from gateway.core.cache import HybridCache, InMemoryCache
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()
TRADING_ASSETS_CACHE_TTL_SECONDS = 600
TRADING_CALENDAR_CACHE_TTL_SECONDS = 3600


async def _execute_trading_call(
    *,
    registry: ProviderRegistry,
    provider_fn: Callable[[Any], Any],
) -> Any:
    return await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: asyncio.to_thread(provider_fn, provider),
    )


async def _execute_trading_cached_call(
    *,
    registry: ProviderRegistry,
    cache: InMemoryCache | HybridCache,
    cache_key: str,
    ttl: int,
    route_label: str,
    provider_fn: Callable[[Any], Any],
) -> Any:
    return await execute_alpaca_cached_call(
        registry=registry,
        cache=cache,
        cache_key=cache_key,
        ttl=ttl,
        route_label=route_label,
        provider_call=lambda provider: asyncio.to_thread(provider_fn, provider),
    )


@router.get("/account", response_model=SuccessResponse)
async def get_account(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account information."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_account(),
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
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Create a new order."""

    async def _call(provider: Any) -> Any:
        try:
            return await asyncio.to_thread(
                provider.create_order,
                symbol=symbol,
                qty=qty,
                notional=notional,
                side=side,
                order_type=order_type,
                time_in_force=time_in_force,
                limit_price=limit_price,
                stop_price=stop_price,
                client_order_id=client_order_id,
                extended_hours=extended_hours,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=_call,
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


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
        provider_fn=lambda provider: provider.get_asset(symbol),
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
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }
