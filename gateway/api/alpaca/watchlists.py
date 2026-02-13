"""Alpaca watchlist endpoints."""

import asyncio

from fastapi import APIRouter, Depends, Query

from gateway.api.alpaca.common import (
    DESC_COMMA_SYMBOLS,
    Client,
    execute_alpaca_provider_call,
    get_registry,
    require_api_key,
)
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()


@router.get("/watchlists", response_model=SuccessResponse)
async def get_watchlists(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get all watchlists."""
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: asyncio.to_thread(provider.get_watchlists),
        rate_limit_key="alpaca_trading",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.post("/watchlists", response_model=SuccessResponse)
async def create_watchlist(
    name: str,
    symbols: str | None = Query(default=None, description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Create a new watchlist."""
    symbols_list = symbols.split(",") if symbols else None
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: asyncio.to_thread(provider.create_watchlist, name, symbols_list),
        rate_limit_key="alpaca_trading",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.get("/watchlists/{watchlist_id}", response_model=SuccessResponse)
async def get_watchlist(
    watchlist_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get a specific watchlist by ID."""
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: asyncio.to_thread(provider.get_watchlist, watchlist_id),
        rate_limit_key="alpaca_trading",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.put("/watchlists/{watchlist_id}", response_model=SuccessResponse)
async def update_watchlist(
    watchlist_id: str,
    name: str | None = None,
    symbols: str | None = Query(default=None, description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Update a watchlist."""
    symbols_list = symbols.split(",") if symbols else None
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: asyncio.to_thread(provider.update_watchlist, watchlist_id, name, symbols_list),
        rate_limit_key="alpaca_trading",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.delete("/watchlists/{watchlist_id}", response_model=SuccessResponse)
async def delete_watchlist(
    watchlist_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Delete a watchlist."""
    success = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: asyncio.to_thread(provider.delete_watchlist, watchlist_id),
        rate_limit_key="alpaca_trading",
    )
    return {
        "success": success,
        "data": {"watchlist_id": watchlist_id, "deleted": success},
        "meta": {"provider": "alpaca"},
    }


@router.post("/watchlists/{watchlist_id}/assets", response_model=SuccessResponse)
async def add_asset_to_watchlist(
    watchlist_id: str,
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Add an asset to a watchlist."""
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: asyncio.to_thread(provider.add_asset_to_watchlist, watchlist_id, symbol),
        rate_limit_key="alpaca_trading",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.delete("/watchlists/{watchlist_id}/assets/{symbol}", response_model=SuccessResponse)
async def remove_asset_from_watchlist(
    watchlist_id: str,
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Remove an asset from a watchlist."""
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: asyncio.to_thread(provider.remove_asset_from_watchlist, watchlist_id, symbol),
        rate_limit_key="alpaca_trading",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
