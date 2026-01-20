"""Alpaca watchlist endpoints."""

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alpaca.common import (
    DESC_COMMA_SYMBOLS,
    ERR_PROVIDER_NOT_AVAILABLE,
    Client,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
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
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_watchlists)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.post("/watchlists", response_model=SuccessResponse)
async def create_watchlist(
    name: str,
    symbols: str | None = Query(default=None, description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Create a new watchlist."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = symbols.split(",") if symbols else None
        data = await asyncio.to_thread(provider.create_watchlist, name, symbols_list)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/watchlists/{watchlist_id}", response_model=SuccessResponse)
async def get_watchlist(
    watchlist_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get a specific watchlist by ID."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_watchlist, watchlist_id)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.put("/watchlists/{watchlist_id}", response_model=SuccessResponse)
async def update_watchlist(
    watchlist_id: str,
    name: str | None = None,
    symbols: str | None = Query(default=None, description=DESC_COMMA_SYMBOLS),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Update a watchlist."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        symbols_list = symbols.split(",") if symbols else None
        data = await asyncio.to_thread(provider.update_watchlist, watchlist_id, name, symbols_list)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.delete("/watchlists/{watchlist_id}", response_model=SuccessResponse)
async def delete_watchlist(
    watchlist_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Delete a watchlist."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        success = await asyncio.to_thread(provider.delete_watchlist, watchlist_id)
        return {
            "success": success,
            "data": {"watchlist_id": watchlist_id, "deleted": success},
            "meta": {"provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.post("/watchlists/{watchlist_id}/assets", response_model=SuccessResponse)
async def add_asset_to_watchlist(
    watchlist_id: str,
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Add an asset to a watchlist."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.add_asset_to_watchlist, watchlist_id, symbol)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.delete("/watchlists/{watchlist_id}/assets/{symbol}", response_model=SuccessResponse)
async def remove_asset_from_watchlist(
    watchlist_id: str,
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Remove an asset from a watchlist."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.remove_asset_from_watchlist, watchlist_id, symbol)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
