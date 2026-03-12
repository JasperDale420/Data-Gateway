"""Alpaca account configuration and activities endpoints."""

import asyncio

from fastapi import APIRouter, Depends, Query

from gateway.api.alpaca.common import (
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
ACCOUNT_CONFIG_CACHE_TTL_SECONDS = 300


@router.get("/account/configurations", response_model=SuccessResponse)
async def get_account_configurations(
    client: Client = Depends(require_api_key),
    cache: InMemoryCache | HybridCache = Depends(get_cache),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account configuration settings."""
    data = await execute_alpaca_cached_call(
        registry=registry,
        cache=cache,
        cache_key="alpaca:account:configurations",
        ttl=ACCOUNT_CONFIG_CACHE_TTL_SECONDS,
        route_label="alpaca_account_configurations",
        provider_call=lambda provider: asyncio.to_thread(provider.get_account_configurations),
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.patch("/account/configurations", response_model=SuccessResponse)
async def set_account_configurations(
    dtbp_check: str | None = None,
    trade_confirm_email: str | None = None,
    suspend_trade: bool | None = None,
    no_shorting: bool | None = None,
    fractional_trading: bool | None = None,
    max_margin_multiplier: str | None = None,
    pdt_check: str | None = None,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Update account configuration settings."""
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: asyncio.to_thread(
            provider.set_account_configurations,
            dtbp_check=dtbp_check,
            trade_confirm_email=trade_confirm_email,
            suspend_trade=suspend_trade,
            no_shorting=no_shorting,
            fractional_trading=fractional_trading,
            max_margin_multiplier=max_margin_multiplier,
            pdt_check=pdt_check,
        ),
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.get("/account/activities", response_model=SuccessResponse)
async def get_account_activities(
    activity_types: str | None = Query(default=None, description="Comma-separated activity types"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account activities."""
    types_list = activity_types.split(",") if activity_types else None
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: asyncio.to_thread(provider.get_account_activities, types_list),
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }
