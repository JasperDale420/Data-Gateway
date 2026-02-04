"""Alpaca account configuration and activities endpoints."""

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alpaca.common import (
    ERR_PROVIDER_NOT_AVAILABLE,
    Client,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()


@router.get("/account/configurations", response_model=SuccessResponse)
async def get_account_configurations(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account configuration settings."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(provider.get_account_configurations)
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


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
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        data = await asyncio.to_thread(
            provider.set_account_configurations,
            dtbp_check=dtbp_check,
            trade_confirm_email=trade_confirm_email,
            suspend_trade=suspend_trade,
            no_shorting=no_shorting,
            fractional_trading=fractional_trading,
            max_margin_multiplier=max_margin_multiplier,
            pdt_check=pdt_check,
        )
        return {"success": True, "data": data, "meta": {"provider": "alpaca"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/account/activities", response_model=SuccessResponse)
async def get_account_activities(
    activity_types: str | None = Query(default=None, description="Comma-separated activity types"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account activities."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        types_list = activity_types.split(",") if activity_types else None
        data = await asyncio.to_thread(provider.get_account_activities, types_list)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "provider": "alpaca"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
