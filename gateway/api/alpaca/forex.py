"""Alpaca forex endpoints."""

from datetime import datetime

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


@router.get("/forex/rates", response_model=SuccessResponse)
async def get_forex_rates(
    pairs: str = Query(..., description="Comma-separated pairs: EUR/USD,GBP/USD"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest forex rates."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        pairs_list = [p.strip().upper() for p in pairs.split(",")]
        data = await provider.get_forex_rates(pairs=pairs_list)

        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data.get("rates", {})), "provider": "alpaca"},
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/forex/rates/historical", response_model=SuccessResponse)
async def get_forex_rates_historical(
    pairs: str = Query(..., description="Comma-separated pairs: EUR/USD,GBP/USD"),
    timeframe: str = Query(default="1Day", description="1Min, 1Hour, 1Day"),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=1000, le=10000),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical forex rates."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        pairs_list = [p.strip().upper() for p in pairs.split(",")]
        data = await provider.get_forex_rates_historical(
            pairs=pairs_list,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        )

        return {
            "success": True,
            "data": data,
            "meta": {"provider": "alpaca"},
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
