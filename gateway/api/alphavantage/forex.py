"""Alpha Vantage forex endpoints."""

from fastapi import APIRouter, Depends, HTTPException

from gateway.api.alphavantage.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    cache_key,
    execute_av_cached,
    get_cache,
    get_registry,
    require_api_key,
)
from gateway.schemas import SuccessResponse

router = APIRouter()


@router.get("/forex/rate/{from_currency}/{to_currency}", response_model=SuccessResponse)
async def get_forex_rate(
    from_currency: str,
    to_currency: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get real-time exchange rate."""
    key = cache_key("av:forex-rate", from_currency.upper(), to_currency.upper())
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=60,
            fetcher=lambda provider: provider.get_forex_rate(from_currency, to_currency),
            cache_transform=lambda data: data,
            endpoint="forex_rate",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/forex/daily/{from_symbol}/{to_symbol}", response_model=SuccessResponse)
async def get_forex_daily(
    from_symbol: str,
    to_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get daily forex time series."""
    key = cache_key("av:forex-daily", from_symbol.upper(), to_symbol.upper())
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=3600,
            fetcher=lambda provider: provider.get_forex_daily(from_symbol, to_symbol),
            cache_transform=lambda data: data,
            miss_meta_builder=lambda data, _cached: {"count": len(data)},
            endpoint="forex_daily",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
