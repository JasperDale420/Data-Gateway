"""Alpha Vantage forex endpoints."""

from fastapi import APIRouter, Depends, HTTPException

from gateway.api.alphavantage.common import (
    PROVIDER_NOT_AVAILABLE,
    Client,
    InMemoryCache,
    ProviderRegistry,
    cache_key,
    get_cache,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
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
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("av:forex-rate", from_currency.upper(), to_currency.upper())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_forex_rate(from_currency, to_currency)
        await cache.set(key, data, ttl=60)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
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
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("av:forex-daily", from_symbol.upper(), to_symbol.upper())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_forex_daily(from_symbol, to_symbol)
        await cache.set(key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
