"""Alpha Vantage calendars and listings endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alphavantage.common import (
    CACHE_TTL_FUNDAMENTALS,
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


@router.get("/calendar/earnings", response_model=SuccessResponse)
async def get_earnings_calendar(
    symbol: str | None = Query(default=None, description="Filter by symbol"),
    horizon: str = Query(default="3month", description="Horizon: 3month, 6month, 12month"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get upcoming earnings calendar."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    symbol_key = symbol.upper() if symbol else "all"
    key = cache_key("av:earnings-calendar", symbol_key, horizon)
    cached = cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_earnings_calendar(symbol=symbol, horizon=horizon)
        cache.set(key, data, ttl=CACHE_TTL_FUNDAMENTALS)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/calendar/ipo", response_model=SuccessResponse)
async def get_ipo_calendar(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get upcoming IPO calendar."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("av:ipo-calendar")
    cached = cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_ipo_calendar()
        cache.set(key, data, ttl=CACHE_TTL_FUNDAMENTALS)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/listing-status", response_model=SuccessResponse)
async def get_listing_status(
    state: str = Query(default="active", description="active or delisted"),
    date: str | None = Query(default=None, description="Historical date for delisted (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get listing status (active stocks or delisted)."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("av:listing-status", state, date or "current")
    cached = cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_listing_status(state=state, date=date)
        cache.set(key, data, ttl=86400)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
