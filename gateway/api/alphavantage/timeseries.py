"""Alpha Vantage time series endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alphavantage.common import (
    CACHE_TTL_BARS,
    CACHE_TTL_QUOTE,
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


@router.get("/quote/{symbol}", response_model=SuccessResponse)
async def get_quote(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get real-time quote for a symbol."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("av:quote", symbol.upper())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        quote = await provider.get_quote(symbol)
        if not quote:
            raise HTTPException(status_code=404, detail=f"No data for symbol: {symbol}")

        data = quote.model_dump(mode="json")
        await cache.set(key, data, ttl=CACHE_TTL_QUOTE)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/intraday/{symbol}", response_model=SuccessResponse)
async def get_intraday(
    symbol: str,
    interval: str = Query(default="5min", description="1min, 5min, 15min, 30min, 60min"),
    outputsize: str = Query(default="compact", description="compact (100 points) or full"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get intraday time series data."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("av:intraday", symbol.upper(), interval, outputsize)
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        bars = await provider.get_intraday(symbol, interval=interval, outputsize=outputsize)
        data = {
            "symbol": symbol.upper(),
            "interval": interval,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        await cache.set(key, data, ttl=CACHE_TTL_BARS)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(bars), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/daily/{symbol}", response_model=SuccessResponse)
async def get_daily(
    symbol: str,
    outputsize: str = Query(default="compact", description="compact (100 days) or full"),
    adjusted: bool = Query(default=True, description="Include split/dividend adjustments"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get daily time series data."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("av:daily", symbol.upper(), outputsize, str(adjusted))
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        bars = await provider.get_daily(symbol, outputsize=outputsize, adjusted=adjusted)
        data = {
            "symbol": symbol.upper(),
            "adjusted": adjusted,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        await cache.set(key, data, ttl=CACHE_TTL_BARS)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(bars), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/weekly/{symbol}", response_model=SuccessResponse)
async def get_weekly(
    symbol: str,
    adjusted: bool = Query(default=True, description="Include adjustments"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get weekly time series data."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("av:weekly", symbol.upper(), str(adjusted))
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        bars = await provider.get_weekly(symbol, adjusted=adjusted)
        data = {
            "symbol": symbol.upper(),
            "adjusted": adjusted,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        await cache.set(key, data, ttl=CACHE_TTL_BARS)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(bars), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/monthly/{symbol}", response_model=SuccessResponse)
async def get_monthly(
    symbol: str,
    adjusted: bool = Query(default=True, description="Use adjusted close prices"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get monthly time series data."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("av:monthly", symbol.upper(), str(adjusted))
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        bars = await provider.get_monthly(symbol, adjusted=adjusted)
        data = [bar.model_dump() for bar in bars]
        await cache.set(key, data, ttl=CACHE_TTL_BARS)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/search", response_model=SuccessResponse)
async def search_symbols(
    q: str = Query(..., min_length=1, description="Search keywords"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Search for symbols by keywords."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("av:search", q.lower())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        results = await provider.search_symbols(q)
        await cache.set(key, results, ttl=86400)
        return {
            "success": True,
            "data": results,
            "meta": {"count": len(results), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
