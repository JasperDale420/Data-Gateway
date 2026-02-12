"""Alpha Vantage time series endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alphavantage.common import (
    CACHE_TTL_BARS,
    CACHE_TTL_QUOTE,
    Client,
    InMemoryCache,
    ProviderRegistry,
    cache_key,
    execute_av_cached,
    get_cache,
    get_registry,
    normalize_search_query,
    require_api_key,
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
    key = cache_key("av:quote", symbol.upper())

    async def _fetch_quote(provider):
        quote = await provider.get_quote(symbol)
        if not quote:
            raise HTTPException(status_code=404, detail=f"No data for symbol: {symbol}")
        return quote

    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_QUOTE,
            fetcher=_fetch_quote,
            cache_transform=lambda quote: quote.model_dump(mode="json"),
            endpoint="quote",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/intraday/{symbol}", response_model=SuccessResponse)
async def get_intraday(
    symbol: str,
    interval: str = Query(default="5min", description="1min, 5min, 15min, 30min, 60min"),
    outputsize: str = Query(default="compact", description="compact (100 points) or full"),
    max_points: int | None = Query(default=None, ge=1, description="Optional max bars to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get intraday time series data."""
    normalized_outputsize = outputsize.lower()
    key = cache_key("av:intraday", symbol.upper(), interval, outputsize, max_points)
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_BARS,
            cache_enabled=normalized_outputsize != "full",
            fetcher=lambda provider: provider.get_intraday(
                symbol,
                interval=interval,
                outputsize=outputsize,
                max_points=max_points,
            ),
            cache_transform=lambda bars: {
                "symbol": symbol.upper(),
                "interval": interval,
                "bars": [bar.model_dump(mode="json") for bar in bars],
            },
            miss_meta_builder=lambda _bars, data: {"count": len(data["bars"])},
            endpoint="intraday",
            cache_mode=normalized_outputsize,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/daily/{symbol}", response_model=SuccessResponse)
async def get_daily(
    symbol: str,
    outputsize: str = Query(default="compact", description="compact (100 days) or full"),
    adjusted: bool = Query(default=True, description="Include split/dividend adjustments"),
    max_points: int | None = Query(default=None, ge=1, description="Optional max bars to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get daily time series data."""
    normalized_outputsize = outputsize.lower()
    key = cache_key("av:daily", symbol.upper(), outputsize, str(adjusted), max_points)
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_BARS,
            cache_enabled=normalized_outputsize != "full",
            fetcher=lambda provider: provider.get_daily(
                symbol,
                outputsize=outputsize,
                adjusted=adjusted,
                max_points=max_points,
            ),
            cache_transform=lambda bars: {
                "symbol": symbol.upper(),
                "adjusted": adjusted,
                "bars": [bar.model_dump(mode="json") for bar in bars],
            },
            miss_meta_builder=lambda _bars, data: {"count": len(data["bars"])},
            endpoint="daily",
            cache_mode=normalized_outputsize,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/weekly/{symbol}", response_model=SuccessResponse)
async def get_weekly(
    symbol: str,
    adjusted: bool = Query(default=True, description="Include adjustments"),
    max_points: int | None = Query(default=None, ge=1, description="Optional max bars to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get weekly time series data."""
    key = cache_key("av:weekly", symbol.upper(), str(adjusted), max_points)
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_BARS,
            fetcher=lambda provider: provider.get_weekly(
                symbol,
                adjusted=adjusted,
                max_points=max_points,
            ),
            cache_transform=lambda bars: {
                "symbol": symbol.upper(),
                "adjusted": adjusted,
                "bars": [bar.model_dump(mode="json") for bar in bars],
            },
            miss_meta_builder=lambda _bars, data: {"count": len(data["bars"])},
            endpoint="weekly",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/monthly/{symbol}", response_model=SuccessResponse)
async def get_monthly(
    symbol: str,
    adjusted: bool = Query(default=True, description="Use adjusted close prices"),
    max_points: int | None = Query(default=None, ge=1, description="Optional max bars to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get monthly time series data."""
    key = cache_key("av:monthly", symbol.upper(), str(adjusted), max_points)
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_BARS,
            fetcher=lambda provider: provider.get_monthly(
                symbol,
                adjusted=adjusted,
                max_points=max_points,
            ),
            cache_transform=lambda bars: [bar.model_dump(mode="json") for bar in bars],
            miss_meta_builder=lambda _bars, data: {"count": len(data)},
            endpoint="monthly",
            cache_mode="default",
        )
    except HTTPException:
        raise
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
    normalized_query = normalize_search_query(q)
    key = cache_key("av:search", normalized_query)
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=86400,
            fetcher=lambda provider: provider.search_symbols(q),
            cache_transform=lambda results: results,
            miss_meta_builder=lambda results, _data: {"count": len(results)},
            endpoint="search",
            cache_mode="query",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
