"""Alpha Vantage crypto endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

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


@router.get("/crypto/rating/{symbol}", response_model=SuccessResponse)
async def get_crypto_rating(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get crypto health rating (FCAS)."""
    key = cache_key("av:crypto-rating", symbol.upper())
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=3600,
            fetcher=lambda provider: provider.get_crypto_rating(symbol),
            cache_transform=lambda data: data,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/crypto/daily/{symbol}", response_model=SuccessResponse)
async def get_crypto_daily(
    symbol: str,
    market: str = Query(default="USD", description="Market currency"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get daily crypto time series."""
    key = cache_key("av:crypto-daily", symbol.upper(), market.upper())
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=3600,
            fetcher=lambda provider: provider.get_crypto_daily(symbol, market),
            cache_transform=lambda data: data,
            miss_meta_builder=lambda data, _cached: {"count": len(data)},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
