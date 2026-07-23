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
from gateway.core.logger import logger
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
    # nosemgrep: empire-no-bare-exception -- route boundary: any provider failure maps to 502; logged with exc_info above the raise
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=3600,
            fetcher=lambda provider: provider.get_crypto_rating(symbol),
            cache_transform=lambda data: data,
            endpoint="crypto_rating",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/crypto/daily/{symbol}", response_model=SuccessResponse)
async def get_crypto_daily(
    symbol: str,
    market: str = Query(default="USD", description="Market currency"),
    max_points: int = Query(default=100, ge=1, description="Max points to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get daily crypto time series."""
    key = cache_key("av:crypto-daily", symbol.upper(), market.upper(), str(max_points))
    # nosemgrep: empire-no-bare-exception -- route boundary: any provider failure maps to 502; logged with exc_info above the raise
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=3600,
            fetcher=lambda provider: provider.get_crypto_daily(symbol, market, max_points=max_points),
            cache_transform=lambda data: data,
            miss_meta_builder=lambda data, _cached: {"count": len(data)},
            endpoint="crypto_daily",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")
