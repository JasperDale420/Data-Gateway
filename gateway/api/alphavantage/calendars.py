"""Alpha Vantage calendars and listings endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alphavantage.common import (
    CACHE_TTL_FUNDAMENTALS,
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


@router.get("/calendar/earnings", response_model=SuccessResponse)
async def get_earnings_calendar(
    symbol: str | None = Query(default=None, description="Filter by symbol"),
    horizon: str = Query(default="3month", description="Horizon: 3month, 6month, 12month"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get upcoming earnings calendar."""
    symbol_key = symbol.upper() if symbol else "all"
    key = cache_key("av:earnings-calendar", symbol_key, horizon)
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_FUNDAMENTALS,
            fetcher=lambda provider: provider.get_earnings_calendar(symbol=symbol, horizon=horizon),
            cache_transform=lambda data: data,
            miss_meta_builder=lambda data, _cached: {"count": len(data)},
            endpoint="earnings_calendar",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/calendar/ipo", response_model=SuccessResponse)
async def get_ipo_calendar(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get upcoming IPO calendar."""
    key = cache_key("av:ipo-calendar")
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_FUNDAMENTALS,
            fetcher=lambda provider: provider.get_ipo_calendar(),
            cache_transform=lambda data: data,
            miss_meta_builder=lambda data, _cached: {"count": len(data)},
            endpoint="ipo_calendar",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/listing-status", response_model=SuccessResponse)
async def get_listing_status(
    state: str = Query(default="active", description="active or delisted"),
    date: str | None = Query(default=None, description="Historical date for delisted (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get listing status (active stocks or delisted)."""
    key = cache_key("av:listing-status", state, date or "current")
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=86400,
            fetcher=lambda provider: provider.get_listing_status(state=state, date=date),
            cache_transform=lambda data: data,
            miss_meta_builder=lambda data, _cached: {"count": len(data)},
            endpoint="listing_status",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")
