"""Market data endpoints (calendar, sector, correlations, alerts) for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    HTTPException,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    execute_uw_cached,
    get_cache,
    get_registry,
    make_response,
    paginate_response,
    require_api_key,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/market/calendar", response_model=SuccessResponse)
async def get_economic_calendar(
    start_date: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get economic calendar events."""
    cache_key = f"uw:calendar:{start_date or 'start'}:{end_date or 'end'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_economic_calendar(
            start_date=start_date,
            end_date=end_date,
        ),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/market/sector/{sector}/tide", response_model=SuccessResponse)
async def get_sector_tide(
    sector: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market tide for a specific sector."""
    cache_key = f"uw:sector-tide:{sector}"

    def _build_response(data):
        if not data:
            raise HTTPException(status_code=404, detail=f"Sector {sector} not found")
        return make_response(data, extra_meta={"sector": sector})

    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_sector_tide(sector=sector),
        build_response=_build_response,
    )


@router.get("/market/top-impact", response_model=SuccessResponse)
async def get_top_net_impact(
    limit: int = Query(default=20, le=100),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get top tickers by net premium (bullish and bearish)."""
    cache_key = f"uw:top-impact:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_top_net_impact(limit=limit),
        build_response=lambda data: make_response(
            data,
            extra_meta={
                "bullish_count": len(data.get("bullish", [])),
                "bearish_count": len(data.get("bearish", [])),
            },
        ),
    )


@router.get("/alerts", response_model=SuccessResponse)
async def get_custom_alerts(
    min_premium: float | None = Query(default=None, description="Min premium"),
    min_volume: int | None = Query(default=None, description="Min volume"),
    limit: int = Query(default=50, le=200),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get custom filtered flow alerts."""
    cache_key = f"uw:alerts:{min_premium}:{min_volume}:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_custom_alerts(
            min_premium=min_premium,
            min_volume=min_volume,
            limit=limit,
        ),
        build_response=lambda data: {
            **paginate_response(data, limit),
            "meta": {"count": len(data), "provider": "unusual_whales"},
        },
    )


@router.get("/market/correlations", response_model=SuccessResponse)
async def get_market_correlations(
    start_date: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get cross-asset correlations."""
    cache_key = f"uw:correlations:{start_date or 'start'}:{end_date or 'end'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_market_correlations(
            start_date=start_date,
            end_date=end_date,
        ),
        build_response=lambda data: make_response(data, count=len(data)),
    )
