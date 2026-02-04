"""Market data endpoints (calendar, sector, correlations, alerts) for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    HTTPException,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    get_cache,
    get_registry,
    get_uw_provider,
    paginate_response,
    require_api_key,
    require_provider_rate_limit,
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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_economic_calendar(start_date=start_date, end_date=end_date)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/market/sector/{sector}/tide", response_model=SuccessResponse)
async def get_sector_tide(
    sector: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market tide for a specific sector."""
    cache_key = f"uw:sector-tide:{sector}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_sector_tide(sector=sector)

    if not data:
        raise HTTPException(status_code=404, detail=f"Sector {sector} not found")

    response = {
        "success": True,
        "data": data,
        "meta": {"sector": sector, "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/top-impact", response_model=SuccessResponse)
async def get_top_net_impact(
    limit: int = Query(default=20, le=100),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get top tickers by net premium (bullish and bearish)."""
    cache_key = f"uw:top-impact:{limit}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_top_net_impact(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "bullish_count": len(data.get("bullish", [])),
            "bearish_count": len(data.get("bearish", [])),
            "provider": "unusual_whales",
        },
    }

    await cache.set(cache_key, response, ttl=60)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_custom_alerts(
        min_premium=min_premium, min_volume=min_volume, limit=limit
    )

    response = paginate_response(data, limit)
    response["meta"] = {"count": len(data), "provider": "unusual_whales"}

    await cache.set(cache_key, response, ttl=60)
    return response


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
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_correlations(start_date=start_date, end_date=end_date)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=3600)
    return response
