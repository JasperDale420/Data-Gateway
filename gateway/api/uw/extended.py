"""Remaining endpoints (congress, seasonality, shorts, market) for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    get_cache,
    get_registry,
    get_uw_provider,
    require_api_key,
    require_provider_rate_limit,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/congress/late-reports", response_model=SuccessResponse)
async def get_congress_late_reports(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get late congressional trading reports."""
    cache_key = f"uw:congress:late-reports:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_congress_late_reports(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/congress/reports", response_model=SuccessResponse)
async def get_congress_reports(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get congressional trading reports."""
    cache_key = f"uw:congress:reports:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_congress_reports(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/seasonality/monthly-top-performers/{month}", response_model=SuccessResponse)
async def get_monthly_top_performers(
    month: int,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get top performing stocks by month (1-12)."""
    cache_key = f"uw:seasonality:monthly-top-performers:{month}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_monthly_top_performers(month=month)

    response = {
        "success": True,
        "data": data,
        "meta": {"month": month, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/seasonality/{symbol}/price-changes-by-month", response_model=SuccessResponse)
async def get_price_changes_by_month_year(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get price changes by month and year for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:seasonality:price-changes:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_price_changes_by_month_year(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/shorts/{symbol}/data", response_model=SuccessResponse)
async def get_shorts_data(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:shorts:data:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_shorts_data(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/shorts/{symbol}/interest-float", response_model=SuccessResponse)
async def get_short_interest_float(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short interest as percentage of float."""
    symbol = symbol.upper()
    cache_key = f"uw:shorts:interest-float:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_short_interest_float(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/shorts/{symbol}/volumes-by-exchange", response_model=SuccessResponse)
async def get_short_volumes_by_exchange(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short volumes by exchange."""
    symbol = symbol.upper()
    cache_key = f"uw:shorts:volumes-by-exchange:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_short_volumes_by_exchange(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/spike", response_model=SuccessResponse)
async def get_market_spike(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market spike data."""
    cache_key = "uw:market:spike"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_spike()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response
