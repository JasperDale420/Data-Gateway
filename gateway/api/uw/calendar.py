"""Market calendar endpoints (economic, FDA, holidays, etc.) for Unusual Whales."""

from fastapi import APIRouter, Depends

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


@router.get("/market/economic-calendar", response_model=SuccessResponse)
async def get_economic_calendar_market(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get economic calendar events."""
    cache_key = "uw:market:economic-calendar"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_economic_calendar()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/market/fda-calendar", response_model=SuccessResponse)
async def get_fda_calendar(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get FDA calendar events."""
    cache_key = "uw:market:fda-calendar"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_fda_calendar()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/market/holidays", response_model=SuccessResponse)
async def get_market_holidays(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market holidays."""
    cache_key = "uw:market:holidays"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_holidays()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=86400)
    return response


@router.get("/market/imbalances", response_model=SuccessResponse)
async def get_market_imbalances(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market imbalances data."""
    cache_key = "uw:market:imbalances"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_imbalances()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


@router.get("/market/options-volume", response_model=SuccessResponse)
async def get_market_options_volume(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get total market options volume."""
    cache_key = "uw:market:options-volume"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_options_volume()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


@router.get("/market/insider-trades", response_model=SuccessResponse)
async def get_market_insider_trades(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market-wide insider trades."""
    cache_key = "uw:market:insider-trades"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_insider_trades()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/sector-stats", response_model=SuccessResponse)
async def get_sector_stats(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get sector statistics."""
    cache_key = "uw:market:sector-stats"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_sector_stats()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/{etf}/etf-tide", response_model=SuccessResponse)
async def get_market_tide_by_etf(
    etf: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market tide data for a specific ETF."""
    etf = etf.upper()
    cache_key = f"uw:market:etf-tide:{etf}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_tide_by_etf(etf=etf)

    response = {
        "success": True,
        "data": data,
        "meta": {"etf": etf, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response
