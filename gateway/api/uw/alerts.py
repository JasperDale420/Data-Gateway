"""Alerts and analyst endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
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


@router.get("/screener/analysts", response_model=SuccessResponse)
async def get_analyst_ratings(
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get analyst ratings from screener."""
    cache_key = f"uw:screener:analysts:{limit}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_analyst_ratings(limit=limit)

    response = paginate_response(data, limit)
    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/alerts/all", response_model=SuccessResponse)
async def get_all_alerts(
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all alerts."""
    cache_key = f"uw:alerts:all:{limit}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_all_alerts(limit=limit)

    response = paginate_response(data, limit)
    await cache.set(cache_key, response, ttl=60)
    return response


@router.get("/alerts/configuration", response_model=SuccessResponse)
async def get_alerts_configuration(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get alerts configuration."""
    cache_key = "uw:alerts:configuration"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_alerts_configuration()

    response = {
        "success": True,
        "data": data,
        "meta": {"provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=3600)
    return response
