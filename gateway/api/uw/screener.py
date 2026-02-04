"""Screener endpoints for Unusual Whales."""

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


@router.get("/screener/stocks", response_model=SuccessResponse)
async def get_screener_stocks(
    limit: int = Query(default=20, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get stock screener results."""
    cache_key = f"uw:screener:stocks:{limit}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_stock_screener(limit=limit)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    await cache.set(cache_key, response, ttl=60)
    return response


@router.get("/screener/options", response_model=SuccessResponse)
async def get_screener_options(
    limit: int = Query(default=20, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get hottest option chains/contracts."""
    cache_key = f"uw:screener:options:{limit}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_hottest_chains(limit=limit)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    await cache.set(cache_key, response, ttl=60)
    return response
