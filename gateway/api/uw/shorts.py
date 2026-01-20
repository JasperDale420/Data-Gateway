"""Shorts data endpoints for Unusual Whales."""

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


@router.get("/{symbol}/short-interest", response_model=SuccessResponse)
async def get_short_interest(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short interest data."""
    symbol = symbol.upper()
    cache_key = f"uw:short-interest:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_short_interest(symbol=symbol)

    response = paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/ftds", response_model=SuccessResponse)
async def get_ftds(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get failures to deliver data."""
    symbol = symbol.upper()
    cache_key = f"uw:ftds:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_ftds(symbol=symbol)

    response = paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/short-volume", response_model=SuccessResponse)
async def get_short_volume(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short volume data."""
    symbol = symbol.upper()
    cache_key = f"uw:short-volume:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_short_volume(symbol=symbol)

    response = paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response
