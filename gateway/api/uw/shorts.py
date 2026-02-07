"""Shorts data endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    execute_uw_cached,
    get_cache,
    get_registry,
    paginate_response,
    require_api_key,
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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_short_interest(symbol=symbol),
        build_response=lambda data: paginate_response(data, limit),
    )


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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_ftds(symbol=symbol),
        build_response=lambda data: paginate_response(data, limit),
    )


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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_short_volume(symbol=symbol),
        build_response=lambda data: paginate_response(data, limit),
    )
