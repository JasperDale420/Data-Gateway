"""Screener endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    execute_uw_cached,
    get_cache,
    get_registry,
    make_response,
    require_api_key,
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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_stock_screener(limit=limit),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/screener/options", response_model=SuccessResponse)
async def get_screener_options(
    limit: int = Query(default=20, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get hottest option chains/contracts."""
    cache_key = f"uw:screener:options:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_hottest_chains(limit=limit),
        build_response=lambda data: make_response(data, count=len(data)),
    )
