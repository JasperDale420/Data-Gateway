"""Alerts and analyst endpoints for Unusual Whales."""

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
    paginate_response,
    require_api_key,
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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_analyst_ratings(limit=limit),
        build_response=lambda data: paginate_response(data, limit),
    )


@router.get("/alerts/all", response_model=SuccessResponse)
async def get_all_alerts(
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all alerts."""
    cache_key = f"uw:alerts:all:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_all_alerts(limit=limit),
        build_response=lambda data: paginate_response(data, limit),
    )


@router.get("/alerts/configuration", response_model=SuccessResponse)
async def get_alerts_configuration(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get alerts configuration."""
    cache_key = "uw:alerts:configuration"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_alerts_configuration(),
        build_response=lambda data: make_response(data),
    )
