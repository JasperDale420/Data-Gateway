"""Politician portfolio endpoints for Unusual Whales (Enterprise-only)."""

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


@router.get("/politicians/people", response_model=SuccessResponse)
async def get_politician_people(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all politician names and IDs."""
    cache_key = "uw:politicians:people"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_politician_people()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/politicians/recent-trades", response_model=SuccessResponse)
async def get_politician_recent_trades(
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get latest transacted trades by congress members."""
    cache_key = f"uw:politicians:trades:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_politician_recent_trades(limit=limit)

    response = paginate_response(data, limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/politicians/{politician_id}/portfolios", response_model=SuccessResponse)
async def get_politician_portfolios(
    politician_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all portfolios and holdings for a politician."""
    cache_key = f"uw:politicians:{politician_id}:portfolios"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_politician_portfolios(politician_id=politician_id)

    response = {
        "success": True,
        "data": data,
        "meta": {"politician_id": politician_id, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/politicians/{symbol}/holders", response_model=SuccessResponse)
async def get_politician_holders(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get politician portfolio holders for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:politicians:holders:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_politician_holders(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response
