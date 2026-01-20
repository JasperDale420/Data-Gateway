"""Flow and Darkpool endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_DATE,
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


@router.get("/flow/all", response_model=SuccessResponse)
async def get_flow_all(
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None, description="Pagination cursor"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all recent options flow alerts."""
    cache_key = f"uw:flow:all:{limit}:{cursor or 'start'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    alerts = await provider.get_flow_alerts(limit=limit + 1)
    data = [a.model_dump(mode="json") for a in alerts]

    response = paginate_response(data, limit, cursor)
    cache.set(cache_key, response, ttl=30)
    return response


@router.get("/flow/{symbol}", response_model=SuccessResponse)
async def get_flow_symbol(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get options flow for a specific ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:flow:{symbol}:{limit}:{date or 'latest'}:{cursor or 'start'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    alerts = await provider.get_ticker_flow(symbol=symbol, date_str=date)
    data = [a.model_dump(mode="json") for a in alerts]

    response = paginate_response(data, limit, cursor)
    cache.set(cache_key, response, ttl=30)
    return response


@router.get("/darkpool/all", response_model=SuccessResponse)
async def get_darkpool_all(
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all recent darkpool trades."""
    cache_key = f"uw:darkpool:all:{limit}:{cursor or 'start'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    trades = await provider.get_darkpool_recent(limit=limit + 1)
    data = [t.model_dump(mode="json") for t in trades]

    response = paginate_response(data, limit, cursor)
    cache.set(cache_key, response, ttl=30)
    return response


@router.get("/darkpool/{symbol}", response_model=SuccessResponse)
async def get_darkpool_symbol(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get darkpool trades for a specific ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:darkpool:{symbol}:{limit}:{date or 'latest'}:{cursor or 'start'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    trades = await provider.get_darkpool_ticker(symbol=symbol, date_str=date)
    data = [t.model_dump(mode="json") for t in trades]

    response = paginate_response(data, limit, cursor)
    cache.set(cache_key, response, ttl=30)
    return response
