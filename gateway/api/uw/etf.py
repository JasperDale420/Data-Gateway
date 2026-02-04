"""ETF analytics endpoints for Unusual Whales."""

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


@router.get("/etf/{symbol}/holdings", response_model=SuccessResponse)
async def get_etf_holdings(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF holdings."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:holdings:{symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_holdings(symbol=symbol)

    response = paginate_response([d.model_dump(mode="json") for d in data], limit)
    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/etf/{symbol}/exposure", response_model=SuccessResponse)
async def get_etf_exposure(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF exposure for a stock (which ETFs hold it)."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:exposure:{symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_exposure(symbol=symbol)

    response = paginate_response([d.model_dump(mode="json") for d in data], limit)
    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/etf/{symbol}/flows", response_model=SuccessResponse)
async def get_etf_flows(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF inflow/outflow data."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:flows:{symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_flows(symbol=symbol)

    response = paginate_response([d.model_dump(mode="json") for d in data], limit)
    await cache.set(cache_key, response, ttl=300)
    return response
