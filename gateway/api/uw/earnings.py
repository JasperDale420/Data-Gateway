"""Earnings calendar endpoints for Unusual Whales."""

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


@router.get("/earnings/premarket", response_model=SuccessResponse)
async def get_earnings_premarket(
    date: str | None = Query(default=None, description=DESC_DATE),
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get premarket earnings calendar."""
    cache_key = f"uw:earnings:premarket:{date or 'latest'}:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_earnings_premarket(date_str=date)

    response = paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/earnings/afterhours", response_model=SuccessResponse)
async def get_earnings_afterhours(
    date: str | None = Query(default=None, description=DESC_DATE),
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get afterhours earnings calendar."""
    cache_key = f"uw:earnings:afterhours:{date or 'latest'}:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_earnings_afterhours(date_str=date)

    response = paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/earnings/{symbol}", response_model=SuccessResponse)
async def get_earnings_ticker(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical earnings for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:earnings:{symbol}:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_earnings_ticker(symbol=symbol)

    response = paginate_response([d.model_dump(mode="json") for d in data], limit)
    cache.set(cache_key, response, ttl=300)
    return response
