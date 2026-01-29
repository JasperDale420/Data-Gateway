"""Seasonality endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends

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


@router.get("/seasonality/market", response_model=SuccessResponse)
async def get_market_seasonality(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market-wide seasonality data."""
    cache_key = "uw:seasonality:market"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_market_seasonality()

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/seasonality/{symbol}", response_model=SuccessResponse)
async def get_ticker_seasonality(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get monthly returns/seasonality for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:seasonality:{symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_monthly_returns(symbol=symbol)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=3600)
    return response
