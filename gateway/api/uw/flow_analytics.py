"""Flow analytics endpoints for Unusual Whales."""

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
    require_api_key,
    require_provider_rate_limit,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/{symbol}/spot-exposures", response_model=SuccessResponse)
async def get_spot_exposures_by_strike(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get gamma and volume confluence per strike."""
    symbol = symbol.upper()
    cache_key = f"uw:spot-exposures:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_spot_exposures_by_strike(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/flow-strike", response_model=SuccessResponse)
async def get_flow_per_strike(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow data aggregated by strike price."""
    symbol = symbol.upper()
    cache_key = f"uw:flow-strike:{symbol}:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_flow_per_strike(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "date": date, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/flow-expiry", response_model=SuccessResponse)
async def get_flow_per_expiry(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow data aggregated by expiration date."""
    symbol = symbol.upper()
    cache_key = f"uw:flow-expiry:{symbol}:{date or 'latest'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_flow_per_expiry(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "date": date, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/greek-flow", response_model=SuccessResponse)
async def get_greek_flow(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get greek flow data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:greek-flow:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greek_flow(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/net-flow-expiry", response_model=SuccessResponse)
async def get_net_flow_expiry(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get net premium flow by expiration category."""
    cache_key = "uw:net-flow-expiry"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_net_flow_expiry()

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/{symbol}/interpolated-iv", response_model=SuccessResponse)
async def get_interpolated_iv(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get interpolated implied volatility."""
    symbol = symbol.upper()
    cache_key = f"uw:interpolated-iv:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_interpolated_iv(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response
