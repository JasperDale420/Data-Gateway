"""Volatility endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends

from gateway.api.uw.common import (
    Client,
    HTTPException,
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


@router.get("/{symbol}/iv-term-structure", response_model=SuccessResponse)
async def get_iv_term_structure(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get IV term structure for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:iv-term-structure:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_iv_term_structure(symbol=symbol)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/realized-vol", response_model=SuccessResponse)
async def get_realized_vol(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get realized volatility for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:realized-vol:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_realized_volatility(symbol=symbol)

    if not data:
        raise HTTPException(status_code=404, detail=f"Volatility data not found for {symbol}")

    response = {
        "success": True,
        "data": data.model_dump(mode="json"),
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/vol-stats", response_model=SuccessResponse)
async def get_vol_stats(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get volatility stats for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:vol-stats:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_volatility_stats(symbol=symbol)

    if not data:
        raise HTTPException(status_code=404, detail=f"Volatility stats not found for {symbol}")

    response = {
        "success": True,
        "data": data.model_dump(mode="json"),
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/iv-surface", response_model=SuccessResponse)
async def get_iv_surface(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get implied volatility surface for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:iv-surface:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_iv_surface(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response
