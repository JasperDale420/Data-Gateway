"""Options analytics endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_DATE,
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


@router.get("/{symbol}/net-premium", response_model=SuccessResponse)
async def get_net_premium(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get net premium ticks for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:net-premium:{symbol}:{date or 'latest'}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_net_premium_ticks(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


@router.get("/{symbol}/max-pain", response_model=SuccessResponse)
async def get_max_pain(
    symbol: str,
    expiry: str | None = Query(default=None, description="Expiration (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get max pain data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:max-pain:{symbol}:{expiry or 'all'}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_max_pain(symbol=symbol, expiry=expiry)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


@router.get("/{symbol}/iv-rank", response_model=SuccessResponse)
async def get_iv_rank(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get IV rank for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:iv-rank:{symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_iv_rank(symbol=symbol)

    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"IV rank not found for {symbol}. This may be due to market hours, data unavailability, or subscription tier.",
        )

    response = {
        "success": True,
        "data": data.model_dump(mode="json"),
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/oi-change", response_model=SuccessResponse)
async def get_oi_change(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get OI change data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:oi-change:{symbol}:{date or 'latest'}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_oi_change(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": [d.model_dump(mode="json") for d in data],
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response
