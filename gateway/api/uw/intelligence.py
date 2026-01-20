"""Market intelligence endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    HTTPException,
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


@router.get("/darkpool/{symbol}/levels", response_model=SuccessResponse)
async def get_off_lit_levels(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get dark pool volume per price level."""
    symbol = symbol.upper()
    cache_key = f"uw:off-lit-levels:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_off_lit_levels(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/congress/recent", response_model=SuccessResponse)
async def get_recent_congress_trades(
    limit: int = Query(default=50, le=200),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get recent congressional trades across all tickers."""
    cache_key = f"uw:congress-recent:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_recent_congress_trades(limit=limit)

    response = paginate_response(data, limit)
    response["meta"] = {"count": len(data), "provider": "unusual_whales"}

    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/nope", response_model=SuccessResponse)
async def get_nope(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get NOPE (Net Options Pricing Effect) for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:nope:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_nope(symbol=symbol)

    if not data:
        raise HTTPException(status_code=404, detail=f"NOPE data not found for {symbol}")

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=60)
    return response


@router.get("/{symbol}/pc-ratio", response_model=SuccessResponse)
async def get_put_call_ratio(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical put/call ratio for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:pc-ratio:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_put_call_ratio(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response
