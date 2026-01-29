"""Market, institutions, congress, and basic insider endpoints for Unusual Whales."""

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


@router.get("/institutions/{symbol}", response_model=SuccessResponse)
async def get_institutions(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get 13F institutional holdings for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:institutions:{symbol}:{limit}:{cursor or 'start'}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    holdings = await provider.get_institutions(symbol=symbol)

    response = paginate_response(holdings, limit, cursor)
    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/congress/{symbol}", response_model=SuccessResponse)
async def get_congress(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get congressional trades for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:congress:{symbol}:{limit}:{cursor or 'start'}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    trades = await provider.get_congress_trades(symbol=symbol)

    response = paginate_response(trades, limit, cursor)
    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/insiders/{symbol}", response_model=SuccessResponse)
async def get_insiders(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insider transactions for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:insiders:{symbol}:{limit}:{cursor or 'start'}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    transactions = await provider.get_insiders(symbol=symbol)

    response = paginate_response(transactions, limit, cursor)
    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/market/tide", response_model=SuccessResponse)
async def get_market_tide(
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market tide/sentiment data."""
    cache_key = f"uw:market:tide:{date or 'latest'}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    tides = await provider.get_market_tide(date_str=date)

    response = {
        "success": True,
        "data": [t.model_dump(mode="json") for t in tides],
        "pagination": {
            "next_cursor": None,
            "has_more": False,
            "total_count": len(tides),
        },
    }

    await cache.set(cache_key, response, ttl=60)
    return response
