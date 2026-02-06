"""Market, institutions, congress, and basic insider endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_DATE,
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    cursor_fetch_limit,
    execute_uw_cached,
    get_cache,
    get_registry,
    make_list_response,
    paginate_response,
    require_api_key,
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
    _, fetch_limit = cursor_fetch_limit(limit=limit, cursor=cursor)
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_institutions(symbol=symbol, limit=fetch_limit),
        build_response=lambda holdings: paginate_response(holdings, limit, cursor),
    )


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
    _, fetch_limit = cursor_fetch_limit(limit=limit, cursor=cursor)
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_congress_trades(symbol=symbol, limit=fetch_limit),
        build_response=lambda trades: paginate_response(trades, limit, cursor),
    )


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
    _, fetch_limit = cursor_fetch_limit(limit=limit, cursor=cursor)
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_insiders(symbol=symbol, limit=fetch_limit),
        build_response=lambda transactions: paginate_response(transactions, limit, cursor),
    )


@router.get("/market/tide", response_model=SuccessResponse)
async def get_market_tide(
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market tide/sentiment data."""
    cache_key = f"uw:market:tide:{date or 'latest'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_market_tide(date_str=date),
        build_response=make_list_response,
    )
