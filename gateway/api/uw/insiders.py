"""Insider trading endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    execute_uw_cached,
    get_cache,
    get_registry,
    make_response,
    paginate_response,
    require_api_key,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/insider/transactions", response_model=SuccessResponse)
async def get_insider_transactions(
    limit: int = Query(default=100, le=500, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all recent insider transactions."""
    cache_key = f"uw:insider:transactions:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_insider_transactions(limit=limit),
        build_response=lambda data: paginate_response(data, limit),
    )


@router.get("/insider/sector-flow", response_model=SuccessResponse)
async def get_insider_sector_flow(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insider trading flow by sector."""
    cache_key = "uw:insider:sector-flow"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_insider_sector_flow(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/insider/ticker-flow", response_model=SuccessResponse)
async def get_insider_ticker_flow(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insider trading flow by ticker."""
    cache_key = "uw:insider:ticker-flow"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_insider_ticker_flow(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/insider/{symbol}/insiders", response_model=SuccessResponse)
async def get_ticker_insiders(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insiders for a specific ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:insider:insiders:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_ticker_insiders(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )
