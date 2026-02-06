"""Greek Exposure (GEX) endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_DATE,
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    execute_uw_cached,
    get_cache,
    get_registry,
    make_response,
    require_api_key,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/gex/{symbol}", response_model=SuccessResponse)
async def get_gex(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get Greek exposure (GEX) data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:gex:{symbol}:{date or 'latest'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_greek_exposure(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/gex/{symbol}/strike", response_model=SuccessResponse)
async def get_gex_by_strike(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get Greek exposure by strike price for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:gex:strike:{symbol}:{date or 'latest'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_greek_exposure_by_strike(
            symbol=symbol, date_str=date
        ),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/gex/{symbol}/expiry", response_model=SuccessResponse)
async def get_gex_by_expiry(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get Greek exposure by expiration date for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:gex:expiry:{symbol}:{date or 'latest'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_greek_exposure_by_expiry(
            symbol=symbol, date_str=date
        ),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )
