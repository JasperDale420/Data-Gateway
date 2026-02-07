"""Earnings calendar endpoints for Unusual Whales."""

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
    paginate_response,
    require_api_key,
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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_earnings_premarket(date_str=date),
        build_response=lambda data: paginate_response(data, limit),
    )


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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_earnings_afterhours(date_str=date),
        build_response=lambda data: paginate_response(data, limit),
    )


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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_earnings_ticker(symbol=symbol),
        build_response=lambda data: paginate_response(data, limit),
    )
