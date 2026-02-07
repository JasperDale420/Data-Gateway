"""Seasonality endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends

from gateway.api.uw.common import (
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


@router.get("/seasonality/market", response_model=SuccessResponse)
async def get_market_seasonality(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market-wide seasonality data."""
    cache_key = "uw:seasonality:market"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_market_seasonality(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_monthly_returns(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )
