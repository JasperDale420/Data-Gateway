"""Extended ETF endpoints for Unusual Whales."""

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


@router.get("/etf/{symbol}/info", response_model=SuccessResponse)
async def get_etf_info(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF information."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:info:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_etf_info(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol),
    )


@router.get("/etf/{symbol}/inflow-outflow", response_model=SuccessResponse)
async def get_etf_inflow_outflow(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF inflow/outflow data."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:inflow-outflow:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_etf_inflow_outflow(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/etf/{symbol}/ticker-exposure", response_model=SuccessResponse)
async def get_etf_ticker_exposure(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ticker exposure within an ETF."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:ticker-exposure:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_etf_ticker_exposure(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/etf/{symbol}/country-weights", response_model=SuccessResponse)
async def get_etf_country_weights(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF country weights."""
    symbol = symbol.upper()
    cache_key = f"uw:etf:country-weights:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_etf_country_weights(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )
