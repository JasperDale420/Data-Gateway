"""Flow analytics endpoints for Unusual Whales."""

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


@router.get("/{symbol}/spot-exposures", response_model=SuccessResponse)
async def get_spot_exposures_by_strike(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get gamma and volume confluence per strike."""
    symbol = symbol.upper()
    cache_key = f"uw:spot-exposures:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_spot_exposures_by_strike(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/{symbol}/flow-strike", response_model=SuccessResponse)
async def get_flow_per_strike(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow data aggregated by strike price."""
    symbol = symbol.upper()
    cache_key = f"uw:flow-strike:{symbol}:{date or 'latest'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_flow_per_strike(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(
            data,
            symbol=symbol,
            count=len(data),
            extra_meta={"date": date},
        ),
    )


@router.get("/{symbol}/flow-expiry", response_model=SuccessResponse)
async def get_flow_per_expiry(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow data aggregated by expiration date."""
    symbol = symbol.upper()
    cache_key = f"uw:flow-expiry:{symbol}:{date or 'latest'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_flow_per_expiry(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(
            data,
            symbol=symbol,
            count=len(data),
            extra_meta={"date": date},
        ),
    )


@router.get("/{symbol}/greek-flow", response_model=SuccessResponse)
async def get_greek_flow(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get greek flow data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:greek-flow:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_greek_flow(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/market/net-flow-expiry", response_model=SuccessResponse)
async def get_net_flow_expiry(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get net premium flow by expiration category."""
    cache_key = "uw:net-flow-expiry"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_net_flow_expiry(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/{symbol}/interpolated-iv", response_model=SuccessResponse)
async def get_interpolated_iv(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get interpolated implied volatility."""
    symbol = symbol.upper()
    cache_key = f"uw:interpolated-iv:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_interpolated_iv(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )
