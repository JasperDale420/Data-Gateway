"""Volatility endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends

from gateway.api.uw.common import (
    Client,
    HTTPException,
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


@router.get("/{symbol}/iv-rank", response_model=SuccessResponse)
async def get_iv_rank_alias(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get IV rank for a ticker (convenience alias for /options/{symbol}/iv-rank).

    Multiple clients (Heber-watch, Kairos) call /uw/{symbol}/iv-rank directly.
    This alias avoids 404 floods by routing to the same provider call.
    """
    symbol = symbol.upper()
    cache_key = f"uw:iv-rank:{symbol}"

    def _build_response(data):
        if not data:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"IV rank not found for {symbol}. This may be due to market hours, "
                    "data unavailability, or subscription tier."
                ),
            )
        return make_response(data, symbol=symbol)

    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_iv_rank(symbol=symbol),
        build_response=_build_response,
    )


@router.get("/{symbol}/iv-term-structure", response_model=SuccessResponse)
async def get_iv_term_structure(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get IV term structure for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:iv-term-structure:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_iv_term_structure(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/{symbol}/realized-vol", response_model=SuccessResponse)
async def get_realized_vol(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get realized volatility for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:realized-vol:{symbol}"

    def _build_response(data):
        if not data:
            raise HTTPException(status_code=404, detail=f"Volatility data not found for {symbol}")
        return make_response(data, symbol=symbol)

    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_realized_volatility(symbol=symbol),
        build_response=_build_response,
    )


@router.get("/{symbol}/vol-stats", response_model=SuccessResponse)
async def get_vol_stats(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get volatility stats for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:vol-stats:{symbol}"

    def _build_response(data):
        if not data:
            raise HTTPException(status_code=404, detail=f"Volatility stats not found for {symbol}")
        return make_response(data, symbol=symbol)

    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_volatility_stats(symbol=symbol),
        build_response=_build_response,
    )


@router.get("/{symbol}/iv-surface", response_model=SuccessResponse)
async def get_iv_surface(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get implied volatility surface for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:iv-surface:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_iv_surface(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )
