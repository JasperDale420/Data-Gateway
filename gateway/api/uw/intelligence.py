"""Market intelligence endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

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
    paginate_response,
    require_api_key,
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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_off_lit_levels(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/congress/recent", response_model=SuccessResponse)
async def get_recent_congress_trades(
    limit: int = Query(default=50, le=200),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get recent congressional trades across all tickers."""
    cache_key = f"uw:congress-recent:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_recent_congress_trades(limit=limit),
        build_response=lambda data: {
            **paginate_response(data, limit),
            "meta": {"count": len(data), "provider": "unusual_whales"},
        },
    )


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

    def _build_response(data):
        if not data:
            raise HTTPException(status_code=404, detail=f"NOPE data not found for {symbol}")
        return make_response(data, symbol=symbol)

    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_nope(symbol=symbol),
        build_response=_build_response,
    )


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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_put_call_ratio(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )
