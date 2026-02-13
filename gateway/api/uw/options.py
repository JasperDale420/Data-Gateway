"""Options analytics endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_DATE,
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


@router.get("/{symbol}/net-premium", response_model=SuccessResponse)
async def get_net_premium(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get net premium ticks for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:net-premium:{symbol}:{date or 'latest'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_net_premium_ticks(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/{symbol}/max-pain", response_model=SuccessResponse)
async def get_max_pain(
    symbol: str,
    expiry: str | None = Query(default=None, description="Expiration (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get max pain data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:max-pain:{symbol}:{expiry or 'all'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_max_pain(symbol=symbol, expiry=expiry),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/{symbol}/iv-rank", response_model=SuccessResponse)
async def get_iv_rank(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get IV rank for a ticker."""
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


@router.get("/{symbol}/oi-change", response_model=SuccessResponse)
async def get_oi_change(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get OI change data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:oi-change:{symbol}:{date or 'latest'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_oi_change(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )
