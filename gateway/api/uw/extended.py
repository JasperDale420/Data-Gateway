"""Remaining endpoints (congress, seasonality, shorts, market) for Unusual Whales."""

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
    require_api_key,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/congress/late-reports", response_model=SuccessResponse)
async def get_congress_late_reports(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get late congressional trading reports."""
    cache_key = f"uw:congress:late-reports:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_congress_late_reports(limit=limit),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/congress/reports", response_model=SuccessResponse)
async def get_congress_reports(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get congressional trading reports."""
    cache_key = f"uw:congress:reports:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_congress_reports(limit=limit),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/seasonality/monthly-top-performers/{month}", response_model=SuccessResponse)
async def get_monthly_top_performers(
    month: int,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get top performing stocks by month (1-12)."""
    cache_key = f"uw:seasonality:monthly-top-performers:{month}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_monthly_top_performers(month=month),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"month": month},
        ),
    )


@router.get("/seasonality/{symbol}/price-changes-by-month", response_model=SuccessResponse)
async def get_price_changes_by_month_year(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get price changes by month and year for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:seasonality:price-changes:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_price_changes_by_month_year(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/shorts/{symbol}/data", response_model=SuccessResponse)
async def get_shorts_data(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short data for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:shorts:data:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_shorts_data(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/shorts/{symbol}/interest-float", response_model=SuccessResponse)
async def get_short_interest_float(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short interest as percentage of float."""
    symbol = symbol.upper()
    cache_key = f"uw:shorts:interest-float:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_short_interest_float(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol),
    )


@router.get("/shorts/{symbol}/volumes-by-exchange", response_model=SuccessResponse)
async def get_short_volumes_by_exchange(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get short volumes by exchange."""
    symbol = symbol.upper()
    cache_key = f"uw:shorts:volumes-by-exchange:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_short_volumes_by_exchange(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )


@router.get("/market/spike", response_model=SuccessResponse)
async def get_market_spike(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market spike data."""
    cache_key = "uw:market:spike"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_market_spike(),
        build_response=lambda data: make_response(data, count=len(data)),
    )
