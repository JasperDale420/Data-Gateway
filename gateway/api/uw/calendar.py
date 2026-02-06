"""Market calendar endpoints (economic, FDA, holidays, etc.) for Unusual Whales."""

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


@router.get("/market/economic-calendar", response_model=SuccessResponse)
async def get_economic_calendar_market(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get economic calendar events."""
    cache_key = "uw:market:economic-calendar"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_economic_calendar(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/market/fda-calendar", response_model=SuccessResponse)
async def get_fda_calendar(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get FDA calendar events."""
    cache_key = "uw:market:fda-calendar"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_fda_calendar(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/market/holidays", response_model=SuccessResponse)
async def get_market_holidays(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market holidays."""
    cache_key = "uw:market:holidays"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=86400,
        fetcher=lambda provider: provider.get_market_holidays(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/market/imbalances", response_model=SuccessResponse)
async def get_market_imbalances(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market imbalances data."""
    cache_key = "uw:market:imbalances"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_market_imbalances(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/market/options-volume", response_model=SuccessResponse)
async def get_market_options_volume(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get total market options volume."""
    cache_key = "uw:market:options-volume"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_market_options_volume(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/market/insider-trades", response_model=SuccessResponse)
async def get_market_insider_trades(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market-wide insider trades."""
    cache_key = "uw:market:insider-trades"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_market_insider_trades(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/market/sector-stats", response_model=SuccessResponse)
async def get_sector_stats(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get sector statistics."""
    cache_key = "uw:market:sector-stats"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_sector_stats(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/market/{etf}/etf-tide", response_model=SuccessResponse)
async def get_market_tide_by_etf(
    etf: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get market tide data for a specific ETF."""
    etf = etf.upper()
    cache_key = f"uw:market:etf-tide:{etf}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_market_tide_by_etf(etf=etf),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"etf": etf},
        ),
    )
