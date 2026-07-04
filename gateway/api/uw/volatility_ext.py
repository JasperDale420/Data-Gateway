"""UW volatility-analytics endpoints — anomaly, character, stats, VRP, VIX term structure.

Raw-HTTP passthrough endpoints not covered by the vendored SDK (v5.1).
"""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_DATE,
    DESC_LIMIT,
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    count_of,
    execute_uw_cached,
    get_cache,
    get_registry,
    make_response,
    require_api_key,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/stock/{symbol}/volatility/anomaly", response_model=SuccessResponse)
async def get_volatility_anomaly(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Volatility anomaly score for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:vol-anomaly:{symbol}:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_volatility_anomaly(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/volatility/character", response_model=SuccessResponse)
async def get_volatility_character(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Volatility character for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:vol-character:{symbol}:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_volatility_character(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/volatility/stats", response_model=SuccessResponse)
async def get_volatility_stats(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Volatility statistics for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:vol-stats:{symbol}:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_ticker_volatility_stats(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/stock/{symbol}/volatility/variance-risk-premium", response_model=SuccessResponse)
async def get_variance_risk_premium(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Variance risk premium for a ticker."""
    symbol = symbol.upper()
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:vol-vrp:{symbol}:{date or 'latest'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_variance_risk_premium(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(data, symbol=symbol, count=count_of(data)),
    )


@router.get("/volatility/anomaly/top", response_model=SuccessResponse)
async def get_top_volatility_anomalies(
    direction: str = Query(description="`short_vol` or `long_vol`"),
    date: str | None = Query(default=None, description=DESC_DATE),
    limit: int = Query(default=50, ge=1, le=200, description=DESC_LIMIT),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Top volatility anomalies market-wide."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:vol-anomaly-top:{direction}:{date or 'latest'}:{limit}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_top_volatility_anomalies(direction=direction, date_str=date, limit=limit),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/volatility/character/top", response_model=SuccessResponse)
async def get_top_volatility_character(
    date: str | None = Query(default=None, description=DESC_DATE),
    limit: int = Query(default=50, ge=1, le=200, description=DESC_LIMIT),
    sort: str | None = Query(default=None, description="`half_life` (default), `hurst`, or `neg_entropy`"),
    direction: str | None = Query(default=None, alias="dir", description="`asc` (default) or `desc`"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Top volatility character market-wide."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:vol-character-top:{date or 'latest'}:{limit}:{sort or 'def'}:{direction or 'def'}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_top_volatility_character(
            date_str=date, limit=limit, sort=sort, direction=direction
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/volatility/vix-term-structure", response_model=SuccessResponse)
async def get_vix_term_structure(
    history_days: int = Query(default=90, ge=1, le=365, description="Days of history (default 90, max 365)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """VIX term structure with optional history window."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:vix-term-structure:{history_days}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_vix_term_structure(history_days=history_days),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )
