"""Finnhub earnings, estimates, and recommendations endpoints."""

import structlog
from fastapi import APIRouter, Depends, Query

from gateway.api.finnhub.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    cache_key,
    datetime,
    execute_finnhub_cached,
    get_cache,
    get_registry,
    require_api_key,
)
from gateway.core.metrics import record_route_cache
from gateway.schemas import SuccessResponse

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.get("/earnings", response_model=SuccessResponse)
async def get_earnings_calendar(
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get earnings calendar."""
    key = cache_key("finnhub:earnings", start, end)

    async def fetcher(provider):
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        return await provider.get_earnings_calendar(start=start_dt, end=end_dt)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_earnings_calendar", status, "finnhub")
    return response


@router.get("/recommendations/{symbol}", response_model=SuccessResponse)
async def get_recommendations(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get analyst recommendation trends."""
    key = cache_key("finnhub:recommendations", symbol.upper())

    async def fetcher(provider):
        return await provider.get_recommendation_trends(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_recommendations", status, "finnhub")
    return response


@router.get("/estimates/eps/{symbol}", response_model=SuccessResponse)
async def get_eps_estimates(
    symbol: str,
    freq: str = Query(default="quarterly", description="quarterly or annual"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get EPS estimates for a symbol."""
    key = cache_key("finnhub:eps-estimates", symbol.upper(), freq)

    async def fetcher(provider):
        return await provider.get_eps_estimates(symbol, freq=freq)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_eps_estimates", status, "finnhub")
    return response


@router.get("/estimates/revenue/{symbol}", response_model=SuccessResponse)
async def get_revenue_estimates(
    symbol: str,
    freq: str = Query(default="quarterly", description="quarterly or annual"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get revenue estimates for a symbol."""
    key = cache_key("finnhub:revenue-estimates", symbol.upper(), freq)

    async def fetcher(provider):
        return await provider.get_revenue_estimates(symbol, freq=freq)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_revenue_estimates", status, "finnhub")
    return response


@router.get("/estimates/ebit/{symbol}", response_model=SuccessResponse)
async def get_ebit_estimates(
    symbol: str,
    freq: str = Query(default="quarterly", description="quarterly or annual"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get EBIT estimates for a symbol."""
    key = cache_key("finnhub:ebit-estimates", symbol.upper(), freq)

    async def fetcher(provider):
        return await provider.get_ebit_estimates(symbol, freq=freq)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_ebit_estimates", status, "finnhub")
    return response


@router.get("/estimates/ebitda/{symbol}", response_model=SuccessResponse)
async def get_ebitda_estimates(
    symbol: str,
    freq: str = Query(default="quarterly", description="quarterly or annual"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get EBITDA estimates for a symbol."""
    key = cache_key("finnhub:ebitda-estimates", symbol.upper(), freq)

    async def fetcher(provider):
        return await provider.get_ebitda_estimates(symbol, freq=freq)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_ebitda_estimates", status, "finnhub")
    return response


@router.get("/price-target/{symbol}", response_model=SuccessResponse)
async def get_price_target(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get analyst price target for a symbol."""
    key = cache_key("finnhub:price-target", symbol.upper())

    async def fetcher(provider):
        return await provider.get_price_target(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_price_target", status, "finnhub")
    return response
