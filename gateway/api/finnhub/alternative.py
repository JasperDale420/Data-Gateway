"""Finnhub alternative data endpoints."""

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


@router.get("/fda-calendar", response_model=SuccessResponse)
async def get_fda_calendar(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get FDA drug approval calendar."""
    key = cache_key("finnhub:fda-calendar")

    async def fetcher(provider):
        return await provider.get_fda_calendar()

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_fda_calendar", status, "finnhub")
    return response


@router.get("/congress-trading", response_model=SuccessResponse)
async def get_congress_trading(
    symbol: str | None = Query(default=None, description="Filter by symbol"),
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get congressional trading data (STOCK Act disclosures)."""
    symbol_key = symbol.upper() if symbol else "all"
    key = cache_key("finnhub:congress-trading", symbol_key, start, end)

    async def fetcher(provider):
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        return await provider.get_congress_trading(symbol=symbol, start=start_dt, end=end_dt)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_congress_trading", status, "finnhub")
    return response


@router.get("/lobbying/{symbol}", response_model=SuccessResponse)
async def get_lobbying(
    symbol: str,
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get lobbying data for a symbol."""
    key = cache_key("finnhub:lobbying", symbol.upper(), start, end)

    async def fetcher(provider):
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        return await provider.get_lobbying(symbol, start=start_dt, end=end_dt)

    def cache_transform(data):
        return {"symbol": symbol.upper(), "lobbying": data}

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_lobbying", status, "finnhub")
    return response


@router.get("/usa-spending/{symbol}", response_model=SuccessResponse)
async def get_usa_spending(
    symbol: str,
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get USA government spending data for a symbol."""
    key = cache_key("finnhub:usa-spending", symbol.upper(), start, end)

    async def fetcher(provider):
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        return await provider.get_usa_spending(symbol, start=start_dt, end=end_dt)

    def cache_transform(data):
        return {"symbol": symbol.upper(), "spending": data}

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_usa_spending", status, "finnhub")
    return response
