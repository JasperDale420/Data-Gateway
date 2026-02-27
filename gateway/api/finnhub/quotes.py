"""Finnhub quotes and bars endpoints."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.finnhub.common import (
    CACHE_TTL,
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


@router.get("/quote/{symbol}", response_model=SuccessResponse)
async def get_quote(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get real-time quote for a symbol."""
    key = cache_key("finnhub:quote", symbol.upper())

    async def fetcher(provider):
        quote = await provider.get_quote(symbol)
        if not quote:
            raise HTTPException(status_code=404, detail=f"No data for symbol: {symbol}")
        return quote

    def cache_transform(quote):
        return quote.model_dump(mode="json")

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=CACHE_TTL,
        fetcher=fetcher,
        cache_transform=cache_transform,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_quote", status, "finnhub")
    return response


@router.get("/bars/{symbol}", response_model=SuccessResponse)
async def get_bars(
    symbol: str,
    resolution: str = Query(default="D", description="1, 5, 15, 30, 60, D, W, M"),
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical OHLCV bars."""
    key = cache_key("finnhub:bars", symbol.upper(), resolution, start, end)

    start_dt = datetime.fromisoformat(start) if start else None
    end_dt = datetime.fromisoformat(end) if end else None

    async def fetcher(provider):
        return await provider.get_bars(symbol, resolution=resolution, start=start_dt, end=end_dt)

    def cache_transform(bars):
        return {
            "symbol": symbol.upper(),
            "resolution": resolution,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=300,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_bars", status, "finnhub")
    return response
