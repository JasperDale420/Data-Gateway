"""Finnhub forex endpoints."""

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

router = APIRouter()


@router.get("/forex/rates", response_model=SuccessResponse)
async def get_forex_rates(
    base: str = Query(default="USD", description="Base currency"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all forex rates for a base currency."""
    key = cache_key("finnhub:forex-rates", base.upper())

    async def fetcher(provider):
        return await provider.get_forex_rates(base=base)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=60,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_forex_rates", status, "finnhub")
    return response


@router.get("/forex/exchanges", response_model=SuccessResponse)
async def get_forex_exchanges(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """List supported forex exchanges."""
    key = cache_key("finnhub:forex-exchanges")

    async def fetcher(provider):
        return await provider.get_forex_exchanges()

    def cache_transform(data):
        return {"exchanges": data}

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_forex_exchanges", status, "finnhub")
    return response


@router.get("/forex/symbols", response_model=SuccessResponse)
async def get_forex_symbols(
    exchange: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get forex symbols for an exchange."""
    key = cache_key("finnhub:forex-symbols", exchange)

    async def fetcher(provider):
        return await provider.get_forex_symbols(exchange)

    def cache_transform(data):
        return {"exchange": exchange, "symbols": data}

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_forex_symbols", status, "finnhub")
    return response


@router.get("/forex/candles/{symbol}", response_model=SuccessResponse)
async def get_forex_candles(
    symbol: str,
    resolution: str = Query(default="D", description="Resolution: 1, 5, 15, 30, 60, D, W, M"),
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get forex OHLC candles."""
    key = cache_key("finnhub:forex-candles", symbol, resolution, start, end)

    async def fetcher(provider):
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        return await provider.get_forex_candles(symbol, resolution=resolution, start=start_dt, end=end_dt)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=300,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_forex_candles", status, "finnhub")
    return response
