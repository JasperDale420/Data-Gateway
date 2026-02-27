"""Finnhub crypto endpoints."""

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


@router.get("/crypto/exchanges", response_model=SuccessResponse)
async def get_crypto_exchanges(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """List supported crypto exchanges."""
    key = cache_key("finnhub:crypto-exchanges")

    async def fetcher(provider):
        return await provider.get_crypto_exchanges()

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
    record_route_cache("finnhub_crypto_exchanges", status, "finnhub")
    return response


@router.get("/crypto/symbols", response_model=SuccessResponse)
async def get_crypto_symbols(
    exchange: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get crypto symbols for an exchange."""
    key = cache_key("finnhub:crypto-symbols", exchange)

    async def fetcher(provider):
        return await provider.get_crypto_symbols(exchange)

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
    record_route_cache("finnhub_crypto_symbols", status, "finnhub")
    return response


@router.get("/crypto/candles/{symbol}", response_model=SuccessResponse)
async def get_crypto_candles(
    symbol: str,
    resolution: str = Query(default="D", description="Resolution: 1, 5, 15, 30, 60, D, W, M"),
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get crypto OHLC candles."""
    key = cache_key("finnhub:crypto-candles", symbol, resolution, start, end)

    async def fetcher(provider):
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        return await provider.get_crypto_candles(symbol, resolution=resolution, start=start_dt, end=end_dt)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=300,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_crypto_candles", status, "finnhub")
    return response


@router.get("/crypto/{symbol}/profile", response_model=SuccessResponse)
async def get_crypto_profile(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get crypto profile/metadata."""
    key = cache_key("finnhub:crypto-profile", symbol.upper())

    async def fetcher(provider):
        return await provider.get_crypto_profile(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_crypto_profile", status, "finnhub")
    return response
