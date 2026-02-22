"""Finnhub forex endpoints."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.finnhub.common import (
    PROVIDER_NOT_AVAILABLE,
    Client,
    InMemoryCache,
    ProviderRegistry,
    cache_key,
    datetime,
    get_cache,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.core.metrics import record_route_cache
from gateway.schemas import SuccessResponse

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.get("/forex/rates", response_model=SuccessResponse)
async def get_forex_rates(
    base: str = Query(default="USD", description="Base currency"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all forex rates for a base currency."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:forex-rates", base.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_forex_rates", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_forex_rates(base=base)
        await cache.set(key, data, ttl=60)
        record_route_cache("finnhub_forex_rates", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/forex/exchanges", response_model=SuccessResponse)
async def get_forex_exchanges(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """List supported forex exchanges."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:forex-exchanges")
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_forex_exchanges", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        exchanges = await provider.get_forex_exchanges()
        await cache.set(key, exchanges, ttl=86400)
        record_route_cache("finnhub_forex_exchanges", "miss", "finnhub")
        return {
            "success": True,
            "data": {"exchanges": exchanges},
            "meta": {"count": len(exchanges), "cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/forex/symbols", response_model=SuccessResponse)
async def get_forex_symbols(
    exchange: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get forex symbols for an exchange."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:forex-symbols", exchange)
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_forex_symbols", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        symbols = await provider.get_forex_symbols(exchange)
        data = {"exchange": exchange, "symbols": symbols}
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_forex_symbols", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(symbols), "cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


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
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:forex-candles", symbol, resolution, start, end)
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_forex_candles", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None

        data = await provider.get_forex_candles(symbol, resolution=resolution, start=start_dt, end=end_dt)
        await cache.set(key, data, ttl=300)
        record_route_cache("finnhub_forex_candles", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")
