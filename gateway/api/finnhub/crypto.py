"""Finnhub crypto endpoints."""

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


@router.get("/crypto/exchanges", response_model=SuccessResponse)
async def get_crypto_exchanges(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """List supported crypto exchanges."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:crypto-exchanges")
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_crypto_exchanges", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        exchanges = await provider.get_crypto_exchanges()
        await cache.set(key, exchanges, ttl=86400)
        record_route_cache("finnhub_crypto_exchanges", "miss", "finnhub")
        return {
            "success": True,
            "data": {"exchanges": exchanges},
            "meta": {"count": len(exchanges), "cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/crypto/symbols", response_model=SuccessResponse)
async def get_crypto_symbols(
    exchange: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get crypto symbols for an exchange."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:crypto-symbols", exchange)
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_crypto_symbols", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        symbols = await provider.get_crypto_symbols(exchange)
        data = {"exchange": exchange, "symbols": symbols}
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_crypto_symbols", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(symbols), "cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


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
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:crypto-candles", symbol, resolution, start, end)
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_crypto_candles", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None

        data = await provider.get_crypto_candles(symbol, resolution=resolution, start=start_dt, end=end_dt)
        await cache.set(key, data, ttl=300)
        record_route_cache("finnhub_crypto_candles", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/crypto/{symbol}/profile", response_model=SuccessResponse)
async def get_crypto_profile(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get crypto profile/metadata."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:crypto-profile", symbol.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_crypto_profile", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_crypto_profile(symbol)
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_crypto_profile", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")
