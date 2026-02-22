"""Finnhub quotes and bars endpoints."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.finnhub.common import (
    CACHE_TTL,
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


@router.get("/quote/{symbol}", response_model=SuccessResponse)
async def get_quote(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get real-time quote for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:quote", symbol.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_quote", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        quote = await provider.get_quote(symbol)
        if not quote:
            raise HTTPException(status_code=404, detail=f"No data for symbol: {symbol}")

        data = quote.model_dump(mode="json")
        await cache.set(key, data, ttl=CACHE_TTL)
        record_route_cache("finnhub_quote", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except HTTPException:
        raise
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


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
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:bars", symbol.upper(), resolution, start, end)
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_bars", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None

        bars = await provider.get_bars(symbol, resolution=resolution, start=start_dt, end=end_dt)
        data = {
            "symbol": symbol.upper(),
            "resolution": resolution,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        await cache.set(key, data, ttl=300)
        record_route_cache("finnhub_bars", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(bars), "cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")
