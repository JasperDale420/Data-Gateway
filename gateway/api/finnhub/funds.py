"""Finnhub mutual fund endpoints."""

import structlog
from fastapi import APIRouter, Depends, HTTPException

from gateway.api.finnhub.common import (
    PROVIDER_NOT_AVAILABLE,
    Client,
    InMemoryCache,
    ProviderRegistry,
    cache_key,
    get_cache,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.core.metrics import record_route_cache
from gateway.schemas import SuccessResponse

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.get("/mutual-fund/{symbol}/profile", response_model=SuccessResponse)
async def get_mutual_fund_profile(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund profile."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:mf-profile", symbol.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_mutual_fund_profile", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_mutual_fund_profile(symbol)
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_mutual_fund_profile", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/mutual-fund/{symbol}/holdings", response_model=SuccessResponse)
async def get_mutual_fund_holdings(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund holdings."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:mf-holdings", symbol.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_mutual_fund_holdings", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_mutual_fund_holdings(symbol)
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_mutual_fund_holdings", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/mutual-fund/{symbol}/sector", response_model=SuccessResponse)
async def get_mutual_fund_sector(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund sector exposure."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:mf-sector", symbol.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_mutual_fund_sector", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_mutual_fund_sector(symbol)
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_mutual_fund_sector", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")
