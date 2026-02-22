"""Finnhub ETF and index endpoints."""

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


@router.get("/etf/{symbol}/profile", response_model=SuccessResponse)
async def get_etf_profile(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF profile."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:etf-profile", symbol.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_etf_profile", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_etf_profile(symbol)
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_etf_profile", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/etf/{symbol}/holdings", response_model=SuccessResponse)
async def get_etf_holdings(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF holdings."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:etf-holdings", symbol.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_etf_holdings", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_etf_holdings(symbol)
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_etf_holdings", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/etf/{symbol}/sector", response_model=SuccessResponse)
async def get_etf_sector(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF sector exposure."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:etf-sector", symbol.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_etf_sector", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_etf_sector(symbol)
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_etf_sector", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/etf/{symbol}/country", response_model=SuccessResponse)
async def get_etf_country(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF country exposure."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:etf-country", symbol.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_etf_country", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_etf_country(symbol)
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_etf_country", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/index/{symbol}/constituents", response_model=SuccessResponse)
async def get_index_constituents(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get index constituents."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:index-constituents", symbol.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_index_constituents", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_index_constituents(symbol)
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_index_constituents", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/index/{symbol}/historical", response_model=SuccessResponse)
async def get_index_historical(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical index constituent changes."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:index-historical", symbol.upper())
    cached = await cache.get(key)
    if cached:
        record_route_cache("finnhub_index_historical", "hit", "finnhub")
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_index_historical(symbol)
        await cache.set(key, data, ttl=86400)
        record_route_cache("finnhub_index_historical", "miss", "finnhub")
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "finnhub"},
        }
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")
