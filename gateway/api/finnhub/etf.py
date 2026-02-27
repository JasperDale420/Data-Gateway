"""Finnhub ETF and index endpoints."""

import structlog
from fastapi import APIRouter, Depends

from gateway.api.finnhub.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    cache_key,
    execute_finnhub_cached,
    get_cache,
    get_registry,
    require_api_key,
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
    key = cache_key("finnhub:etf-profile", symbol.upper())

    async def fetcher(provider):
        return await provider.get_etf_profile(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_etf_profile", status, "finnhub")
    return response


@router.get("/etf/{symbol}/holdings", response_model=SuccessResponse)
async def get_etf_holdings(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF holdings."""
    key = cache_key("finnhub:etf-holdings", symbol.upper())

    async def fetcher(provider):
        return await provider.get_etf_holdings(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_etf_holdings", status, "finnhub")
    return response


@router.get("/etf/{symbol}/sector", response_model=SuccessResponse)
async def get_etf_sector(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF sector exposure."""
    key = cache_key("finnhub:etf-sector", symbol.upper())

    async def fetcher(provider):
        return await provider.get_etf_sector(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_etf_sector", status, "finnhub")
    return response


@router.get("/etf/{symbol}/country", response_model=SuccessResponse)
async def get_etf_country(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF country exposure."""
    key = cache_key("finnhub:etf-country", symbol.upper())

    async def fetcher(provider):
        return await provider.get_etf_country(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_etf_country", status, "finnhub")
    return response


@router.get("/index/{symbol}/constituents", response_model=SuccessResponse)
async def get_index_constituents(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get index constituents."""
    key = cache_key("finnhub:index-constituents", symbol.upper())

    async def fetcher(provider):
        return await provider.get_index_constituents(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_index_constituents", status, "finnhub")
    return response


@router.get("/index/{symbol}/historical", response_model=SuccessResponse)
async def get_index_historical(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical index constituent changes."""
    key = cache_key("finnhub:index-historical", symbol.upper())

    async def fetcher(provider):
        return await provider.get_index_historical(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_index_historical", status, "finnhub")
    return response
