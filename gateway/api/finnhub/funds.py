"""Finnhub mutual fund endpoints."""

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

router = APIRouter()


@router.get("/mutual-fund/{symbol}/profile", response_model=SuccessResponse)
async def get_mutual_fund_profile(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund profile."""
    key = cache_key("finnhub:mf-profile", symbol.upper())

    async def fetcher(provider):
        return await provider.get_mutual_fund_profile(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_mutual_fund_profile", status, "finnhub")
    return response


@router.get("/mutual-fund/{symbol}/holdings", response_model=SuccessResponse)
async def get_mutual_fund_holdings(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund holdings."""
    key = cache_key("finnhub:mf-holdings", symbol.upper())

    async def fetcher(provider):
        return await provider.get_mutual_fund_holdings(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_mutual_fund_holdings", status, "finnhub")
    return response


@router.get("/mutual-fund/{symbol}/sector", response_model=SuccessResponse)
async def get_mutual_fund_sector(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund sector exposure."""
    key = cache_key("finnhub:mf-sector", symbol.upper())

    async def fetcher(provider):
        return await provider.get_mutual_fund_sector(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=86400,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_mutual_fund_sector", status, "finnhub")
    return response
