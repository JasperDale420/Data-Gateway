"""Finnhub analysis endpoints - sentiment, upgrade/downgrade."""

from fastapi import APIRouter, Depends, Query

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


@router.get("/insider-sentiment/{symbol}", response_model=SuccessResponse)
async def get_insider_sentiment(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get aggregate insider sentiment."""
    key = cache_key("finnhub:insider-sentiment", symbol.upper())

    async def fetcher(provider):
        return await provider.get_insider_sentiment(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_insider_sentiment", status, "finnhub")
    return response


@router.get("/upgrade-downgrade/{symbol}", response_model=SuccessResponse)
async def get_upgrade_downgrade(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get analyst upgrade/downgrade history."""
    key = cache_key("finnhub:upgrade-downgrade", symbol.upper())

    async def fetcher(provider):
        return await provider.get_upgrade_downgrade(symbol)

    def cache_transform(data):
        return {"symbol": symbol.upper(), "history": data}

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_upgrade_downgrade", status, "finnhub")
    return response


@router.get("/social-sentiment/{symbol}", response_model=SuccessResponse)
async def get_social_sentiment(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get social media sentiment."""
    key = cache_key("finnhub:social-sentiment", symbol.upper())

    async def fetcher(provider):
        return await provider.get_social_sentiment(symbol)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=1800,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_social_sentiment", status, "finnhub")
    return response


@router.get("/support-resistance/{symbol}", response_model=SuccessResponse)
async def get_support_resistance(
    symbol: str,
    resolution: str = Query(default="D", description="Resolution: 1, 5, 15, 30, 60, D, W, M"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get support/resistance levels."""
    key = cache_key("finnhub:support-resistance", symbol.upper(), resolution)

    async def fetcher(provider):
        return await provider.get_support_resistance(symbol, resolution=resolution)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_support_resistance", status, "finnhub")
    return response


@router.get("/patterns/{symbol}", response_model=SuccessResponse)
async def get_pattern_recognition(
    symbol: str,
    resolution: str = Query(default="D", description="Resolution: 1, 5, 15, 30, 60, D, W, M"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get recognized chart patterns."""
    key = cache_key("finnhub:patterns", symbol.upper(), resolution)

    async def fetcher(provider):
        return await provider.get_pattern_recognition(symbol, resolution=resolution)

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=3600,
        fetcher=fetcher,
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_pattern_recognition", status, "finnhub")
    return response
