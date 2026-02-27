"""Finnhub news endpoints."""

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


@router.get("/news/{symbol}", response_model=SuccessResponse)
async def get_company_news(
    symbol: str,
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company news articles."""
    key = cache_key("finnhub:news", symbol.upper(), start, end)

    start_dt = datetime.fromisoformat(start) if start else None
    end_dt = datetime.fromisoformat(end) if end else None

    async def fetcher(provider):
        return await provider.get_news(symbol, start=start_dt, end=end_dt)

    def cache_transform(articles):
        return {"symbol": symbol.upper(), "articles": articles}

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=300,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_company_news", status, "finnhub")
    return response


@router.get("/news/market/{category}", response_model=SuccessResponse)
async def get_market_news(
    category: str = "general",
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get general market news. Categories: general, forex, crypto, merger"""
    key = cache_key("finnhub:market-news", category)

    async def fetcher(provider):
        return await provider.get_market_news(category=category)

    def cache_transform(articles):
        return {"category": category, "articles": articles}

    response = await execute_finnhub_cached(
        cache=cache,
        cache_key_value=key,
        registry=registry,
        ttl=300,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta_builder=lambda orig, xform: {"count": len(orig)},
    )

    status = "hit" if response["meta"]["cached"] else "miss"
    record_route_cache("finnhub_market_news", status, "finnhub")
    return response
