"""Finnhub news endpoints."""

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
from gateway.schemas import SuccessResponse

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
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:news", symbol.upper(), start, end)
    cached = cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None

        articles = await provider.get_news(symbol, start=start_dt, end=end_dt)
        data = {"symbol": symbol.upper(), "articles": articles}
        cache.set(key, data, ttl=300)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(articles), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/news/market/{category}", response_model=SuccessResponse)
async def get_market_news(
    category: str = "general",
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get general market news. Categories: general, forex, crypto, merger"""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key("finnhub:market-news", category)
    cached = cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "finnhub"},
        }

    try:
        await require_provider_rate_limit("finnhub")
        articles = await provider.get_market_news(category=category)
        data = {"category": category, "articles": articles}
        cache.set(key, data, ttl=300)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(articles), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
