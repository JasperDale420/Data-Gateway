"""News API endpoints (EventRegistry)."""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import get_cache, require_api_key, require_provider_rate_limit
from gateway.schemas import SuccessResponse
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.providers.news import NewsProvider

router = APIRouter(prefix="/api/v1/news", tags=["news"])

# Module-level provider instance
_provider: NewsProvider | None = None


def get_news_provider() -> NewsProvider:
    """Get the news provider instance."""
    global _provider
    if _provider is None:
        _provider = NewsProvider()
    return _provider


async def _ensure_initialized(provider: NewsProvider) -> None:
    """Ensure provider is initialized."""
    if provider._client is None:
        await provider.initialize({})


@router.get("/articles", response_model=SuccessResponse)
async def get_articles(
    symbols: str | None = Query(default=None, description="Comma-separated tickers"),
    keywords: str | None = Query(default=None, description="Search terms"),
    start: str | None = Query(default=None, description="Start timestamp (ISO8601)"),
    end: str | None = Query(default=None, description="End timestamp (ISO8601)"),
    limit: int = Query(default=50, le=200, ge=1),
    cursor: str | None = Query(default=None, description="Pagination cursor"),
    sort: str = Query(default="desc", pattern="^(asc|desc)$"),
    client: Client = Depends(require_api_key),
    cache: InMemoryCache = Depends(get_cache),
):
    """Search news articles with optional filters."""
    provider = get_news_provider()
    await _ensure_initialized(provider)

    # Parse symbols
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else None

    # Parse dates
    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00")) if start else None
    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None

    # Build cache key
    cache_key = f"news:articles:{symbols}:{keywords}:{start}:{end}:{limit}:{cursor}:{sort}"
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "cached": True}

    try:
        await require_provider_rate_limit("news")
        result = await provider.get_articles(
            symbols=symbol_list,
            keywords=keywords,
            start=start_dt,
            end=end_dt,
            limit=limit,
            cursor=cursor,
            sort=sort,
        )
        # Cache for 60s per PRD
        cache.set(cache_key, result, ttl=60)
        return {"success": True, "data": result, "cached": False}

    except RuntimeError as e:
        raise HTTPException(
            status_code=503,
            detail={"error_code": "GW-E5002", "message": str(e)},
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error_code": "GW-E5003", "message": f"Failed to fetch articles: {e}"},
        )


@router.get("/articles/{article_id}", response_model=SuccessResponse)
async def get_article(
    article_id: str,
    client: Client = Depends(require_api_key),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get specific article by ID."""
    provider = get_news_provider()
    await _ensure_initialized(provider)

    cache_key = f"news:article:{article_id}"
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "cached": True}

    try:
        await require_provider_rate_limit("news")
        result = await provider.get_article(article_id)
        if not result:
            raise HTTPException(
                status_code=404,
                detail={"error_code": "GW-E5004", "message": "Article not found"},
            )
        # Cache for 60s
        cache.set(cache_key, result, ttl=60)
        return {"success": True, "data": result, "cached": False}

    except HTTPException:
        raise
    except RuntimeError as e:
        raise HTTPException(
            status_code=503,
            detail={"error_code": "GW-E5002", "message": str(e)},
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error_code": "GW-E5005", "message": f"Failed to fetch article: {e}"},
        )


@router.get("/sentiment/{symbol}", response_model=SuccessResponse)
async def get_sentiment(
    symbol: str,
    client: Client = Depends(require_api_key),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get aggregated sentiment for ticker."""
    provider = get_news_provider()
    await _ensure_initialized(provider)

    symbol = symbol.upper()
    cache_key = f"news:sentiment:{symbol}"
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "cached": True}

    try:
        await require_provider_rate_limit("news")
        result = await provider.get_sentiment(symbol)
        # Cache for 60s
        cache.set(cache_key, result, ttl=60)
        return {"success": True, "data": result, "cached": False}

    except RuntimeError as e:
        raise HTTPException(
            status_code=503,
            detail={"error_code": "GW-E5002", "message": str(e)},
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error_code": "GW-E5006", "message": f"Failed to fetch sentiment: {e}"},
        )
