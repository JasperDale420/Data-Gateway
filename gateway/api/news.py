"""News API endpoints (NewsAPI.org)."""

from datetime import datetime

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import get_cache, require_api_key, require_provider_rate_limit
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.dedup import get_deduplicator
from gateway.core.metrics import record_route_cache
from gateway.providers.news import NewsProvider
from gateway.schemas import SuccessResponse

router = APIRouter(prefix="/api/v1/news", tags=["news"])

from gateway.core.logger import logger

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

    # Build cache key
    cache_key = f"news:articles:{symbols}:{keywords}:{start}:{end}:{limit}:{cursor}:{sort}"
    cached = await cache.get(cache_key)
    if cached:
        record_route_cache("news_articles", "hit", "query")
        return {"success": True, "data": cached, "cached": True}

    # Parse symbols/dates only on cache miss.
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else None
    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00")) if start else None
    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None

    try:
        deduper = get_deduplicator()

        async def _fetch():
            await require_provider_rate_limit("news")
            return await provider.get_articles(
                symbols=symbol_list,
                keywords=keywords,
                start=start_dt,
                end=end_dt,
                limit=limit,
                cursor=cursor,
                sort=sort,
            )

        result = await deduper.dedupe(cache_key, _fetch)
        # Cache for 60s per PRD
        await cache.set(cache_key, result, ttl=60)
        record_route_cache("news_articles", "miss", "query")
        return {"success": True, "data": result, "cached": False}

    except httpx.HTTPStatusError as e:
        logger.error("news_request_failed", route="get_articles", status_code=e.response.status_code, error=str(e))
        raise HTTPException(
            status_code=e.response.status_code,
            detail={
                "error_code": f"GW-E{e.response.status_code}",
                "message": f"Upstream provider error: {e.response.status_code}",
            },
        )
    except RuntimeError as e:
        raise HTTPException(
            status_code=503,
            detail={"error_code": "GW-E5002", "message": str(e)},
        )
    except Exception as e:
        logger.error("news_request_failed", route="get_articles", exc_info=True)
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
    cached = await cache.get(cache_key)
    if cached:
        record_route_cache("news_article", "hit", "id")
        return {"success": True, "data": cached, "cached": True}

    try:
        deduper = get_deduplicator()

        async def _fetch():
            await require_provider_rate_limit("news")
            data = await provider.get_article(article_id)
            if not data:
                raise HTTPException(
                    status_code=404,
                    detail={"error_code": "GW-E5004", "message": "Article not found"},
                )
            return data

        result = await deduper.dedupe(cache_key, _fetch)
        # Cache for 60s
        await cache.set(cache_key, result, ttl=60)
        record_route_cache("news_article", "miss", "id")
        return {"success": True, "data": result, "cached": False}

    except NotImplementedError as e:
        raise HTTPException(
            status_code=501,
            detail={"error_code": "GW-E5010", "message": str(e)},
        )
    except httpx.HTTPStatusError as e:
        logger.error("news_request_failed", route="get_article", status_code=e.response.status_code, error=str(e))
        raise HTTPException(
            status_code=e.response.status_code,
            detail={
                "error_code": f"GW-E{e.response.status_code}",
                "message": f"Upstream provider error: {e.response.status_code}",
            },
        )
    except HTTPException:
        raise
    except RuntimeError as e:
        raise HTTPException(
            status_code=503,
            detail={"error_code": "GW-E5002", "message": str(e)},
        )
    except Exception as e:
        logger.error("news_request_failed", route="get_article", exc_info=True)
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
    cached = await cache.get(cache_key)
    if cached:
        record_route_cache("news_sentiment", "hit", "symbol")
        return {"success": True, "data": cached, "cached": True}

    try:
        deduper = get_deduplicator()

        async def _fetch():
            await require_provider_rate_limit("news")
            return await provider.get_sentiment(symbol)

        result = await deduper.dedupe(cache_key, _fetch)
        # Cache for 60s
        await cache.set(cache_key, result, ttl=60)
        record_route_cache("news_sentiment", "miss", "symbol")
        return {"success": True, "data": result, "cached": False}

    except httpx.HTTPStatusError as e:
        logger.error("news_request_failed", route="get_sentiment", status_code=e.response.status_code, error=str(e))
        raise HTTPException(
            status_code=e.response.status_code,
            detail={
                "error_code": f"GW-E{e.response.status_code}",
                "message": f"Upstream provider error: {e.response.status_code}",
            },
        )
    except RuntimeError as e:
        raise HTTPException(
            status_code=503,
            detail={"error_code": "GW-E5002", "message": str(e)},
        )
    except Exception as e:
        logger.error("news_request_failed", route="get_sentiment", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error_code": "GW-E5006", "message": f"Failed to fetch sentiment: {e}"},
        )
