"""yfinance API endpoints for fundamentals and financials."""

from collections.abc import Awaitable, Callable
from typing import TypeVar

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import get_cache, get_registry, require_api_key, require_provider_rate_limit
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.dedup import get_deduplicator
from gateway.core.logger import logger
from gateway.core.metrics import record_route_cache
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter(prefix="/api/v1/yf", tags=["yfinance"])

PROVIDER_NOT_AVAILABLE = "yfinance provider not available"
CACHE_TTL = 300  # 5 minutes per PRD


def _normalize_cache_arg(value) -> str:
    if value is None:
        return "<none>"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value == "":
        return "<empty>"
    return str(value)


def _cache_key(prefix: str, symbol: str, *args) -> str:
    """Generate cache key for yfinance data."""
    parts = [prefix, symbol.upper()] + [_normalize_cache_arg(a) for a in args]
    return ":".join(parts)


T = TypeVar("T")


async def execute_yf_cached(
    provider_method: Callable[[], Awaitable[T]],
    cache: InMemoryCache,
    cache_key: str,
    route_name: str,
    miss_meta_builder: Callable[[T], dict] | None = None,
) -> dict:
    """Centralized yfinance execution wrapper with caching, dedup, and error handling."""
    cached = await cache.get(cache_key)
    if cached:
        record_route_cache(route_name, "hit")
        meta: dict = {"cached": True, "provider": "yfinance"}
        if miss_meta_builder:
            meta.update(miss_meta_builder(cached))
        return {"success": True, "data": cached, "meta": meta}

    record_route_cache(route_name, "miss")
    try:
        data = await get_deduplicator().dedupe(cache_key, provider_method)
        await cache.set(cache_key, data, ttl=CACHE_TTL)
        meta = {"cached": False, "provider": "yfinance"}
        if miss_meta_builder:
            meta.update(miss_meta_builder(data))
        return {"success": True, "data": data, "meta": meta}
    except httpx.HTTPStatusError as e:
        logger.error("yfinance_request_failed", route=route_name, error=str(e), status_code=e.response.status_code)
        raise HTTPException(
            status_code=e.response.status_code, detail=f"Upstream yfinance error: HTTP {e.response.status_code}"
        )
    except HTTPException:
        raise
    except Exception:
        logger.error("yfinance_request_failed", route=route_name, exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/ticker/{symbol}", response_model=SuccessResponse)
async def get_ticker_info(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get full ticker information (price, volume, market cap)."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        return await provider.get_ticker_info(symbol)

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:ticker", symbol),
        route_name="yf_ticker",
    )


@router.get("/ticker/{symbol}/info", response_model=SuccessResponse)
async def get_company_info(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company info (sector, industry, description)."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        return await provider.get_company_info(symbol)

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:info", symbol),
        route_name="yf_info",
    )


@router.get("/ticker/{symbol}/financials", response_model=SuccessResponse)
async def get_financials(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get income statement, balance sheet, cash flow."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        return await provider.get_financials(symbol)

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:financials", symbol),
        route_name="yf_financials",
    )


@router.get("/ticker/{symbol}/earnings", response_model=SuccessResponse)
async def get_earnings(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get quarterly and annual earnings."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        return await provider.get_earnings(symbol)

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:earnings", symbol),
        route_name="yf_earnings",
    )


@router.get("/ticker/{symbol}/history", response_model=SuccessResponse)
async def get_history(
    symbol: str,
    period: str = Query(default="1mo", description="1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max"),
    interval: str = Query(default="1d", description="1m, 5m, 15m, 30m, 1h, 1d, 1wk, 1mo"),
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical OHLCV data."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        bars = await provider.get_history(symbol, period=period, interval=interval, start=start, end=end)
        return {
            "symbol": symbol.upper(),
            "period": period,
            "interval": interval,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:history", symbol, period, interval, start, end),
        route_name="yf_history",
        miss_meta_builder=lambda d: {"count": len(d.get("bars", []))},
    )


@router.get("/ticker/{symbol}/options", response_model=SuccessResponse)
async def get_options(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get available option expirations."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        expirations = await provider.get_options_expirations(symbol)
        return {"symbol": symbol.upper(), "expirations": expirations}

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:options", symbol),
        route_name="yf_options",
        miss_meta_builder=lambda d: {"count": len(d.get("expirations", []))},
    )


@router.get("/ticker/{symbol}/options/{expiration}", response_model=SuccessResponse)
async def get_options_chain(
    symbol: str,
    expiration: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get option chain for specific expiration."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        chain = await provider.get_options_chain(symbol, expiration)
        return {"symbol": symbol.upper(), "expiration": expiration, **chain}

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:chain", symbol, expiration),
        route_name="yf_options_chain",
    )


@router.get("/ticker/{symbol}/recommendations", response_model=SuccessResponse)
async def get_recommendations(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get analyst recommendations."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        recs = await provider.get_recommendations(symbol)
        return {"symbol": symbol.upper(), "recommendations": recs}

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:recs", symbol),
        route_name="yf_recommendations",
        miss_meta_builder=lambda d: {"count": len(d.get("recommendations", []))},
    )


@router.get("/ticker/{symbol}/holders", response_model=SuccessResponse)
async def get_holders(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get institutional and insider holders."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        data = await provider.get_holders(symbol)
        data["symbol"] = symbol.upper()
        return data

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:holders", symbol),
        route_name="yf_holders",
    )


@router.get("/ticker/{symbol}/calendar", response_model=SuccessResponse)
async def get_calendar(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get earnings and dividend calendar."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        data = await provider.get_calendar(symbol)
        data["symbol"] = symbol.upper()
        return data

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:calendar", symbol),
        route_name="yf_calendar",
    )


# ─────────────────────────────────────────────────────────────────
# Phase 4: Additional Data
# ─────────────────────────────────────────────────────────────────


@router.get("/ticker/{symbol}/dividends", response_model=SuccessResponse)
async def get_dividends(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical dividend payments."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        divs = await provider.get_dividends(symbol)
        return {"symbol": symbol.upper(), "dividends": divs}

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:dividends", symbol),
        route_name="yf_dividends",
        miss_meta_builder=lambda d: {"count": len(d.get("dividends", []))},
    )


@router.get("/ticker/{symbol}/splits", response_model=SuccessResponse)
async def get_splits(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical stock splits."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        splits = await provider.get_splits(symbol)
        return {"symbol": symbol.upper(), "splits": splits}

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:splits", symbol),
        route_name="yf_splits",
        miss_meta_builder=lambda d: {"count": len(d.get("splits", []))},
    )


@router.get("/ticker/{symbol}/actions", response_model=SuccessResponse)
async def get_actions(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get combined dividends and splits."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        actions = await provider.get_actions(symbol)
        return {"symbol": symbol.upper(), "actions": actions}

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:actions", symbol),
        route_name="yf_actions",
    )


@router.get("/ticker/{symbol}/news", response_model=SuccessResponse)
async def get_news(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get recent news articles."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        news = await provider.get_news(symbol)
        return {"symbol": symbol.upper(), "articles": news}

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:news", symbol),
        route_name="yf_news",
        miss_meta_builder=lambda d: {"count": len(d.get("articles", []))},
    )


@router.get("/ticker/{symbol}/sustainability", response_model=SuccessResponse)
async def get_sustainability(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ESG sustainability scores."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        sus = await provider.get_sustainability(symbol)
        return {"symbol": symbol.upper(), "sustainability": sus}

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:sustainability", symbol),
        route_name="yf_sustainability",
    )


@router.get("/ticker/{symbol}/major-holders", response_model=SuccessResponse)
async def get_major_holders(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get major holders breakdown (% held by insiders, institutions)."""
    provider = registry.get("yfinance")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("yfinance")
        holders = await provider.get_major_holders(symbol)
        return {"symbol": symbol.upper(), "major_holders": holders}

    return await execute_yf_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("yf:major-holders", symbol),
        route_name="yf_major_holders",
    )
