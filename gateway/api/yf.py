"""yfinance API endpoints for fundamentals and financials."""

from collections.abc import Callable
from typing import TypeVar

from fastapi import APIRouter, Depends, Query

from gateway.api.deps import (
    execute_provider_cached,
    get_cache,
    get_registry,
    make_cache_key,
    require_api_key,
)
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.dedup import get_deduplicator
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter(prefix="/api/v1/yf", tags=["yfinance"])

PROVIDER_NOT_AVAILABLE = "yfinance provider not available"
CACHE_TTL = 300  # 5 minutes per PRD


def _cache_key(prefix: str, symbol: str, *args) -> str:
    """Generate cache key for yfinance data."""
    return make_cache_key(prefix, symbol.upper(), *args)


T = TypeVar("T")


async def execute_yf_cached[T](
    *,
    registry: ProviderRegistry,
    cache: InMemoryCache,
    cache_key: str,
    route_name: str,
    fetcher: Callable,
    miss_meta_builder: Callable[[T], dict] | None = None,
) -> dict:
    """Centralized yfinance execution wrapper with caching, dedup, and error handling.

    Args:
        registry: Provider registry for provider lookup.
        cache: Cache backend.
        cache_key: Pre-built cache key.
        route_name: Label for cache-hit/miss metrics.
        fetcher: ``async (provider) -> data`` callable.
        miss_meta_builder: Optional ``(data) -> dict`` for extra metadata.
    """
    deduplicator = get_deduplicator()

    async def _deduped_fetcher(provider):
        return await deduplicator.dedupe(cache_key, lambda: fetcher(provider))

    return await execute_provider_cached(
        provider_name="yfinance",
        registry=registry,
        cache=cache,
        cache_key=cache_key,
        ttl=CACHE_TTL,
        fetcher=_deduped_fetcher,
        miss_meta_builder=miss_meta_builder,
        route_label=route_name,
        error_label="yfinance_request_failed",
        not_available_msg=PROVIDER_NOT_AVAILABLE,
    )


@router.get("/ticker/{symbol}", response_model=SuccessResponse)
async def get_ticker_info(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get full ticker information (price, volume, market cap)."""
    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:ticker", symbol),
        route_name="yf_ticker",
        fetcher=lambda p: p.get_ticker_info(symbol),
    )


@router.get("/ticker/{symbol}/info", response_model=SuccessResponse)
async def get_company_info(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company info (sector, industry, description)."""
    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:info", symbol),
        route_name="yf_info",
        fetcher=lambda p: p.get_company_info(symbol),
    )


@router.get("/ticker/{symbol}/financials", response_model=SuccessResponse)
async def get_financials(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get income statement, balance sheet, cash flow."""
    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:financials", symbol),
        route_name="yf_financials",
        fetcher=lambda p: p.get_financials(symbol),
    )


@router.get("/ticker/{symbol}/earnings", response_model=SuccessResponse)
async def get_earnings(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get quarterly and annual earnings."""
    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:earnings", symbol),
        route_name="yf_earnings",
        fetcher=lambda p: p.get_earnings(symbol),
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

    async def _fetch(provider):
        bars = await provider.get_history(symbol, period=period, interval=interval, start=start, end=end)
        return {
            "symbol": symbol.upper(),
            "period": period,
            "interval": interval,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:history", symbol, period, interval, start, end),
        route_name="yf_history",
        fetcher=_fetch,
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

    async def _fetch(provider):
        expirations = await provider.get_options_expirations(symbol)
        return {"symbol": symbol.upper(), "expirations": expirations}

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:options", symbol),
        route_name="yf_options",
        fetcher=_fetch,
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

    async def _fetch(provider):
        chain = await provider.get_options_chain(symbol, expiration)
        return {"symbol": symbol.upper(), "expiration": expiration, **chain}

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:chain", symbol, expiration),
        route_name="yf_options_chain",
        fetcher=_fetch,
    )


@router.get("/ticker/{symbol}/recommendations", response_model=SuccessResponse)
async def get_recommendations(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get analyst recommendations."""

    async def _fetch(provider):
        recs = await provider.get_recommendations(symbol)
        return {"symbol": symbol.upper(), "recommendations": recs}

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:recs", symbol),
        route_name="yf_recommendations",
        fetcher=_fetch,
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

    async def _fetch(provider):
        data = await provider.get_holders(symbol)
        data["symbol"] = symbol.upper()
        return data

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:holders", symbol),
        route_name="yf_holders",
        fetcher=_fetch,
    )


@router.get("/ticker/{symbol}/calendar", response_model=SuccessResponse)
async def get_calendar(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get earnings and dividend calendar."""

    async def _fetch(provider):
        data = await provider.get_calendar(symbol)
        data["symbol"] = symbol.upper()
        return data

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:calendar", symbol),
        route_name="yf_calendar",
        fetcher=_fetch,
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

    async def _fetch(provider):
        divs = await provider.get_dividends(symbol)
        return {"symbol": symbol.upper(), "dividends": divs}

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:dividends", symbol),
        route_name="yf_dividends",
        fetcher=_fetch,
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

    async def _fetch(provider):
        splits = await provider.get_splits(symbol)
        return {"symbol": symbol.upper(), "splits": splits}

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:splits", symbol),
        route_name="yf_splits",
        fetcher=_fetch,
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

    async def _fetch(provider):
        actions = await provider.get_actions(symbol)
        return {"symbol": symbol.upper(), "actions": actions}

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:actions", symbol),
        route_name="yf_actions",
        fetcher=_fetch,
    )


@router.get("/ticker/{symbol}/news", response_model=SuccessResponse)
async def get_news(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get recent news articles."""

    async def _fetch(provider):
        news = await provider.get_news(symbol)
        return {"symbol": symbol.upper(), "articles": news}

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:news", symbol),
        route_name="yf_news",
        fetcher=_fetch,
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

    async def _fetch(provider):
        sus = await provider.get_sustainability(symbol)
        return {"symbol": symbol.upper(), "sustainability": sus}

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:sustainability", symbol),
        route_name="yf_sustainability",
        fetcher=_fetch,
    )


@router.get("/ticker/{symbol}/major-holders", response_model=SuccessResponse)
async def get_major_holders(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get major holders breakdown (% held by insiders, institutions)."""

    async def _fetch(provider):
        holders = await provider.get_major_holders(symbol)
        return {"symbol": symbol.upper(), "major_holders": holders}

    return await execute_yf_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("yf:major-holders", symbol),
        route_name="yf_major_holders",
        fetcher=_fetch,
    )
