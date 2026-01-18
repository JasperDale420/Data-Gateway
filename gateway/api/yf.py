"""yfinance API endpoints for fundamentals and financials."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import get_cache, get_registry, require_api_key, require_provider_rate_limit
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.registry import ProviderRegistry

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/yf", tags=["yfinance"])

PROVIDER_NOT_AVAILABLE = "yfinance provider not available"
CACHE_TTL = 300  # 5 minutes per PRD


def _cache_key(prefix: str, symbol: str, *args) -> str:
    """Generate cache key for yfinance data."""
    parts = [prefix, symbol.upper()] + [str(a) for a in args if a]
    return ":".join(parts)


@router.get("/ticker/{symbol}")
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

    cache_key = _cache_key("yf:ticker", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        data = await provider.get_ticker_info(symbol)
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "yfinance"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/info")
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

    cache_key = _cache_key("yf:info", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        data = await provider.get_company_info(symbol)
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "yfinance"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/financials")
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

    cache_key = _cache_key("yf:financials", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        data = await provider.get_financials(symbol)
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "yfinance"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/earnings")
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

    cache_key = _cache_key("yf:earnings", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        data = await provider.get_earnings(symbol)
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "yfinance"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/history")
async def get_history(
    symbol: str,
    period: str = Query(
        default="1mo", description="1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max"
    ),
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

    cache_key = _cache_key("yf:history", symbol, period, interval, start, end)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        bars = await provider.get_history(
            symbol, period=period, interval=interval, start=start, end=end
        )
        data = {
            "symbol": symbol.upper(),
            "period": period,
            "interval": interval,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(bars), "cached": False, "provider": "yfinance"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/options")
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

    cache_key = _cache_key("yf:options", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        expirations = await provider.get_options_expirations(symbol)
        data = {"symbol": symbol.upper(), "expirations": expirations}
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(expirations), "cached": False, "provider": "yfinance"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/options/{expiration}")
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

    cache_key = _cache_key("yf:chain", symbol, expiration)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        chain = await provider.get_options_chain(symbol, expiration)
        data = {"symbol": symbol.upper(), "expiration": expiration, **chain}
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "yfinance"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/recommendations")
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

    cache_key = _cache_key("yf:recs", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        recs = await provider.get_recommendations(symbol)
        data = {"symbol": symbol.upper(), "recommendations": recs}
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(recs), "cached": False, "provider": "yfinance"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/holders")
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

    cache_key = _cache_key("yf:holders", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        data = await provider.get_holders(symbol)
        data["symbol"] = symbol.upper()
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "yfinance"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/calendar")
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

    cache_key = _cache_key("yf:calendar", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        data = await provider.get_calendar(symbol)
        data["symbol"] = symbol.upper()
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "yfinance"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Phase 4: Additional Data
# ─────────────────────────────────────────────────────────────────


@router.get("/ticker/{symbol}/dividends")
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

    cache_key = _cache_key("yf:dividends", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        divs = await provider.get_dividends(symbol)
        data = {"symbol": symbol.upper(), "dividends": divs}
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(divs), "cached": False, "provider": "yfinance"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/splits")
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

    cache_key = _cache_key("yf:splits", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        splits = await provider.get_splits(symbol)
        data = {"symbol": symbol.upper(), "splits": splits}
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(splits), "cached": False, "provider": "yfinance"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/actions")
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

    cache_key = _cache_key("yf:actions", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        actions = await provider.get_actions(symbol)
        data = {"symbol": symbol.upper(), "actions": actions}
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "yfinance"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/news")
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

    cache_key = _cache_key("yf:news", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        news = await provider.get_news(symbol)
        data = {"symbol": symbol.upper(), "articles": news}
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(news), "cached": False, "provider": "yfinance"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/sustainability")
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

    cache_key = _cache_key("yf:sustainability", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        sus = await provider.get_sustainability(symbol)
        data = {"symbol": symbol.upper(), "sustainability": sus}
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "yfinance"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ticker/{symbol}/major-holders")
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

    cache_key = _cache_key("yf:major-holders", symbol)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "yfinance"}}

    try:
        await require_provider_rate_limit("yfinance")
        holders = await provider.get_major_holders(symbol)
        data = {"symbol": symbol.upper(), "major_holders": holders}
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "yfinance"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
