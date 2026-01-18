"""Finnhub API endpoints for quotes, profiles, news, and fundamentals."""

from datetime import datetime

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import get_cache, get_registry, require_api_key, require_provider_rate_limit
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.registry import ProviderRegistry

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/finnhub", tags=["finnhub"])

PROVIDER_NOT_AVAILABLE = "Finnhub provider not available"
CACHE_TTL = 60  # 1 minute for real-time data


def _cache_key(prefix: str, *args) -> str:
    """Generate cache key."""
    parts = [prefix] + [str(a) for a in args if a]
    return ":".join(parts)


# ─────────────────────────────────────────────────────────────────
# Quotes
# ─────────────────────────────────────────────────────────────────


@router.get("/quote/{symbol}")
async def get_quote(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get real-time quote for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:quote", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        # Check provider rate limit before upstream call
        await require_provider_rate_limit("finnhub")
        quote = await provider.get_quote(symbol)
        if not quote:
            raise HTTPException(status_code=404, detail=f"No data for symbol: {symbol}")

        data = quote.model_dump(mode="json")
        cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Historical Bars
# ─────────────────────────────────────────────────────────────────


@router.get("/bars/{symbol}")
async def get_bars(
    symbol: str,
    resolution: str = Query(default="D", description="1, 5, 15, 30, 60, D, W, M"),
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical OHLCV bars."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:bars", symbol.upper(), resolution, start, end)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        # Check provider rate limit before upstream call
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None

        bars = await provider.get_bars(symbol, resolution=resolution, start=start_dt, end=end_dt)
        data = {
            "symbol": symbol.upper(),
            "resolution": resolution,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        cache.set(cache_key, data, ttl=300)  # Cache bars for 5 min
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(bars), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Company Profile & Fundamentals
# ─────────────────────────────────────────────────────────────────


@router.get("/profile/{symbol}")
async def get_company_profile(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company profile information."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:profile", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_company_profile(symbol)
        if not data:
            raise HTTPException(status_code=404, detail=f"No profile for symbol: {symbol}")

        cache.set(cache_key, data, ttl=3600)  # Cache profile for 1 hour
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/financials/{symbol}")
async def get_financials(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get financial metrics (P/E, EPS, beta, etc)."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:financials", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_financials(symbol)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# News
# ─────────────────────────────────────────────────────────────────


@router.get("/news/{symbol}")
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

    cache_key = _cache_key("finnhub:news", symbol.upper(), start, end)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None

        articles = await provider.get_news(symbol, start=start_dt, end=end_dt)
        data = {"symbol": symbol.upper(), "articles": articles}
        cache.set(cache_key, data, ttl=300)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(articles), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/news/market/{category}")
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

    cache_key = _cache_key("finnhub:market-news", category)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        articles = await provider.get_market_news(category=category)
        data = {"category": category, "articles": articles}
        cache.set(cache_key, data, ttl=300)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(articles), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Earnings & Recommendations
# ─────────────────────────────────────────────────────────────────


@router.get("/earnings")
async def get_earnings_calendar(
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get earnings calendar."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:earnings", start, end)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None

        earnings = await provider.get_earnings_calendar(start=start_dt, end=end_dt)
        cache.set(cache_key, earnings, ttl=3600)
        return {
            "success": True,
            "data": earnings,
            "meta": {"count": len(earnings), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/recommendations/{symbol}")
async def get_recommendations(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get analyst recommendation trends."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:recommendations", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        recs = await provider.get_recommendation_trends(symbol)
        cache.set(cache_key, recs, ttl=3600)
        return {
            "success": True,
            "data": recs,
            "meta": {"count": len(recs), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Stock Estimates
# ─────────────────────────────────────────────────────────────────


@router.get("/estimates/eps/{symbol}")
async def get_eps_estimates(
    symbol: str,
    freq: str = Query(default="quarterly", description="quarterly or annual"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get EPS estimates for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:eps-estimates", symbol.upper(), freq)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_eps_estimates(symbol, freq=freq)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/estimates/revenue/{symbol}")
async def get_revenue_estimates(
    symbol: str,
    freq: str = Query(default="quarterly", description="quarterly or annual"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get revenue estimates for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:revenue-estimates", symbol.upper(), freq)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_revenue_estimates(symbol, freq=freq)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/estimates/ebit/{symbol}")
async def get_ebit_estimates(
    symbol: str,
    freq: str = Query(default="quarterly", description="quarterly or annual"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get EBIT estimates for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:ebit-estimates", symbol.upper(), freq)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_ebit_estimates(symbol, freq=freq)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/estimates/ebitda/{symbol}")
async def get_ebitda_estimates(
    symbol: str,
    freq: str = Query(default="quarterly", description="quarterly or annual"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get EBITDA estimates for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:ebitda-estimates", symbol.upper(), freq)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_ebitda_estimates(symbol, freq=freq)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/price-target/{symbol}")
async def get_price_target(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get analyst price target for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:price-target", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_price_target(symbol)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Stock Fundamentals Extended
# ─────────────────────────────────────────────────────────────────


@router.get("/peers/{symbol}")
async def get_peers(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get peer companies for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:peers", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        peers = await provider.get_peers(symbol)
        data = {"symbol": symbol.upper(), "peers": peers}
        cache.set(cache_key, data, ttl=86400)  # 24h - changes rarely
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(peers), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/metrics/{symbol}")
async def get_metrics(
    symbol: str,
    metric: str = Query(default="all", description="all, margin, valuation, price, profitability"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get key financial metrics for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:metrics", symbol.upper(), metric)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_metrics(symbol, metric=metric)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/executives/{symbol}")
async def get_executives(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company executives and compensation."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:executives", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        execs = await provider.get_executives(symbol)
        data = {"symbol": symbol.upper(), "executives": execs}
        cache.set(cache_key, data, ttl=86400)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(execs), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/ownership/{symbol}")
async def get_ownership(
    symbol: str,
    limit: int = Query(default=20, le=100, description="Max owners to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get institutional ownership data."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:ownership", symbol.upper(), str(limit))
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_ownership(symbol, limit=limit)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/fund-ownership/{symbol}")
async def get_fund_ownership(
    symbol: str,
    limit: int = Query(default=20, le=100, description="Max fund owners to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund and ETF ownership data."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:fund-ownership", symbol.upper(), str(limit))
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_fund_ownership(symbol, limit=limit)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/insider-transactions/{symbol}")
async def get_insider_transactions(
    symbol: str,
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insider transactions (SEC Form 4)."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:insider-tx", symbol.upper(), start, end)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None

        txs = await provider.get_insider_transactions(symbol, start=start_dt, end=end_dt)
        data = {"symbol": symbol.upper(), "transactions": txs}
        cache.set(cache_key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(txs), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Stock Analysis
# ─────────────────────────────────────────────────────────────────


@router.get("/insider-sentiment/{symbol}")
async def get_insider_sentiment(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get aggregate insider sentiment."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:insider-sentiment", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_insider_sentiment(symbol)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/upgrade-downgrade/{symbol}")
async def get_upgrade_downgrade(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get analyst upgrade/downgrade history."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:upgrade-downgrade", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_upgrade_downgrade(symbol)
        cache.set(cache_key, data, ttl=3600)
        return {
            "success": True,
            "data": {"symbol": symbol.upper(), "history": data},
            "meta": {"count": len(data), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/social-sentiment/{symbol}")
async def get_social_sentiment(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get social media sentiment."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:social-sentiment", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_social_sentiment(symbol)
        cache.set(cache_key, data, ttl=1800)  # 30 min - social changes often
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# ETFs
# ─────────────────────────────────────────────────────────────────


@router.get("/etf/{symbol}/profile")
async def get_etf_profile(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF profile."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:etf-profile", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_etf_profile(symbol)
        cache.set(cache_key, data, ttl=86400)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/etf/{symbol}/holdings")
async def get_etf_holdings(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF holdings."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:etf-holdings", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_etf_holdings(symbol)
        cache.set(cache_key, data, ttl=86400)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/etf/{symbol}/sector")
async def get_etf_sector(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF sector exposure."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:etf-sector", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_etf_sector_exposure(symbol)
        cache.set(cache_key, data, ttl=86400)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/etf/{symbol}/country")
async def get_etf_country(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF country exposure."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:etf-country", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_etf_country_exposure(symbol)
        cache.set(cache_key, data, ttl=86400)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Indices
# ─────────────────────────────────────────────────────────────────


@router.get("/index/{symbol}/constituents")
async def get_index_constituents(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get index constituents."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:index-constituents", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_index_constituents(symbol)
        cache.set(cache_key, data, ttl=86400)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/index/{symbol}/historical")
async def get_index_historical(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical index constituent changes."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:index-historical", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_index_historical_constituents(symbol)
        cache.set(cache_key, data, ttl=86400)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Technical Analysis
# ─────────────────────────────────────────────────────────────────


@router.get("/technical/support-resistance/{symbol}")
async def get_support_resistance(
    symbol: str,
    resolution: str = Query(default="D", description="Resolution: 1, 5, 15, 30, 60, D, W, M"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get support/resistance levels."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:support-resistance", symbol.upper(), resolution)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_support_resistance(symbol, resolution=resolution)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/technical/pattern/{symbol}")
async def get_pattern_recognition(
    symbol: str,
    resolution: str = Query(default="D", description="Resolution: 1, 5, 15, 30, 60, D, W, M"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get recognized chart patterns."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:pattern", symbol.upper(), resolution)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_pattern_recognition(symbol, resolution=resolution)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Forex
# ─────────────────────────────────────────────────────────────────


@router.get("/forex/rates")
async def get_forex_rates(
    base: str = Query(default="USD", description="Base currency"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all forex rates for a base currency."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:forex-rates", base.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_forex_rates(base)
        cache.set(cache_key, data, ttl=300)  # 5 min
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/forex/exchanges")
async def get_forex_exchanges(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """List supported forex exchanges."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:forex-exchanges")
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_forex_exchanges()
        cache.set(cache_key, data, ttl=86400)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/forex/symbols/{exchange}")
async def get_forex_symbols(
    exchange: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get forex symbols for an exchange."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:forex-symbols", exchange)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_forex_symbols(exchange)
        cache.set(cache_key, data, ttl=86400)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/forex/candles/{symbol}")
async def get_forex_candles(
    symbol: str,
    resolution: str = Query(default="D", description="Resolution: 1, 5, 15, 30, 60, D, W, M"),
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get forex OHLC candles."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:forex-candles", symbol, resolution, start, end)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        data = await provider.get_forex_candles(symbol, resolution, start_dt, end_dt)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Crypto
# ─────────────────────────────────────────────────────────────────


@router.get("/crypto/exchanges")
async def get_crypto_exchanges(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """List supported crypto exchanges."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:crypto-exchanges")
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_crypto_exchanges()
        cache.set(cache_key, data, ttl=86400)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/crypto/symbols/{exchange}")
async def get_crypto_symbols(
    exchange: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get crypto symbols for an exchange."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:crypto-symbols", exchange)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_crypto_symbols(exchange)
        cache.set(cache_key, data, ttl=86400)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/crypto/candles/{symbol}")
async def get_crypto_candles(
    symbol: str,
    resolution: str = Query(default="D", description="Resolution: 1, 5, 15, 30, 60, D, W, M"),
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get crypto OHLC candles."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:crypto-candles", symbol, resolution, start, end)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        data = await provider.get_crypto_candles(symbol, resolution, start_dt, end_dt)
        cache.set(cache_key, data, ttl=3600)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/crypto/profile/{symbol}")
async def get_crypto_profile(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get crypto profile/metadata."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:crypto-profile", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_crypto_profile(symbol)
        cache.set(cache_key, data, ttl=86400)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Mutual Funds
# ─────────────────────────────────────────────────────────────────


@router.get("/mutual-fund/{symbol}/profile")
async def get_mutual_fund_profile(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund profile."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:mf-profile", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_mutual_fund_profile(symbol)
        cache.set(cache_key, data, ttl=86400)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/mutual-fund/{symbol}/holdings")
async def get_mutual_fund_holdings(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund holdings."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:mf-holdings", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_mutual_fund_holdings(symbol)
        cache.set(cache_key, data, ttl=86400)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/mutual-fund/{symbol}/sector")
async def get_mutual_fund_sector(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get mutual fund sector exposure."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:mf-sector", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_mutual_fund_sector(symbol)
        cache.set(cache_key, data, ttl=86400)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "finnhub"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Alternative Data
# ─────────────────────────────────────────────────────────────────


@router.get("/alt/fda-calendar")
async def get_fda_calendar(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get FDA drug approval calendar."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:fda-calendar")
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        data = await provider.get_fda_calendar()
        cache.set(cache_key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/alt/congress-trading")
async def get_congress_trading(
    symbol: str | None = Query(default=None, description="Filter by symbol"),
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get congressional trading data (STOCK Act disclosures)."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:congress-trading", symbol or "all", start, end)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        data = await provider.get_congress_trading(symbol, start_dt, end_dt)
        cache.set(cache_key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/alt/lobbying/{symbol}")
async def get_lobbying(
    symbol: str,
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get lobbying data for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:lobbying", symbol.upper(), start, end)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        data = await provider.get_lobbying(symbol, start_dt, end_dt)
        cache.set(cache_key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/alt/usa-spending/{symbol}")
async def get_usa_spending(
    symbol: str,
    start: str | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get USA government spending data for a symbol."""
    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("finnhub:usa-spending", symbol.upper(), start, end)
    cached = cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "finnhub"}}

    try:
        await require_provider_rate_limit("finnhub")
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        data = await provider.get_usa_spending(symbol, start_dt, end_dt)
        cache.set(cache_key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "finnhub"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
