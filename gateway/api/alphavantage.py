"""Alpha Vantage API endpoints for quotes, time series, and fundamentals."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import get_cache, get_registry, require_api_key, require_provider_rate_limit
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/alphavantage", tags=["alphavantage"])

PROVIDER_NOT_AVAILABLE = "Alpha Vantage provider not available"
CACHE_TTL_QUOTE = 60  # 1 minute for quotes
CACHE_TTL_BARS = 300  # 5 minutes for bars
CACHE_TTL_FUNDAMENTALS = 3600  # 1 hour for fundamentals


def _cache_key(prefix: str, *args) -> str:
    """Generate cache key."""
    parts = [prefix] + [str(a) for a in args if a]
    return ":".join(parts)


@router.get("/quote/{symbol}", response_model=SuccessResponse)
async def get_quote(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get real-time quote for a symbol."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:quote", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        quote = await provider.get_quote(symbol)
        if not quote:
            raise HTTPException(status_code=404, detail=f"No data for symbol: {symbol}")

        data = quote.model_dump(mode="json")
        cache.set(cache_key, data, ttl=CACHE_TTL_QUOTE)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/intraday/{symbol}", response_model=SuccessResponse)
async def get_intraday(
    symbol: str,
    interval: str = Query(default="5min", description="1min, 5min, 15min, 30min, 60min"),
    outputsize: str = Query(default="compact", description="compact (100 points) or full"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get intraday time series data."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:intraday", symbol.upper(), interval, outputsize)
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        bars = await provider.get_intraday(symbol, interval=interval, outputsize=outputsize)
        data = {
            "symbol": symbol.upper(),
            "interval": interval,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        cache.set(cache_key, data, ttl=CACHE_TTL_BARS)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(bars), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/daily/{symbol}", response_model=SuccessResponse)
async def get_daily(
    symbol: str,
    outputsize: str = Query(default="compact", description="compact (100 days) or full"),
    adjusted: bool = Query(default=True, description="Include split/dividend adjustments"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get daily time series data."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:daily", symbol.upper(), outputsize, str(adjusted))
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        bars = await provider.get_daily(symbol, outputsize=outputsize, adjusted=adjusted)
        data = {
            "symbol": symbol.upper(),
            "adjusted": adjusted,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        cache.set(cache_key, data, ttl=CACHE_TTL_BARS)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(bars), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/weekly/{symbol}", response_model=SuccessResponse)
async def get_weekly(
    symbol: str,
    adjusted: bool = Query(default=True, description="Include adjustments"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get weekly time series data."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:weekly", symbol.upper(), str(adjusted))
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        bars = await provider.get_weekly(symbol, adjusted=adjusted)
        data = {
            "symbol": symbol.upper(),
            "adjusted": adjusted,
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        cache.set(cache_key, data, ttl=CACHE_TTL_BARS)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(bars), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/overview/{symbol}", response_model=SuccessResponse)
async def get_company_overview(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company overview (fundamentals, ratios, etc)."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:overview", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_company_overview(symbol)
        if not data:
            raise HTTPException(status_code=404, detail=f"No data for symbol: {symbol}")

        cache.set(cache_key, data, ttl=CACHE_TTL_FUNDAMENTALS)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/earnings/{symbol}", response_model=SuccessResponse)
async def get_earnings(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get earnings data (annual and quarterly)."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:earnings", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_earnings(symbol)
        cache.set(cache_key, data, ttl=CACHE_TTL_FUNDAMENTALS)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/income-statement/{symbol}", response_model=SuccessResponse)
async def get_income_statement(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get income statement data."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:income", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_income_statement(symbol)
        cache.set(cache_key, data, ttl=CACHE_TTL_FUNDAMENTALS)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/balance-sheet/{symbol}", response_model=SuccessResponse)
async def get_balance_sheet(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get balance sheet data."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:balance", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_balance_sheet(symbol)
        cache.set(cache_key, data, ttl=CACHE_TTL_FUNDAMENTALS)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/cash-flow/{symbol}", response_model=SuccessResponse)
async def get_cash_flow(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get cash flow statement data."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:cashflow", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_cash_flow(symbol)
        cache.set(cache_key, data, ttl=CACHE_TTL_FUNDAMENTALS)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Extended Time Series & Utilities
# ─────────────────────────────────────────────────────────────────


@router.get("/monthly/{symbol}", response_model=SuccessResponse)
async def get_monthly(
    symbol: str,
    adjusted: bool = Query(default=True, description="Use adjusted close prices"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get monthly time series data."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:monthly", symbol.upper(), str(adjusted))
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        bars = await provider.get_monthly(symbol, adjusted=adjusted)
        data = [bar.model_dump() for bar in bars]
        cache.set(cache_key, data, ttl=CACHE_TTL_BARS)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/search", response_model=SuccessResponse)
async def search_symbols(
    q: str = Query(..., min_length=1, description="Search keywords"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Search for symbols by keywords."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:search", q.lower())
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        results = await provider.search_symbols(q)
        cache.set(cache_key, results, ttl=86400)  # 24h
        return {
            "success": True,
            "data": results,
            "meta": {"count": len(results), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Technical Indicators
# ─────────────────────────────────────────────────────────────────

CACHE_TTL_INDICATOR = 3600  # 1 hour


@router.get("/indicator/{indicator}/{symbol}", response_model=SuccessResponse)
async def get_technical_indicator(
    indicator: str,
    symbol: str,
    interval: str = Query(
        default="daily",
        description="Interval: 1min, 5min, 15min, 30min, 60min, daily, weekly, monthly",
    ),
    time_period: int = Query(default=14, ge=1, le=200, description="Time period"),
    series_type: str = Query(default="close", description="Series type: close, open, high, low"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get any technical indicator. Supports: SMA, EMA, RSI, MACD, BBANDS, STOCH, ADX, CCI, ATR, OBV."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key(
        f"av:indicator:{indicator}", symbol.upper(), interval, str(time_period), series_type
    )
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_technical_indicator(
            symbol, indicator, interval, time_period, series_type
        )
        cache.set(cache_key, data, ttl=CACHE_TTL_INDICATOR)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# Convenience endpoints for popular indicators
@router.get("/indicator/sma/{symbol}", response_model=SuccessResponse)
async def get_sma(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=20),
    series_type: str = Query(default="close"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Simple Moving Average."""
    return await get_technical_indicator(
        "SMA", symbol, interval, time_period, series_type, client, registry, cache
    )


@router.get("/indicator/ema/{symbol}", response_model=SuccessResponse)
async def get_ema(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=20),
    series_type: str = Query(default="close"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Exponential Moving Average."""
    return await get_technical_indicator(
        "EMA", symbol, interval, time_period, series_type, client, registry, cache
    )


@router.get("/indicator/rsi/{symbol}", response_model=SuccessResponse)
async def get_rsi(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=14),
    series_type: str = Query(default="close"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Relative Strength Index."""
    return await get_technical_indicator(
        "RSI", symbol, interval, time_period, series_type, client, registry, cache
    )


@router.get("/indicator/macd/{symbol}", response_model=SuccessResponse)
async def get_macd(
    symbol: str,
    interval: str = Query(default="daily"),
    series_type: str = Query(default="close"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Moving Average Convergence Divergence."""
    return await get_technical_indicator(
        "MACD", symbol, interval, 14, series_type, client, registry, cache
    )


@router.get("/indicator/bbands/{symbol}", response_model=SuccessResponse)
async def get_bbands(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=20),
    series_type: str = Query(default="close"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Bollinger Bands."""
    return await get_technical_indicator(
        "BBANDS", symbol, interval, time_period, series_type, client, registry, cache
    )


@router.get("/indicator/stoch/{symbol}", response_model=SuccessResponse)
async def get_stoch(
    symbol: str,
    interval: str = Query(default="daily"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Stochastic Oscillator."""
    return await get_technical_indicator(
        "STOCH", symbol, interval, 14, "close", client, registry, cache
    )


@router.get("/indicator/adx/{symbol}", response_model=SuccessResponse)
async def get_adx(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=14),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Average Directional Index."""
    return await get_technical_indicator(
        "ADX", symbol, interval, time_period, "close", client, registry, cache
    )


@router.get("/indicator/cci/{symbol}", response_model=SuccessResponse)
async def get_cci(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=20),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Commodity Channel Index."""
    return await get_technical_indicator(
        "CCI", symbol, interval, time_period, "close", client, registry, cache
    )


@router.get("/indicator/atr/{symbol}", response_model=SuccessResponse)
async def get_atr(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=14),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Average True Range."""
    return await get_technical_indicator(
        "ATR", symbol, interval, time_period, "close", client, registry, cache
    )


@router.get("/indicator/obv/{symbol}", response_model=SuccessResponse)
async def get_obv(
    symbol: str,
    interval: str = Query(default="daily"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """On Balance Volume."""
    return await get_technical_indicator(
        "OBV", symbol, interval, 14, "close", client, registry, cache
    )


# ─────────────────────────────────────────────────────────────────
# Forex
# ─────────────────────────────────────────────────────────────────


@router.get("/forex/rate/{from_currency}/{to_currency}", response_model=SuccessResponse)
async def get_forex_rate(
    from_currency: str,
    to_currency: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get real-time exchange rate."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:forex-rate", from_currency.upper(), to_currency.upper())
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_forex_rate(from_currency, to_currency)
        cache.set(cache_key, data, ttl=60)  # 1 min - rates change fast
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/forex/daily/{from_symbol}/{to_symbol}", response_model=SuccessResponse)
async def get_forex_daily(
    from_symbol: str,
    to_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get daily forex time series."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:forex-daily", from_symbol.upper(), to_symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_forex_daily(from_symbol, to_symbol)
        cache.set(cache_key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Crypto
# ─────────────────────────────────────────────────────────────────


@router.get("/crypto/rating/{symbol}", response_model=SuccessResponse)
async def get_crypto_rating(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get crypto health rating (FCAS)."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:crypto-rating", symbol.upper())
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_crypto_rating(symbol)
        cache.set(cache_key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/crypto/daily/{symbol}", response_model=SuccessResponse)
async def get_crypto_daily(
    symbol: str,
    market: str = Query(default="USD", description="Market currency"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get daily crypto time series."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:crypto-daily", symbol.upper(), market.upper())
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_crypto_daily(symbol, market)
        cache.set(cache_key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Economic Indicators
# ─────────────────────────────────────────────────────────────────


@router.get("/economic/{indicator}", response_model=SuccessResponse)
async def get_economic_indicator(
    indicator: str,
    interval: str = Query(default="annual", description="annual, quarterly, monthly"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get economic indicator data.

    Supported indicators: REAL_GDP, REAL_GDP_PER_CAPITA, TREASURY_YIELD,
    FEDERAL_FUNDS_RATE, CPI, INFLATION, RETAIL_SALES, DURABLES,
    UNEMPLOYMENT, NONFARM_PAYROLL
    """
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:economic", indicator.upper(), interval)
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_economic_indicator(indicator, interval)
        cache.set(cache_key, data, ttl=86400)  # 24h - economic data changes slowly
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Calendars & Listings
# ─────────────────────────────────────────────────────────────────


@router.get("/calendar/earnings", response_model=SuccessResponse)
async def get_earnings_calendar(
    symbol: str | None = Query(default=None, description="Filter by symbol"),
    horizon: str = Query(default="3month", description="Horizon: 3month, 6month, 12month"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get upcoming earnings calendar."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:earnings-calendar", symbol or "all", horizon)
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_earnings_calendar(symbol, horizon)
        cache.set(cache_key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/calendar/ipo", response_model=SuccessResponse)
async def get_ipo_calendar(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get upcoming IPO calendar."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:ipo-calendar")
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_ipo_calendar()
        cache.set(cache_key, data, ttl=3600)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/listing-status", response_model=SuccessResponse)
async def get_listing_status(
    state: str = Query(default="active", description="active or delisted"),
    date: str | None = Query(default=None, description="Historical date for delisted (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get listing status (active stocks or delisted)."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("av:listing-status", state, date or "current")
    cached = cache.get(cache_key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_listing_status(state, date)
        cache.set(cache_key, data, ttl=86400)  # 24h
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data), "cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
