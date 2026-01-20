"""Alpha Vantage technical indicators endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alphavantage.common import (
    CACHE_TTL_INDICATOR,
    PROVIDER_NOT_AVAILABLE,
    Client,
    InMemoryCache,
    ProviderRegistry,
    cache_key,
    get_cache,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.schemas import SuccessResponse

router = APIRouter()


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
    """Get technical indicator. Supports: SMA, EMA, RSI, MACD, BBANDS, STOCH, ADX, CCI, ATR, OBV."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    key = cache_key(
        f"av:indicator:{indicator}",
        symbol.upper(),
        interval,
        str(time_period),
        series_type,
    )
    cached = cache.get(key)
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
        cache.set(key, data, ttl=CACHE_TTL_INDICATOR)
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
