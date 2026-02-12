"""Alpha Vantage technical indicators endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alphavantage.common import (
    CACHE_TTL_INDICATOR,
    Client,
    InMemoryCache,
    ProviderRegistry,
    cache_key,
    execute_av_cached,
    get_cache,
    get_registry,
    require_api_key,
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
    max_points: int = Query(default=100, ge=1, description="Max points to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get technical indicator. Supports: SMA, EMA, RSI, MACD, BBANDS, STOCH, ADX, CCI, ATR, OBV."""
    key = cache_key(
        f"av:indicator:{indicator}",
        symbol.upper(),
        interval,
        str(time_period),
        series_type,
        str(max_points),
    )
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_INDICATOR,
            fetcher=lambda provider: provider.get_technical_indicator(
                symbol,
                indicator,
                interval,
                time_period,
                series_type,
            ),
            cache_transform=lambda data: data,
            endpoint="indicator",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# Convenience endpoints for popular indicators
@router.get("/indicator/sma/{symbol}", response_model=SuccessResponse)
async def get_sma(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=20),
    series_type: str = Query(default="close"),
    max_points: int = Query(default=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Simple Moving Average."""
    return await get_technical_indicator(
        "SMA", symbol, interval, time_period, series_type, max_points, client, registry, cache
    )


@router.get("/indicator/ema/{symbol}", response_model=SuccessResponse)
async def get_ema(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=20),
    series_type: str = Query(default="close"),
    max_points: int = Query(default=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Exponential Moving Average."""
    return await get_technical_indicator(
        "EMA", symbol, interval, time_period, series_type, max_points, client, registry, cache
    )


@router.get("/indicator/rsi/{symbol}", response_model=SuccessResponse)
async def get_rsi(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=14),
    series_type: str = Query(default="close"),
    max_points: int = Query(default=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Relative Strength Index."""
    return await get_technical_indicator(
        "RSI", symbol, interval, time_period, series_type, max_points, client, registry, cache
    )


@router.get("/indicator/macd/{symbol}", response_model=SuccessResponse)
async def get_macd(
    symbol: str,
    interval: str = Query(default="daily"),
    series_type: str = Query(default="close"),
    max_points: int = Query(default=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Moving Average Convergence Divergence."""
    return await get_technical_indicator("MACD", symbol, interval, 14, series_type, max_points, client, registry, cache)


@router.get("/indicator/bbands/{symbol}", response_model=SuccessResponse)
async def get_bbands(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=20),
    series_type: str = Query(default="close"),
    max_points: int = Query(default=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Bollinger Bands."""
    return await get_technical_indicator(
        "BBANDS", symbol, interval, time_period, series_type, max_points, client, registry, cache
    )


@router.get("/indicator/stoch/{symbol}", response_model=SuccessResponse)
async def get_stoch(
    symbol: str,
    interval: str = Query(default="daily"),
    max_points: int = Query(default=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Stochastic Oscillator."""
    return await get_technical_indicator("STOCH", symbol, interval, 14, "close", max_points, client, registry, cache)


@router.get("/indicator/adx/{symbol}", response_model=SuccessResponse)
async def get_adx(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=14),
    max_points: int = Query(default=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Average Directional Index."""
    return await get_technical_indicator(
        "ADX", symbol, interval, time_period, "close", max_points, client, registry, cache
    )


@router.get("/indicator/cci/{symbol}", response_model=SuccessResponse)
async def get_cci(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=20),
    max_points: int = Query(default=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Commodity Channel Index."""
    return await get_technical_indicator(
        "CCI", symbol, interval, time_period, "close", max_points, client, registry, cache
    )


@router.get("/indicator/atr/{symbol}", response_model=SuccessResponse)
async def get_atr(
    symbol: str,
    interval: str = Query(default="daily"),
    time_period: int = Query(default=14),
    max_points: int = Query(default=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Average True Range."""
    return await get_technical_indicator(
        "ATR", symbol, interval, time_period, "close", max_points, client, registry, cache
    )


@router.get("/indicator/obv/{symbol}", response_model=SuccessResponse)
async def get_obv(
    symbol: str,
    interval: str = Query(default="daily"),
    max_points: int = Query(default=100, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """On Balance Volume."""
    return await get_technical_indicator("OBV", symbol, interval, 14, "close", max_points, client, registry, cache)
