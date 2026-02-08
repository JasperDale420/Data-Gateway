"""Alpaca forex endpoints."""

from datetime import datetime

from fastapi import APIRouter, Depends, Query

from gateway.api.alpaca.common import (
    Client,
    execute_alpaca_cached_call,
    get_cache,
    get_registry,
    parse_comma_values,
    require_api_key,
)
from gateway.core.cache import HybridCache, InMemoryCache
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()
FOREX_RATES_CACHE_TTL_SECONDS = 10
FOREX_HISTORICAL_CACHE_TTL_SECONDS = 300


@router.get("/forex/rates", response_model=SuccessResponse)
async def get_forex_rates(
    pairs: str = Query(..., description="Comma-separated pairs: EUR/USD,GBP/USD"),
    client: Client = Depends(require_api_key),
    cache: InMemoryCache | HybridCache = Depends(get_cache),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest forex rates."""
    pairs_list = parse_comma_values(pairs, uppercase=True)
    pairs_key = ",".join(pairs_list)
    data = await execute_alpaca_cached_call(
        registry=registry,
        cache=cache,
        cache_key=f"alpaca:forex:rates:{pairs_key}",
        ttl=FOREX_RATES_CACHE_TTL_SECONDS,
        route_label="alpaca_forex_rates",
        provider_call=lambda provider: provider.get_forex_rates(pairs=pairs_list),
    )

    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data.get("rates", {})), "provider": "alpaca"},
    }


@router.get("/forex/rates/historical", response_model=SuccessResponse)
async def get_forex_rates_historical(
    pairs: str = Query(..., description="Comma-separated pairs: EUR/USD,GBP/USD"),
    timeframe: str = Query(default="1Day", description="1Min, 1Hour, 1Day"),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=1000, le=10000),
    client: Client = Depends(require_api_key),
    cache: InMemoryCache | HybridCache = Depends(get_cache),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get historical forex rates."""
    pairs_list = parse_comma_values(pairs, uppercase=True)
    pairs_key = ",".join(pairs_list)
    start_key = start.isoformat() if start else "none"
    end_key = end.isoformat() if end else "none"
    data = await execute_alpaca_cached_call(
        registry=registry,
        cache=cache,
        cache_key=f"alpaca:forex:historical:{pairs_key}:{timeframe}:{start_key}:{end_key}:{limit}",
        ttl=FOREX_HISTORICAL_CACHE_TTL_SECONDS,
        route_label="alpaca_forex_rates_historical",
        provider_call=lambda provider: provider.get_forex_rates_historical(
            pairs=pairs_list,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        ),
    )

    return {
        "success": True,
        "data": data,
        "meta": {"provider": "alpaca"},
    }
