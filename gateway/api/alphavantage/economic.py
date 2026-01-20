"""Alpha Vantage economic indicators endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alphavantage.common import (
    CACHE_TTL_FUNDAMENTALS,
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

    key = cache_key("av:economic", indicator.upper(), interval)
    cached = cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_economic_indicator(indicator, interval)
        cache.set(key, data, ttl=CACHE_TTL_FUNDAMENTALS)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
