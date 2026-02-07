"""Alpha Vantage economic indicators endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alphavantage.common import (
    CACHE_TTL_FUNDAMENTALS,
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
    key = cache_key("av:economic", indicator.upper(), interval)
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_FUNDAMENTALS,
            fetcher=lambda provider: provider.get_economic_indicator(indicator, interval),
            cache_transform=lambda data: data,
            endpoint="economic_indicator",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
