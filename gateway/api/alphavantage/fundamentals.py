"""Alpha Vantage fundamentals endpoints."""

from fastapi import APIRouter, Depends, HTTPException

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

    key = cache_key("av:overview", symbol.upper())
    cached = await cache.get(key)
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

        await cache.set(key, data, ttl=CACHE_TTL_FUNDAMENTALS)
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

    key = cache_key("av:earnings", symbol.upper())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_earnings(symbol)
        await cache.set(key, data, ttl=CACHE_TTL_FUNDAMENTALS)
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

    key = cache_key("av:income", symbol.upper())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_income_statement(symbol)
        await cache.set(key, data, ttl=CACHE_TTL_FUNDAMENTALS)
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

    key = cache_key("av:balance", symbol.upper())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_balance_sheet(symbol)
        await cache.set(key, data, ttl=CACHE_TTL_FUNDAMENTALS)
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

    key = cache_key("av:cashflow", symbol.upper())
    cached = await cache.get(key)
    if cached:
        return {
            "success": True,
            "data": cached,
            "meta": {"cached": True, "provider": "alphavantage"},
        }

    try:
        await require_provider_rate_limit("alphavantage")
        data = await provider.get_cash_flow(symbol)
        await cache.set(key, data, ttl=CACHE_TTL_FUNDAMENTALS)
        return {
            "success": True,
            "data": data,
            "meta": {"cached": False, "provider": "alphavantage"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
