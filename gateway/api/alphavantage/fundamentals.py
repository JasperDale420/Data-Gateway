"""Alpha Vantage fundamentals endpoints."""

from fastapi import APIRouter, Depends, HTTPException

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
from gateway.core.logger import logger
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
    key = cache_key("av:overview", symbol.upper())

    async def _fetch_overview(provider):
        data = await provider.get_company_overview(symbol)
        if not data:
            raise HTTPException(status_code=404, detail=f"No data for symbol: {symbol}")
        return data

    # nosemgrep: empire-no-bare-exception -- route boundary: any provider failure maps to 502; logged with exc_info above the raise
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_FUNDAMENTALS,
            fetcher=_fetch_overview,
            cache_transform=lambda data: data,
            endpoint="overview",
            cache_mode="default",
        )
    except HTTPException:
        raise
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/earnings/{symbol}", response_model=SuccessResponse)
async def get_earnings(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get earnings data (annual and quarterly)."""
    key = cache_key("av:earnings", symbol.upper())
    # nosemgrep: empire-no-bare-exception -- route boundary: any provider failure maps to 502; logged with exc_info above the raise
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_FUNDAMENTALS,
            fetcher=lambda provider: provider.get_earnings(symbol),
            cache_transform=lambda data: data,
            endpoint="earnings",
            cache_mode="default",
        )
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/income-statement/{symbol}", response_model=SuccessResponse)
async def get_income_statement(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get income statement data."""
    key = cache_key("av:income", symbol.upper())
    # nosemgrep: empire-no-bare-exception -- route boundary: any provider failure maps to 502; logged with exc_info above the raise
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_FUNDAMENTALS,
            fetcher=lambda provider: provider.get_income_statement(symbol),
            cache_transform=lambda data: data,
            endpoint="income_statement",
            cache_mode="default",
        )
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/balance-sheet/{symbol}", response_model=SuccessResponse)
async def get_balance_sheet(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get balance sheet data."""
    key = cache_key("av:balance", symbol.upper())
    # nosemgrep: empire-no-bare-exception -- route boundary: any provider failure maps to 502; logged with exc_info above the raise
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_FUNDAMENTALS,
            fetcher=lambda provider: provider.get_balance_sheet(symbol),
            cache_transform=lambda data: data,
            endpoint="balance_sheet",
            cache_mode="default",
        )
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


@router.get("/cash-flow/{symbol}", response_model=SuccessResponse)
async def get_cash_flow(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get cash flow statement data."""
    key = cache_key("av:cashflow", symbol.upper())
    # nosemgrep: empire-no-bare-exception -- route boundary: any provider failure maps to 502; logged with exc_info above the raise
    try:
        return await execute_av_cached(
            cache=cache,
            cache_key_value=key,
            registry=registry,
            ttl=CACHE_TTL_FUNDAMENTALS,
            fetcher=lambda provider: provider.get_cash_flow(symbol),
            cache_transform=lambda data: data,
            endpoint="cash_flow",
            cache_mode="default",
        )
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")
