"""SEC EDGAR API endpoints for filings and company data."""

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import get_cache, get_registry, require_api_key, require_provider_rate_limit
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/sec", tags=["sec"])

PROVIDER_NOT_AVAILABLE = "SEC provider not available"
CACHE_TTL = 3600  # 1 hour (filings update infrequently)


def _normalize_cache_arg(value) -> str:
    if value is None:
        return "<none>"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value == "":
        return "<empty>"
    return str(value)


def _cache_key(prefix: str, *args) -> str:
    """Generate cache key for SEC data."""
    parts = [prefix] + [_normalize_cache_arg(a) for a in args]
    return ":".join(parts)


@router.get("/company/{cik}", response_model=SuccessResponse)
async def get_company_info(
    cik: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company info by CIK."""
    provider = registry.get("sec")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("sec:company", cik)
    cached = await cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "sec"}}

    try:
        await require_provider_rate_limit("sec")
        data = await provider.get_company_info(cik)
        await cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "sec"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/company/ticker/{ticker}", response_model=SuccessResponse)
async def get_company_by_ticker(
    ticker: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Lookup company info by ticker symbol."""
    provider = registry.get("sec")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("sec:ticker", ticker.upper())
    cached = await cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "sec"}}

    try:
        await require_provider_rate_limit("sec")
        data = await provider.get_company_by_ticker(ticker)
        await cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "sec"}}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/filings/{cik}", response_model=SuccessResponse)
async def get_filings(
    cik: str,
    form_type: str | None = Query(
        default=None, description="Filter by form type (10-K, 10-Q, 8-K, etc)"
    ),
    limit: int = Query(default=100, le=500, ge=1, description="Max filings to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company filings by CIK."""
    provider = registry.get("sec")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("sec:filings", cik, form_type, str(limit))
    cached = await cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "sec"}}

    try:
        await require_provider_rate_limit("sec")
        filings = await provider.get_filings(cik, form_type=form_type, limit=limit)
        data = {"cik": cik, "form_type": form_type, "filings": filings}
        await cache.set(cache_key, data, ttl=CACHE_TTL)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(filings), "cached": False, "provider": "sec"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/filings/{cik}/{form_type}", response_model=SuccessResponse)
async def get_filings_by_type(
    cik: str,
    form_type: str,
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company filings filtered by form type."""
    provider = registry.get("sec")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("sec:filings", cik, form_type.upper(), str(limit))
    cached = await cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "sec"}}

    try:
        await require_provider_rate_limit("sec")
        filings = await provider.get_filings(cik, form_type=form_type.upper(), limit=limit)
        data = {"cik": cik, "form_type": form_type.upper(), "filings": filings}
        await cache.set(cache_key, data, ttl=CACHE_TTL)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(filings), "cached": False, "provider": "sec"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/13f/{cik}", response_model=SuccessResponse)
async def get_13f_holdings(
    cik: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get 13F institutional holdings filings for an investment manager."""
    provider = registry.get("sec")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("sec:13f", cik)
    cached = await cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "sec"}}

    try:
        await require_provider_rate_limit("sec")
        filings = await provider.get_13f_holdings(cik)
        data = {"cik": cik, "filings": filings}
        await cache.set(cache_key, data, ttl=CACHE_TTL)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(filings), "cached": False, "provider": "sec"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/insiders/{cik}", response_model=SuccessResponse)
async def get_insider_trades(
    cik: str,
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get insider trading filings (Form 3, 4, 5)."""
    provider = registry.get("sec")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("sec:insiders", cik, str(limit))
    cached = await cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "sec"}}

    try:
        await require_provider_rate_limit("sec")
        filings = await provider.get_insider_trades(cik, limit=limit)
        data = {"cik": cik, "filings": filings}
        await cache.set(cache_key, data, ttl=CACHE_TTL)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(filings), "cached": False, "provider": "sec"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/facts/{cik}", response_model=SuccessResponse)
async def get_company_facts(
    cik: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get XBRL company facts (structured financial data from filings)."""
    provider = registry.get("sec")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("sec:facts", cik)
    cached = await cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "sec"}}

    try:
        await require_provider_rate_limit("sec")
        data = await provider.get_company_facts(cik)
        await cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "sec"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


# ─────────────────────────────────────────────────────────────────
# Phase 4: Advanced XBRL & Search
# ─────────────────────────────────────────────────────────────────


@router.get("/concept/{cik}/{concept}", response_model=SuccessResponse)
async def get_company_concept(
    cik: str,
    concept: str,
    taxonomy: str = Query(default="us-gaap", description="XBRL taxonomy: us-gaap, dei, etc"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historical values for a single XBRL concept (e.g., Revenues, EarningsPerShareBasic)."""
    provider = registry.get("sec")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("sec:concept", cik, taxonomy, concept)
    cached = await cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "sec"}}

    try:
        await require_provider_rate_limit("sec")
        data = await provider.get_company_concept(cik, taxonomy=taxonomy, concept=concept)
        await cache.set(cache_key, data, ttl=CACHE_TTL)
        return {"success": True, "data": data, "meta": {"cached": False, "provider": "sec"}}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/frames/{concept}/{period}", response_model=SuccessResponse)
async def get_xbrl_frames(
    concept: str,
    period: str,
    taxonomy: str = Query(default="us-gaap", description="XBRL taxonomy"),
    unit: str = Query(default="USD", description="Unit: USD, shares, pure"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get aggregated XBRL data across all companies for a period (e.g., CY2023Q1)."""
    provider = registry.get("sec")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("sec:frames", taxonomy, concept, unit, period)
    cached = await cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "sec"}}

    try:
        await require_provider_rate_limit("sec")
        data = await provider.get_xbrl_frames(
            taxonomy=taxonomy, concept=concept, unit=unit, period=period
        )
        await cache.set(cache_key, data, ttl=CACHE_TTL)
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(data.get("data", [])), "cached": False, "provider": "sec"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/search", response_model=SuccessResponse)
async def search_filings(
    q: str = Query(..., description="Search query"),
    form_type: str | None = Query(default=None, description="Filter by form type"),
    limit: int = Query(default=100, le=500, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Search filings by keywords using EDGAR full-text search."""
    provider = registry.get("sec")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    cache_key = _cache_key("sec:search", q, form_type, str(limit))
    cached = await cache.get(cache_key)
    if cached:
        return {"success": True, "data": cached, "meta": {"cached": True, "provider": "sec"}}

    try:
        await require_provider_rate_limit("sec")
        results = await provider.search_filings(query=q, form_type=form_type, limit=limit)
        data = {"query": q, "results": results}
        await cache.set(cache_key, data, ttl=300)  # Shorter TTL for search
        return {
            "success": True,
            "data": data,
            "meta": {"count": len(results), "cached": False, "provider": "sec"},
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
