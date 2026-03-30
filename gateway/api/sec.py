"""SEC EDGAR API endpoints for filings and company data."""

from collections.abc import Awaitable, Callable
from typing import TypeVar

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import get_cache, get_registry, require_api_key, require_provider_rate_limit
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.dedup import get_deduplicator
from gateway.core.logger import logger
from gateway.core.metrics import record_route_cache
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

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


T = TypeVar("T")


async def execute_sec_cached(
    provider_method: Callable[[], Awaitable[T]],
    cache: InMemoryCache,
    cache_key: str,
    route_name: str,
    cache_ttl: int = CACHE_TTL,
    miss_meta_builder: Callable[[T], dict] | None = None,
) -> dict:
    """Centralized SEC execution wrapper with caching, dedup, and error handling."""
    cached = await cache.get(cache_key)
    if cached:
        record_route_cache(route_name, "hit")
        meta: dict = {"cached": True, "provider": "sec"}
        if miss_meta_builder:
            meta.update(miss_meta_builder(cached))
        return {"success": True, "data": cached, "meta": meta}

    record_route_cache(route_name, "miss")
    try:
        data = await get_deduplicator().dedupe(cache_key, provider_method)
        await cache.set(cache_key, data, ttl=cache_ttl)
        meta = {"cached": False, "provider": "sec"}
        if miss_meta_builder:
            meta.update(miss_meta_builder(data))
        return {"success": True, "data": data, "meta": meta}
    except httpx.HTTPStatusError as e:
        logger.error("sec_request_failed", route=route_name, error=str(e), status_code=e.response.status_code)
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="SEC resource not found")
        if e.response.status_code == 429:
            raise HTTPException(status_code=429, detail="SEC rate limit exceeded")
        if e.response.status_code == 403:
            raise HTTPException(status_code=403, detail="SEC access forbidden")
        raise HTTPException(status_code=502, detail="Upstream SEC error")
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception:
        logger.error("sec_request_failed", route=route_name, exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


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

    async def _fetch():
        await require_provider_rate_limit("sec")
        return await provider.get_company_info(cik)

    return await execute_sec_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("sec:company", cik),
        route_name="sec_company",
    )


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

    async def _fetch():
        await require_provider_rate_limit("sec")
        return await provider.get_company_by_ticker(ticker)

    return await execute_sec_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("sec:ticker", ticker.upper()),
        route_name="sec_ticker",
    )


@router.get("/filings/{cik}", response_model=SuccessResponse)
async def get_filings(
    cik: str,
    form_type: str | None = Query(default=None, description="Filter by form type (10-K, 10-Q, 8-K, etc)"),
    limit: int = Query(default=100, le=500, ge=1, description="Max filings to return"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company filings by CIK."""
    provider = registry.get("sec")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    async def _fetch():
        await require_provider_rate_limit("sec")
        filings = await provider.get_filings(cik, form_type=form_type, limit=limit)
        return {"cik": cik, "form_type": form_type, "filings": filings}

    return await execute_sec_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("sec:filings", cik, form_type, str(limit)),
        route_name="sec_filings",
        miss_meta_builder=lambda d: {"count": len(d.get("filings", []))},
    )


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

    async def _fetch():
        await require_provider_rate_limit("sec")
        filings = await provider.get_filings(cik, form_type=form_type.upper(), limit=limit)
        return {"cik": cik, "form_type": form_type.upper(), "filings": filings}

    return await execute_sec_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("sec:filings", cik, form_type.upper(), str(limit)),
        route_name="sec_filings_by_type",
        miss_meta_builder=lambda d: {"count": len(d.get("filings", []))},
    )


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

    async def _fetch():
        await require_provider_rate_limit("sec")
        filings = await provider.get_13f_holdings(cik)
        return {"cik": cik, "filings": filings}

    return await execute_sec_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("sec:13f", cik),
        route_name="sec_13f",
        miss_meta_builder=lambda d: {"count": len(d.get("filings", []))},
    )


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

    async def _fetch():
        await require_provider_rate_limit("sec")
        filings = await provider.get_insider_trades(cik, limit=limit)
        return {"cik": cik, "filings": filings}

    return await execute_sec_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("sec:insiders", cik, str(limit)),
        route_name="sec_insiders",
        miss_meta_builder=lambda d: {"count": len(d.get("filings", []))},
    )


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

    async def _fetch():
        await require_provider_rate_limit("sec")
        return await provider.get_company_facts(cik)

    return await execute_sec_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("sec:facts", cik),
        route_name="sec_facts",
    )


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

    async def _fetch():
        await require_provider_rate_limit("sec")
        return await provider.get_company_concept(cik, taxonomy=taxonomy, concept=concept)

    return await execute_sec_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("sec:concept", cik, taxonomy, concept),
        route_name="sec_concept",
    )


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

    async def _fetch():
        await require_provider_rate_limit("sec")
        return await provider.get_xbrl_frames(taxonomy=taxonomy, concept=concept, unit=unit, period=period)

    return await execute_sec_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("sec:frames", taxonomy, concept, unit, period),
        route_name="sec_frames",
        miss_meta_builder=lambda d: {"count": len(d.get("data", []))},
    )


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

    async def _fetch():
        await require_provider_rate_limit("sec")
        results = await provider.search_filings(query=q, form_type=form_type, limit=limit)
        return {"query": q, "results": results}

    return await execute_sec_cached(
        provider_method=_fetch,
        cache=cache,
        cache_key=_cache_key("sec:search", q, form_type, str(limit)),
        route_name="sec_search",
        cache_ttl=300,  # Shorter TTL for search results
        miss_meta_builder=lambda d: {"count": len(d.get("results", []))},
    )
