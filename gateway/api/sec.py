"""SEC EDGAR API endpoints for filings and company data."""

from collections.abc import Callable
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import (
    execute_provider_cached,
    get_cache,
    get_registry,
    make_cache_key,
    require_api_key,
)
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.dedup import get_deduplicator
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter(prefix="/api/v1/sec", tags=["sec"])

PROVIDER_NOT_AVAILABLE = "SEC provider not available"
CACHE_TTL = 3600  # 1 hour (filings update infrequently)


def _cache_key(prefix: str, *args) -> str:
    """Generate cache key for SEC data."""
    return make_cache_key(prefix, *args)


def _sec_error_handler(exc: Exception) -> None:
    """Map SEC-specific HTTP status codes to user-friendly errors."""
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status == 404:
            raise HTTPException(status_code=404, detail="SEC resource not found")
        if status == 429:
            raise HTTPException(status_code=429, detail="SEC rate limit exceeded")
        if status == 403:
            raise HTTPException(status_code=403, detail="SEC access forbidden")
    if isinstance(exc, ValueError):
        raise HTTPException(status_code=404, detail=str(exc))


async def execute_sec_cached(
    *,
    registry: ProviderRegistry,
    cache: InMemoryCache,
    cache_key: str,
    route_name: str,
    fetcher: Callable,
    cache_ttl: int = CACHE_TTL,
    miss_meta_builder: Callable[[Any], dict] | None = None,
) -> dict:
    """Centralized SEC execution wrapper with caching, dedup, and error handling.

    Args:
        registry: Provider registry for provider lookup.
        cache: Cache backend.
        cache_key: Pre-built cache key.
        route_name: Label for cache-hit/miss metrics.
        fetcher: ``async (provider) -> data`` callable.
        cache_ttl: Cache TTL in seconds.
        miss_meta_builder: Optional ``(data) -> dict`` for extra metadata.
    """
    deduplicator = get_deduplicator()

    async def _deduped_fetcher(provider):
        return await deduplicator.dedupe(cache_key, lambda: fetcher(provider))

    return await execute_provider_cached(
        provider_name="sec",
        registry=registry,
        cache=cache,
        cache_key=cache_key,
        ttl=cache_ttl,
        fetcher=_deduped_fetcher,
        miss_meta_builder=miss_meta_builder,
        route_label=route_name,
        error_label="sec_request_failed",
        error_handlers=[_sec_error_handler],
        not_available_msg=PROVIDER_NOT_AVAILABLE,
    )


@router.get("/company/{cik}", response_model=SuccessResponse)
async def get_company_info(
    cik: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get company info by CIK."""
    return await execute_sec_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("sec:company", cik),
        route_name="sec_company",
        fetcher=lambda p: p.get_company_info(cik),
    )


@router.get("/company/ticker/{ticker}", response_model=SuccessResponse)
async def get_company_by_ticker(
    ticker: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Lookup company info by ticker symbol."""
    return await execute_sec_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("sec:ticker", ticker.upper()),
        route_name="sec_ticker",
        fetcher=lambda p: p.get_company_by_ticker(ticker),
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

    async def _fetch(provider):
        filings = await provider.get_filings(cik, form_type=form_type, limit=limit)
        return {"cik": cik, "form_type": form_type, "filings": filings}

    return await execute_sec_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("sec:filings", cik, form_type, str(limit)),
        route_name="sec_filings",
        fetcher=_fetch,
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

    async def _fetch(provider):
        filings = await provider.get_filings(cik, form_type=form_type.upper(), limit=limit)
        return {"cik": cik, "form_type": form_type.upper(), "filings": filings}

    return await execute_sec_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("sec:filings", cik, form_type.upper(), str(limit)),
        route_name="sec_filings_by_type",
        fetcher=_fetch,
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

    async def _fetch(provider):
        filings = await provider.get_13f_holdings(cik)
        return {"cik": cik, "filings": filings}

    return await execute_sec_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("sec:13f", cik),
        route_name="sec_13f",
        fetcher=_fetch,
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

    async def _fetch(provider):
        filings = await provider.get_insider_trades(cik, limit=limit)
        return {"cik": cik, "filings": filings}

    return await execute_sec_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("sec:insiders", cik, str(limit)),
        route_name="sec_insiders",
        fetcher=_fetch,
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
    return await execute_sec_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("sec:facts", cik),
        route_name="sec_facts",
        fetcher=lambda p: p.get_company_facts(cik),
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
    return await execute_sec_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("sec:concept", cik, taxonomy, concept),
        route_name="sec_concept",
        fetcher=lambda p: p.get_company_concept(cik, taxonomy=taxonomy, concept=concept),
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
    return await execute_sec_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("sec:frames", taxonomy, concept, unit, period),
        route_name="sec_frames",
        fetcher=lambda p: p.get_xbrl_frames(taxonomy=taxonomy, concept=concept, unit=unit, period=period),
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

    async def _fetch(provider):
        results = await provider.search_filings(query=q, form_type=form_type, limit=limit)
        return {"query": q, "results": results}

    return await execute_sec_cached(
        registry=registry,
        cache=cache,
        cache_key=_cache_key("sec:search", q, form_type, str(limit)),
        route_name="sec_search",
        fetcher=_fetch,
        cache_ttl=300,  # Shorter TTL for search results
        miss_meta_builder=lambda d: {"count": len(d.get("results", []))},
    )
