"""UW private-markets endpoints — Nasdaq Private Markets (pre-IPO) companies, investors, pricing.

Raw-HTTP passthrough endpoints not covered by the vendored SDK (v5.1). Premium data.
"""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_LIMIT,
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    count_of,
    execute_uw_cached,
    get_cache,
    get_registry,
    make_response,
    require_api_key,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/private-markets/companies", response_model=SuccessResponse)
async def get_private_markets_companies(
    sector: str | None = Query(default=None, description='Exact-match sector filter (e.g. "Technology").'),
    name: str | None = Query(default=None, description="Case-insensitive substring match against company name."),
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    offset: int | None = Query(default=None, description="Pagination offset."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """List Nasdaq Private Markets companies, optionally filtered by sector or name."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pm-companies:{sector or 'all'}:{name or 'all'}:{limit}:{offset}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_private_markets_companies(
            sector=sector, name=name, limit=limit, offset=offset
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/private-markets/companies/{npm_ticker}", response_model=SuccessResponse)
async def get_private_markets_company(
    npm_ticker: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Profile for a single private-markets company."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pm-company:{npm_ticker}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_private_markets_company(npm_ticker=npm_ticker),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/private-markets/companies/{npm_ticker}/funding", response_model=SuccessResponse)
async def get_private_markets_funding(
    npm_ticker: str,
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    offset: int | None = Query(default=None, description="Pagination offset."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Funding round history for a single private-markets company."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pm-funding:{npm_ticker}:{limit}:{offset}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_private_markets_funding(
            npm_ticker=npm_ticker, limit=limit, offset=offset
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/private-markets/companies/{npm_ticker}/investors", response_model=SuccessResponse)
async def get_private_markets_company_investors(
    npm_ticker: str,
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    offset: int | None = Query(default=None, description="Pagination offset."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Disclosed investors for a single private-markets company."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pm-company-investors:{npm_ticker}:{limit}:{offset}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_private_markets_company_investors(
            npm_ticker=npm_ticker, limit=limit, offset=offset
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/private-markets/companies/{npm_ticker}/management", response_model=SuccessResponse)
async def get_private_markets_management(
    npm_ticker: str,
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    offset: int | None = Query(default=None, description="Pagination offset."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Disclosed management/leadership for a single private-markets company."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pm-management:{npm_ticker}:{limit}:{offset}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_private_markets_management(
            npm_ticker=npm_ticker, limit=limit, offset=offset
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/private-markets/companies/{npm_ticker}/pricing", response_model=SuccessResponse)
async def get_private_markets_pricing(
    npm_ticker: str,
    start_date: str | None = Query(default=None, description="Inclusive lower bound on pricing date (ISO 8601)."),
    end_date: str | None = Query(default=None, description="Inclusive upper bound on pricing date (ISO 8601)."),
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    offset: int | None = Query(default=None, description="Pagination offset."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Historical implied per-share pricing for a single private-markets company."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pm-pricing:{npm_ticker}:{start_date or 'any'}:{end_date or 'any'}:{limit}:{offset}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_private_markets_pricing(
            npm_ticker=npm_ticker, start_date=start_date, end_date=end_date, limit=limit, offset=offset
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/private-markets/investors", response_model=SuccessResponse)
async def get_top_private_markets_investors(
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    offset: int | None = Query(default=None, description="Pagination offset."),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Most prolific investors across the private-markets dataset."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pm-top-investors:{limit}:{offset}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_top_private_markets_investors(limit=limit, offset=offset),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/private-markets/investors/{name}", response_model=SuccessResponse)
async def get_private_markets_investor(
    name: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Portfolio of companies for a specific investor (by name)."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pm-investor:{name}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_private_markets_investor(name=name),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/private-markets/search", response_model=SuccessResponse)
async def get_private_markets_search(
    query: str = Query(description="Search string (case-insensitive substring match)."),
    limit: int | None = Query(default=None, description=DESC_LIMIT),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Substring-search across private-markets companies and investors."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pm-search:{query}:{limit}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_private_markets_search(query=query, limit=limit),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )
