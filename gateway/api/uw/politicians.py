"""Politician portfolio endpoints for Unusual Whales (Enterprise-only)."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    execute_uw_cached,
    get_cache,
    get_registry,
    make_response,
    paginate_response,
    require_api_key,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/politicians/people", response_model=SuccessResponse)
async def get_politician_people(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all politician names and IDs."""
    cache_key = "uw:politicians:people"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_politician_people(),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/politicians/recent-trades", response_model=SuccessResponse)
async def get_politician_recent_trades(
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get latest transacted trades by congress members."""
    cache_key = f"uw:politicians:trades:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_politician_recent_trades(limit=limit),
        build_response=lambda data: paginate_response(data, limit),
    )


@router.get("/politicians/{politician_id}/portfolios", response_model=SuccessResponse)
async def get_politician_portfolios(
    politician_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all portfolios and holdings for a politician."""
    cache_key = f"uw:politicians:{politician_id}:portfolios"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_politician_portfolios(politician_id=politician_id),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"politician_id": politician_id},
        ),
    )


@router.get("/politicians/{symbol}/holders", response_model=SuccessResponse)
async def get_politician_holders(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get politician portfolio holders for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:politicians:holders:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_politician_holders(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )
