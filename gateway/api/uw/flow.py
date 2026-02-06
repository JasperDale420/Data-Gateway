"""Flow and Darkpool endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_DATE,
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    cursor_fetch_limit,
    execute_uw_cached,
    get_cache,
    get_registry,
    paginate_response,
    require_api_key,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/flow/all", response_model=SuccessResponse)
async def get_flow_all(
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None, description="Pagination cursor"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all recent options flow alerts."""
    cache_key = f"uw:flow:all:{limit}:{cursor or 'start'}"
    _, fetch_limit = cursor_fetch_limit(limit=limit, cursor=cursor)
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=30,
        fetcher=lambda provider: provider.get_flow_alerts(limit=fetch_limit),
        build_response=lambda alerts: paginate_response(alerts, limit, cursor),
    )


@router.get("/flow/{symbol}", response_model=SuccessResponse)
async def get_flow_symbol(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get options flow for a specific ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:flow:{symbol}:{limit}:{date or 'latest'}:{cursor or 'start'}"
    _, fetch_limit = cursor_fetch_limit(limit=limit, cursor=cursor)
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=30,
        fetcher=lambda provider: provider.get_ticker_flow(
            symbol=symbol,
            date_str=date,
            limit=fetch_limit,
        ),
        build_response=lambda alerts: paginate_response(alerts, limit, cursor),
    )


@router.get("/darkpool/all", response_model=SuccessResponse)
async def get_darkpool_all(
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get all recent darkpool trades."""
    cache_key = f"uw:darkpool:all:{limit}:{cursor or 'start'}"
    _, fetch_limit = cursor_fetch_limit(limit=limit, cursor=cursor)
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=30,
        fetcher=lambda provider: provider.get_darkpool_recent(limit=fetch_limit),
        build_response=lambda trades: paginate_response(trades, limit, cursor),
    )


@router.get("/darkpool/{symbol}", response_model=SuccessResponse)
async def get_darkpool_symbol(
    symbol: str,
    limit: int = Query(default=50, le=100, ge=1),
    cursor: str | None = Query(default=None),
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get darkpool trades for a specific ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:darkpool:{symbol}:{limit}:{date or 'latest'}:{cursor or 'start'}"
    _, fetch_limit = cursor_fetch_limit(limit=limit, cursor=cursor)
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=30,
        fetcher=lambda provider: provider.get_darkpool_ticker(
            symbol=symbol,
            date_str=date,
            limit=fetch_limit,
        ),
        build_response=lambda trades: paginate_response(trades, limit, cursor),
    )
