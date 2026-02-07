"""Institution endpoints for Unusual Whales."""

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


@router.get("/institutions", response_model=SuccessResponse)
async def get_all_institutions(
    limit: int = Query(default=100, le=500, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get list of all institutions."""
    cache_key = f"uw:institutions:all:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_all_institutions(limit=limit),
        build_response=lambda data: paginate_response(data, limit),
    )


@router.get("/institutions/latest-filings", response_model=SuccessResponse)
async def get_latest_institutional_filings(
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get latest institutional filings."""
    cache_key = f"uw:institutions:filings:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_latest_institutional_filings(limit=limit),
        build_response=lambda data: paginate_response(data, limit),
    )


@router.get("/institutions/{institution_id}/activity", response_model=SuccessResponse)
async def get_institution_activity(
    institution_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get latest activity by an institution."""
    cache_key = f"uw:institutions:{institution_id}:activity"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_institution_activity(institution_id=institution_id),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"institution_id": institution_id},
        ),
    )


@router.get("/institutions/{institution_id}/holdings", response_model=SuccessResponse)
async def get_institution_holdings(
    institution_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get current holdings of an institution."""
    cache_key = f"uw:institutions:{institution_id}:holdings"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_institution_holdings(institution_id=institution_id),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"institution_id": institution_id},
        ),
    )


@router.get("/institutions/{institution_id}/sectors", response_model=SuccessResponse)
async def get_institution_sector_exposure(
    institution_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get sector exposure of an institution."""
    cache_key = f"uw:institutions:{institution_id}:sectors"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_institution_sector_exposure(
            institution_id=institution_id
        ),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"institution_id": institution_id},
        ),
    )


@router.get("/institutions/{symbol}/ownership", response_model=SuccessResponse)
async def get_institutional_ownership(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get institutional ownership for a ticker."""
    symbol = symbol.upper()
    cache_key = f"uw:institutions:ownership:{symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=3600,
        fetcher=lambda provider: provider.get_institutional_ownership(symbol=symbol),
        build_response=lambda data: make_response(data, symbol=symbol, count=len(data)),
    )
