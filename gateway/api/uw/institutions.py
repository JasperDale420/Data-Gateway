"""Institution endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    get_cache,
    get_registry,
    get_uw_provider,
    paginate_response,
    require_api_key,
    require_provider_rate_limit,
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_all_institutions(limit=limit)

    response = paginate_response(data, limit)
    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/institutions/latest-filings", response_model=SuccessResponse)
async def get_latest_institutional_filings(
    limit: int = Query(default=50, le=200, ge=1),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get latest institutional filings."""
    cache_key = f"uw:institutions:filings:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_latest_institutional_filings(limit=limit)

    response = paginate_response(data, limit)
    cache.set(cache_key, response, ttl=300)
    return response


@router.get("/institutions/{institution_id}/activity", response_model=SuccessResponse)
async def get_institution_activity(
    institution_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get latest activity by an institution."""
    cache_key = f"uw:institutions:{institution_id}:activity"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_institution_activity(institution_id=institution_id)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "institution_id": institution_id,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/institutions/{institution_id}/holdings", response_model=SuccessResponse)
async def get_institution_holdings(
    institution_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get current holdings of an institution."""
    cache_key = f"uw:institutions:{institution_id}:holdings"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_institution_holdings(institution_id=institution_id)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "institution_id": institution_id,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/institutions/{institution_id}/sectors", response_model=SuccessResponse)
async def get_institution_sector_exposure(
    institution_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get sector exposure of an institution."""
    cache_key = f"uw:institutions:{institution_id}:sectors"
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_institution_sector_exposure(institution_id=institution_id)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "institution_id": institution_id,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    cache.set(cache_key, response, ttl=3600)
    return response


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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_institutional_ownership(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=3600)
    return response
