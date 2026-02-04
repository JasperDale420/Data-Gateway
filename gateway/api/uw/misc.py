"""Final gap endpoints (ETF tide, volume levels, tape, profile, news) for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
    HTTPException,
    InMemoryCache,
    ProviderRegistry,
    SuccessResponse,
    get_cache,
    get_registry,
    get_uw_provider,
    require_api_key,
    require_provider_rate_limit,
)

router = APIRouter(tags=["unusual_whales"])


@router.get("/etf/{symbol}/tide", response_model=SuccessResponse)
async def get_etf_tide(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get ETF-level tide data (premium flow sentiment)."""
    symbol = symbol.upper()
    cache_key = f"uw:etf-tide:{symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_etf_tide(symbol=symbol)

    if not data:
        raise HTTPException(status_code=404, detail=f"ETF tide not found for {symbol}")

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


@router.get("/{symbol}/volume-levels", response_model=SuccessResponse)
async def get_option_volume_levels(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get option volume levels per price."""
    symbol = symbol.upper()
    cache_key = f"uw:volume-levels:{symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_volume_levels(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/trades/full-tape/{date}", response_model=SuccessResponse)
async def get_full_tape(
    date: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get full options trade tape for a date."""
    cache_key = f"uw:full-tape:{date}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_full_tape(date_str=date)

    response = {
        "success": True,
        "data": data,
        "meta": {"date": date, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=3600)
    return response


@router.get("/contract/{contract_id}/volume-profile", response_model=SuccessResponse)
async def get_volume_profile(
    contract_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get volume profile of an option contract by fill price."""
    cache_key = f"uw:volume-profile:{contract_id}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_volume_profile(contract_id=contract_id)

    response = {
        "success": True,
        "data": data,
        "meta": {"contract": contract_id, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/{symbol}/greek-flow-expiry", response_model=SuccessResponse)
async def get_greek_flow_expiry(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get greek flow data aggregated by expiration."""
    symbol = symbol.upper()
    cache_key = f"uw:greek-flow-expiry:{symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_greek_flow_expiry(symbol=symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/news/headlines", response_model=SuccessResponse)
async def get_news_headlines(
    sources: str | None = Query(default=None, description="Comma-separated sources"),
    search_term: str | None = Query(default=None, description="Search term"),
    major_only: bool | None = Query(default=None, description="Major news only"),
    limit: int = Query(default=50, le=200, ge=1),
    page: int | None = Query(default=None, description="Page number"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get latest financial news headlines."""
    cache_key = f"uw:news:headlines:{sources or 'all'}:{search_term or 'none'}:{major_only}:{limit}:{page or 1}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    sources_list = sources.split(",") if sources else None
    data = await provider.get_news_headlines(
        sources=sources_list,
        search_term=search_term,
        major_only=major_only,
        limit=limit,
        page=page,
    )

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response
