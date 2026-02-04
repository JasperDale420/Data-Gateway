"""Option contract endpoints for Unusual Whales."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    Client,
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


@router.get("/option-contract/{option_symbol}/flow", response_model=SuccessResponse)
async def get_option_contract_flow(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow for a specific option contract."""
    cache_key = f"uw:option-contract:flow:{option_symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_contract_flow(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


@router.get("/option-contract/{option_symbol}/historic", response_model=SuccessResponse)
async def get_option_contract_historic(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historic data for a specific option contract."""
    cache_key = f"uw:option-contract:historic:{option_symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_contract_historic(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/option-contract/{option_symbol}/intraday", response_model=SuccessResponse)
async def get_option_contract_intraday(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get intraday data for a specific option contract."""
    cache_key = f"uw:option-contract:intraday:{option_symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_contract_intraday(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


@router.get("/option-contract/{option_symbol}/volume-profile", response_model=SuccessResponse)
async def get_option_contract_volume_profile(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get volume profile for a specific option contract."""
    cache_key = f"uw:option-contract:volume-profile:{option_symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_option_contract_volume_profile(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/contract/{option_symbol}/price-history", response_model=SuccessResponse)
async def get_contract_price_history(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get price history for an option contract."""
    cache_key = f"uw:contract:price-history:{option_symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_contract_price_history(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=300)
    return response


@router.get("/flow/contract/{option_symbol}", response_model=SuccessResponse)
async def get_contract_flow(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow for a specific contract."""
    cache_key = f"uw:flow:contract:{option_symbol}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_contract_flow(option_symbol=option_symbol)

    response = {
        "success": True,
        "data": data,
        "meta": {"option_symbol": option_symbol, "count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


@router.get("/flow/full-tape", response_model=SuccessResponse)
async def get_full_tape_flow(
    limit: int = Query(default=100, ge=1, le=1000, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get full options tape."""
    cache_key = f"uw:flow:full-tape:{limit}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_full_tape(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=30)
    return response


@router.get("/screener/option-contracts", response_model=SuccessResponse)
async def get_screener_option_contracts(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get option contracts from screener."""
    cache_key = f"uw:screener:option-contracts:{limit}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_screener_option_contracts(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response


@router.get("/screener/stocks", response_model=SuccessResponse)
async def get_screener_stocks_extended(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get stocks from screener."""
    cache_key = f"uw:screener:stocks:{limit}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_screener_stocks(limit=limit)

    response = {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "unusual_whales"},
    }

    await cache.set(cache_key, response, ttl=60)
    return response
