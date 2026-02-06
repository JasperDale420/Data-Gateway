"""Option contract endpoints for Unusual Whales."""

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
    require_api_key,
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
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_option_contract_flow(option_symbol=option_symbol),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"option_symbol": option_symbol},
        ),
    )


@router.get("/option-contract/{option_symbol}/historic", response_model=SuccessResponse)
async def get_option_contract_historic(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historic data for a specific option contract."""
    cache_key = f"uw:option-contract:historic:{option_symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_option_contract_historic(option_symbol=option_symbol),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"option_symbol": option_symbol},
        ),
    )


@router.get("/option-contract/{option_symbol}/intraday", response_model=SuccessResponse)
async def get_option_contract_intraday(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get intraday data for a specific option contract."""
    cache_key = f"uw:option-contract:intraday:{option_symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_option_contract_intraday(option_symbol=option_symbol),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"option_symbol": option_symbol},
        ),
    )


@router.get("/option-contract/{option_symbol}/volume-profile", response_model=SuccessResponse)
async def get_option_contract_volume_profile(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get volume profile for a specific option contract."""
    cache_key = f"uw:option-contract:volume-profile:{option_symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_option_contract_volume_profile(
            option_symbol=option_symbol
        ),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"option_symbol": option_symbol},
        ),
    )


@router.get("/contract/{option_symbol}/price-history", response_model=SuccessResponse)
async def get_contract_price_history(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get price history for an option contract."""
    cache_key = f"uw:contract:price-history:{option_symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_contract_price_history(option_symbol=option_symbol),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"option_symbol": option_symbol},
        ),
    )


@router.get("/flow/contract/{option_symbol}", response_model=SuccessResponse)
async def get_contract_flow(
    option_symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get flow for a specific contract."""
    cache_key = f"uw:flow:contract:{option_symbol}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_contract_flow(option_symbol=option_symbol),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"option_symbol": option_symbol},
        ),
    )


@router.get("/flow/full-tape", response_model=SuccessResponse)
async def get_full_tape_flow(
    limit: int = Query(default=100, ge=1, le=1000, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get full options tape."""
    cache_key = f"uw:flow:full-tape:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=30,
        fetcher=lambda provider: provider.get_full_tape(limit=limit),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/screener/option-contracts", response_model=SuccessResponse)
async def get_screener_option_contracts(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get option contracts from screener."""
    cache_key = f"uw:screener:option-contracts:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_screener_option_contracts(limit=limit),
        build_response=lambda data: make_response(data, count=len(data)),
    )


@router.get("/screener/stocks", response_model=SuccessResponse)
async def get_screener_stocks_extended(
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get stocks from screener."""
    cache_key = f"uw:screener:stocks:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_screener_stocks(limit=limit),
        build_response=lambda data: make_response(data, count=len(data)),
    )
