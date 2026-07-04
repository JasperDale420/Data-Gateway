"""UW prediction-market endpoints — insiders, whales, smart-money, markets, users.

Raw-HTTP passthrough endpoints not covered by the vendored SDK (v5.1).
"""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
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


@router.get("/predictions/insiders", response_model=SuccessResponse)
async def get_prediction_insiders(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Potential insider activity on prediction markets."""
    return await execute_uw_cached(
        cache=cache,
        cache_key="uw:pred-insiders",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_prediction_insiders(),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/predictions/market/{asset_id}", response_model=SuccessResponse)
async def get_prediction_market(
    asset_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Prediction market details for a given asset ID."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pred-market:{asset_id}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_prediction_market(asset_id=asset_id),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/predictions/market/{asset_id}/liquidity", response_model=SuccessResponse)
async def get_prediction_market_liquidity(
    asset_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Liquidity data for a given prediction market asset."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pred-market-liquidity:{asset_id}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_prediction_market_liquidity(asset_id=asset_id),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/predictions/market/{asset_id}/positions", response_model=SuccessResponse)
async def get_prediction_market_positions(
    asset_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Positions for a given prediction market asset."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pred-market-positions:{asset_id}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_prediction_market_positions(asset_id=asset_id),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/predictions/search-users", response_model=SuccessResponse)
async def get_prediction_search_users(
    q: str = Query(description="Search query"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Search for prediction market users by query."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pred-search-users:{q}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_prediction_search_users(q=q),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/predictions/smart-money", response_model=SuccessResponse)
async def get_prediction_smart_money(
    categories: str | None = Query(default=None, description="Comma-separated categories"),
    min_price: float | None = Query(default=None, description="Minimum price filter"),
    max_price: float | None = Query(default=None, description="Maximum price filter"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Profitable prediction market traders."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pred-smart-money:{categories or 'all'}:{min_price}:{max_price}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_prediction_smart_money(
            categories=categories, min_price=min_price, max_price=max_price
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/predictions/unusual", response_model=SuccessResponse)
async def get_prediction_unusual_markets(
    categories: str | None = Query(default=None, description="Comma-separated categories"),
    limit: int | None = Query(default=None, description="Maximum number of results"),
    offset: int | None = Query(default=None, description="Result offset"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Prediction markets with unusual activity."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pred-unusual:{categories or 'all'}:{limit}:{offset}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_prediction_unusual_markets(
            categories=categories, limit=limit, offset=offset
        ),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/predictions/user/{user_id}", response_model=SuccessResponse)
async def get_prediction_user(
    user_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Prediction market user profile by user/wallet ID."""
    return await execute_uw_cached(
        cache=cache,
        cache_key=f"uw:pred-user:{user_id}",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_prediction_user(user_id=user_id),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )


@router.get("/predictions/whales", response_model=SuccessResponse)
async def get_prediction_whales(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Large prediction market traders."""
    return await execute_uw_cached(
        cache=cache,
        cache_key="uw:pred-whales",
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_prediction_whales(),
        build_response=lambda data: make_response(data, count=count_of(data)),
    )
