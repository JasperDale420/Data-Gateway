"""Options data endpoints for Unusual Whales (option volume, intraday, contracts screener)."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_DATE,
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


@router.get("/{symbol}/option-volume", response_model=SuccessResponse)
async def get_historic_option_volume(
    symbol: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get historic option volume and open interest by expiry."""
    symbol = symbol.upper()
    cache_key = f"uw:option-volume:{symbol}:{date or 'latest'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=300,
        fetcher=lambda provider: provider.get_historic_option_volume(symbol=symbol, date_str=date),
        build_response=lambda data: make_response(
            data,
            symbol=symbol,
            count=len(data),
            extra_meta={"date": date},
        ),
    )


@router.get("/contract/{contract_id}/intraday", response_model=SuccessResponse)
async def get_intraday_option_data(
    contract_id: str,
    date: str | None = Query(default=None, description=DESC_DATE),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get intraday 1-min OHLC data for an option contract."""
    cache_key = f"uw:intraday:{contract_id}:{date or 'latest'}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_intraday_option_data(
            contract_id=contract_id,
            date_str=date,
        ),
        build_response=lambda data: make_response(
            data,
            count=len(data),
            extra_meta={"contract": contract_id, "date": date},
        ),
    )


@router.get("/screener/contracts", response_model=SuccessResponse)
async def get_options_screener(
    min_volume: int | None = Query(default=None, description="Min volume"),
    min_premium: float | None = Query(default=None, description="Min premium"),
    limit: int = Query(default=50, le=200),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
):
    """Get option contracts screener with filters."""
    cache_key = f"uw:contracts-screener:{min_volume}:{min_premium}:{limit}"
    return await execute_uw_cached(
        cache=cache,
        cache_key=cache_key,
        registry=registry,
        ttl=60,
        fetcher=lambda provider: provider.get_options_screener(
            min_volume=min_volume,
            min_premium=min_premium,
            limit=limit,
        ),
        build_response=lambda data: {
            **paginate_response(data, limit),
            "meta": {"count": len(data), "provider": "unusual_whales"},
        },
    )
