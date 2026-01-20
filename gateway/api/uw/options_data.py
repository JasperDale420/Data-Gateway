"""Options data endpoints for Unusual Whales (option volume, intraday, contracts screener)."""

from fastapi import APIRouter, Depends, Query

from gateway.api.uw.common import (
    DESC_DATE,
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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_historic_option_volume(symbol=symbol, date_str=date)

    response = {
        "success": True,
        "data": data,
        "meta": {"symbol": symbol, "date": date, "count": len(data), "provider": "unusual_whales"},
    }

    cache.set(cache_key, response, ttl=300)
    return response


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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_intraday_option_data(contract_id=contract_id, date_str=date)

    response = {
        "success": True,
        "data": data,
        "meta": {
            "contract": contract_id,
            "date": date,
            "count": len(data),
            "provider": "unusual_whales",
        },
    }

    cache.set(cache_key, response, ttl=60)
    return response


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
    cached = cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    data = await provider.get_options_screener(
        min_volume=min_volume, min_premium=min_premium, limit=limit
    )

    response = paginate_response(data, limit)
    response["meta"] = {"count": len(data), "provider": "unusual_whales"}

    cache.set(cache_key, response, ttl=60)
    return response
