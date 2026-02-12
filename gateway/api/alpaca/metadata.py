"""Alpaca metadata endpoints - condition codes, exchange codes, logos, fixed income."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alpaca.common import (
    Client,
    execute_alpaca_cached_call,
    execute_alpaca_provider_call,
    get_cache,
    get_registry,
    parse_comma_values,
    require_api_key,
)
from gateway.core.cache import HybridCache, InMemoryCache
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()
META_CACHE_TTL_SECONDS = 1800  # 30 min


@router.get("/meta/conditions", response_model=SuccessResponse)
async def get_condition_codes(
    asset_class: str = Query(default="stocks", description="Asset class: stocks, options, crypto"),
    client: Client = Depends(require_api_key),
    cache: InMemoryCache | HybridCache = Depends(get_cache),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get condition codes for trades/quotes."""
    normalized_asset_class = asset_class.strip().lower()
    data = await execute_alpaca_cached_call(
        registry=registry,
        cache=cache,
        cache_key=f"alpaca:meta:conditions:{normalized_asset_class}",
        ttl=META_CACHE_TTL_SECONDS,
        provider_call=lambda provider: provider.get_condition_codes(asset_class=asset_class),
        route_label="alpaca_meta_conditions",
    )

    return {
        "success": True,
        "data": data,
        "meta": {
            "asset_class": asset_class,
            "provider": "alpaca",
        },
    }


@router.get("/meta/exchanges", response_model=SuccessResponse)
async def get_exchange_codes(
    asset_class: str = Query(default="stocks", description="Asset class: stocks, options, crypto"),
    client: Client = Depends(require_api_key),
    cache: InMemoryCache | HybridCache = Depends(get_cache),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get exchange codes."""
    normalized_asset_class = asset_class.strip().lower()
    data = await execute_alpaca_cached_call(
        registry=registry,
        cache=cache,
        cache_key=f"alpaca:meta:exchanges:{normalized_asset_class}",
        ttl=META_CACHE_TTL_SECONDS,
        provider_call=lambda provider: provider.get_exchange_codes(asset_class=asset_class),
        route_label="alpaca_meta_exchanges",
    )

    return {
        "success": True,
        "data": data,
        "meta": {
            "asset_class": asset_class,
            "provider": "alpaca",
        },
    }


@router.get("/logos/{symbol}")
async def get_logo(
    symbol: str,
    placeholder: bool = Query(default=True, description="Return placeholder if logo not found"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get company logo image for a symbol."""
    from fastapi.responses import Response

    logo_bytes = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_logo(
            symbol=symbol.upper(), placeholder=placeholder
        ),
    )
    if logo_bytes is None:
        raise HTTPException(status_code=404, detail="Logo not found")

    return Response(content=logo_bytes, media_type="image/png")


@router.get("/fixed-income/prices", response_model=SuccessResponse)
async def get_fixed_income_prices(
    isins: str = Query(..., description="Comma-separated list of ISINs (max 1000)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest prices for fixed income securities (US Treasuries)."""
    isin_list = parse_comma_values(isins, drop_empty=True)
    data = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_fixed_income_prices(isins=isin_list),
    )

    return {
        "success": True,
        "data": data,
        "meta": {
            "isin_count": len(isin_list),
            "provider": "alpaca",
        },
    }
