"""Alpaca metadata endpoints - condition codes, exchange codes, logos, fixed income."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alpaca.common import (
    ERR_PROVIDER_NOT_AVAILABLE,
    Client,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()


@router.get("/meta/conditions", response_model=SuccessResponse)
async def get_condition_codes(
    asset_class: str = Query(default="stocks", description="Asset class: stocks, options, crypto"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get condition codes for trades/quotes."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        data = await provider.get_condition_codes(asset_class=asset_class)

        return {
            "success": True,
            "data": data,
            "meta": {
                "asset_class": asset_class,
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/meta/exchanges", response_model=SuccessResponse)
async def get_exchange_codes(
    asset_class: str = Query(default="stocks", description="Asset class: stocks, options, crypto"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get exchange codes."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        data = await provider.get_exchange_codes(asset_class=asset_class)

        return {
            "success": True,
            "data": data,
            "meta": {
                "asset_class": asset_class,
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/logos/{symbol}")
async def get_logo(
    symbol: str,
    placeholder: bool = Query(default=True, description="Return placeholder if logo not found"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get company logo image for a symbol."""
    from fastapi.responses import Response

    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        logo_bytes = await provider.get_logo(symbol=symbol.upper(), placeholder=placeholder)

        if logo_bytes is None:
            raise HTTPException(status_code=404, detail="Logo not found")

        return Response(content=logo_bytes, media_type="image/png")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")


@router.get("/fixed-income/prices", response_model=SuccessResponse)
async def get_fixed_income_prices(
    isins: str = Query(..., description="Comma-separated list of ISINs (max 1000)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get latest prices for fixed income securities (US Treasuries)."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        isin_list = [s.strip() for s in isins.split(",") if s.strip()]
        data = await provider.get_fixed_income_prices(isins=isin_list)

        return {
            "success": True,
            "data": data,
            "meta": {
                "isin_count": len(isin_list),
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
