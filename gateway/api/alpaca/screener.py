"""Alpaca screener endpoints - most active stocks and movers."""

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alpaca.common import (
    Client,
    execute_alpaca_provider_call,
    get_registry,
    require_api_key,
)
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()


@router.get("/screener/most-actives", response_model=SuccessResponse)
async def get_most_actives(
    by: str = Query(default="volume", description="Sort by: volume or trades"),
    top: int = Query(default=10, le=100, description="Number of results"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get most active stocks by volume or trade count."""
    if by not in ("volume", "trades"):
        raise HTTPException(status_code=400, detail="'by' must be 'volume' or 'trades'")

    actives = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_most_actives(by=by, top=top),
    )

    return {
        "success": True,
        "data": {
            "most_actives": [a.model_dump(mode="json") for a in actives],
        },
        "meta": {
            "count": len(actives),
            "by": by,
            "provider": "alpaca",
        },
    }


@router.get("/screener/movers", response_model=SuccessResponse)
async def get_movers(
    market_type: str = Query(default="stocks", description="Market type: stocks"),
    top: int = Query(default=10, le=50, description="Number of results per side"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get top movers - gainers and losers."""
    movers = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_movers(market_type=market_type, top=top),
    )

    return {
        "success": True,
        "data": {
            "gainers": [g.model_dump(mode="json") for g in movers.get("gainers", [])],
            "losers": [loser.model_dump(mode="json") for loser in movers.get("losers", [])],
        },
        "meta": {
            "gainers_count": len(movers.get("gainers", [])),
            "losers_count": len(movers.get("losers", [])),
            "market_type": market_type,
            "provider": "alpaca",
        },
    }
