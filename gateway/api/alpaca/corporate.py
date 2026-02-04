"""Alpaca corporate actions endpoints."""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.alpaca.common import (
    DESC_END_TIME,
    DESC_START_TIME,
    ERR_PROVIDER_NOT_AVAILABLE,
    Client,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()


@router.get("/corporate-actions/{symbol}", response_model=SuccessResponse)
async def get_corporate_actions(
    symbol: str,
    types: str | None = Query(
        default=None, description="Action types: dividend,split,merger,spinoff"
    ),
    start: datetime | None = Query(default=None, description=DESC_START_TIME),
    end: datetime | None = Query(default=None, description=DESC_END_TIME),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get corporate actions for a symbol."""
    provider = registry.get("alpaca")

    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    try:
        await require_provider_rate_limit("alpaca")
        types_list = None
        if types:
            types_list = [t.strip() for t in types.split(",")]

        actions = await provider.get_corporate_actions(
            symbols=[symbol.upper()],
            types=types_list,
            start=start,
            end=end,
        )

        return {
            "success": True,
            "data": {
                "symbol": symbol.upper(),
                "actions": [a.model_dump(mode="json") for a in actions],
            },
            "meta": {
                "count": len(actions),
                "provider": "alpaca",
            },
        }

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Provider error: {str(e)}")
