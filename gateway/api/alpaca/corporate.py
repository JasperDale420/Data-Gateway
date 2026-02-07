"""Alpaca corporate actions endpoints."""

from datetime import datetime

from fastapi import APIRouter, Depends, Query

from gateway.api.alpaca.common import (
    DESC_END_TIME,
    DESC_START_TIME,
    Client,
    execute_alpaca_provider_call,
    get_registry,
    parse_comma_values,
    require_api_key,
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
    types_list = parse_comma_values(types) if types else None
    actions = await execute_alpaca_provider_call(
        registry=registry,
        provider_call=lambda provider: provider.get_corporate_actions(
            symbols=[symbol.upper()],
            types=types_list,
            start=start,
            end=end,
        ),
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
