"""Corporate Actions and Adjustment Factors API endpoints.

Provides corporate action history and adjustment factors
as specified in PRD (lines 1126-1204).
"""

from datetime import date

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from gateway.core.adjustments import get_adjustment_service
from gateway.core.corporate_actions import (
    ActionType,
    get_corporate_actions_service,
)

router = APIRouter(prefix="/corporate-actions", tags=["corporate-actions"])
adjustments_router = APIRouter(prefix="/adjustment-factors", tags=["adjustment-factors"])


# ─────────────────────────────────────────────────────────────────────────────
# Response Models
# ─────────────────────────────────────────────────────────────────────────────


class CorporateActionsResponse(BaseModel):
    """Response for corporate actions query."""

    symbol: str
    actions: list[dict]


class AdjustmentFactorsResponse(BaseModel):
    """Response for adjustment factors query."""

    symbol: str
    factors: list[dict]


class AdjustPricesRequest(BaseModel):
    """Request for price adjustment."""

    symbol: str
    prices: list[dict]  # [{"date": "2024-01-15", "price": 100.00}]


class AdjustPricesResponse(BaseModel):
    """Response for price adjustment."""

    symbol: str
    adjusted_prices: list[dict]


# ─────────────────────────────────────────────────────────────────────────────
# Corporate Actions Endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/{symbol}", response_model=CorporateActionsResponse)
async def get_corporate_actions(
    symbol: str,
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
    action_type: str | None = Query(
        None,
        description="Filter by action type (split, dividend, merger, spinoff)",
    ),
) -> CorporateActionsResponse:
    """Get corporate actions for a symbol.

    Returns splits, dividends, mergers, spinoffs, and other corporate events
    within the specified date range.
    """
    if end < start:
        raise HTTPException(
            status_code=400,
            detail={"code": "GW-E8003", "message": "End date must be after start date"},
        )

    service = get_corporate_actions_service()

    # Parse action type filter
    action_types = None
    if action_type:
        try:
            action_types = [ActionType(action_type.lower())]
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "GW-E8004",
                    "message": f"Invalid action type: {action_type}",
                },
            )

    actions = await service.get_actions(symbol.upper(), start, end, action_types=action_types)

    return CorporateActionsResponse(
        symbol=symbol.upper(),
        actions=[a.to_dict() for a in actions],
    )


@router.get("/{symbol}/splits")
async def get_splits(
    symbol: str,
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
) -> CorporateActionsResponse:
    """Get only stock splits for a symbol."""
    service = get_corporate_actions_service()
    actions = await service.get_splits(symbol.upper(), start, end)

    return CorporateActionsResponse(
        symbol=symbol.upper(),
        actions=[a.to_dict() for a in actions],
    )


@router.get("/{symbol}/dividends")
async def get_dividends(
    symbol: str,
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
) -> CorporateActionsResponse:
    """Get only dividends for a symbol."""
    service = get_corporate_actions_service()
    actions = await service.get_dividends(symbol.upper(), start, end)

    return CorporateActionsResponse(
        symbol=symbol.upper(),
        actions=[a.to_dict() for a in actions],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Adjustment Factors Endpoints
# ─────────────────────────────────────────────────────────────────────────────


@adjustments_router.get("/{symbol}", response_model=AdjustmentFactorsResponse)
async def get_adjustment_factors(
    symbol: str,
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
) -> AdjustmentFactorsResponse:
    """Get adjustment factors for a symbol.

    Returns cumulative adjustment factors based on splits and dividends,
    enabling proper point-in-time (PIT) price adjustments for backtesting.
    """
    if end < start:
        raise HTTPException(
            status_code=400,
            detail={"code": "GW-E8003", "message": "End date must be after start date"},
        )

    service = get_adjustment_service()
    factors = await service.get_factors(symbol.upper(), start, end)

    return AdjustmentFactorsResponse(
        symbol=symbol.upper(),
        factors=[f.to_dict() for f in factors],
    )


@adjustments_router.post("/adjust-prices", response_model=AdjustPricesResponse)
async def adjust_prices(
    request: AdjustPricesRequest,
) -> AdjustPricesResponse:
    """Adjust prices using factor history.

    Takes a list of price/date pairs and returns adjusted prices
    using the cumulative adjustment factor for each date.
    """
    service = get_adjustment_service()

    # Get date range from prices
    dates = [date.fromisoformat(p["date"]) for p in request.prices]
    start = min(dates)
    end = max(dates)

    # Get factors
    factors = await service.get_factors(request.symbol.upper(), start, end)

    # Adjust each price
    adjusted = []
    for p in request.prices:
        from decimal import Decimal

        price_date = date.fromisoformat(p["date"])
        original = Decimal(str(p["price"]))
        adjusted_price = service.apply_adjustment(original, price_date, factors)
        adjusted.append(
            {
                "date": p["date"],
                "original_price": float(original),
                "adjusted_price": float(adjusted_price),
            }
        )

    return AdjustPricesResponse(
        symbol=request.symbol.upper(),
        adjusted_prices=adjusted,
    )
