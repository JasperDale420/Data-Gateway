"""Corporate Actions and Adjustment Factors API endpoints.

Provides corporate action history and adjustment factors
as specified in PRD (lines 1126-1204).
"""

from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from gateway.api.deps import get_registry, require_api_key, require_provider_rate_limit
from gateway.config import get_settings
from gateway.core.adjustments import get_adjustment_service
from gateway.core.corporate_actions import (
    ActionType,
    CorporateAction,
    get_corporate_actions_service,
)
from gateway.core.registry import ProviderRegistry
from gateway.types.provider_protocols import SupportsAlpacaCorporateActions

base_router = APIRouter()
base_adjustments_router = APIRouter()
router = APIRouter(
    prefix="/api/v1/corporate-actions",
    tags=["corporate-actions"],
    dependencies=[Depends(require_api_key)],
)
legacy_router = APIRouter(
    prefix="/corporate-actions",
    tags=["corporate-actions"],
    include_in_schema=False,
    dependencies=[Depends(require_api_key)],
)
adjustments_router = APIRouter(
    prefix="/api/v1/adjustment-factors",
    tags=["adjustment-factors"],
    dependencies=[Depends(require_api_key)],
)
legacy_adjustments_router = APIRouter(
    prefix="/adjustment-factors",
    tags=["adjustment-factors"],
    include_in_schema=False,
    dependencies=[Depends(require_api_key)],
)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    # nosemgrep: empire-no-return-none-for-failure -- documented Optional parse helper: unparseable provider date yields None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _parse_ratio(value: str | int | float | Decimal | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float | Decimal):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if isinstance(value, str):
        if ":" in value:
            parts = value.split(":")
            if len(parts) == 2:
                try:
                    num = float(parts[0])
                    den = float(parts[1])
                    return num / den if den else None
                except (TypeError, ValueError):
                    return None
        # nosemgrep: empire-no-return-none-for-failure -- documented Optional parse helper: unparseable provider ratio yields None
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _configure_corporate_fetcher(
    registry: ProviderRegistry,
) -> None:
    service = get_corporate_actions_service()
    provider = cast(SupportsAlpacaCorporateActions | None, registry.get("alpaca"))
    if provider:
        type_map = {
            ActionType.DIVIDEND: "dividends",
            ActionType.SPLIT: "splits",
            ActionType.SPINOFF: "spinoffs",
            ActionType.MERGER: "mergers",
        }

        async def _fetch(
            symbol: str,
            start: date,
            end: date,
            action_types: list[ActionType] | None,
        ) -> list[CorporateAction]:
            await require_provider_rate_limit("alpaca")
            types = None
            if action_types:
                mapped = [type_map[t] for t in action_types if t in type_map]
                types = mapped or None

            start_dt = datetime.combine(start, time.min, tzinfo=UTC)
            end_dt = datetime.combine(end, time.max, tzinfo=UTC)
            normalized = await provider.get_corporate_actions(
                symbols=[symbol],
                types=types,
                start=start_dt,
                end=end_dt,
            )

            actions: list[CorporateAction] = []
            for item in normalized:
                raw_type = getattr(item, "action_type", None)
                if not raw_type:
                    continue
                try:
                    action_type = ActionType(str(raw_type))
                except ValueError:
                    continue
                if action_types and action_type not in action_types:
                    continue

                ex_date = _parse_date(getattr(item, "ex_date", None))
                if not ex_date:
                    continue

                action = CorporateAction(
                    type=action_type,
                    ex_date=ex_date,
                    record_date=_parse_date(getattr(item, "record_date", None)),
                    pay_date=_parse_date(getattr(item, "payable_date", None)),
                )

                if action_type == ActionType.DIVIDEND:
                    amount = getattr(item, "amount", None)
                    if amount is not None:
                        action.amount = Decimal(str(amount))
                elif action_type == ActionType.SPLIT:
                    action.ratio = _parse_ratio(getattr(item, "ratio", None))

                actions.append(action)

            return actions

        if not service.has_fetcher():
            service.set_fetcher(_fetch)
        return

    settings = get_settings()
    if not settings.allow_stub_data:
        raise HTTPException(status_code=503, detail="Alpaca provider not available")


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


@base_router.get("/{symbol}", response_model=CorporateActionsResponse)
async def get_corporate_actions(
    symbol: str,
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
    action_type: str | None = Query(
        None,
        description="Filter by action type (split, dividend, merger, spinoff)",
    ),
    registry: ProviderRegistry = Depends(get_registry),
) -> CorporateActionsResponse:
    """Get corporate actions for a symbol.

    Returns splits, dividends, mergers, spinoffs, and other corporate events
    within the specified date range.
    """
    _configure_corporate_fetcher(registry)
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


@base_router.get("/{symbol}/splits")
async def get_splits(
    symbol: str,
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
    registry: ProviderRegistry = Depends(get_registry),
) -> CorporateActionsResponse:
    """Get only stock splits for a symbol."""
    _configure_corporate_fetcher(registry)
    service = get_corporate_actions_service()
    actions = await service.get_splits(symbol.upper(), start, end)

    return CorporateActionsResponse(
        symbol=symbol.upper(),
        actions=[a.to_dict() for a in actions],
    )


@base_router.get("/{symbol}/dividends")
async def get_dividends(
    symbol: str,
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
    registry: ProviderRegistry = Depends(get_registry),
) -> CorporateActionsResponse:
    """Get only dividends for a symbol."""
    _configure_corporate_fetcher(registry)
    service = get_corporate_actions_service()
    actions = await service.get_dividends(symbol.upper(), start, end)

    return CorporateActionsResponse(
        symbol=symbol.upper(),
        actions=[a.to_dict() for a in actions],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Adjustment Factors Endpoints
# ─────────────────────────────────────────────────────────────────────────────


@base_adjustments_router.get("/{symbol}", response_model=AdjustmentFactorsResponse)
async def get_adjustment_factors(
    symbol: str,
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
    registry: ProviderRegistry = Depends(get_registry),
) -> AdjustmentFactorsResponse:
    """Get adjustment factors for a symbol.

    Returns cumulative adjustment factors based on splits and dividends,
    enabling proper point-in-time (PIT) price adjustments for backtesting.
    """
    _configure_corporate_fetcher(registry)
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


@base_adjustments_router.post("/adjust-prices", response_model=AdjustPricesResponse)
async def adjust_prices(
    request: AdjustPricesRequest,
    registry: ProviderRegistry = Depends(get_registry),
) -> AdjustPricesResponse:
    """Adjust prices using factor history.

    Takes a list of price/date pairs and returns adjusted prices
    using the cumulative adjustment factor for each date.
    """
    _configure_corporate_fetcher(registry)
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


router.include_router(base_router)
legacy_router.include_router(base_router)
adjustments_router.include_router(base_adjustments_router)
legacy_adjustments_router.include_router(base_adjustments_router)
