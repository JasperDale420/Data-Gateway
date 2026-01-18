"""Trading Calendar API endpoints.

Implements market hours, trading days, and earnings calendar as specified in PRD.
"""

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from gateway.api.deps import require_api_key
from gateway.core.calendar import (
    get_earnings_calendar,
    get_trading_calendar,
)

router = APIRouter(prefix="/api/v1/calendar", tags=["Trading Calendar"])


# ─────────────────────────────────────────────────────────────────────────────
# Response Models
# ─────────────────────────────────────────────────────────────────────────────


class MarketHoursResponse(BaseModel):
    """Response for market hours endpoint."""

    date: str
    market: str
    status: str
    sessions: dict[str, dict[str, str]] | None = None
    timezone: str
    is_holiday: bool
    is_early_close: bool
    holiday_name: str | None = None


class TradingDaysResponse(BaseModel):
    """Response for trading days endpoint."""

    trading_days: list[str]
    holidays: list[dict[str, str]]
    early_closes: list[dict[str, str]]


class EarningsResponse(BaseModel):
    """Response for earnings endpoint."""

    earnings: list[dict[str, Any]]


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/market-hours",
    response_model=MarketHoursResponse,
    summary="Get market hours",
    description="Get market hours for a specific date including pre-market, regular, and after-hours sessions.",
)
async def get_market_hours(
    date_str: str = Query(
        alias="date",
        description="Date in YYYY-MM-DD format",
        examples=["2024-01-15"],
    ),
    client: Any = Depends(require_api_key),
) -> MarketHoursResponse:
    """Get market hours for a specific date."""
    try:
        query_date = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format: {date_str}. Use YYYY-MM-DD.",
        )

    calendar = get_trading_calendar()
    hours = calendar.get_market_hours(query_date)

    return MarketHoursResponse(**hours.to_dict())


@router.get(
    "/trading-days",
    response_model=TradingDaysResponse,
    summary="Get trading days",
    description="Get trading days in a date range, including holidays and early closes.",
)
async def get_trading_days(
    start: str = Query(
        description="Start date in YYYY-MM-DD format",
        examples=["2024-01-01"],
    ),
    end: str = Query(
        description="End date in YYYY-MM-DD format",
        examples=["2024-01-31"],
    ),
    client: Any = Depends(require_api_key),
) -> TradingDaysResponse:
    """Get trading days in a date range."""
    try:
        start_date = date.fromisoformat(start)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid start date: {start}")

    try:
        end_date = date.fromisoformat(end)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid end date: {end}")

    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start must be before end")

    # Limit range to 1 year
    days_diff = (end_date - start_date).days
    if days_diff > 366:
        raise HTTPException(status_code=400, detail="Date range cannot exceed 1 year")

    calendar = get_trading_calendar()
    trading_days, holidays, early_closes = calendar.get_trading_days(start_date, end_date)

    return TradingDaysResponse(
        trading_days=[d.isoformat() for d in trading_days],
        holidays=[h.to_dict() for h in holidays],
        early_closes=[e.to_dict() for e in early_closes],
    )


@router.get(
    "/earnings",
    response_model=EarningsResponse,
    summary="Get earnings calendar",
    description="Get earnings announcements for symbols in a date range.",
)
async def get_earnings(
    symbols: str = Query(
        description="Comma-separated list of symbols",
        examples=["AAPL,MSFT,GOOGL"],
    ),
    start: str = Query(
        description="Start date in YYYY-MM-DD format",
        examples=["2024-01-01"],
    ),
    end: str = Query(
        description="End date in YYYY-MM-DD format",
        examples=["2024-03-31"],
    ),
    client: Any = Depends(require_api_key),
) -> EarningsResponse:
    """Get earnings calendar for symbols."""
    try:
        start_date = date.fromisoformat(start)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid start date: {start}")

    try:
        end_date = date.fromisoformat(end)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid end date: {end}")

    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]

    if not symbol_list:
        raise HTTPException(status_code=400, detail="At least one symbol required")

    if len(symbol_list) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 symbols allowed")

    earnings_calendar = get_earnings_calendar()
    events = await earnings_calendar.fetch_earnings(symbol_list, start_date, end_date)

    return EarningsResponse(
        earnings=[e.to_dict() for e in events],
    )


@router.get(
    "/is-open",
    summary="Check if market is open",
    description="Check if the market is currently open for trading.",
)
async def is_market_open(
    client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Check if market is currently open."""
    calendar = get_trading_calendar()
    is_open = calendar.is_market_open()
    today = date.today()
    hours = calendar.get_market_hours(today)

    return {
        "is_open": is_open,
        "date": today.isoformat(),
        "status": hours.status.value,
        "is_holiday": hours.is_holiday,
        "is_early_close": hours.is_early_close,
        "market": calendar.market,
        "timezone": calendar.timezone,
    }


@router.get(
    "/next-trading-day",
    summary="Get next trading day",
    description="Get the next trading day from today or a specified date.",
)
async def next_trading_day(
    from_date: str | None = Query(
        default=None,
        alias="from",
        description="Start date (defaults to today)",
    ),
    client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Get next trading day."""
    calendar = get_trading_calendar()

    if from_date:
        try:
            query_date = date.fromisoformat(from_date)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid date: {from_date}")
    else:
        query_date = date.today()

    next_day = calendar.next_trading_day(query_date)
    hours = calendar.get_market_hours(next_day)

    return {
        "next_trading_day": next_day.isoformat(),
        "from": query_date.isoformat(),
        "status": hours.status.value,
        "is_early_close": hours.is_early_close,
    }
