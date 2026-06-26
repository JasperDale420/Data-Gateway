"""Trading Calendar API endpoints.

Implements market hours, trading days, and earnings calendar as specified in PRD.
"""

import asyncio
import time as time_module
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, cast

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from gateway.api.deps import get_registry, require_api_key, require_provider_rate_limit
from gateway.config import get_settings
from gateway.core.calendar import (
    EarlyClose,
    EarningsEvent,
    EarningsTime,
    Holiday,
    MarketHours,
    MarketSession,
    MarketStatus,
    get_earnings_calendar,
    get_trading_calendar,
)
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse
from gateway.types.provider_protocols import (
    SupportsAlpacaCalendar,
    SupportsFinnhubEarningsCalendar,
)

router = APIRouter(prefix="/api/v1/calendar", tags=["Trading Calendar"])
from gateway.core.logger import logger

_CALENDAR_PROVIDER_DEGRADE_WINDOW_SECONDS = 30.0
_CALENDAR_PROVIDER_DEGRADED_UNTIL: dict[str, float] = {}
_CALENDAR_PROVIDER_LAST_LOG_AT: dict[str, float] = {}


def _parse_alpaca_time(value: str | None) -> time | None:
    if not value:
        return None
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            continue
    return None


def _calendar_provider_is_degraded(route_key: str, now_monotonic: float | None = None) -> bool:
    now = now_monotonic if now_monotonic is not None else time_module.monotonic()
    degraded_until = _CALENDAR_PROVIDER_DEGRADED_UNTIL.get(route_key, 0.0)
    return now < degraded_until


def _mark_calendar_provider_failure(
    route_key: str,
    error: Exception,
    now_monotonic: float | None = None,
) -> None:
    now = now_monotonic if now_monotonic is not None else time_module.monotonic()
    _CALENDAR_PROVIDER_DEGRADED_UNTIL[route_key] = now + _CALENDAR_PROVIDER_DEGRADE_WINDOW_SECONDS

    last_log = _CALENDAR_PROVIDER_LAST_LOG_AT.get(route_key, 0.0)
    if last_log <= 0 or (now - last_log) >= _CALENDAR_PROVIDER_DEGRADE_WINDOW_SECONDS:
        _CALENDAR_PROVIDER_LAST_LOG_AT[route_key] = now
        logger.warning(
            "calendar_provider_fallback_degraded",
            route=route_key,
            cooldown_seconds=_CALENDAR_PROVIDER_DEGRADE_WINDOW_SECONDS,
            error=str(error),
        )


def _mark_calendar_provider_success(route_key: str) -> None:
    _CALENDAR_PROVIDER_DEGRADED_UNTIL.pop(route_key, None)
    _CALENDAR_PROVIDER_LAST_LOG_AT.pop(route_key, None)


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
    registry: ProviderRegistry = Depends(get_registry),
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
    provider = cast(SupportsAlpacaCalendar | None, registry.get("alpaca"))

    if provider and not _calendar_provider_is_degraded("market_hours"):
        try:
            await require_provider_rate_limit("alpaca")
            entries = await asyncio.to_thread(provider.get_calendar, query_date, query_date)
            if entries:
                entry = entries[0]
                open_time = _parse_alpaca_time(entry.get("open"))
                close_time = _parse_alpaca_time(entry.get("close"))
                is_early = bool(close_time and close_time < calendar.REGULAR_END)
                regular_start = open_time or calendar.REGULAR_START
                regular_end = close_time or calendar.REGULAR_END
                afterhours_start = calendar.REGULAR_EARLY_END if is_early else calendar.AFTERHOURS_START

                hours = MarketHours(
                    date=query_date,
                    market=calendar.market,
                    status=MarketStatus.EARLY_CLOSE if is_early else MarketStatus.OPEN,
                    premarket=MarketSession(calendar.PREMARKET_START, calendar.PREMARKET_END),
                    regular=MarketSession(regular_start, regular_end),
                    afterhours=MarketSession(afterhours_start, calendar.AFTERHOURS_END),
                    timezone=calendar.timezone,
                    is_holiday=False,
                    is_early_close=is_early,
                )
                return MarketHoursResponse(**hours.to_dict())

            # No trading day returned by provider
            is_weekend = query_date.weekday() >= 5
            hours = MarketHours(
                date=query_date,
                market=calendar.market,
                status=MarketStatus.CLOSED,
                premarket=None,
                regular=None,
                afterhours=None,
                timezone=calendar.timezone,
                is_holiday=not is_weekend,
                is_early_close=False,
                holiday_name=None,
            )
            return MarketHoursResponse(**hours.to_dict())
        except Exception as exc:
            _mark_calendar_provider_failure("market_hours", exc)
            # Fall back to static calendar

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
    registry: ProviderRegistry = Depends(get_registry),
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
    provider = cast(SupportsAlpacaCalendar | None, registry.get("alpaca"))

    if provider:
        try:
            await require_provider_rate_limit("alpaca")
            entries = await asyncio.to_thread(provider.get_calendar, start_date, end_date)
            trading_days: list[date] = []
            early_closes: list[EarlyClose] = []
            trading_set: set[date] = set()

            for entry in entries:
                day = None
                date_value = entry.get("date")
                if isinstance(date_value, str):
                    try:
                        day = date.fromisoformat(date_value)
                    except ValueError:
                        day = None
                if not day:
                    continue
                trading_days.append(day)
                trading_set.add(day)

                close_time = _parse_alpaca_time(entry.get("close"))
                if close_time and close_time < calendar.REGULAR_END:
                    early_closes.append(
                        EarlyClose(
                            date=day,
                            close_time=close_time,
                            reason="Early close",
                        )
                    )

            holidays: list[Holiday] = []
            current = start_date
            while current <= end_date:
                if current.weekday() < 5 and current not in trading_set:
                    holidays.append(Holiday(date=current, name="Market Holiday"))
                current += timedelta(days=1)

            return TradingDaysResponse(
                trading_days=[d.isoformat() for d in trading_days],
                holidays=[h.to_dict() for h in holidays],
                early_closes=[e.to_dict() for e in early_closes],
            )
        except Exception as e:
            # The live Alpaca calendar is authoritative (incl. early closes).
            # Falling back to the hardcoded static calendar silently would serve
            # trading clients a less-accurate calendar (e.g. wrong early-close
            # days) with zero observability. Log the degradation before falling
            # through.
            logger.warning(
                "calendar_alpaca_fallback",
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                error=str(e),
                exc_info=True,
            )

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
    registry: ProviderRegistry = Depends(get_registry),
) -> EarningsResponse:
    """Get earnings calendar for symbols."""
    settings = get_settings()
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
    provider = cast(SupportsFinnhubEarningsCalendar | None, registry.get("finnhub"))
    if provider:

        async def _fetch_earnings(
            symbols: list[str],
            start_date: date,
            end_date: date,
        ) -> list[EarningsEvent]:
            await require_provider_rate_limit("finnhub")
            start_dt = datetime.combine(start_date, time.min, tzinfo=UTC)
            end_dt = datetime.combine(end_date, time.max, tzinfo=UTC)
            raw_events = await provider.get_earnings_calendar(start=start_dt, end=end_dt)

            symbol_set = {s.upper() for s in symbols}
            hour_map = {
                "bmo": EarningsTime.BEFORE_OPEN,
                "amc": EarningsTime.AFTER_CLOSE,
                "dmh": EarningsTime.DURING_MARKET,
            }
            mapped: list[EarningsEvent] = []

            for item in raw_events:
                symbol = str(item.get("symbol", "")).upper()
                if symbol_set and symbol not in symbol_set:
                    continue
                date_str = item.get("date")
                if not date_str:
                    continue
                try:
                    event_date = date.fromisoformat(date_str)
                except ValueError:
                    continue
                hour = str(item.get("hour", "")).lower()
                mapped.append(
                    EarningsEvent(
                        symbol=symbol,
                        date=event_date,
                        time=hour_map.get(hour, EarningsTime.UNKNOWN),
                        eps_estimate=item.get("eps_estimate"),
                        revenue_estimate=item.get("revenue_estimate"),
                    )
                )

            return mapped

        if not earnings_calendar.has_fetcher():
            earnings_calendar.set_fetcher(_fetch_earnings)
    elif not settings.allow_stub_data:
        raise HTTPException(status_code=503, detail="Finnhub provider not available")

    events = await earnings_calendar.fetch_earnings(symbol_list, start_date, end_date)

    return EarningsResponse(
        earnings=[e.to_dict() for e in events],
    )


@router.get(
    "/is-open",
    response_model=SuccessResponse,
    summary="Check if market is open",
    description="Check if the market is currently open for trading.",
)
async def is_market_open(
    client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Check if market is currently open."""
    calendar = get_trading_calendar()
    is_open = calendar.is_market_open()
    # Use the calendar's TZ (ET) — date.today() is host-local and produces an
    # off-by-one when the host runs in UTC during evening ET hours.
    today = calendar.today()
    hours = calendar.get_market_hours(today)

    return {
        "success": True,
        "data": {
            "is_open": is_open,
            "date": today.isoformat(),
            "status": hours.status.value,
            "is_holiday": hours.is_holiday,
            "is_early_close": hours.is_early_close,
            "market": calendar.market,
            "timezone": calendar.timezone,
        },
    }


@router.get(
    "/next-trading-day",
    response_model=SuccessResponse,
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
        # Use the calendar's TZ (ET) — date.today() is host-local and produces
        # an off-by-one when the host runs in UTC during evening ET hours.
        query_date = calendar.today()

    next_day = calendar.next_trading_day(query_date)
    hours = calendar.get_market_hours(next_day)

    return {
        "success": True,
        "data": {
            "next_trading_day": next_day.isoformat(),
            "from": query_date.isoformat(),
            "status": hours.status.value,
            "is_early_close": hours.is_early_close,
        },
    }
