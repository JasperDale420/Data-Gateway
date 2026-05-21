"""Trading Calendar Service.

Provides market hours, trading days, holidays, and earnings calendar.
Uses exchange_calendars (NYSE/XNYS) for dynamic holiday/early-close data
with a hardcoded fallback for 2024-2026 if the library is unavailable.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

from gateway.config import get_settings
from gateway.core.logger import logger

# ─────────────────────────────────────────────────────────────────────────────
# exchange_calendars integration (optional — graceful degradation)
# ─────────────────────────────────────────────────────────────────────────────

try:
    import exchange_calendars as xcals
    import pandas as pd

    _XCALS_AVAILABLE = True
except ImportError:
    _XCALS_AVAILABLE = False
    logger.warning("exchange_calendars_unavailable", msg="Falling back to hardcoded holiday calendar (2024-2026 only)")

# Module-level cached NYSE calendar instance (created once on first access)
_nyse_cal: Any | None = None


def _get_nyse_calendar() -> Any | None:
    """Return the cached NYSE exchange calendar, or None if unavailable.

    Loaded with an explicit forward window of [today - 5 years, today + 10 years]
    so queries about future dates (earnings calendars, expiry dates,
    multi-year backtests) don't fall through to the hardcoded fallback. The
    underlying exchange_calendars package's holiday rules can compute arbitrary
    dates — the start/end here just bound the cached session index. Default
    range without these args is only 1 year forward, which produced
    out-of-range results for ~all 2027+ queries on a gateway running today.
    """
    global _nyse_cal
    if not _XCALS_AVAILABLE:
        return None
    if _nyse_cal is None:
        try:
            today = date.today()
            start = pd.Timestamp(date(today.year - 5, 1, 1))
            end = pd.Timestamp(date(today.year + 10, 12, 31))
            _nyse_cal = xcals.get_calendar("XNYS", start=start, end=end)
            logger.info(
                "exchange_calendar_loaded",
                exchange="XNYS",
                first_session=str(_nyse_cal.first_session.date()),
                last_session=str(_nyse_cal.last_session.date()),
            )
        except Exception:
            logger.exception("exchange_calendar_load_failed", exchange="XNYS")
            return None
    return _nyse_cal


def _xcals_is_holiday(query_date: date) -> tuple[bool, str | None]:
    """Check if a date is a NYSE holiday using exchange_calendars.

    Returns (is_holiday, holiday_name_or_none).
    """
    cal = _get_nyse_calendar()
    if cal is None:
        return False, None

    ts = pd.Timestamp(query_date)

    # Out-of-range dates fall back to hardcoded dict
    if ts < cal.first_session or ts > cal.last_session:
        return False, None

    if cal.is_session(ts):
        return False, None

    # It's not a session day — determine holiday name from the adhoc/regular holidays
    name = _resolve_holiday_name(cal, ts)
    return True, name


# Normalize exchange_calendars holiday names to match Empire's canonical names.
_XCALS_NAME_ALIASES: dict[str, str] = {
    "Dr. Martin Luther King Jr. Day": "Martin Luther King Jr. Day",
}


def _resolve_holiday_name(cal: Any, ts: Any) -> str:
    """Best-effort holiday name resolution from the exchange calendar."""
    # exchange_calendars stores holidays in .regular_holidays (a HolidayCalendar)
    # which exposes its individual Holiday rules via .rules
    try:
        rules = getattr(cal.regular_holidays, "rules", None)
        if rules is not None:
            for holiday_rule in rules:
                dates = holiday_rule.dates(ts - pd.Timedelta(days=1), ts + pd.Timedelta(days=1))
                if ts in dates:
                    name = str(holiday_rule.name)
                    return _XCALS_NAME_ALIASES.get(name, name)
        for holiday_rule in getattr(cal, "adhoc_holidays", []):
            if hasattr(holiday_rule, "__iter__") and ts in holiday_rule:
                return "Ad-hoc Holiday"
    except Exception:
        pass
    return "Market Holiday"


def _xcals_is_early_close(query_date: date) -> tuple[bool, str | None]:
    """Check if a date is an NYSE early close using exchange_calendars.

    Returns (is_early_close, reason_or_none).
    """
    cal = _get_nyse_calendar()
    if cal is None:
        return False, None

    ts = pd.Timestamp(query_date)

    if ts < cal.first_session or ts > cal.last_session:
        return False, None

    if not cal.is_session(ts):
        return False, None

    close_time = cal.session_close(ts)
    # NYSE regular close is 16:00 ET. Early closes are typically 13:00 ET.
    et = ZoneInfo("America/New_York")
    close_local = close_time.to_pydatetime().astimezone(et)
    if close_local.hour < 16:
        return True, "Early Close"

    return False, None


def _xcals_is_session(query_date: date) -> bool | None:
    """Check if a date is a trading session. Returns None if xcals unavailable or out of range."""
    cal = _get_nyse_calendar()
    if cal is None:
        return None

    ts = pd.Timestamp(query_date)
    if ts < cal.first_session or ts > cal.last_session:
        return None

    return bool(cal.is_session(ts))


# ─────────────────────────────────────────────────────────────────────────────
# Market Session Types
# ─────────────────────────────────────────────────────────────────────────────


class MarketStatus(str, Enum):
    """Market status for a given date."""

    OPEN = "open"
    CLOSED = "closed"
    EARLY_CLOSE = "early_close"


class EarningsTime(str, Enum):
    """When earnings are announced."""

    BEFORE_OPEN = "before_open"
    AFTER_CLOSE = "after_close"
    DURING_MARKET = "during_market"
    UNKNOWN = "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class MarketSession:
    """Trading session times."""

    start: time
    end: time

    def to_dict(self) -> dict[str, str]:
        return {
            "start": self.start.strftime("%H:%M"),
            "end": self.end.strftime("%H:%M"),
        }


@dataclass
class MarketHours:
    """Market hours for a trading day."""

    date: date
    market: str
    status: MarketStatus
    premarket: MarketSession | None
    regular: MarketSession | None
    afterhours: MarketSession | None
    timezone: str
    is_holiday: bool
    is_early_close: bool
    holiday_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "date": self.date.isoformat(),
            "market": self.market,
            "status": self.status.value,
            "timezone": self.timezone,
            "is_holiday": self.is_holiday,
            "is_early_close": self.is_early_close,
        }

        if self.premarket or self.regular or self.afterhours:
            result["sessions"] = {}
            if self.premarket:
                result["sessions"]["premarket"] = self.premarket.to_dict()
            if self.regular:
                result["sessions"]["regular"] = self.regular.to_dict()
            if self.afterhours:
                result["sessions"]["afterhours"] = self.afterhours.to_dict()

        if self.holiday_name:
            result["holiday_name"] = self.holiday_name

        return result


@dataclass
class Holiday:
    """Market holiday."""

    date: date
    name: str

    def to_dict(self) -> dict[str, str]:
        return {
            "date": self.date.isoformat(),
            "name": self.name,
        }


@dataclass
class EarlyClose:
    """Early market close."""

    date: date
    close_time: time
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "date": self.date.isoformat(),
            "close_time": self.close_time.strftime("%H:%M"),
            "reason": self.reason,
        }


@dataclass
class EarningsEvent:
    """Earnings announcement."""

    symbol: str
    date: date
    time: EarningsTime
    eps_estimate: float | None = None
    revenue_estimate: int | None = None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "symbol": self.symbol,
            "date": self.date.isoformat(),
            "time": self.time.value,
        }
        if self.eps_estimate is not None:
            result["eps_estimate"] = self.eps_estimate
        if self.revenue_estimate is not None:
            result["revenue_estimate"] = self.revenue_estimate
        return result


# ─────────────────────────────────────────────────────────────────────────────
# US Market Calendar — hardcoded fallback (used only when exchange_calendars
# is unavailable or the requested date is outside its supported range)
# ─────────────────────────────────────────────────────────────────────────────

# 2024-2026 NYSE holidays (fallback)
US_HOLIDAYS: dict[date, str] = {
    # 2024
    date(2024, 1, 1): "New Year's Day",
    date(2024, 1, 15): "Martin Luther King Jr. Day",
    date(2024, 2, 19): "Presidents' Day",
    date(2024, 3, 29): "Good Friday",
    date(2024, 5, 27): "Memorial Day",
    date(2024, 6, 19): "Juneteenth",
    date(2024, 7, 4): "Independence Day",
    date(2024, 9, 2): "Labor Day",
    date(2024, 11, 28): "Thanksgiving Day",
    date(2024, 12, 25): "Christmas Day",
    # 2025
    date(2025, 1, 1): "New Year's Day",
    date(2025, 1, 20): "Martin Luther King Jr. Day",
    date(2025, 2, 17): "Presidents' Day",
    date(2025, 4, 18): "Good Friday",
    date(2025, 5, 26): "Memorial Day",
    date(2025, 6, 19): "Juneteenth",
    date(2025, 7, 4): "Independence Day",
    date(2025, 9, 1): "Labor Day",
    date(2025, 11, 27): "Thanksgiving Day",
    date(2025, 12, 25): "Christmas Day",
    # 2026
    date(2026, 1, 1): "New Year's Day",
    date(2026, 1, 19): "Martin Luther King Jr. Day",
    date(2026, 2, 16): "Presidents' Day",
    date(2026, 4, 3): "Good Friday",
    date(2026, 5, 25): "Memorial Day",
    date(2026, 6, 19): "Juneteenth",
    date(2026, 7, 3): "Independence Day (observed)",
    date(2026, 9, 7): "Labor Day",
    date(2026, 11, 26): "Thanksgiving Day",
    date(2026, 12, 25): "Christmas Day",
}

# Early closes (1pm ET) (fallback)
US_EARLY_CLOSES: dict[date, str] = {
    # 2024
    date(2024, 7, 3): "Independence Day Eve",
    date(2024, 11, 29): "Day After Thanksgiving",
    date(2024, 12, 24): "Christmas Eve",
    # 2025
    date(2025, 7, 3): "Independence Day Eve",
    date(2025, 11, 28): "Day After Thanksgiving",
    date(2025, 12, 24): "Christmas Eve",
    # 2026
    date(2026, 11, 27): "Day After Thanksgiving",
    date(2026, 12, 24): "Christmas Eve",
}


# ─────────────────────────────────────────────────────────────────────────────
# Trading Calendar Service
# ─────────────────────────────────────────────────────────────────────────────


class TradingCalendar:
    """Trading calendar service for market hours and trading days."""

    # Standard US market sessions (Eastern Time)
    PREMARKET_START = time(4, 0)
    PREMARKET_END = time(9, 30)
    REGULAR_START = time(9, 30)
    REGULAR_END = time(16, 0)
    REGULAR_EARLY_END = time(13, 0)
    AFTERHOURS_START = time(16, 0)
    AFTERHOURS_END = time(20, 0)
    MAX_TRADING_RANGE_DAYS = 3660

    def __init__(self, market: str = "NYSE") -> None:
        self.market = market
        self.timezone = "America/New_York"
        self._tz = ZoneInfo(self.timezone)

    # ── internal helpers for holiday / early-close resolution ──────────

    def _is_holiday(self, query_date: date) -> tuple[bool, str | None]:
        """Return (is_holiday, name) using exchange_calendars first, then fallback."""
        xcals_holiday, xcals_name = _xcals_is_holiday(query_date)
        if xcals_holiday:
            return True, xcals_name
        # If xcals had a definitive answer (date in range), trust it
        xcals_session = _xcals_is_session(query_date)
        if xcals_session is not None:
            return False, None
        # Fallback to hardcoded dict (xcals unavailable or date out of range)
        if query_date in US_HOLIDAYS:
            return True, US_HOLIDAYS[query_date]
        return False, None

    def _is_early_close(self, query_date: date) -> tuple[bool, str | None]:
        """Return (is_early_close, reason) using exchange_calendars first, then fallback."""
        xcals_early, xcals_reason = _xcals_is_early_close(query_date)
        if xcals_early:
            return True, xcals_reason
        # If xcals had a definitive answer for this date, trust it
        xcals_session = _xcals_is_session(query_date)
        if xcals_session is not None:
            return False, None
        # Fallback to hardcoded dict
        if query_date in US_EARLY_CLOSES:
            return True, US_EARLY_CLOSES[query_date]
        return False, None

    # ── public interface ─────────────────────────────────────────────

    def get_market_hours(self, query_date: date) -> MarketHours:
        """Get market hours for a specific date."""
        # Check if weekend
        if query_date.weekday() >= 5:
            return MarketHours(
                date=query_date,
                market=self.market,
                status=MarketStatus.CLOSED,
                premarket=None,
                regular=None,
                afterhours=None,
                timezone=self.timezone,
                is_holiday=False,
                is_early_close=False,
            )

        # Check if holiday (dynamic via exchange_calendars, fallback to hardcoded)
        is_holiday, holiday_name = self._is_holiday(query_date)
        if is_holiday:
            return MarketHours(
                date=query_date,
                market=self.market,
                status=MarketStatus.CLOSED,
                premarket=None,
                regular=None,
                afterhours=None,
                timezone=self.timezone,
                is_holiday=True,
                is_early_close=False,
                holiday_name=holiday_name,
            )

        # Check if early close (dynamic via exchange_calendars, fallback to hardcoded)
        is_early, _reason = self._is_early_close(query_date)
        regular_end = self.REGULAR_EARLY_END if is_early else self.REGULAR_END
        afterhours_start = self.REGULAR_EARLY_END if is_early else self.AFTERHOURS_START

        return MarketHours(
            date=query_date,
            market=self.market,
            status=MarketStatus.EARLY_CLOSE if is_early else MarketStatus.OPEN,
            premarket=MarketSession(self.PREMARKET_START, self.PREMARKET_END),
            regular=MarketSession(self.REGULAR_START, regular_end),
            afterhours=MarketSession(afterhours_start, self.AFTERHOURS_END),
            timezone=self.timezone,
            is_holiday=False,
            is_early_close=is_early,
        )

    def get_trading_days(
        self,
        start: date,
        end: date,
    ) -> tuple[list[date], list[Holiday], list[EarlyClose]]:
        """Get trading days in a date range.

        Returns:
            Tuple of (trading_days, holidays, early_closes)
        """
        if end < start:
            return [], [], []

        date_span_days = (end - start).days
        if date_span_days > self.MAX_TRADING_RANGE_DAYS:
            raise ValueError(f"Date range cannot exceed {self.MAX_TRADING_RANGE_DAYS} days")

        trading_days = []
        holidays = []
        early_closes = []

        current = start
        while current <= end:
            if current.weekday() < 5:  # Weekday
                is_holiday, holiday_name = self._is_holiday(current)
                if is_holiday:
                    holidays.append(
                        Holiday(
                            date=current,
                            name=holiday_name or "Market Holiday",
                        )
                    )
                else:
                    trading_days.append(current)

                    is_early, early_reason = self._is_early_close(current)
                    if is_early:
                        early_closes.append(
                            EarlyClose(
                                date=current,
                                close_time=self.REGULAR_EARLY_END,
                                reason=early_reason or "Early Close",
                            )
                        )

            # Move to next day
            current += timedelta(days=1)

            # Safety limit
            if len(trading_days) > 1000:
                break

        return trading_days, holidays, early_closes

    def is_trading_day(self, query_date: date) -> bool:
        """Check if a date is a trading day."""
        if query_date.weekday() >= 5:
            return False
        is_holiday, _ = self._is_holiday(query_date)
        return not is_holiday

    def is_market_open(self, dt: datetime | None = None) -> bool:
        """Check if market is currently open."""
        if dt is None:
            dt = datetime.now(UTC)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)

        local_dt = dt.astimezone(self._tz)
        query_date = local_dt.date()

        if not self.is_trading_day(query_date):
            return False

        current_time = local_dt.time()

        is_early, _ = self._is_early_close(query_date)
        if is_early:
            return self.REGULAR_START <= current_time < self.REGULAR_EARLY_END

        return self.REGULAR_START <= current_time < self.REGULAR_END

    def today(self) -> date:
        """Return today's date in the calendar's timezone (ET).

        Use this instead of ``datetime.date.today()`` whenever the date will
        be passed to other calendar methods. ``date.today()`` returns a
        host-local date, which produces an off-by-one when the host runs in
        UTC and the wall clock is past 8pm ET (UTC date is already tomorrow
        in ET terms).
        """
        return datetime.now(self._tz).date()

    def next_trading_day(self, from_date: date | None = None) -> date:
        """Get the next trading day.

        ``from_date`` defaults to today in the calendar's own timezone (ET),
        via :meth:`today`.
        """
        if from_date is None:
            from_date = self.today()

        current = from_date
        for _ in range(10):  # Max 10 days lookahead
            current += timedelta(days=1)

            if self.is_trading_day(current):
                return current

        return current


# ─────────────────────────────────────────────────────────────────────────────
# Earnings Calendar (Stub - would integrate with provider)
# ─────────────────────────────────────────────────────────────────────────────


class EarningsCalendar:
    """Earnings calendar service."""

    def __init__(self) -> None:
        # In production, this would fetch from Alpaca or other provider
        self._cache: dict[str, list[EarningsEvent]] = {}
        self._fetch_earnings_func: Callable[[list[str], date, date], Awaitable[list[EarningsEvent]]] | None = None

    def set_fetcher(
        self,
        func: Callable[[list[str], date, date], Awaitable[list[EarningsEvent]]],
    ) -> None:
        """Set the function used to fetch earnings events."""
        self._fetch_earnings_func = func

    def has_fetcher(self) -> bool:
        """Check whether an earnings fetcher is configured."""
        return self._fetch_earnings_func is not None

    def get_earnings(
        self,
        symbols: list[str],
        start: date,
        end: date,
    ) -> list[EarningsEvent]:
        """Get earnings events for symbols in date range.

        Note: This is a stub. In production, integrate with data provider.
        """
        if self._fetch_earnings_func:
            raise RuntimeError("Async fetcher configured; use fetch_earnings")

        settings = get_settings()
        if not settings.allow_stub_data:
            raise RuntimeError("Earnings calendar fetcher not configured")

        # Stub mode: return empty
        return []

    async def fetch_earnings(
        self,
        symbols: list[str],
        start: date,
        end: date,
    ) -> list[EarningsEvent]:
        """Async fetch earnings (for provider integration)."""
        if self._fetch_earnings_func:
            return await self._fetch_earnings_func(symbols, start, end)
        return self.get_earnings(symbols, start, end)


# ─────────────────────────────────────────────────────────────────────────────
# Singletons
# ─────────────────────────────────────────────────────────────────────────────

_calendar: TradingCalendar | None = None
_earnings: EarningsCalendar | None = None


def get_trading_calendar() -> TradingCalendar:
    """Get the singleton trading calendar."""
    global _calendar
    if _calendar is None:
        _calendar = TradingCalendar()
    return _calendar


def get_earnings_calendar() -> EarningsCalendar:
    """Get the singleton earnings calendar."""
    global _earnings
    if _earnings is None:
        _earnings = EarningsCalendar()
    return _earnings
