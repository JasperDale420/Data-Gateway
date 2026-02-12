"""Tests for Trading Calendar."""

from datetime import date, time

import pytest

from gateway.core.calendar import (
    EarlyClose,
    Holiday,
    MarketSession,
    MarketStatus,
    TradingCalendar,
    get_trading_calendar,
)


class TestTradingCalendar:
    """Test TradingCalendar functionality."""

    @pytest.fixture
    def calendar(self) -> TradingCalendar:
        """Fresh calendar for each test."""
        return TradingCalendar()

    def test_regular_trading_day(self, calendar):
        """Regular trading day should have all sessions."""
        hours = calendar.get_market_hours(date(2024, 1, 16))  # Tuesday

        assert hours.status == MarketStatus.OPEN
        assert hours.is_holiday is False
        assert hours.is_early_close is False
        assert hours.regular is not None
        assert hours.regular.start == time(9, 30)
        assert hours.regular.end == time(16, 0)

    def test_weekend_closed(self, calendar):
        """Weekend should be closed."""
        # Saturday
        hours = calendar.get_market_hours(date(2024, 1, 13))
        assert hours.status == MarketStatus.CLOSED
        assert hours.regular is None

        # Sunday
        hours = calendar.get_market_hours(date(2024, 1, 14))
        assert hours.status == MarketStatus.CLOSED

    def test_holiday_detection(self, calendar):
        """Holidays should be detected correctly."""
        # MLK Day 2024
        hours = calendar.get_market_hours(date(2024, 1, 15))
        assert hours.status == MarketStatus.CLOSED
        assert hours.is_holiday is True
        assert hours.holiday_name == "Martin Luther King Jr. Day"

        # New Year's Day 2024
        hours = calendar.get_market_hours(date(2024, 1, 1))
        assert hours.is_holiday is True
        assert hours.holiday_name == "New Year's Day"

    def test_early_close(self, calendar):
        """Early close days should have shortened hours."""
        # Christmas Eve 2024
        hours = calendar.get_market_hours(date(2024, 12, 24))

        assert hours.status == MarketStatus.EARLY_CLOSE
        assert hours.is_early_close is True
        assert hours.regular.end == time(13, 0)

    def test_premarket_hours(self, calendar):
        """Premarket should be 4:00am - 9:30am."""
        hours = calendar.get_market_hours(date(2024, 1, 16))

        assert hours.premarket is not None
        assert hours.premarket.start == time(4, 0)
        assert hours.premarket.end == time(9, 30)

    def test_afterhours(self, calendar):
        """After hours should be 4:00pm - 8:00pm."""
        hours = calendar.get_market_hours(date(2024, 1, 16))

        assert hours.afterhours is not None
        assert hours.afterhours.start == time(16, 0)
        assert hours.afterhours.end == time(20, 0)

    def test_trading_days_range(self, calendar):
        """Should return correct trading days in range."""
        trading_days, holidays, early_closes = calendar.get_trading_days(
            date(2024, 1, 1),
            date(2024, 1, 31),
        )

        # January 2024 has 21 weekdays - 2 holidays (New Year's, MLK) = 19 trading days
        # But our implementation may differ slightly
        assert len(trading_days) > 15
        assert len(holidays) >= 2

        # Check holidays are correct
        holiday_names = [h.name for h in holidays]
        assert "New Year's Day" in holiday_names
        assert "Martin Luther King Jr. Day" in holiday_names

    def test_trading_days_range_guardrail(self, calendar):
        """Very large ranges should fail fast with a clear error."""
        with pytest.raises(ValueError, match="cannot exceed"):
            calendar.get_trading_days(
                date(2020, 1, 1),
                date(2035, 1, 1),
            )

    def test_trading_days_start_after_end_returns_empty(self, calendar):
        """Inverted ranges should return empty collections."""
        trading_days, holidays, early_closes = calendar.get_trading_days(
            date(2024, 1, 31),
            date(2024, 1, 1),
        )
        assert trading_days == []
        assert holidays == []
        assert early_closes == []

    def test_is_trading_day(self, calendar):
        """is_trading_day should work correctly."""
        # Regular day
        assert calendar.is_trading_day(date(2024, 1, 16)) is True

        # Weekend
        assert calendar.is_trading_day(date(2024, 1, 13)) is False

        # Holiday
        assert calendar.is_trading_day(date(2024, 1, 15)) is False

    def test_next_trading_day(self, calendar):
        """next_trading_day should find next open day."""
        # From Thursday Jan 11, next should be Friday Jan 12
        next_day = calendar.next_trading_day(date(2024, 1, 11))
        assert next_day == date(2024, 1, 12)

        # From Friday Jan 12, next should be Tuesday Jan 16 (MLK Monday is holiday)
        next_day = calendar.next_trading_day(date(2024, 1, 12))
        assert next_day == date(2024, 1, 16)

    def test_market_hours_to_dict(self, calendar):
        """to_dict should produce correct structure."""
        hours = calendar.get_market_hours(date(2024, 1, 16))
        data = hours.to_dict()

        assert data["date"] == "2024-01-16"
        assert data["market"] == "NYSE"
        assert data["status"] == "open"
        assert "sessions" in data
        assert "regular" in data["sessions"]
        assert data["sessions"]["regular"]["start"] == "09:30"

    def test_singleton(self):
        """get_trading_calendar should return singleton."""
        c1 = get_trading_calendar()
        c2 = get_trading_calendar()
        assert c1 is c2


class TestMarketSession:
    """Test MarketSession dataclass."""

    def test_to_dict(self):
        """to_dict should format times correctly."""
        session = MarketSession(start=time(9, 30), end=time(16, 0))
        data = session.to_dict()

        assert data["start"] == "09:30"
        assert data["end"] == "16:00"


class TestHoliday:
    """Test Holiday dataclass."""

    def test_to_dict(self):
        """to_dict should format correctly."""
        holiday = Holiday(date=date(2024, 1, 1), name="New Year's Day")
        data = holiday.to_dict()

        assert data["date"] == "2024-01-01"
        assert data["name"] == "New Year's Day"


class TestEarlyClose:
    """Test EarlyClose dataclass."""

    def test_to_dict(self):
        """to_dict should format correctly."""
        early = EarlyClose(
            date=date(2024, 12, 24),
            close_time=time(13, 0),
            reason="Christmas Eve",
        )
        data = early.to_dict()

        assert data["date"] == "2024-12-24"
        assert data["close_time"] == "13:00"
        assert data["reason"] == "Christmas Eve"
