"""Tests for Data Normalizer."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from gateway.core.normalizer import (
    DataNormalizer,
    NormalizedBar,
    get_normalizer,
)


class TestNormalizedBar:
    """Test NormalizedBar dataclass."""

    def test_to_dict(self):
        """to_dict should produce correct structure."""
        bar = NormalizedBar(
            symbol="AAPL",
            timestamp=datetime(2024, 1, 15, 14, 30, tzinfo=UTC),
            open=Decimal("185.50"),
            high=Decimal("186.00"),
            low=Decimal("185.25"),
            close=Decimal("185.75"),
            volume=100000,
            vwap=Decimal("185.60"),
            trade_count=500,
            provider="alpaca",
            timeframe="1Min",
        )

        data = bar.to_dict()

        assert data["symbol"] == "AAPL"
        assert data["open"] == 185.50
        assert data["high"] == 186.00
        assert data["close"] == 185.75
        assert data["volume"] == 100000
        assert data["provider"] == "alpaca"


class TestDataNormalizer:
    """Test DataNormalizer functionality."""

    @pytest.fixture
    def normalizer(self) -> DataNormalizer:
        return DataNormalizer()

    # ─────────────────────────────────────────────────────────────────────
    # Alpaca Bar Normalization
    # ─────────────────────────────────────────────────────────────────────

    def test_normalize_alpaca_bar_short_names(self, normalizer):
        """Alpaca short field names should be normalized."""
        raw = {
            "S": "AAPL",
            "t": "2024-01-15T14:30:00Z",
            "o": 185.50,
            "h": 186.00,
            "l": 185.25,
            "c": 185.75,
            "v": 100000,
            "vw": 185.60,
            "n": 500,
        }

        bar = normalizer.normalize_bar(raw, "alpaca")

        assert bar.symbol == "AAPL"
        assert bar.open == Decimal("185.5")
        assert bar.high == Decimal("186.0")
        assert bar.close == Decimal("185.75")
        assert bar.volume == 100000
        assert bar.vwap == Decimal("185.6")
        assert bar.trade_count == 500
        assert bar.provider == "alpaca"

    def test_normalize_alpaca_bar_long_names(self, normalizer):
        """Alpaca REST format with long names should work."""
        raw = {
            "symbol": "MSFT",
            "timestamp": "2024-01-15T14:30:00Z",
            "open": 380.00,
            "high": 382.50,
            "low": 379.00,
            "close": 381.25,
            "volume": 50000,
            "vwap": 380.50,
        }

        bar = normalizer.normalize_bar(raw, "alpaca")

        assert bar.symbol == "MSFT"
        assert bar.open == Decimal("380.0")
        assert bar.close == Decimal("381.25")

    # ─────────────────────────────────────────────────────────────────────
    # YFinance Bar Normalization
    # ─────────────────────────────────────────────────────────────────────

    def test_normalize_yfinance_bar(self, normalizer):
        """YFinance format should be normalized."""
        raw = {
            "symbol": "GOOGL",
            "Date": "2024-01-15",
            "Open": 140.00,
            "High": 142.50,
            "Low": 139.50,
            "Close": 141.75,
            "Volume": 2000000,
            "interval": "1d",
        }

        bar = normalizer.normalize_bar(raw, "yfinance")

        assert bar.symbol == "GOOGL"
        assert bar.open == Decimal("140.0")
        assert bar.close == Decimal("141.75")
        assert bar.vwap is None  # YFinance doesn't provide VWAP
        assert bar.provider == "yfinance"
        assert bar.timeframe == "1d"

    # ─────────────────────────────────────────────────────────────────────
    # Alpaca Quote Normalization
    # ─────────────────────────────────────────────────────────────────────

    def test_normalize_alpaca_quote(self, normalizer):
        """Alpaca quote format should be normalized."""
        raw = {
            "S": "AAPL",
            "t": "2024-01-15T14:30:00.123Z",
            "bp": 185.50,
            "ap": 185.52,
            "bs": 100,
            "as": 200,
            "bx": "N",
            "ax": "Q",
        }

        quote = normalizer.normalize_quote(raw, "alpaca")

        assert quote.symbol == "AAPL"
        assert quote.bid_price == Decimal("185.5")
        assert quote.ask_price == Decimal("185.52")
        assert quote.bid_size == 100
        assert quote.ask_size == 200
        assert quote.bid_exchange == "N"
        assert quote.ask_exchange == "Q"
        assert quote.provider == "alpaca"

    # ─────────────────────────────────────────────────────────────────────
    # Alpaca Trade Normalization
    # ─────────────────────────────────────────────────────────────────────

    def test_normalize_alpaca_trade(self, normalizer):
        """Alpaca trade format should be normalized."""
        raw = {
            "S": "AAPL",
            "t": "2024-01-15T14:30:00.456Z",
            "p": 185.51,
            "s": 50,
            "i": "12345",
            "x": "V",
            "c": ["@", "F"],
            "z": "A",
        }

        trade = normalizer.normalize_trade(raw, "alpaca")

        assert trade.symbol == "AAPL"
        assert trade.price == Decimal("185.51")
        assert trade.size == 50
        assert trade.trade_id == "12345"
        assert trade.exchange == "V"
        assert trade.conditions == ["@", "F"]
        assert trade.tape == "A"
        assert trade.provider == "alpaca"

    # ─────────────────────────────────────────────────────────────────────
    # Generic Normalization
    # ─────────────────────────────────────────────────────────────────────

    def test_normalize_generic_bar(self, normalizer):
        """Generic provider format should be normalized."""
        raw = {
            "symbol": "TEST",
            "timestamp": "2024-01-15T14:30:00Z",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000,
        }

        bar = normalizer.normalize_bar(raw, "unknown_provider")

        assert bar.symbol == "TEST"
        assert bar.provider == "unknown_provider"

    # ─────────────────────────────────────────────────────────────────────
    # Edge Cases
    # ─────────────────────────────────────────────────────────────────────

    def test_handles_missing_fields(self, normalizer):
        """Missing fields should use defaults."""
        raw = {"S": "AAPL"}

        bar = normalizer.normalize_bar(raw, "alpaca")

        assert bar.symbol == "AAPL"
        assert bar.volume == 0
        assert bar.vwap is None

    def test_handles_none_values(self, normalizer):
        """None values should be handled gracefully."""
        raw = {
            "S": "AAPL",
            "t": None,
            "o": None,
            "h": None,
            "l": None,
            "c": None,
            "v": None,
        }

        bar = normalizer.normalize_bar(raw, "alpaca")

        assert bar.symbol == "AAPL"
        assert bar.open == Decimal("0")

    def test_singleton(self):
        """get_normalizer should return singleton."""
        n1 = get_normalizer()
        n2 = get_normalizer()
        assert n1 is n2
