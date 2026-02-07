"""Unit tests for DataValidator."""

from datetime import UTC, datetime, timedelta

import pytest

from gateway.core.validator import (
    DataValidator,
    ValidationErrorCodes,
    get_validator,
)


@pytest.fixture
def validator() -> DataValidator:
    """Get a fresh DataValidator instance."""
    return DataValidator()


class TestValidateBar:
    """Tests for bar validation."""

    def test_valid_bar_passes(self, validator: DataValidator) -> None:
        """Valid bar data should pass validation."""
        bar = {
            "symbol": "AAPL",
            "timestamp": "2026-01-15T10:30:00Z",
            "open": 185.50,
            "high": 186.00,
            "low": 185.00,
            "close": 185.75,
            "volume": 100000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is True
        assert len(result.errors) == 0
        assert result.data == bar

    def test_invalid_symbol_rejected(self, validator: DataValidator) -> None:
        """Invalid symbol format should be rejected with GW-E7007."""
        bar = {
            "symbol": "invalid123",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 102,
            "volume": 1000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is False
        assert ValidationErrorCodes.INVALID_SYMBOL in result.error_codes

    def test_future_timestamp_rejected(self, validator: DataValidator) -> None:
        """Future timestamp should be rejected with GW-E7001."""
        future = datetime.now(UTC) + timedelta(days=1)
        bar = {
            "symbol": "AAPL",
            "timestamp": future.isoformat(),
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 102,
            "volume": 1000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is False
        assert ValidationErrorCodes.FUTURE_TIMESTAMP in result.error_codes

    def test_negative_price_rejected(self, validator: DataValidator) -> None:
        """Negative prices should be rejected with GW-E7002."""
        bar = {
            "symbol": "AAPL",
            "open": -100,
            "high": 105,
            "low": 99,
            "close": 102,
            "volume": 1000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is False
        assert ValidationErrorCodes.INVALID_PRICE in result.error_codes

    def test_zero_price_rejected(self, validator: DataValidator) -> None:
        """Zero price should be rejected with GW-E7002."""
        bar = {
            "symbol": "AAPL",
            "open": 0,
            "high": 105,
            "low": 99,
            "close": 102,
            "volume": 1000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is False
        assert ValidationErrorCodes.INVALID_PRICE in result.error_codes

    def test_high_less_than_low_rejected(self, validator: DataValidator) -> None:
        """High < Low should be rejected with GW-E7003."""
        bar = {
            "symbol": "AAPL",
            "open": 100,
            "high": 95,
            "low": 99,
            "close": 102,
            "volume": 1000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is False
        assert ValidationErrorCodes.HIGH_LESS_THAN_LOW in result.error_codes

    def test_high_less_than_open_rejected(self, validator: DataValidator) -> None:
        """High < Open should be rejected with GW-E7004."""
        bar = {
            "symbol": "AAPL",
            "open": 110,
            "high": 105,
            "low": 99,
            "close": 102,
            "volume": 1000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is False
        assert ValidationErrorCodes.HIGH_LESS_THAN_OPEN_CLOSE in result.error_codes

    def test_high_less_than_close_rejected(self, validator: DataValidator) -> None:
        """High < Close should be rejected with GW-E7004."""
        bar = {
            "symbol": "AAPL",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 110,
            "volume": 1000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is False
        assert ValidationErrorCodes.HIGH_LESS_THAN_OPEN_CLOSE in result.error_codes

    def test_low_greater_than_open_rejected(self, validator: DataValidator) -> None:
        """Low > Open should be rejected with GW-E7005."""
        bar = {
            "symbol": "AAPL",
            "open": 95,
            "high": 105,
            "low": 99,
            "close": 102,
            "volume": 1000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is False
        assert ValidationErrorCodes.LOW_GREATER_THAN_OPEN_CLOSE in result.error_codes

    def test_low_greater_than_close_rejected(self, validator: DataValidator) -> None:
        """Low > Close should be rejected with GW-E7005."""
        bar = {
            "symbol": "AAPL",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 95,
            "volume": 1000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is False
        assert ValidationErrorCodes.LOW_GREATER_THAN_OPEN_CLOSE in result.error_codes

    def test_negative_volume_rejected(self, validator: DataValidator) -> None:
        """Negative volume should be rejected with GW-E7006."""
        bar = {
            "symbol": "AAPL",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 102,
            "volume": -1000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is False
        assert ValidationErrorCodes.NEGATIVE_VOLUME in result.error_codes

    def test_zero_volume_passes(self, validator: DataValidator) -> None:
        """Zero volume should be allowed (market closed, halted, etc)."""
        bar = {
            "symbol": "AAPL",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 102,
            "volume": 0,
        }
        result = validator.validate_bar(bar)
        assert result.valid is True

    def test_stream_bar_shape_passes(self, validator: DataValidator) -> None:
        """Raw stream bar payload shape should be validated without remapping."""
        bar = {
            "S": "AAPL",
            "t": "2026-01-15T10:30:00Z",
            "o": 185.50,
            "h": 186.00,
            "l": 185.00,
            "c": 185.75,
            "v": 100000,
        }
        result = validator.validate_bar(bar)
        assert result.valid is True


class TestValidateQuote:
    """Tests for quote validation."""

    def test_valid_quote_passes(self, validator: DataValidator) -> None:
        """Valid quote data should pass validation."""
        quote = {
            "symbol": "AAPL",
            "bid_price": 185.50,
            "ask_price": 185.55,
            "bid_size": 100,
            "ask_size": 200,
        }
        result = validator.validate_quote(quote)
        assert result.valid is True

    def test_negative_bid_rejected(self, validator: DataValidator) -> None:
        """Negative bid price should be rejected."""
        quote = {
            "symbol": "AAPL",
            "bid_price": -1,
            "ask_price": 185.55,
            "bid_size": 100,
            "ask_size": 200,
        }
        result = validator.validate_quote(quote)
        assert result.valid is False
        assert ValidationErrorCodes.INVALID_PRICE in result.error_codes

    def test_stream_quote_shape_passes(self, validator: DataValidator) -> None:
        """Raw stream quote payload shape should be validated without remapping."""
        quote = {
            "S": "AAPL",
            "t": "2026-01-15T10:30:00Z",
            "bp": 185.50,
            "ap": 185.55,
            "bs": 100,
            "as": 200,
        }
        result = validator.validate_quote(quote)
        assert result.valid is True


class TestValidateTrade:
    """Tests for trade validation."""

    def test_valid_trade_passes(self, validator: DataValidator) -> None:
        """Valid trade data should pass validation."""
        trade = {
            "symbol": "AAPL",
            "price": 185.50,
            "size": 100,
        }
        result = validator.validate_trade(trade)
        assert result.valid is True

    def test_negative_price_rejected(self, validator: DataValidator) -> None:
        """Negative trade price should be rejected."""
        trade = {
            "symbol": "AAPL",
            "price": -185.50,
            "size": 100,
        }
        result = validator.validate_trade(trade)
        assert result.valid is False
        assert ValidationErrorCodes.INVALID_PRICE in result.error_codes

    def test_stream_trade_shape_passes(self, validator: DataValidator) -> None:
        """Raw stream trade payload shape should be validated without remapping."""
        trade = {
            "S": "AAPL",
            "t": "2026-01-15T10:30:00Z",
            "p": 185.50,
            "s": 100,
        }
        result = validator.validate_trade(trade)
        assert result.valid is True


class TestSymbolPatterns:
    """Tests for symbol pattern matching."""

    def test_stock_symbols(self, validator: DataValidator) -> None:
        """Standard 1-5 letter stock symbols should pass."""
        for symbol in ["A", "AA", "AAPL", "GOOGL"]:
            assert validator._is_valid_symbol(symbol) is True

    def test_occ_option_symbols(self, validator: DataValidator) -> None:
        """OCC option format should pass."""
        # AAPL210319C00125000
        assert validator._is_valid_symbol("AAPL210319C00125000") is True

    def test_crypto_symbols(self, validator: DataValidator) -> None:
        """Crypto pair format should pass."""
        assert validator._is_valid_symbol("BTC/USD") is True
        assert validator._is_valid_symbol("ETH/USDT") is True

    def test_invalid_symbols(self, validator: DataValidator) -> None:
        """Invalid symbols should fail."""
        for symbol in ["", "123", "TOOLONGSYMBOL", "aapl", "AAPL$"]:
            assert validator._is_valid_symbol(symbol) is False


class TestGetValidator:
    """Tests for singleton accessor."""

    def test_returns_same_instance(self) -> None:
        """get_validator should return the same instance."""
        v1 = get_validator()
        v2 = get_validator()
        assert v1 is v2
