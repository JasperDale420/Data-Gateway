"""Data validation layer for gateway.

Validates incoming data before delivery to clients, ensuring data integrity
as specified in the PRD Data Validation section.
"""

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog

logger = structlog.get_logger()


# ─────────────────────────────────────────────────────────────────────────────
# Validation Error Codes
# ─────────────────────────────────────────────────────────────────────────────


class ValidationErrorCodes:
    """PRD-defined validation error codes."""

    FUTURE_TIMESTAMP = "GW-E7001"
    INVALID_PRICE = "GW-E7002"
    HIGH_LESS_THAN_LOW = "GW-E7003"
    HIGH_LESS_THAN_OPEN_CLOSE = "GW-E7004"
    LOW_GREATER_THAN_OPEN_CLOSE = "GW-E7005"
    NEGATIVE_VOLUME = "GW-E7006"
    INVALID_SYMBOL = "GW-E7007"


# ─────────────────────────────────────────────────────────────────────────────
# Validation Result
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ValidationError:
    """Single validation error."""

    code: str
    message: str
    field: str | None = None


@dataclass
class ValidationResult:
    """Result of data validation."""

    valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    data: Any | None = None

    def add_error(self, code: str, message: str, field: str | None = None) -> None:
        """Add an error to the result."""
        self.errors.append(ValidationError(code=code, message=message, field=field))
        self.valid = False

    @property
    def error_codes(self) -> list[str]:
        """Get list of error codes."""
        return [e.code for e in self.errors]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging/response."""
        return {
            "valid": self.valid,
            "errors": [
                {"code": e.code, "message": e.message, "field": e.field} for e in self.errors
            ],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Data Validator
# ─────────────────────────────────────────────────────────────────────────────


class DataValidator:
    """Validates incoming market data before delivery to clients.

    All validation rules follow PRD specification in the Data Validation section.
    Invalid data is rejected and logged with appropriate error codes.
    """

    # Symbol patterns
    STOCK_PATTERN = re.compile(r"^[A-Z]{1,5}$")
    OCC_OPTION_PATTERN = re.compile(r"^[A-Z]{1,6}\d{6}[CP]\d{8}$")
    CRYPTO_PATTERN = re.compile(r"^[A-Z]{2,10}/[A-Z]{2,10}$")

    def validate_bar(self, bar: dict[str, Any]) -> ValidationResult:
        """Validate bar/OHLCV data.

        Args:
            bar: Dictionary with keys: symbol, timestamp, open, high, low, close, volume

        Returns:
            ValidationResult with valid=True if all checks pass
        """
        result = ValidationResult(valid=True, data=bar)

        # Extract fields with defaults
        symbol = bar.get("symbol", "")
        timestamp = bar.get("timestamp")
        open_price = self._to_decimal(bar.get("open"))
        high = self._to_decimal(bar.get("high"))
        low = self._to_decimal(bar.get("low"))
        close = self._to_decimal(bar.get("close"))
        volume = bar.get("volume", 0)

        # Symbol validation (GW-E7007)
        if not self._is_valid_symbol(symbol):
            result.add_error(
                ValidationErrorCodes.INVALID_SYMBOL, f"Invalid symbol format: {symbol}", "symbol"
            )

        # Timestamp validation (GW-E7001)
        if timestamp:
            ts = self._parse_timestamp(timestamp)
            if ts and ts > datetime.now(UTC):
                result.add_error(
                    ValidationErrorCodes.FUTURE_TIMESTAMP,
                    f"Future timestamp: {timestamp}",
                    "timestamp",
                )

        # Price validation (GW-E7002)
        if open_price is not None and open_price <= 0:
            result.add_error(
                ValidationErrorCodes.INVALID_PRICE, f"Invalid open price: {open_price}", "open"
            )
        if close is not None and close <= 0:
            result.add_error(
                ValidationErrorCodes.INVALID_PRICE, f"Invalid close price: {close}", "close"
            )
        if high is not None and high <= 0:
            result.add_error(
                ValidationErrorCodes.INVALID_PRICE, f"Invalid high price: {high}", "high"
            )
        if low is not None and low <= 0:
            result.add_error(ValidationErrorCodes.INVALID_PRICE, f"Invalid low price: {low}", "low")

        # OHLC relationship validation
        if high is not None and low is not None:
            # High >= Low (GW-E7003)
            if high < low:
                result.add_error(
                    ValidationErrorCodes.HIGH_LESS_THAN_LOW, f"High ({high}) < Low ({low})", "high"
                )

            # High >= Open and Close (GW-E7004)
            if open_price is not None and high < open_price:
                result.add_error(
                    ValidationErrorCodes.HIGH_LESS_THAN_OPEN_CLOSE,
                    f"High ({high}) < Open ({open_price})",
                    "high",
                )
            if close is not None and high < close:
                result.add_error(
                    ValidationErrorCodes.HIGH_LESS_THAN_OPEN_CLOSE,
                    f"High ({high}) < Close ({close})",
                    "high",
                )

            # Low <= Open and Close (GW-E7005)
            if open_price is not None and low > open_price:
                result.add_error(
                    ValidationErrorCodes.LOW_GREATER_THAN_OPEN_CLOSE,
                    f"Low ({low}) > Open ({open_price})",
                    "low",
                )
            if close is not None and low > close:
                result.add_error(
                    ValidationErrorCodes.LOW_GREATER_THAN_OPEN_CLOSE,
                    f"Low ({low}) > Close ({close})",
                    "low",
                )

        # Volume validation (GW-E7006)
        if volume is not None and volume < 0:
            result.add_error(
                ValidationErrorCodes.NEGATIVE_VOLUME, f"Negative volume: {volume}", "volume"
            )

        # Clear data if invalid
        if not result.valid:
            result.data = None
            self._log_validation_failure(bar, result)

        return result

    def validate_quote(self, quote: dict[str, Any]) -> ValidationResult:
        """Validate quote data.

        Args:
            quote: Dictionary with keys: symbol, bid_price, ask_price, bid_size, ask_size

        Returns:
            ValidationResult with valid=True if all checks pass
        """
        result = ValidationResult(valid=True, data=quote)

        symbol = quote.get("symbol", "")
        bid_price = self._to_decimal(quote.get("bid_price"))
        ask_price = self._to_decimal(quote.get("ask_price"))
        bid_size = quote.get("bid_size", 0)
        ask_size = quote.get("ask_size", 0)
        timestamp = quote.get("timestamp")

        # Symbol validation
        if not self._is_valid_symbol(symbol):
            result.add_error(
                ValidationErrorCodes.INVALID_SYMBOL, f"Invalid symbol format: {symbol}", "symbol"
            )

        # Timestamp validation
        if timestamp:
            ts = self._parse_timestamp(timestamp)
            if ts and ts > datetime.now(UTC):
                result.add_error(
                    ValidationErrorCodes.FUTURE_TIMESTAMP,
                    f"Future timestamp: {timestamp}",
                    "timestamp",
                )

        # Price validation
        if bid_price is not None and bid_price < 0:
            result.add_error(
                ValidationErrorCodes.INVALID_PRICE, f"Negative bid price: {bid_price}", "bid_price"
            )
        if ask_price is not None and ask_price < 0:
            result.add_error(
                ValidationErrorCodes.INVALID_PRICE, f"Negative ask price: {ask_price}", "ask_price"
            )

        # Size validation
        if bid_size < 0:
            result.add_error(
                ValidationErrorCodes.NEGATIVE_VOLUME, f"Negative bid size: {bid_size}", "bid_size"
            )
        if ask_size < 0:
            result.add_error(
                ValidationErrorCodes.NEGATIVE_VOLUME, f"Negative ask size: {ask_size}", "ask_size"
            )

        if not result.valid:
            result.data = None
            self._log_validation_failure(quote, result)

        return result

    def validate_trade(self, trade: dict[str, Any]) -> ValidationResult:
        """Validate trade data.

        Args:
            trade: Dictionary with keys: symbol, price, size, timestamp

        Returns:
            ValidationResult with valid=True if all checks pass
        """
        result = ValidationResult(valid=True, data=trade)

        symbol = trade.get("symbol", "")
        price = self._to_decimal(trade.get("price"))
        size = trade.get("size", 0)
        timestamp = trade.get("timestamp")

        # Symbol validation
        if not self._is_valid_symbol(symbol):
            result.add_error(
                ValidationErrorCodes.INVALID_SYMBOL, f"Invalid symbol format: {symbol}", "symbol"
            )

        # Timestamp validation
        if timestamp:
            ts = self._parse_timestamp(timestamp)
            if ts and ts > datetime.now(UTC):
                result.add_error(
                    ValidationErrorCodes.FUTURE_TIMESTAMP,
                    f"Future timestamp: {timestamp}",
                    "timestamp",
                )

        # Price validation
        if price is not None and price <= 0:
            result.add_error(
                ValidationErrorCodes.INVALID_PRICE, f"Invalid trade price: {price}", "price"
            )

        # Size validation
        if size < 0:
            result.add_error(
                ValidationErrorCodes.NEGATIVE_VOLUME, f"Negative trade size: {size}", "size"
            )

        if not result.valid:
            result.data = None
            self._log_validation_failure(trade, result)

        return result

    def _is_valid_symbol(self, symbol: str) -> bool:
        """Check if symbol matches known patterns."""
        if not symbol:
            return False

        return bool(
            self.STOCK_PATTERN.match(symbol)
            or self.OCC_OPTION_PATTERN.match(symbol)
            or self.CRYPTO_PATTERN.match(symbol)
        )

    def _to_decimal(self, value: Any) -> Decimal | None:
        """Convert value to Decimal, handling None."""
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    def _parse_timestamp(self, ts: Any) -> datetime | None:
        """Parse timestamp to datetime."""
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, str):
            try:
                # Handle Z suffix
                if ts.endswith("Z"):
                    ts = ts[:-1] + "+00:00"
                return datetime.fromisoformat(ts)
            except ValueError:
                return None
        return None

    def _log_validation_failure(self, data: dict[str, Any], result: ValidationResult) -> None:
        """Log validation failure with structured context."""
        for error in result.errors:
            logger.warning(
                "data_validation_failed",
                error_code=error.code,
                message=error.message,
                field=error.field,
                symbol=data.get("symbol"),
            )


# ─────────────────────────────────────────────────────────────────────────────
# Singleton Accessor
# ─────────────────────────────────────────────────────────────────────────────

_validator: DataValidator | None = None


def get_validator() -> DataValidator:
    """Get singleton DataValidator instance."""
    global _validator
    if _validator is None:
        _validator = DataValidator()
    return _validator
