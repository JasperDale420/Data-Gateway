"""Data Normalization Layer.

Converts provider-specific data formats to normalized dataclasses
as specified in PRD Data Normalization section (lines 374-405).
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog

logger = structlog.get_logger()


# ─────────────────────────────────────────────────────────────────────────────
# Normalized Data Types
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class NormalizedBar:
    """Normalized OHLCV bar data."""

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    vwap: Decimal | None = None
    trade_count: int | None = None
    provider: str = ""
    timeframe: str = "1Min"

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "open": float(self.open),
            "high": float(self.high),
            "low": float(self.low),
            "close": float(self.close),
            "volume": self.volume,
            "vwap": float(self.vwap) if self.vwap else None,
            "trade_count": self.trade_count,
            "provider": self.provider,
            "timeframe": self.timeframe,
        }


@dataclass
class NormalizedQuote:
    """Normalized quote/NBBO data."""

    symbol: str
    timestamp: datetime
    bid_price: Decimal
    ask_price: Decimal
    bid_size: int
    ask_size: int
    bid_exchange: str | None = None
    ask_exchange: str | None = None
    provider: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "bid_price": float(self.bid_price),
            "ask_price": float(self.ask_price),
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "bid_exchange": self.bid_exchange,
            "ask_exchange": self.ask_exchange,
            "provider": self.provider,
        }


@dataclass
class NormalizedTrade:
    """Normalized trade data."""

    symbol: str
    timestamp: datetime
    price: Decimal
    size: int
    trade_id: str | None = None
    exchange: str | None = None
    conditions: list[str] = field(default_factory=list)
    tape: str | None = None
    provider: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "price": float(self.price),
            "size": self.size,
            "trade_id": self.trade_id,
            "exchange": self.exchange,
            "conditions": self.conditions,
            "tape": self.tape,
            "provider": self.provider,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Data Normalizer
# ─────────────────────────────────────────────────────────────────────────────


class DataNormalizer:
    """Normalizes provider-specific data formats to standard structures.

    Supports:
    - Alpaca (WebSocket and REST formats)
    - YFinance
    - Unusual Whales
    """

    def normalize_bar(self, raw: dict[str, Any], provider: str) -> NormalizedBar:
        """Normalize bar data from any provider.

        Args:
            raw: Raw bar data from provider
            provider: Provider name (alpaca, yfinance, etc.)

        Returns:
            NormalizedBar with consistent field names
        """
        if provider == "alpaca":
            return self._normalize_alpaca_bar(raw)
        elif provider == "yfinance":
            return self._normalize_yfinance_bar(raw)
        else:
            return self._normalize_generic_bar(raw, provider)

    def normalize_quote(self, raw: dict[str, Any], provider: str) -> NormalizedQuote:
        """Normalize quote data from any provider."""
        if provider == "alpaca":
            return self._normalize_alpaca_quote(raw)
        else:
            return self._normalize_generic_quote(raw, provider)

    def normalize_trade(self, raw: dict[str, Any], provider: str) -> NormalizedTrade:
        """Normalize trade data from any provider."""
        if provider == "alpaca":
            return self._normalize_alpaca_trade(raw)
        else:
            return self._normalize_generic_trade(raw, provider)

    # ─────────────────────────────────────────────────────────────────────
    # Alpaca Normalizers
    # ─────────────────────────────────────────────────────────────────────

    def _normalize_alpaca_bar(self, raw: dict[str, Any]) -> NormalizedBar:
        """Normalize Alpaca bar format.

        Alpaca uses short field names: S, t, o, h, l, c, v, vw, n
        """
        return NormalizedBar(
            symbol=raw.get("S", raw.get("symbol", "")),
            timestamp=self._parse_timestamp(raw.get("t", raw.get("timestamp"))),
            open=self._to_decimal(raw.get("o", raw.get("open"))),
            high=self._to_decimal(raw.get("h", raw.get("high"))),
            low=self._to_decimal(raw.get("l", raw.get("low"))),
            close=self._to_decimal(raw.get("c", raw.get("close"))),
            volume=self._to_int(raw.get("v", raw.get("volume", 0))),
            vwap=(
                self._to_decimal(raw.get("vw", raw.get("vwap")))
                if raw.get("vw") or raw.get("vwap")
                else None
            ),
            trade_count=raw.get("n", raw.get("trade_count")),
            provider="alpaca",
            timeframe=raw.get("timeframe", "1Min"),
        )

    def _normalize_alpaca_quote(self, raw: dict[str, Any]) -> NormalizedQuote:
        """Normalize Alpaca quote format.

        Alpaca uses: S, t, bp, ap, bs, as, bx, ax
        """
        return NormalizedQuote(
            symbol=raw.get("S", raw.get("symbol", "")),
            timestamp=self._parse_timestamp(raw.get("t", raw.get("timestamp"))),
            bid_price=self._to_decimal(raw.get("bp", raw.get("bid_price", 0))),
            ask_price=self._to_decimal(raw.get("ap", raw.get("ask_price", 0))),
            bid_size=int(raw.get("bs", raw.get("bid_size", 0))),
            ask_size=int(raw.get("as", raw.get("ask_size", 0))),
            bid_exchange=raw.get("bx", raw.get("bid_exchange")),
            ask_exchange=raw.get("ax", raw.get("ask_exchange")),
            provider="alpaca",
        )

    def _normalize_alpaca_trade(self, raw: dict[str, Any]) -> NormalizedTrade:
        """Normalize Alpaca trade format.

        Alpaca uses: S, t, p, s, i, x, c, z
        """
        return NormalizedTrade(
            symbol=raw.get("S", raw.get("symbol", "")),
            timestamp=self._parse_timestamp(raw.get("t", raw.get("timestamp"))),
            price=self._to_decimal(raw.get("p", raw.get("price", 0))),
            size=int(raw.get("s", raw.get("size", 0))),
            trade_id=raw.get("i", raw.get("trade_id")),
            exchange=raw.get("x", raw.get("exchange")),
            conditions=raw.get("c", raw.get("conditions", [])) or [],
            tape=raw.get("z", raw.get("tape")),
            provider="alpaca",
        )

    # ─────────────────────────────────────────────────────────────────────
    # YFinance Normalizers
    # ─────────────────────────────────────────────────────────────────────

    def _normalize_yfinance_bar(self, raw: dict[str, Any]) -> NormalizedBar:
        """Normalize YFinance bar format.

        YFinance uses: symbol, Date, Open, High, Low, Close, Volume
        """
        return NormalizedBar(
            symbol=raw.get("symbol", ""),
            timestamp=self._parse_timestamp(raw.get("Date", raw.get("Datetime"))),
            open=self._to_decimal(raw.get("Open")),
            high=self._to_decimal(raw.get("High")),
            low=self._to_decimal(raw.get("Low")),
            close=self._to_decimal(raw.get("Close")),
            volume=int(raw.get("Volume", 0)),
            vwap=None,  # YFinance doesn't provide VWAP
            trade_count=None,
            provider="yfinance",
            timeframe=raw.get("interval", "1d"),
        )

    # ─────────────────────────────────────────────────────────────────────
    # Generic Normalizers
    # ─────────────────────────────────────────────────────────────────────

    def _normalize_generic_bar(self, raw: dict[str, Any], provider: str) -> NormalizedBar:
        """Normalize bar with common field name guessing."""
        return NormalizedBar(
            symbol=raw.get("symbol", raw.get("S", "")),
            timestamp=self._parse_timestamp(
                raw.get("timestamp", raw.get("t", raw.get("time", raw.get("datetime"))))
            ),
            open=self._to_decimal(raw.get("open", raw.get("o", 0))),
            high=self._to_decimal(raw.get("high", raw.get("h", 0))),
            low=self._to_decimal(raw.get("low", raw.get("l", 0))),
            close=self._to_decimal(raw.get("close", raw.get("c", 0))),
            volume=int(raw.get("volume", raw.get("v", 0))),
            vwap=(
                self._to_decimal(raw.get("vwap", raw.get("vw")))
                if raw.get("vwap") or raw.get("vw")
                else None
            ),
            trade_count=raw.get("trade_count", raw.get("n")),
            provider=provider,
            timeframe=raw.get("timeframe", "1Min"),
        )

    def _normalize_generic_quote(self, raw: dict[str, Any], provider: str) -> NormalizedQuote:
        """Normalize quote with common field name guessing."""
        return NormalizedQuote(
            symbol=raw.get("symbol", raw.get("S", "")),
            timestamp=self._parse_timestamp(raw.get("timestamp", raw.get("t"))),
            bid_price=self._to_decimal(raw.get("bid_price", raw.get("bid", raw.get("bp", 0)))),
            ask_price=self._to_decimal(raw.get("ask_price", raw.get("ask", raw.get("ap", 0)))),
            bid_size=int(raw.get("bid_size", raw.get("bs", 0))),
            ask_size=int(raw.get("ask_size", raw.get("as", 0))),
            provider=provider,
        )

    def _normalize_generic_trade(self, raw: dict[str, Any], provider: str) -> NormalizedTrade:
        """Normalize trade with common field name guessing."""
        return NormalizedTrade(
            symbol=raw.get("symbol", raw.get("S", "")),
            timestamp=self._parse_timestamp(raw.get("timestamp", raw.get("t"))),
            price=self._to_decimal(raw.get("price", raw.get("p", 0))),
            size=int(raw.get("size", raw.get("s", raw.get("volume", 0)))),
            trade_id=raw.get("trade_id", raw.get("id", raw.get("i"))),
            exchange=raw.get("exchange", raw.get("x")),
            conditions=raw.get("conditions", raw.get("c", [])) or [],
            provider=provider,
        )

    # ─────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────

    def _to_decimal(self, value: Any) -> Decimal:
        """Convert value to Decimal, handling various input types."""
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return Decimal("0")

    def _to_int(self, value: Any) -> int:
        """Convert value to int, handling None and invalid values."""
        if value is None:
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _parse_timestamp(self, ts: Any) -> datetime:
        """Parse timestamp to datetime, handling various formats."""
        if ts is None:
            return datetime.now(UTC)

        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                return ts.replace(tzinfo=UTC)
            return ts

        if isinstance(ts, str):
            # Handle ISO format with Z suffix
            ts = ts.replace("Z", "+00:00")
            try:
                return datetime.fromisoformat(ts)
            except ValueError:
                pass

        # Fallback to current time
        return datetime.now(UTC)


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

_normalizer: DataNormalizer | None = None


def get_normalizer() -> DataNormalizer:
    """Get singleton DataNormalizer instance."""
    global _normalizer
    if _normalizer is None:
        _normalizer = DataNormalizer()
    return _normalizer
