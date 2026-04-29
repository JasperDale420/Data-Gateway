"""Core market data models — bars, quotes, trades."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class NormalizedBar(BaseModel):
    """Normalized OHLCV bar."""

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    vwap: Decimal | None = None
    trade_count: int | None = None
    provider: str = ""
    timeframe: str = "1Min"  # 1Min, 5Min, 15Min, 1Hour, 1Day


class NormalizedQuote(BaseModel):
    """Normalized quote (bid/ask)."""

    symbol: str
    timestamp: datetime
    bid_price: Decimal
    bid_size: Decimal
    ask_price: Decimal
    ask_size: Decimal
    bid_exchange: str | None = None
    ask_exchange: str | None = None
    conditions: list[str] = Field(default_factory=list)
    tape: str | None = None  # A=NYSE, B=ARCA, C=NASDAQ
    provider: str = ""


class NormalizedTrade(BaseModel):
    """Normalized trade (equity, option, or crypto)."""

    symbol: str
    timestamp: datetime
    price: Decimal
    size: Decimal
    trade_id: str | None = None
    exchange: str | None = None  # SIP exchange code (equity) or OPRA exchange code (option)
    conditions: list[str] = Field(default_factory=list)  # SIP conditions (equity) or OPRA conditions (option)
    tape: str | None = None  # SIP tape (equity only: A=NYSE, B=ARCA, C=NASDAQ)
    taker_side: str | None = None  # Crypto only (B=buy, S=sell)
    update: str | None = None  # SIP trade correction (equity only)
    instrument_type: str = "equity"  # equity, option, or crypto
    exchange_type: str | None = None  # Code table for exchange: "sip", "opra", or None
    condition_type: str | None = None  # Code table for conditions: "sip", "opra", or None
    provider: str = ""


class NormalizedForexRate(BaseModel):
    """Forex currency pair rate."""

    pair: str  # e.g., "EUR/USD"
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    mid: Decimal | None = None
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    provider: str = ""
