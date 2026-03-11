"""Core market data models."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

__all__ = [
    "NormalizedBar",
    "NormalizedQuote",
    "NormalizedTrade",
    "NormalizedMostActive",
    "NormalizedMover",
    "NormalizedOrderbookLevel",
    "NormalizedOrderbook",
    "NormalizedForexRate",
    "StockSnapshot",
    "Auction",
]


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
    provider: str
    timeframe: str = "1Min"  # 1Min, 5Min, 15Min, 1Hour, 1Day


class NormalizedQuote(BaseModel):
    """Normalized quote (bid/ask)."""

    symbol: str
    timestamp: datetime
    bid_price: Decimal
    bid_size: Decimal
    ask_price: Decimal
    ask_size: Decimal
    bid_exchange: str | None = None  # Alpaca WS: bx
    ask_exchange: str | None = None  # Alpaca WS: ax
    conditions: list[str] = Field(default_factory=list)  # Alpaca WS: c
    tape: str | None = None  # Alpaca WS: z (A=NYSE, B=ARCA, C=NASDAQ)
    provider: str


class NormalizedTrade(BaseModel):
    """Normalized trade."""

    symbol: str
    timestamp: datetime
    price: Decimal
    size: Decimal
    trade_id: str | None = None  # Alpaca WS: i - Unique trade identifier
    exchange: str | None = None  # Alpaca WS: x (stocks only)
    conditions: list[str] = Field(default_factory=list)  # Alpaca WS: c (stocks only)
    tape: str | None = None  # Alpaca WS: z (stocks: A=NYSE, B=ARCA, C=NASDAQ)
    taker_side: str | None = None  # Crypto only (B=buy, S=sell)
    update: str | None = None  # Alpaca: u - Trade correction status (canceled, incorrect, corrected)
    provider: str


class NormalizedMostActive(BaseModel):
    """Most active stock data."""

    symbol: str
    volume: int
    trade_count: int
    provider: str


class NormalizedMover(BaseModel):
    """Top mover (gainer/loser) data."""

    symbol: str
    price: Decimal
    change: Decimal
    percent_change: Decimal
    provider: str


class NormalizedOrderbookLevel(BaseModel):
    """Orderbook level data."""

    price: Decimal
    size: Decimal
    side: str  # "bid" or "ask"


class NormalizedOrderbook(BaseModel):
    """Full orderbook snapshot."""

    symbol: str
    timestamp: datetime
    bids: list[NormalizedOrderbookLevel]
    asks: list[NormalizedOrderbookLevel]
    provider: str


class NormalizedForexRate(BaseModel):
    """Forex currency pair rate."""

    pair: str  # e.g., "EUR/USD"
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    mid: Decimal | None = None  # Computed: (bid + ask) / 2
    open: Decimal | None = None  # For bars
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    provider: str


class StockSnapshot(BaseModel):
    """Stock snapshot data."""

    daily_bar: NormalizedBar | None = None
    latest_quote: NormalizedQuote | None = None
    latest_trade: NormalizedTrade | None = None
    minute_bar: NormalizedBar | None = None
    prev_daily_bar: NormalizedBar | None = None


class Auction(BaseModel):
    """Auction data."""

    date: str
    opening: list[dict] = Field(default_factory=list)
    closing: list[dict] = Field(default_factory=list)
