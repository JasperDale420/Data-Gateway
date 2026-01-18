"""Pydantic schemas for messages and data."""

from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field

# WebSocket Messages


class AuthMessage(BaseModel):
    """Client authentication message."""

    action: Literal["auth"]
    key: str
    request_id: str | None = None


class SubscribeMessage(BaseModel):
    """Subscribe to symbols."""

    action: Literal["subscribe"]
    symbols: list[str]
    feeds: list[str] = Field(default_factory=lambda: ["bars"])
    request_id: str | None = None


class UnsubscribeMessage(BaseModel):
    """Unsubscribe from symbols."""

    action: Literal["unsubscribe"]
    symbols: list[str]
    request_id: str | None = None


class AuthResult(BaseModel):
    """Authentication result."""

    type: Literal["auth_result"]
    status: Literal["ok", "error"]
    client_id: str | None = None
    code: str | None = None
    message: str | None = None


class SubscriptionAck(BaseModel):
    """Subscription acknowledgement."""

    type: Literal["subscription_ack"]
    subscribed: list[str]
    failed: list[str] = Field(default_factory=list)


# Normalized Data Schemas


class NormalizedBar(BaseModel):
    """Normalized OHLCV bar."""

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    vwap: Decimal | None = None
    trade_count: int | None = None
    provider: str
    timeframe: str = "1Min"  # 1Min, 5Min, 15Min, 1Hour, 1Day


class NormalizedQuote(BaseModel):
    """Normalized quote (bid/ask)."""

    symbol: str
    timestamp: datetime
    bid_price: Decimal
    bid_size: int
    ask_price: Decimal
    ask_size: int
    provider: str


class NormalizedTrade(BaseModel):
    """Normalized trade."""

    symbol: str
    timestamp: datetime
    price: Decimal
    size: int
    trade_id: str | None = None  # Unique trade identifier
    exchange: str | None = None
    conditions: list[str] = Field(default_factory=list)
    provider: str


class NormalizedFlowAlert(BaseModel):
    """Normalized options flow alert."""

    symbol: str
    timestamp: datetime
    strike: Decimal
    expiry: str  # YYYY-MM-DD format
    put_call: str  # "put" or "call"
    premium: Decimal
    volume: int
    open_interest: int
    side: str  # "bid", "ask", "mid"
    is_sweep: bool = False
    is_unusual: bool = False
    sentiment: str | None = None  # "bullish", "bearish"
    provider: str = "unusual_whales"


class NormalizedDarkpoolTrade(BaseModel):
    """Normalized darkpool trade."""

    symbol: str
    timestamp: datetime
    price: Decimal
    size: int
    notional: Decimal
    exchange: str | None = None
    provider: str = "unusual_whales"


class NormalizedMarketTide(BaseModel):
    """Market sentiment snapshot."""

    timestamp: datetime
    net_call_premium: Decimal
    net_put_premium: Decimal
    call_volume: int
    put_volume: int
    sentiment: str  # "bullish", "bearish", "neutral"
    provider: str = "unusual_whales"


class NormalizedOptionContract(BaseModel):
    """Normalized option contract with greeks."""

    contract_symbol: str  # OCC format (e.g., AAPL250117C00200000)
    underlying: str
    expiration: str  # YYYY-MM-DD
    strike: Decimal
    option_type: str  # "call" or "put"
    bid: Decimal
    ask: Decimal
    last: Decimal
    volume: int
    open_interest: int
    delta: Decimal | None = None
    gamma: Decimal | None = None
    theta: Decimal | None = None
    vega: Decimal | None = None
    iv: Decimal | None = None
    provider: str
    timestamp: datetime


# Phase 1 Schemas: News, Greek Exposure, Earnings, Screeners


class NormalizedNewsArticle(BaseModel):
    """Normalized news article."""

    article_id: str
    headline: str
    summary: str | None = None
    content: str | None = None
    url: str | None = None
    source: str
    author: str | None = None
    published_at: datetime
    symbols: list[str] = Field(default_factory=list)
    provider: str


class NormalizedGreekExposure(BaseModel):
    """Greek exposure (GEX/DEX/VEX) data."""

    symbol: str
    timestamp: datetime
    gamma_exposure: Decimal
    delta_exposure: Decimal | None = None
    vanna_exposure: Decimal | None = None
    charm_exposure: Decimal | None = None
    strike: Decimal | None = None  # For strike-level data
    expiry: str | None = None  # For expiry-level data
    provider: str = "unusual_whales"


class NormalizedEarnings(BaseModel):
    """Earnings calendar entry."""

    symbol: str
    date: str  # YYYY-MM-DD
    time: str  # "premarket", "afterhours", "unknown"
    eps_estimate: Decimal | None = None
    eps_actual: Decimal | None = None
    revenue_estimate: Decimal | None = None
    revenue_actual: Decimal | None = None
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


class NormalizedScreenerResult(BaseModel):
    """Stock screener result."""

    symbol: str
    price: Decimal | None = None
    volume: int | None = None
    market_cap: Decimal | None = None
    sector: str | None = None
    call_volume: int | None = None
    put_volume: int | None = None
    iv_rank: Decimal | None = None
    provider: str


class NormalizedHottestChain(BaseModel):
    """Hottest options chain/contract."""

    contract_symbol: str
    underlying: str
    strike: Decimal
    expiry: str
    option_type: str  # "call" or "put"
    volume: int
    open_interest: int
    premium: Decimal
    iv: Decimal | None = None
    provider: str = "unusual_whales"


# ─────────────────────────────────────────────────────────────────
# Phase 2: Analytics Schemas
# ─────────────────────────────────────────────────────────────────


class NormalizedNetPremiumTick(BaseModel):
    """Net premium tick data."""

    symbol: str
    timestamp: datetime
    net_call_premium: Decimal
    net_put_premium: Decimal
    call_volume: int
    put_volume: int
    provider: str


class NormalizedMaxPain(BaseModel):
    """Max pain strike data."""

    symbol: str
    expiry: str
    max_pain_strike: Decimal
    call_oi: int | None = None
    put_oi: int | None = None
    provider: str


class NormalizedIVRank(BaseModel):
    """IV rank data."""

    symbol: str
    iv_rank: Decimal
    iv_percentile: Decimal | None = None
    current_iv: Decimal | None = None
    one_year_high: Decimal | None = None
    one_year_low: Decimal | None = None
    provider: str


class NormalizedOIChange(BaseModel):
    """Open interest change data."""

    symbol: str
    date: str
    call_oi: int
    put_oi: int
    call_oi_change: int
    put_oi_change: int
    provider: str


class NormalizedETFHolding(BaseModel):
    """ETF holding data."""

    etf_symbol: str
    holding_symbol: str
    weight: Decimal
    shares: int | None = None
    market_value: Decimal | None = None
    provider: str


class NormalizedETFFlow(BaseModel):
    """ETF inflow/outflow data."""

    symbol: str
    date: str
    inflow: Decimal
    outflow: Decimal
    net_flow: Decimal
    provider: str


class NormalizedShortData(BaseModel):
    """Short interest data."""

    symbol: str
    date: str
    short_interest: int
    days_to_cover: Decimal | None = None
    short_percent_float: Decimal | None = None
    short_percent_outstanding: Decimal | None = None
    provider: str


class NormalizedFTD(BaseModel):
    """Failure to deliver data."""

    symbol: str
    date: str
    quantity: int
    price: Decimal | None = None
    value: Decimal | None = None
    provider: str


class NormalizedCorporateAction(BaseModel):
    """Corporate action data (splits, dividends, etc.)."""

    symbol: str
    action_type: str  # "split", "dividend", "merger", "spinoff"
    ex_date: str
    record_date: str | None = None
    payable_date: str | None = None
    amount: Decimal | None = None
    ratio: str | None = None  # For splits: "4:1", "2:1"
    provider: str


# ─────────────────────────────────────────────────────────────────
# Phase 3: Advanced Analytics Schemas
# ─────────────────────────────────────────────────────────────────


class NormalizedIVTermStructure(BaseModel):
    """IV term structure data point."""

    symbol: str
    expiry: str
    iv: Decimal
    days_to_expiry: int
    call_iv: Decimal | None = None
    put_iv: Decimal | None = None
    provider: str


class NormalizedVolatilityStats(BaseModel):
    """Volatility statistics."""

    symbol: str
    realized_vol_30d: Decimal | None = None
    realized_vol_60d: Decimal | None = None
    realized_vol_90d: Decimal | None = None
    iv_30d: Decimal | None = None
    iv_percentile: Decimal | None = None
    hv_iv_ratio: Decimal | None = None
    provider: str


class NormalizedSeasonality(BaseModel):
    """Seasonality data."""

    symbol: str | None = None
    month: int
    avg_return: Decimal
    median_return: Decimal | None = None
    win_rate: Decimal
    sample_years: int | None = None
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


# API Response Schemas


class HealthResponse(BaseModel):
    """Health check response."""

    status: str


class ErrorResponse(BaseModel):
    """Error response."""

    success: bool = False
    error: dict


class SuccessResponse(BaseModel):
    """Generic success response."""

    success: bool = True
    data: dict
    meta: dict | None = None
