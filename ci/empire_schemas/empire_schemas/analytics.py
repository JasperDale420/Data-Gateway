"""Analytics data models — net premium, max pain, IV, OI, ETFs, shorts, volatility, seasonality, orderbook."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


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


class NormalizedMarketTide(BaseModel):
    """Market sentiment snapshot from UW market/tide endpoint."""

    timestamp: datetime
    date: str | None = None
    net_call_premium: Decimal
    net_put_premium: Decimal
    net_volume: int | None = None
    sentiment: str  # "bullish", "bearish", "neutral"
    call_put_ratio: Decimal | None = None
    provider: str = "unusual_whales"


class NormalizedSectorTide(BaseModel):
    """Sector sentiment snapshot from UW sector/tide endpoint."""

    timestamp: datetime
    sector: str
    net_call_premium: Decimal
    net_put_premium: Decimal
    net_volume: int | None = None
    sentiment: str  # "bullish", "bearish", "neutral"
    call_put_ratio: Decimal | None = None
    provider: str = "unusual_whales"


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
