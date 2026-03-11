"""Fundamentals models."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

__all__ = [
    "NormalizedEarnings",
    "NormalizedScreenerResult",
    "NormalizedBorrowCost",
    "NormalizedFundamentals",
    "NormalizedShortData",
    "NormalizedFTD",
]


class NormalizedEarnings(BaseModel):
    """Earnings calendar entry."""

    symbol: str
    date: str  # YYYY-MM-DD
    time: str  # "premarket", "afterhours", "unknown"
    eps_estimate: Decimal | None = None
    eps_actual: Decimal | None = None
    revenue_estimate: Decimal | None = None
    revenue_actual: Decimal | None = None
    # Additional UW fields from OpenAPI spec
    expected_move: Decimal | None = None  # Expected move in dollars
    expected_move_pct: Decimal | None = None  # Expected move as percentage (expected_move_perc in UW)
    prior_close: Decimal | None = None  # Pre-earnings close price (pre_earnings_close in UW)
    has_options: bool | None = None  # Whether ticker has options
    market_cap: Decimal | None = None  # Market capitalization
    sector: str | None = None  # GICS sector
    provider: str


class NormalizedScreenerResult(BaseModel):
    """Stock screener result with comprehensive UW fields."""

    symbol: str
    price: Decimal | None = None  # close in UW
    volume: int | None = None
    market_cap: Decimal | None = None  # marketcap in UW
    sector: str | None = None
    call_volume: int | None = None
    put_volume: int | None = None
    iv_rank: Decimal | None = None
    # IV metrics
    iv30d: Decimal | None = None  # 30-day implied volatility
    iv30d_1d: Decimal | None = None  # IV30d 1-day change
    iv30d_1w: Decimal | None = None  # IV30d 1-week change
    iv30d_1m: Decimal | None = None  # IV30d 1-month change
    volatility: Decimal | None = None  # Historical volatility
    # Flow / premium metrics
    bearish_premium: Decimal | None = None
    bullish_premium: Decimal | None = None
    call_premium: Decimal | None = None
    put_premium: Decimal | None = None
    net_call_premium: Decimal | None = None
    net_put_premium: Decimal | None = None
    # OI data
    call_open_interest: int | None = None
    put_open_interest: int | None = None
    total_open_interest: int | None = None
    prev_call_oi: int | None = None
    prev_put_oi: int | None = None
    # Volume breakdowns
    call_volume_ask_side: int | None = None
    call_volume_bid_side: int | None = None
    put_volume_ask_side: int | None = None
    put_volume_bid_side: int | None = None
    # Average volume metrics
    avg_30_day_call_volume: int | None = None
    avg_30_day_put_volume: int | None = None
    avg_3_day_call_volume: int | None = None
    avg_3_day_put_volume: int | None = None
    avg_7_day_call_volume: int | None = None
    avg_7_day_put_volume: int | None = None
    # Market data
    prev_close: Decimal | None = None
    week_52_high: Decimal | None = None
    week_52_low: Decimal | None = None
    relative_volume: Decimal | None = None
    implied_move: Decimal | None = None
    implied_move_perc: Decimal | None = None
    put_call_ratio: Decimal | None = None
    # Metadata
    is_index: bool | None = None
    issue_type: str | None = None
    er_time: str | None = None  # Earnings report time
    next_earnings_date: str | None = None
    next_dividend_date: str | None = None
    full_name: str | None = None
    provider: str


class NormalizedBorrowCost(BaseModel):
    """Borrow cost / short availability data from UW shorts endpoint.

    Based on the UW OpenAPI spec Short Data schema.
    """

    symbol: str
    timestamp: datetime
    fee_rate: Decimal | None = None  # Annual borrow fee rate (percentage)
    rebate_rate: Decimal | None = None  # Rebate rate
    short_shares_available: int | None = None  # Shares available to short
    currency: str | None = None  # Currency denomination
    name: str | None = None  # Company name
    provider: str = "unusual_whales"


class NormalizedFundamentals(BaseModel):
    """Company fundamentals from various providers."""

    symbol: str
    name: str | None = None
    sector: str | None = None
    industry: str | None = None
    market_cap: Decimal | None = None
    pe_ratio: Decimal | None = None  # Price/Earnings
    forward_pe: Decimal | None = None
    peg_ratio: Decimal | None = None  # P/E to Growth
    pb_ratio: Decimal | None = None  # Price/Book
    ps_ratio: Decimal | None = None  # Price/Sales
    dividend_yield: Decimal | None = None
    dividend_per_share: Decimal | None = None
    eps: Decimal | None = None  # Earnings per share
    eps_growth: Decimal | None = None  # YoY EPS growth
    revenue: Decimal | None = None
    revenue_growth: Decimal | None = None
    profit_margin: Decimal | None = None
    operating_margin: Decimal | None = None
    roe: Decimal | None = None  # Return on equity
    roa: Decimal | None = None  # Return on assets
    debt_to_equity: Decimal | None = None
    current_ratio: Decimal | None = None
    beta: Decimal | None = None
    week_52_high: Decimal | None = None
    week_52_low: Decimal | None = None
    shares_outstanding: int | None = None
    float_shares: int | None = None
    avg_volume: int | None = None
    exchange: str | None = None
    country: str | None = None
    description: str | None = None
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
