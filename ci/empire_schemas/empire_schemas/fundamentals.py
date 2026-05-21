"""Fundamental data models — company fundamentals, earnings, corporate actions."""

from decimal import Decimal

from pydantic import BaseModel


class NormalizedFundamentals(BaseModel):
    """Company fundamentals from various providers."""

    symbol: str
    name: str | None = None
    sector: str | None = None
    industry: str | None = None
    market_cap: Decimal | None = None
    pe_ratio: Decimal | None = None
    forward_pe: Decimal | None = None
    peg_ratio: Decimal | None = None
    pb_ratio: Decimal | None = None
    ps_ratio: Decimal | None = None
    dividend_yield: Decimal | None = None
    dividend_per_share: Decimal | None = None
    eps: Decimal | None = None
    eps_growth: Decimal | None = None
    revenue: Decimal | None = None
    revenue_growth: Decimal | None = None
    profit_margin: Decimal | None = None
    operating_margin: Decimal | None = None
    roe: Decimal | None = None
    roa: Decimal | None = None
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
