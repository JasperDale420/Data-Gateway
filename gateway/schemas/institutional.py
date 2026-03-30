"""Institutional/alternative data models."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

__all__ = [
    "NormalizedInsiderTrade",
    "NormalizedInstitutionHolding",
    "NormalizedPoliticianTrade",
    "NormalizedETFHolding",
    "NormalizedETFFlow",
]


class NormalizedInsiderTrade(BaseModel):
    """Insider trade from SEC Form 4 filings (UW insiders endpoint)."""

    symbol: str
    transaction_date: datetime
    filing_date: str  # YYYY-MM-DD
    insider_name: str
    insider_title: str | None = None  # CEO, CFO, Director, etc.
    transaction_type: str  # "buy", "sell", "grant", "exercise"
    shares: int
    price: Decimal | None = None  # Price per share
    value: Decimal | None = None  # Total transaction value
    shares_owned: int | None = None  # Shares owned after transaction
    is_10b5_1: bool = False  # 10b5-1 plan transaction
    transaction_id: str | None = None  # Unique ID for dedup
    provider: str = "unusual_whales"


class NormalizedInstitutionHolding(BaseModel):
    """Institutional holding from 13F filings (UW institutions endpoint)."""

    symbol: str
    institution_id: str
    institution_name: str
    filing_date: str  # YYYY-MM-DD
    report_date: str | None = None  # Quarter end date
    shares: int
    market_value: Decimal | None = None
    percent_portfolio: Decimal | None = None  # % of institution portfolio
    percent_outstanding: Decimal | None = None  # % of shares outstanding
    change_shares: int | None = None  # Change from previous filing
    change_type: str | None = None  # "new", "increased", "decreased", "sold_all"
    provider: str = "unusual_whales"


class NormalizedPoliticianTrade(BaseModel):
    """Congressional trade disclosure (UW politicians endpoint)."""

    symbol: str
    transaction_date: datetime
    filing_date: str  # YYYY-MM-DD
    politician_id: str
    politician_name: str
    chamber: str | None = None  # "senate", "house"
    party: str | None = None  # "D", "R", "I"
    state: str | None = None  # State abbreviation
    transaction_type: str  # "buy", "sell"
    amount_range: str | None = None  # "$1,001 - $15,000", etc.
    asset_type: str | None = None  # "stock", "option", "bond"
    description: str | None = None  # Asset description
    owner: str | None = None  # "self", "spouse", "child", "joint"
    cap_gains_over_200: bool | None = None  # Capital gains indicator
    transaction_id: str | None = None  # Unique ID for dedup
    provider: str = "unusual_whales"


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
