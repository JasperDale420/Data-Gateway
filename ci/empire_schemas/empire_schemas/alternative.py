"""Alternative data models — darkpool, insider trades, institution holdings, politician trades."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class NormalizedDarkpoolTrade(BaseModel):
    """Normalized darkpool trade from UW darkpool endpoint."""

    symbol: str
    timestamp: datetime
    price: Decimal
    size: int
    notional: Decimal
    exchange: str | None = None
    tracking_id: str | None = None
    nbbo_bid: Decimal | None = None
    nbbo_ask: Decimal | None = None
    ext_hours: str | None = None
    trade_settlement: str | None = None
    canceled: bool = False
    provider: str = "unusual_whales"


class NormalizedInsiderTrade(BaseModel):
    """Insider trade from SEC Form 4 filings."""

    symbol: str
    transaction_date: datetime
    filing_date: str  # YYYY-MM-DD
    insider_name: str
    insider_title: str | None = None
    transaction_type: str  # "buy", "sell", "grant", "exercise"
    shares: int
    price: Decimal | None = None
    value: Decimal | None = None
    shares_owned: int | None = None
    is_10b5_1: bool = False
    transaction_id: str | None = None
    provider: str = "unusual_whales"


class NormalizedInstitutionHolding(BaseModel):
    """Institutional holding from 13F filings."""

    symbol: str
    institution_id: str
    institution_name: str
    filing_date: str  # YYYY-MM-DD
    report_date: str | None = None
    shares: int
    market_value: Decimal | None = None
    percent_portfolio: Decimal | None = None
    percent_outstanding: Decimal | None = None
    change_shares: int | None = None
    change_type: str | None = None  # "new", "increased", "decreased", "sold_all"
    provider: str = "unusual_whales"


class NormalizedPoliticianTrade(BaseModel):
    """Congressional trade disclosure."""

    symbol: str
    transaction_date: datetime
    filing_date: str  # YYYY-MM-DD
    politician_id: str
    politician_name: str
    chamber: str | None = None  # "senate", "house"
    party: str | None = None  # "D", "R", "I"
    state: str | None = None
    transaction_type: str  # "buy", "sell"
    amount_range: str | None = None
    asset_type: str | None = None
    description: str | None = None
    owner: str | None = None  # "self", "spouse", "child", "joint"
    cap_gains_over_200: bool | None = None
    transaction_id: str | None = None
    provider: str = "unusual_whales"
