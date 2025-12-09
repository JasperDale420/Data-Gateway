from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Optional, Literal


class CanonicalBar(BaseModel):
    """
    Silver Layer Schema for OHLCV Data.
    Strictly enforces UTC timestamps and float64 for prices.
    """

    symbol: str
    timestamp_utc: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str

    @field_validator("timestamp_utc")
    def enforce_utc(cls, v):
        if v.tzinfo is None:
            raise ValueError("Timestamp must be timezone-aware (UTC)")
        return v


class CanonicalFinancial(BaseModel):
    """
    Silver Layer Schema for Point-in-Time Financials.
    """

    symbol: str
    filing_date_utc: datetime
    report_date: datetime
    concept: str  # e.g., "Assets", "NetIncome"
    value: float
    unit: str
    duration: Literal["instant", "Q", "FY", "YTD"]
    form: str
    accession_number: str
    frame: Optional[str] = None


class CanonicalCorporateAction(BaseModel):
    """
    Silver Layer Schema for Corporate Actions (Splits/Dividends).
    """

    symbol: str
    ex_date_utc: datetime
    action_type: Literal["split", "dividend"]
    value: float  # Split ratio (e.g., 0.5 for 2-for-1) or Dividend amount
    source: str


class CanonicalGoldBar(BaseModel):
    """
    Gold Layer Schema for Canonical Market Data.
    Includes Raw and Fully Adjusted prices.
    """

    symbol: str
    timestamp_utc: datetime

    # Raw Prices
    open: float
    high: float
    low: float
    close: float
    volume: float

    # Fully Adjusted Prices (Splits + Dividends)
    adj_open: float
    adj_high: float
    adj_low: float
    adj_close: float
    adj_volume: float

    # Metadata
    source: str  # Primary source (e.g. "alpaca" or "yfinance")
    is_interpolated: bool = False  # For filling gaps if needed
    session: Literal["PRE", "REG", "AFT"] = "REG"  # Market Session


class CanonicalFeatureVector(BaseModel):
    """
    Feature Layer Schema.
    Point-in-Time Correct, Model-Ready Data.
    """

    symbol: str
    timestamp_utc: datetime

    # Market Data (from Gold)
    price_close: float
    price_adj_close: float
    volume: float
    market_cap: Optional[float] = None

    # Valuation Metrics
    pe_ratio: Optional[float] = None
    peg_ratio: Optional[float] = None
    price_to_book: Optional[float] = None
    fcf_yield: Optional[float] = None

    # Quality / Moat Metrics
    roe_ttm: Optional[float] = None
    gross_margin_ttm: Optional[float] = None
    gross_margin_stability_5y: Optional[float] = None  # StdDev of Margins
    net_income_consistency_5y: Optional[float] = None  # % of positive years

    # Financial Health
    debt_to_equity: Optional[float] = None
    interest_coverage: Optional[float] = None

    # Metadata
    data_version: str = "v1"
