"""Options models."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

__all__ = [
    "NormalizedOptionContract",
    "NormalizedOptionTrade",
    "NormalizedHottestChain",
    "NormalizedGreekExposure",
    "NormalizedMaxPain",
    "NormalizedIVRank",
    "NormalizedIVTermStructure",
    "NormalizedVolatilityStats",
    "NormalizedSeasonality",
]


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
    underlying_price: Decimal | None = None
    delta: Decimal | None = None
    gamma: Decimal | None = None
    theta: Decimal | None = None
    vega: Decimal | None = None
    rho: Decimal | None = None
    iv: Decimal | None = None
    provider: str
    timestamp: datetime


class NormalizedOptionTrade(BaseModel):
    """Normalized option trade from UW option trades endpoint.

    Based on the UW OpenAPI spec Option Trade schema with all fields mapped.
    """

    underlying_symbol: str  # Underlying ticker
    option_symbol: str | None = None  # OCC option symbol (option_chain_id in UW)
    timestamp: datetime  # executed_at in UW
    strike: Decimal
    expiry: str  # YYYY-MM-DD
    option_type: str  # "call" or "put"
    price: Decimal  # Trade price
    size: int  # Number of contracts
    premium: Decimal  # Total premium (price * size * 100)
    volume: int | None = None  # Total volume for this contract
    open_interest: int | None = None  # Current OI
    underlying_price: Decimal | None = None  # Stock price at time of trade
    # Greeks at time of trade
    delta: Decimal | None = None
    gamma: Decimal | None = None
    theta: Decimal | None = None
    vega: Decimal | None = None
    rho: Decimal | None = None
    implied_volatility: Decimal | None = None
    # NBBO at time of trade
    nbbo_ask: Decimal | None = None
    nbbo_bid: Decimal | None = None
    # Volume breakdowns
    ask_vol: int | None = None  # Volume on ask side
    bid_vol: int | None = None  # Volume on bid side
    mid_vol: int | None = None  # Volume at midpoint
    multi_vol: int | None = None  # Multi-leg volume
    stock_multi_vol: int | None = None  # Stock+multi-leg volume
    no_side_vol: int | None = None  # Volume with no side
    # Metadata
    exchange: str | None = None  # Exchange where traded
    trade_id: str | None = None  # flow_alert_id or id in UW
    canceled: bool = False
    er_time: str | None = None  # Earnings report time
    next_earnings_date: str | None = None
    sector: str | None = None
    tags: list[str] = Field(default_factory=list)
    provider: str = "unusual_whales"


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


class NormalizedGreekExposure(BaseModel):
    """Greek exposure (GEX/DEX/VEX) data -- split by call/put per UW API."""

    symbol: str
    timestamp: datetime
    call_gamma: Decimal
    put_gamma: Decimal | None = None
    call_delta: Decimal | None = None
    put_delta: Decimal | None = None
    call_vanna: Decimal | None = None
    put_vanna: Decimal | None = None
    call_charm: Decimal | None = None
    put_charm: Decimal | None = None
    strike: Decimal | None = None  # For strike-level data
    expiry: str | None = None  # For expiry-level data
    dte: int | None = None  # For expiry-level data
    provider: str = "unusual_whales"


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
