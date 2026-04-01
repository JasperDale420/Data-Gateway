"""Options models — re-exported from empire_schemas, plus gateway-specific models."""

from datetime import datetime
from decimal import Decimal

from empire_schemas.analytics import (
    NormalizedIVRank,
    NormalizedIVTermStructure,
    NormalizedMaxPain,
    NormalizedSeasonality,
    NormalizedVolatilityStats,
)
from empire_schemas.options import (
    NormalizedGreekExposure,
    NormalizedHottestChain,
    NormalizedOptionContract,
)
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


# Gateway-specific model (not in empire_schemas)
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
