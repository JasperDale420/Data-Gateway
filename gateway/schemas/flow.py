"""Flow/darkpool models."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

__all__ = [
    "NormalizedFlowAlert",
    "NormalizedDarkpoolTrade",
    "NormalizedMarketTide",
    "NormalizedSectorTide",
    "NormalizedNetPremiumTick",
    "NormalizedOIChange",
]


class NormalizedFlowAlert(BaseModel):
    """Normalized options flow alert from UW flow endpoint."""

    symbol: str  # ticker in UW
    timestamp: datetime  # created_at in UW
    strike: Decimal
    expiry: str  # YYYY-MM-DD format
    put_call: str  # "put" or "call" (type in UW)
    premium: Decimal  # total_premium in UW
    volume: int
    open_interest: int
    side: str  # "bid", "ask", "mid"
    is_sweep: bool = False  # has_sweep in UW
    is_unusual: bool = False
    sentiment: str | None = None  # "bullish", "bearish"
    # Additional UW fields
    option_chain: str | None = None  # OCC contract symbol (e.g., MSFT231222C00375000)
    price: Decimal | None = None  # Option price at alert
    underlying_price: Decimal | None = None  # Stock price at alert
    alert_rule: str | None = None  # RepeatedHits, FloorTrade, etc.
    total_size: int | None = None  # Total contracts traded
    trade_count: int | None = None  # Number of trades in alert
    volume_oi_ratio: Decimal | None = None  # Volume / OI ratio
    total_ask_side_prem: Decimal | None = None  # Ask-side premium
    total_bid_side_prem: Decimal | None = None  # Bid-side premium
    all_opening_trades: bool = False  # All trades are opening
    has_floor: bool = False  # Has floor trades
    has_multileg: bool = False  # Multi-leg order
    has_singleleg: bool = True  # Single-leg order
    expiry_count: int | None = None  # Number of expiries in alert
    provider: str = "unusual_whales"


class NormalizedDarkpoolTrade(BaseModel):
    """Normalized darkpool trade from UW darkpool endpoint."""

    symbol: str
    timestamp: datetime
    price: Decimal
    size: int
    notional: Decimal  # premium in UW (price * size)
    exchange: str | None = None  # market_center in UW
    tracking_id: str | None = None  # Unique trade ID for dedup
    nbbo_bid: Decimal | None = None  # NBBO bid at time of trade
    nbbo_ask: Decimal | None = None  # NBBO ask at time of trade
    nbbo_bid_size: int | None = None  # NBBO bid quantity (nbbo_bid_quantity in UW)
    nbbo_ask_size: int | None = None  # NBBO ask quantity (nbbo_ask_quantity in UW)
    ext_hours: str | None = None  # ext_hour_sold_codes
    sale_cond_codes: str | None = None  # Sale condition codes
    trade_code: str | None = None  # Trade code
    trade_settlement: str | None = None  # regular_settlement, etc.
    canceled: bool = False  # Whether trade was cancelled
    provider: str = "unusual_whales"


class NormalizedMarketTide(BaseModel):
    """Market sentiment snapshot from UW market/tide endpoint."""

    timestamp: datetime
    date: str | None = None  # Trading date YYYY-MM-DD
    net_call_premium: Decimal
    net_put_premium: Decimal
    net_volume: int | None = None  # Net volume from UW (call - put)
    sentiment: str  # "bullish", "bearish", "neutral" (computed)
    call_put_ratio: Decimal | None = None  # Computed: net_call_premium / net_put_premium
    provider: str = "unusual_whales"


class NormalizedSectorTide(BaseModel):
    """Sector sentiment snapshot from UW sector/tide endpoint.

    Per-sector GICS tide data with call/put premium breakdown.
    """

    timestamp: datetime
    sector: str  # GICS sector name (e.g., "Technology", "Energy")
    net_call_premium: Decimal
    net_put_premium: Decimal
    net_volume: int | None = None
    sentiment: str  # "bullish", "bearish", "neutral"
    call_put_ratio: Decimal | None = None  # Computed: net_call_premium / net_put_premium
    provider: str = "unusual_whales"


class NormalizedNetPremiumTick(BaseModel):
    """Net premium tick data."""

    symbol: str
    timestamp: datetime
    net_call_premium: Decimal
    net_put_premium: Decimal
    call_volume: int
    put_volume: int
    # Additional UW fields from OpenAPI spec
    net_delta: Decimal | None = None  # Net delta exposure
    net_call_volume: int | None = None  # Net call volume
    net_put_volume: int | None = None  # Net put volume
    call_volume_ask_side: int | None = None  # Call volume on ask side
    call_volume_bid_side: int | None = None  # Call volume on bid side
    put_volume_ask_side: int | None = None  # Put volume on ask side
    put_volume_bid_side: int | None = None  # Put volume on bid side
    tape_time: str | None = None  # Tape time from exchange
    provider: str


class NormalizedOIChange(BaseModel):
    """Open interest change data."""

    symbol: str
    date: str
    call_oi: int
    put_oi: int
    call_oi_change: int
    put_oi_change: int
    # Additional UW fields from OpenAPI spec
    avg_price: Decimal | None = None  # Average fill price
    prev_oi: int | None = None  # Previous open interest (last_oi in UW)
    option_symbol: str | None = None  # OCC option symbol
    volume: int | None = None  # Total volume
    trades: int | None = None  # Number of trades
    provider: str
