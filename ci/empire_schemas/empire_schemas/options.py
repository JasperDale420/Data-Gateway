"""Options data models — flow alerts, contracts, greek exposure, hottest chains."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class NormalizedFlowAlert(BaseModel):
    """Normalized options flow alert from UW flow endpoint."""

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
    option_chain: str | None = None  # OCC contract symbol
    price: Decimal | None = None
    underlying_price: Decimal | None = None
    alert_rule: str | None = None
    total_size: int | None = None
    trade_count: int | None = None
    volume_oi_ratio: Decimal | None = None
    total_ask_side_prem: Decimal | None = None
    total_bid_side_prem: Decimal | None = None
    all_opening_trades: bool = False
    has_floor: bool = False
    has_multileg: bool = False
    has_singleleg: bool = True
    expiry_count: int | None = None
    provider: str = "unusual_whales"


class NormalizedOptionContract(BaseModel):
    """Normalized option contract with greeks."""

    contract_symbol: str  # OCC format
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
    underlying_price: Decimal | None = None
    provider: str
    timestamp: datetime


class NormalizedGreekExposure(BaseModel):
    """Greek exposure (GEX/DEX/VEX) data — split by call/put per UW API."""

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
    strike: Decimal | None = None
    expiry: str | None = None
    dte: int | None = None
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
