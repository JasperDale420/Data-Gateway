"""Trading API models — accounts, orders, positions, portfolio history."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from empire_schemas.responses import ResponseMeta


class Account(BaseModel):
    """Trading account data."""

    id: str
    account_number: str
    status: str
    currency: str
    cash: Decimal
    portfolio_value: Decimal
    buying_power: Decimal
    equity: Decimal
    pattern_day_trader: bool = False
    shorting_enabled: bool = False
    daytrade_count: int = 0


class Order(BaseModel):
    """Order data."""

    id: str
    client_order_id: str | None = None
    created_at: datetime
    updated_at: datetime | None = None
    submitted_at: datetime | None = None
    filled_at: datetime | None = None
    expired_at: datetime | None = None
    canceled_at: datetime | None = None
    asset_id: str | None = None
    symbol: str
    asset_class: str | None = None
    qty: str | None = None
    filled_qty: str | None = None
    notional: str | None = None
    side: str
    type: str
    time_in_force: str
    limit_price: str | None = None
    stop_price: str | None = None
    filled_avg_price: str | None = None
    status: str
    extended_hours: bool = False


class Position(BaseModel):
    """Position data."""

    asset_id: str
    symbol: str
    exchange: str | None = None
    asset_class: str
    avg_entry_price: Decimal
    qty: Decimal
    qty_available: Decimal | None = None
    side: str
    market_value: Decimal
    cost_basis: Decimal
    unrealized_pl: Decimal
    unrealized_plpc: Decimal
    current_price: Decimal
    change_today: Decimal | None = None


class PortfolioHistory(BaseModel):
    """Portfolio history data."""

    timestamp: list[int]
    equity: list[Decimal]
    profit_loss: list[Decimal]
    profit_loss_pct: list[Decimal]
    base_value: Decimal
    timeframe: str


class Watchlist(BaseModel):
    """Watchlist data."""

    id: str
    account_id: str
    created_at: datetime
    updated_at: datetime | None = None
    name: str
    assets: list[dict] = Field(default_factory=list)


class Clock(BaseModel):
    """Market clock data."""

    timestamp: datetime
    is_open: bool
    next_open: datetime
    next_close: datetime


class Calendar(BaseModel):
    """Trading calendar entry."""

    date: str
    open: str
    close: str
    settlement_date: str | None = None


class Asset(BaseModel):
    """Asset data."""

    id: str
    class_: str = Field(alias="class")
    exchange: str
    symbol: str
    name: str
    status: str
    tradable: bool
    marginable: bool
    shortable: bool
    fractionable: bool


class AccountConfigurations(BaseModel):
    """Account configurations."""

    dtbp_check: str | None = None
    trade_confirm_email: str | None = None
    suspend_trade: bool = False
    no_shorting: bool = False
    fractional_trading: bool = False
    max_margin_multiplier: str | None = None
    pdt_check: str | None = None


class Activity(BaseModel):
    """Account activity."""

    id: str
    activity_type: str
    transaction_time: datetime | None = None
    symbol: str | None = None
    qty: str | None = None
    price: str | None = None
    side: str | None = None
    net_amount: str | None = None


# ─── Trading Response Wrappers ───


class AccountResponse(BaseModel):
    success: bool = True
    data: Account
    meta: ResponseMeta | None = None


class OrderResponse(BaseModel):
    success: bool = True
    data: Order
    meta: ResponseMeta | None = None


class OrdersListResponse(BaseModel):
    success: bool = True
    data: list[Order]
    meta: ResponseMeta | None = None


class PositionResponse(BaseModel):
    success: bool = True
    data: Position
    meta: ResponseMeta | None = None


class PositionsListResponse(BaseModel):
    success: bool = True
    data: list[Position]
    meta: ResponseMeta | None = None


class PortfolioHistoryResponse(BaseModel):
    success: bool = True
    data: PortfolioHistory
    meta: ResponseMeta | None = None


class WatchlistResponse(BaseModel):
    success: bool = True
    data: Watchlist
    meta: ResponseMeta | None = None


class WatchlistsListResponse(BaseModel):
    success: bool = True
    data: list[Watchlist]
    meta: ResponseMeta | None = None


class ClockResponse(BaseModel):
    success: bool = True
    data: Clock
    meta: ResponseMeta | None = None


class CalendarResponse(BaseModel):
    success: bool = True
    data: list[Calendar]
    meta: ResponseMeta | None = None


class AccountConfigResponse(BaseModel):
    success: bool = True
    data: AccountConfigurations
    meta: ResponseMeta | None = None


class ActivitiesResponse(BaseModel):
    success: bool = True
    data: list[Activity]
    meta: ResponseMeta | None = None


class AssetsResponse(BaseModel):
    success: bool = True
    data: list[Asset]
    meta: ResponseMeta | None = None


class AssetResponse(BaseModel):
    success: bool = True
    data: Asset
    meta: ResponseMeta | None = None
