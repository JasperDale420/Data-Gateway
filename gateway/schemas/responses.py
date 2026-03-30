"""All Response wrappers and Alpaca account models."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from gateway.schemas.base import ResponseMeta
from gateway.schemas.corporate import NormalizedCorporateAction
from gateway.schemas.market_data import (
    Auction,
    NormalizedBar,
    NormalizedMostActive,
    NormalizedMover,
    NormalizedOrderbook,
    NormalizedQuote,
    NormalizedTrade,
    StockSnapshot,
)
from gateway.schemas.news import NormalizedNewsArticle
from gateway.schemas.options import NormalizedOptionContract

__all__ = [
    # Stock Market Data Responses
    "StockBarsResponse",
    "StockQuoteResponse",
    "StockTradesResponse",
    "StockSnapshotResponse",
    "LatestBarsResponse",
    "LatestTradesResponse",
    "HistoricalQuotesResponse",
    "SnapshotsResponse",
    "AuctionsResponse",
    # Options Market Data Responses
    "OptionChainResponse",
    "OptionSnapshotResponse",
    "OptionBarsResponse",
    "OptionQuoteResponse",
    "OptionTradesResponse",
    "OptionLatestTradesResponse",
    "OptionSnapshotsResponse",
    # Crypto Market Data Responses
    "CryptoBarsResponse",
    "CryptoTradesResponse",
    "CryptoQuoteResponse",
    "CryptoSnapshotResponse",
    "CryptoLatestBarsResponse",
    "CryptoLatestTradesResponse",
    "CryptoOrderbookResponse",
    # Forex Responses
    "ForexRatesResponse",
    "ForexHistoricalResponse",
    # News & Screener Responses
    "NewsResponse",
    "MostActivesResponse",
    "MoversData",
    "MoversResponse",
    # Corporate Actions & Meta Responses
    "CorporateActionsResponse",
    "ConditionCodesResponse",
    "ExchangeCodesResponse",
    "FixedIncomeResponse",
    # Alpaca Account Models
    "Account",
    "Order",
    "Position",
    "PortfolioHistory",
    "Watchlist",
    "Clock",
    "Calendar",
    "Asset",
    "AccountConfigurations",
    "Activity",
    # Trading Response Wrappers
    "AccountResponse",
    "OrderResponse",
    "OrdersListResponse",
    "PositionResponse",
    "PositionsListResponse",
    "PortfolioHistoryResponse",
    "WatchlistResponse",
    "WatchlistsListResponse",
    "ClockResponse",
    "CalendarResponse",
    "AccountConfigResponse",
    "ActivitiesResponse",
    "AssetsResponse",
    "AssetResponse",
]


# Stock Market Data Responses


class StockBarsResponse(BaseModel):
    """Response for stock bars endpoint."""

    success: bool = True
    data: list[NormalizedBar]
    meta: ResponseMeta


class StockQuoteResponse(BaseModel):
    """Response for stock quote endpoint."""

    success: bool = True
    data: NormalizedQuote
    meta: ResponseMeta


class StockTradesResponse(BaseModel):
    """Response for stock trades endpoint."""

    success: bool = True
    data: list[NormalizedTrade]
    meta: ResponseMeta


class StockSnapshotResponse(BaseModel):
    """Response for stock snapshot endpoint."""

    success: bool = True
    data: StockSnapshot
    meta: ResponseMeta


class LatestBarsResponse(BaseModel):
    """Response for latest bars endpoint."""

    success: bool = True
    data: dict[str, NormalizedBar]
    meta: ResponseMeta


class LatestTradesResponse(BaseModel):
    """Response for latest trades endpoint."""

    success: bool = True
    data: dict[str, NormalizedTrade]
    meta: ResponseMeta


class HistoricalQuotesResponse(BaseModel):
    """Response for historical quotes endpoint."""

    success: bool = True
    data: dict[str, list[NormalizedQuote]]
    meta: ResponseMeta


class SnapshotsResponse(BaseModel):
    """Response for snapshots endpoint."""

    success: bool = True
    data: dict[str, StockSnapshot]
    meta: ResponseMeta


class AuctionsResponse(BaseModel):
    """Response for auctions endpoint."""

    success: bool = True
    data: dict[str, list[Auction]]
    meta: ResponseMeta


# Options Market Data Responses


class OptionChainResponse(BaseModel):
    """Response for option chain endpoint."""

    success: bool = True
    data: list[NormalizedOptionContract]
    meta: ResponseMeta


class OptionSnapshotResponse(BaseModel):
    """Response for option snapshot endpoint."""

    success: bool = True
    data: dict
    meta: ResponseMeta


class OptionBarsResponse(BaseModel):
    """Response for option bars endpoint."""

    success: bool = True
    data: list[NormalizedBar]
    meta: ResponseMeta


class OptionQuoteResponse(BaseModel):
    """Response for option quote endpoint."""

    success: bool = True
    data: dict
    meta: ResponseMeta


class OptionTradesResponse(BaseModel):
    """Response for option trades endpoint."""

    success: bool = True
    data: dict[str, list]
    meta: ResponseMeta


class OptionLatestTradesResponse(BaseModel):
    """Response for option latest trades endpoint."""

    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta


class OptionSnapshotsResponse(BaseModel):
    """Response for option snapshots endpoint."""

    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta


# Crypto Market Data Responses


class CryptoBarsResponse(BaseModel):
    """Response for crypto bars endpoint."""

    success: bool = True
    data: list[NormalizedBar]
    meta: ResponseMeta


class CryptoTradesResponse(BaseModel):
    """Response for crypto trades endpoint."""

    success: bool = True
    data: list[NormalizedTrade]
    meta: ResponseMeta


class CryptoQuoteResponse(BaseModel):
    """Response for crypto quote endpoint."""

    success: bool = True
    data: NormalizedQuote
    meta: ResponseMeta


class CryptoSnapshotResponse(BaseModel):
    """Response for crypto snapshot endpoint."""

    success: bool = True
    data: dict
    meta: ResponseMeta


class CryptoLatestBarsResponse(BaseModel):
    """Response for crypto latest bars endpoint."""

    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta


class CryptoLatestTradesResponse(BaseModel):
    """Response for crypto latest trades endpoint."""

    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta


class CryptoOrderbookResponse(BaseModel):
    """Response for crypto orderbook endpoint."""

    success: bool = True
    data: NormalizedOrderbook | dict
    meta: ResponseMeta


# Forex Responses


class ForexRatesResponse(BaseModel):
    """Response for forex rates endpoint."""

    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta


class ForexHistoricalResponse(BaseModel):
    """Response for forex historical endpoint."""

    success: bool = True
    data: dict[str, list]
    meta: ResponseMeta


# News & Screener Responses


class NewsResponse(BaseModel):
    """Response for news endpoint."""

    success: bool = True
    data: list[NormalizedNewsArticle]
    meta: ResponseMeta


class MostActivesResponse(BaseModel):
    """Response for most actives endpoint."""

    success: bool = True
    data: list[NormalizedMostActive]
    meta: ResponseMeta


class MoversData(BaseModel):
    """Movers data structure."""

    gainers: list[NormalizedMover]
    losers: list[NormalizedMover]


class MoversResponse(BaseModel):
    """Response for movers endpoint."""

    success: bool = True
    data: MoversData
    meta: ResponseMeta


# Corporate Actions & Meta Responses


class CorporateActionsResponse(BaseModel):
    """Response for corporate actions endpoint."""

    success: bool = True
    data: list[NormalizedCorporateAction]
    meta: ResponseMeta


class ConditionCodesResponse(BaseModel):
    """Response for condition codes endpoint."""

    success: bool = True
    data: dict
    meta: ResponseMeta


class ExchangeCodesResponse(BaseModel):
    """Response for exchange codes endpoint."""

    success: bool = True
    data: dict
    meta: ResponseMeta


class FixedIncomeResponse(BaseModel):
    """Response for fixed income prices endpoint."""

    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta


# Alpaca Account Models


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


# Trading Response Wrappers


class AccountResponse(BaseModel):
    """Response for account endpoint."""

    success: bool = True
    data: Account
    meta: ResponseMeta | None = None


class OrderResponse(BaseModel):
    """Response for order endpoint."""

    success: bool = True
    data: Order
    meta: ResponseMeta | None = None


class OrdersListResponse(BaseModel):
    """Response for orders list endpoint."""

    success: bool = True
    data: list[Order]
    meta: ResponseMeta | None = None


class PositionResponse(BaseModel):
    """Response for position endpoint."""

    success: bool = True
    data: Position
    meta: ResponseMeta | None = None


class PositionsListResponse(BaseModel):
    """Response for positions list endpoint."""

    success: bool = True
    data: list[Position]
    meta: ResponseMeta | None = None


class PortfolioHistoryResponse(BaseModel):
    """Response for portfolio history endpoint."""

    success: bool = True
    data: PortfolioHistory
    meta: ResponseMeta | None = None


class WatchlistResponse(BaseModel):
    """Response for watchlist endpoint."""

    success: bool = True
    data: Watchlist
    meta: ResponseMeta | None = None


class WatchlistsListResponse(BaseModel):
    """Response for watchlists list endpoint."""

    success: bool = True
    data: list[Watchlist]
    meta: ResponseMeta | None = None


class ClockResponse(BaseModel):
    """Response for clock endpoint."""

    success: bool = True
    data: Clock
    meta: ResponseMeta | None = None


class CalendarResponse(BaseModel):
    """Response for calendar endpoint."""

    success: bool = True
    data: list[Calendar]
    meta: ResponseMeta | None = None


class AccountConfigResponse(BaseModel):
    """Response for account config endpoint."""

    success: bool = True
    data: AccountConfigurations
    meta: ResponseMeta | None = None


class ActivitiesResponse(BaseModel):
    """Response for activities endpoint."""

    success: bool = True
    data: list[Activity]
    meta: ResponseMeta | None = None


class AssetsResponse(BaseModel):
    """Response for assets list endpoint."""

    success: bool = True
    data: list[Asset]
    meta: ResponseMeta | None = None


class AssetResponse(BaseModel):
    """Response for asset endpoint."""

    success: bool = True
    data: Asset
    meta: ResponseMeta | None = None
