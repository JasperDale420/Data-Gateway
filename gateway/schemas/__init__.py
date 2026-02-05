"""Pydantic schemas for messages and data."""

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field

# WebSocket Messages


class AuthMessage(BaseModel):
    """Client authentication message."""

    action: Literal["auth"]
    key: str
    request_id: str | None = None


class SubscribeMessage(BaseModel):
    """Subscribe to symbols."""

    action: Literal["subscribe"]
    symbols: list[str]
    feeds: list[str] = Field(default_factory=lambda: ["bars"])
    request_id: str | None = None


class UnsubscribeMessage(BaseModel):
    """Unsubscribe from symbols."""

    action: Literal["unsubscribe"]
    symbols: list[str]
    request_id: str | None = None


class AuthResult(BaseModel):
    """Authentication result."""

    type: Literal["auth_result"]
    status: Literal["ok", "error"]
    client_id: str | None = None
    code: str | None = None
    message: str | None = None


class SubscriptionAck(BaseModel):
    """Subscription acknowledgement."""

    type: Literal["subscription_ack"]
    subscribed: list[str]
    failed: list[str] = Field(default_factory=list)


# Normalized Data Schemas


class NormalizedBar(BaseModel):
    """Normalized OHLCV bar."""

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    vwap: Decimal | None = None
    trade_count: int | None = None
    provider: str
    timeframe: str = "1Min"  # 1Min, 5Min, 15Min, 1Hour, 1Day


class NormalizedQuote(BaseModel):
    """Normalized quote (bid/ask)."""

    symbol: str
    timestamp: datetime
    bid_price: Decimal
    bid_size: int
    ask_price: Decimal
    ask_size: int
    bid_exchange: str | None = None  # Alpaca WS: bx
    ask_exchange: str | None = None  # Alpaca WS: ax
    conditions: list[str] = Field(default_factory=list)  # Alpaca WS: c
    tape: str | None = None  # Alpaca WS: z (A=NYSE, B=ARCA, C=NASDAQ)
    provider: str


class NormalizedTrade(BaseModel):
    """Normalized trade."""

    symbol: str
    timestamp: datetime
    price: Decimal
    size: int
    trade_id: str | None = None  # Alpaca WS: i - Unique trade identifier
    exchange: str | None = None  # Alpaca WS: x (stocks only)
    conditions: list[str] = Field(default_factory=list)  # Alpaca WS: c (stocks only)
    tape: str | None = None  # Alpaca WS: z (stocks: A=NYSE, B=ARCA, C=NASDAQ)
    taker_side: str | None = None  # Alpaca WS: tks (crypto: B=buy, S=sell)
    provider: str


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
    ext_hours: str | None = None  # ext_hour_sold_codes
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
    delta: Decimal | None = None
    gamma: Decimal | None = None
    theta: Decimal | None = None
    vega: Decimal | None = None
    iv: Decimal | None = None
    provider: str
    timestamp: datetime


# Phase 1 Schemas: News, Greek Exposure, Earnings, Screeners


class NormalizedNewsArticle(BaseModel):
    """Normalized news article."""

    article_id: str
    headline: str
    summary: str | None = None
    content: str | None = None
    url: str | None = None
    source: str
    author: str | None = None
    published_at: datetime
    symbols: list[str] = Field(default_factory=list)
    provider: str


class NormalizedGreekExposure(BaseModel):
    """Greek exposure (GEX/DEX/VEX) data."""

    symbol: str
    timestamp: datetime
    gamma_exposure: Decimal
    delta_exposure: Decimal | None = None
    vanna_exposure: Decimal | None = None
    charm_exposure: Decimal | None = None
    strike: Decimal | None = None  # For strike-level data
    expiry: str | None = None  # For expiry-level data
    provider: str = "unusual_whales"


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


class NormalizedMostActive(BaseModel):
    """Most active stock data."""

    symbol: str
    volume: int
    trade_count: int
    provider: str


class NormalizedMover(BaseModel):
    """Top mover (gainer/loser) data."""

    symbol: str
    price: Decimal
    change: Decimal
    percent_change: Decimal
    provider: str


class NormalizedScreenerResult(BaseModel):
    """Stock screener result."""

    symbol: str
    price: Decimal | None = None
    volume: int | None = None
    market_cap: Decimal | None = None
    sector: str | None = None
    call_volume: int | None = None
    put_volume: int | None = None
    iv_rank: Decimal | None = None
    provider: str


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


# ─────────────────────────────────────────────────────────────────
# Phase 2: Analytics Schemas
# ─────────────────────────────────────────────────────────────────


class NormalizedNetPremiumTick(BaseModel):
    """Net premium tick data."""

    symbol: str
    timestamp: datetime
    net_call_premium: Decimal
    net_put_premium: Decimal
    call_volume: int
    put_volume: int
    provider: str


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


class NormalizedOIChange(BaseModel):
    """Open interest change data."""

    symbol: str
    date: str
    call_oi: int
    put_oi: int
    call_oi_change: int
    put_oi_change: int
    provider: str


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


class NormalizedShortData(BaseModel):
    """Short interest data."""

    symbol: str
    date: str
    short_interest: int
    days_to_cover: Decimal | None = None
    short_percent_float: Decimal | None = None
    short_percent_outstanding: Decimal | None = None
    provider: str


class NormalizedFTD(BaseModel):
    """Failure to deliver data."""

    symbol: str
    date: str
    quantity: int
    price: Decimal | None = None
    value: Decimal | None = None
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


# ─────────────────────────────────────────────────────────────────
# Alternative Data: Insider, Institution, Politician Trades
# ─────────────────────────────────────────────────────────────────


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


class NormalizedForexRate(BaseModel):
    """Forex currency pair rate."""

    pair: str  # e.g., "EUR/USD"
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    mid: Decimal | None = None  # Computed: (bid + ask) / 2
    open: Decimal | None = None  # For bars
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    provider: str


class NormalizedFundamentals(BaseModel):
    """Company fundamentals from various providers."""

    symbol: str
    name: str | None = None
    sector: str | None = None
    industry: str | None = None
    market_cap: Decimal | None = None
    pe_ratio: Decimal | None = None  # Price/Earnings
    forward_pe: Decimal | None = None
    peg_ratio: Decimal | None = None  # P/E to Growth
    pb_ratio: Decimal | None = None  # Price/Book
    ps_ratio: Decimal | None = None  # Price/Sales
    dividend_yield: Decimal | None = None
    dividend_per_share: Decimal | None = None
    eps: Decimal | None = None  # Earnings per share
    eps_growth: Decimal | None = None  # YoY EPS growth
    revenue: Decimal | None = None
    revenue_growth: Decimal | None = None
    profit_margin: Decimal | None = None
    operating_margin: Decimal | None = None
    roe: Decimal | None = None  # Return on equity
    roa: Decimal | None = None  # Return on assets
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


# ─────────────────────────────────────────────────────────────────
# Phase 3: Advanced Analytics Schemas
# ─────────────────────────────────────────────────────────────────


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


class NormalizedOrderbookLevel(BaseModel):
    """Orderbook level data."""

    price: Decimal
    size: Decimal
    side: str  # "bid" or "ask"


class NormalizedOrderbook(BaseModel):
    """Full orderbook snapshot."""

    symbol: str
    timestamp: datetime
    bids: list[NormalizedOrderbookLevel]
    asks: list[NormalizedOrderbookLevel]
    provider: str


# API Response Schemas


class HealthResponse(BaseModel):
    """Health check response."""

    status: str


class ErrorResponse(BaseModel):
    """Error response."""

    success: bool = False
    error: dict


class SuccessResponse(BaseModel):
    """Generic success response."""

    success: bool = True
    data: dict | list | None = None  # Can be dict, list, or None depending on endpoint
    meta: dict | None = None
    pagination: dict | None = None  # For paginated endpoints


# ─────────────────────────────────────────────────────────────────
# Alpaca API Response Schemas
# ─────────────────────────────────────────────────────────────────


class ResponseMeta(BaseModel):
    """Common metadata for API responses."""

    provider: str = "alpaca"
    symbol: str | None = None
    timeframe: str | None = None
    count: int | None = None


# Stock Market Data Responses


class StockSnapshot(BaseModel):
    """Stock snapshot data."""

    daily_bar: NormalizedBar | None = None
    latest_quote: NormalizedQuote | None = None
    latest_trade: NormalizedTrade | None = None
    minute_bar: NormalizedBar | None = None
    prev_daily_bar: NormalizedBar | None = None


class Auction(BaseModel):
    """Auction data."""

    date: str
    opening: list[dict] = Field(default_factory=list)
    closing: list[dict] = Field(default_factory=list)


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


# Trading API Responses


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
