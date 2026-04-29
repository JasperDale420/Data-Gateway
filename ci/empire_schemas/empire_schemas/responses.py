"""API response models and typed response wrappers."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from empire_schemas.analytics import (
    NormalizedMostActive,
    NormalizedMover,
    NormalizedOrderbook,
)
from empire_schemas.fundamentals import NormalizedCorporateAction
from empire_schemas.market_data import NormalizedBar, NormalizedQuote, NormalizedTrade
from empire_schemas.options import NormalizedOptionContract

# ─── WebSocket Protocol Messages ───


class AuthMessage(BaseModel):
    """Client authentication message."""

    action: str = "auth"
    key: str
    request_id: str | None = None


class SubscribeMessage(BaseModel):
    """Subscribe to symbols."""

    action: str = "subscribe"
    symbols: list[str]
    feeds: list[str] = Field(default_factory=lambda: ["bars"])
    feed: str | None = Field(default=None, description="Legacy: use feeds instead")
    request_id: str | None = None


class UnsubscribeMessage(BaseModel):
    """Unsubscribe from symbols."""

    action: str = "unsubscribe"
    symbols: list[str]
    feeds: list[str] | None = None
    feed: str | None = Field(default=None, description="Legacy: use feeds instead")
    request_id: str | None = None


class AuthResult(BaseModel):
    """Authentication result."""

    type: str = "auth_result"
    status: str  # "ok" or "error"
    client_id: str | None = None
    code: str | None = None
    message: str | None = None


class SubscriptionAck(BaseModel):
    """Subscription acknowledgement."""

    type: str = "subscription_ack"
    subscribed: list[str]
    failed: list[str] = Field(default_factory=list)
    feeds: list[str] | None = None


# ─── API Response Envelopes ───


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
    data: dict | list | None = None
    meta: dict | None = None
    pagination: dict | None = None


class ResponseMeta(BaseModel):
    """Common metadata for API responses."""

    provider: str = "alpaca"
    symbol: str | None = None
    timeframe: str | None = None
    count: int | None = None


# ─── Stock Data Responses ───


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
    success: bool = True
    data: list[NormalizedBar]
    meta: ResponseMeta


class StockQuoteResponse(BaseModel):
    success: bool = True
    data: NormalizedQuote
    meta: ResponseMeta


class StockTradesResponse(BaseModel):
    success: bool = True
    data: list[NormalizedTrade]
    meta: ResponseMeta


class StockSnapshotResponse(BaseModel):
    success: bool = True
    data: StockSnapshot
    meta: ResponseMeta


class LatestBarsResponse(BaseModel):
    success: bool = True
    data: dict[str, NormalizedBar]
    meta: ResponseMeta


class LatestTradesResponse(BaseModel):
    success: bool = True
    data: dict[str, NormalizedTrade]
    meta: ResponseMeta


class HistoricalQuotesResponse(BaseModel):
    success: bool = True
    data: dict[str, list[NormalizedQuote]]
    meta: ResponseMeta


class SnapshotsResponse(BaseModel):
    success: bool = True
    data: dict[str, StockSnapshot]
    meta: ResponseMeta


class AuctionsResponse(BaseModel):
    success: bool = True
    data: dict[str, list[Auction]]
    meta: ResponseMeta


# ─── Options Responses ───


class OptionChainResponse(BaseModel):
    success: bool = True
    data: list[NormalizedOptionContract]
    meta: ResponseMeta


class OptionSnapshotResponse(BaseModel):
    success: bool = True
    data: dict
    meta: ResponseMeta


class OptionBarsResponse(BaseModel):
    success: bool = True
    data: list[NormalizedBar]
    meta: ResponseMeta


class OptionQuoteResponse(BaseModel):
    success: bool = True
    data: dict
    meta: ResponseMeta


class OptionTradesResponse(BaseModel):
    success: bool = True
    data: dict[str, list]
    meta: ResponseMeta


class OptionLatestTradesResponse(BaseModel):
    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta


class OptionSnapshotsResponse(BaseModel):
    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta


# ─── Crypto Responses ───


class CryptoBarsResponse(BaseModel):
    success: bool = True
    data: list[NormalizedBar]
    meta: ResponseMeta


class CryptoTradesResponse(BaseModel):
    success: bool = True
    data: list[NormalizedTrade]
    meta: ResponseMeta


class CryptoQuoteResponse(BaseModel):
    success: bool = True
    data: NormalizedQuote
    meta: ResponseMeta


class CryptoSnapshotResponse(BaseModel):
    success: bool = True
    data: dict
    meta: ResponseMeta


class CryptoLatestBarsResponse(BaseModel):
    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta


class CryptoLatestTradesResponse(BaseModel):
    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta


class CryptoOrderbookResponse(BaseModel):
    success: bool = True
    data: NormalizedOrderbook | dict
    meta: ResponseMeta


# ─── Forex Responses ───


class ForexRatesResponse(BaseModel):
    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta


class ForexHistoricalResponse(BaseModel):
    success: bool = True
    data: dict[str, list]
    meta: ResponseMeta


# ─── News & Screener Responses ───


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


class NewsResponse(BaseModel):
    success: bool = True
    data: list[NormalizedNewsArticle]
    meta: ResponseMeta


class MostActivesResponse(BaseModel):
    success: bool = True
    data: list[NormalizedMostActive]
    meta: ResponseMeta


class MoversData(BaseModel):
    gainers: list[NormalizedMover]
    losers: list[NormalizedMover]


class MoversResponse(BaseModel):
    success: bool = True
    data: MoversData
    meta: ResponseMeta


class CorporateActionsResponse(BaseModel):
    success: bool = True
    data: list[NormalizedCorporateAction]
    meta: ResponseMeta


class ConditionCodesResponse(BaseModel):
    success: bool = True
    data: dict
    meta: ResponseMeta


class ExchangeCodesResponse(BaseModel):
    success: bool = True
    data: dict
    meta: ResponseMeta


class FixedIncomeResponse(BaseModel):
    success: bool = True
    data: dict[str, dict]
    meta: ResponseMeta
