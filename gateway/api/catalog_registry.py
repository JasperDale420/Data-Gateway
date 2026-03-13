"""Structured endpoint registry for parameter-level API discovery.

This module defines every REST endpoint exposed by the Data Gateway, including
path parameters, query parameters, response models, use-case tags, and example
requests. It is the single source of truth consumed by:

    * ``/catalog/endpoints`` — full searchable registry
    * ``/catalog/search``    — keyword / tag search
    * ``/catalog/capabilities`` — use-case grouping
    * Downstream systems that ``from gateway.api.catalog_registry import ENDPOINT_REGISTRY``
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PathParam:
    """Describes a path parameter such as ``{symbol}``."""

    name: str
    type: str  # "string", "integer", etc.
    description: str


@dataclass(frozen=True, slots=True)
class QueryParam:
    """Describes a query parameter for an endpoint."""

    name: str
    type: str
    required: bool = False
    default: Any = None
    valid_values: list[str] | None = None
    description: str = ""


@dataclass(frozen=True, slots=True)
class EndpointDef:
    """Full definition of a single REST endpoint."""

    path: str
    method: str
    description: str
    provider: str
    path_params: list[PathParam] = field(default_factory=list)
    query_params: list[QueryParam] = field(default_factory=list)
    response_fields: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    example: str = ""


# ---------------------------------------------------------------------------
# Capability / use-case taxonomy
# ---------------------------------------------------------------------------

CAPABILITY_TAXONOMY: dict[str, dict[str, Any]] = {
    "price_history": {
        "label": "Price History",
        "description": "Historical OHLCV bar data for stocks, options, and crypto",
        "tags": ["historical", "bars"],
    },
    "real_time_quotes": {
        "label": "Real-Time Quotes",
        "description": "Latest quotes, snapshots, and live trade data",
        "tags": ["real-time", "quotes", "snapshot"],
    },
    "options_flow": {
        "label": "Options Flow",
        "description": "Unusual options activity, flow alerts, and options analytics",
        "tags": ["options", "flow"],
    },
    "options_chains": {
        "label": "Options Chains",
        "description": "Option chain data with greeks, strikes, and expirations",
        "tags": ["options", "chain"],
    },
    "dark_pool": {
        "label": "Dark Pool & Off-Exchange",
        "description": "Dark pool prints, block trades, and off-exchange activity",
        "tags": ["dark-pool"],
    },
    "fundamentals": {
        "label": "Fundamentals",
        "description": "Company profiles, financials, earnings, and valuation metrics",
        "tags": ["fundamentals"],
    },
    "institutional": {
        "label": "Institutional Activity",
        "description": "13F filings, institutional holdings, and hedge fund data",
        "tags": ["institutional"],
    },
    "insider_activity": {
        "label": "Insider Activity",
        "description": "Corporate insider transactions and congressional trading",
        "tags": ["insider"],
    },
    "sentiment": {
        "label": "Sentiment & News",
        "description": "News articles, social sentiment, and analyst ratings",
        "tags": ["sentiment", "news"],
    },
    "technical_indicators": {
        "label": "Technical Indicators",
        "description": "SMA, EMA, RSI, MACD, and other computed indicators",
        "tags": ["technical"],
    },
    "economic_data": {
        "label": "Economic Data",
        "description": "GDP, inflation, interest rates, and macro indicators",
        "tags": ["economic"],
    },
    "sec_filings": {
        "label": "SEC Filings",
        "description": "SEC EDGAR filings, XBRL data, and company facts",
        "tags": ["sec", "filings"],
    },
    "trading": {
        "label": "Trading & Execution",
        "description": "Order management, positions, account info, and execution",
        "tags": ["trading", "execution"],
    },
    "etf": {
        "label": "ETF Data",
        "description": "ETF holdings, sector exposure, and fund flow",
        "tags": ["etf"],
    },
    "screeners": {
        "label": "Screeners",
        "description": "Stock and options screener endpoints",
        "tags": ["screener"],
    },
    "crypto": {
        "label": "Cryptocurrency",
        "description": "Crypto bars, quotes, trades, and orderbooks",
        "tags": ["crypto"],
    },
    "forex": {
        "label": "Forex",
        "description": "Foreign exchange rates and currency pair data",
        "tags": ["forex"],
    },
}


# ---------------------------------------------------------------------------
# Helper: provider aliases (route prefix -> canonical provider key)
# ---------------------------------------------------------------------------

PROVIDER_ALIASES: dict[str, str] = {
    "uw": "unusual_whales",
    "yf": "yahoo_finance",
}


# ---------------------------------------------------------------------------
# Registry: Alpaca
# ---------------------------------------------------------------------------

_ALPACA_ENDPOINTS: list[EndpointDef] = [
    # ── Stock bars ────────────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/alpaca/stocks/{symbol}/bars",
        method="GET",
        description="Get historical OHLCV bars for a single stock symbol",
        provider="alpaca",
        path_params=[
            PathParam(name="symbol", type="string", description="Ticker symbol (e.g. AAPL)"),
        ],
        query_params=[
            QueryParam(
                name="timeframe",
                type="string",
                required=False,
                default="1Day",
                valid_values=["1Min", "5Min", "15Min", "30Min", "1Hour", "4Hour", "1Day", "1Week", "1Month"],
                description="Bar aggregation period",
            ),
            QueryParam(
                name="start",
                type="datetime",
                required=False,
                description="Start time (ISO 8601). Defaults to 24h ago",
            ),
            QueryParam(
                name="end",
                type="datetime",
                required=False,
                description="End time (ISO 8601). Defaults to now",
            ),
            QueryParam(
                name="limit",
                type="integer",
                required=False,
                default=1000,
                description="Maximum bars to return (max 10000)",
            ),
            QueryParam(
                name="feed",
                type="string",
                required=False,
                default="sip",
                valid_values=["sip", "iex"],
                description="Data feed: SIP (all exchanges) or IEX (free tier)",
            ),
        ],
        response_fields=[
            "symbol",
            "timeframe",
            "bars[].t",
            "bars[].o",
            "bars[].h",
            "bars[].l",
            "bars[].c",
            "bars[].v",
            "bars[].vw",
            "bars[].n",
        ],
        tags=["historical", "bars", "stocks"],
        example="GET /api/v1/alpaca/stocks/AAPL/bars?timeframe=1Day&start=2025-01-01T00:00:00Z&limit=100",
    ),
    EndpointDef(
        path="/api/v1/alpaca/stocks/bars",
        method="GET",
        description="Get historical bars for multiple stocks (comma-separated)",
        provider="alpaca",
        query_params=[
            QueryParam(name="symbols", type="string", required=True, description="Comma-separated ticker symbols"),
            QueryParam(
                name="timeframe",
                type="string",
                required=False,
                default="1Day",
                valid_values=["1Min", "5Min", "15Min", "30Min", "1Hour", "4Hour", "1Day", "1Week", "1Month"],
                description="Bar aggregation period",
            ),
            QueryParam(name="start", type="datetime", required=False, description="Start time (ISO 8601)"),
            QueryParam(name="end", type="datetime", required=False, description="End time (ISO 8601)"),
            QueryParam(
                name="limit",
                type="integer",
                required=False,
                default=1000,
                description="Max bars per symbol (max 10000)",
            ),
            QueryParam(
                name="feed",
                type="string",
                required=False,
                default="sip",
                valid_values=["sip", "iex"],
                description="Data feed",
            ),
        ],
        response_fields=[
            "bars{symbol}[].t",
            "bars{symbol}[].o",
            "bars{symbol}[].h",
            "bars{symbol}[].l",
            "bars{symbol}[].c",
            "bars{symbol}[].v",
        ],
        tags=["historical", "bars", "stocks"],
        example="GET /api/v1/alpaca/stocks/bars?symbols=AAPL,MSFT,GOOGL&timeframe=1Hour&limit=50",
    ),
    # ── Stock quotes ──────────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/alpaca/stocks/{symbol}/quotes",
        method="GET",
        description="Get the latest NBBO quote for a stock",
        provider="alpaca",
        path_params=[
            PathParam(name="symbol", type="string", description="Ticker symbol"),
        ],
        response_fields=["ap", "as", "ax", "bp", "bs", "bx", "t"],
        tags=["real-time", "quotes", "stocks"],
        example="GET /api/v1/alpaca/stocks/AAPL/quotes",
    ),
    EndpointDef(
        path="/api/v1/alpaca/stocks/quotes",
        method="GET",
        description="Get latest quotes for multiple stocks",
        provider="alpaca",
        query_params=[
            QueryParam(name="symbols", type="string", required=True, description="Comma-separated ticker symbols"),
            QueryParam(
                name="feed",
                type="string",
                required=False,
                default="sip",
                valid_values=["sip", "iex"],
                description="Data feed",
            ),
        ],
        response_fields=["quotes{symbol}.ap", "quotes{symbol}.bp", "quotes{symbol}.t"],
        tags=["real-time", "quotes", "stocks"],
        example="GET /api/v1/alpaca/stocks/quotes?symbols=AAPL,MSFT",
    ),
    # ── Stock trades ──────────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/alpaca/stocks/{symbol}/trades",
        method="GET",
        description="Get recent trade executions for a stock",
        provider="alpaca",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        query_params=[
            QueryParam(name="start", type="datetime", required=False, description="Start time (ISO 8601)"),
            QueryParam(name="end", type="datetime", required=False, description="End time (ISO 8601)"),
            QueryParam(name="limit", type="integer", required=False, default=1000, description="Max trades"),
            QueryParam(
                name="feed",
                type="string",
                required=False,
                default="sip",
                valid_values=["sip", "iex"],
                description="Data feed",
            ),
        ],
        response_fields=["trades[].t", "trades[].p", "trades[].s", "trades[].x", "trades[].c"],
        tags=["real-time", "trades", "stocks"],
        example="GET /api/v1/alpaca/stocks/AAPL/trades?limit=100",
    ),
    EndpointDef(
        path="/api/v1/alpaca/stocks/trades",
        method="GET",
        description="Get trades for multiple stocks",
        provider="alpaca",
        query_params=[
            QueryParam(name="symbols", type="string", required=True, description="Comma-separated symbols"),
            QueryParam(name="start", type="datetime", required=False, description="Start time (ISO 8601)"),
            QueryParam(name="end", type="datetime", required=False, description="End time (ISO 8601)"),
            QueryParam(name="limit", type="integer", required=False, default=1000, description="Max trades per symbol"),
            QueryParam(
                name="feed",
                type="string",
                required=False,
                default="sip",
                valid_values=["sip", "iex"],
                description="Data feed",
            ),
        ],
        response_fields=["trades{symbol}[].t", "trades{symbol}[].p", "trades{symbol}[].s"],
        tags=["real-time", "trades", "stocks"],
        example="GET /api/v1/alpaca/stocks/trades?symbols=AAPL,MSFT&limit=50",
    ),
    # ── Stock snapshots ───────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/alpaca/stocks/{symbol}/snapshot",
        method="GET",
        description="Get complete market snapshot for a stock (latest bar, quote, trade, VWAP)",
        provider="alpaca",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        query_params=[
            QueryParam(
                name="feed",
                type="string",
                required=False,
                default="sip",
                valid_values=["sip", "iex"],
                description="Data feed",
            ),
        ],
        response_fields=["latestTrade", "latestQuote", "minuteBar", "dailyBar", "prevDailyBar"],
        tags=["real-time", "snapshot", "stocks"],
        example="GET /api/v1/alpaca/stocks/AAPL/snapshot",
    ),
    EndpointDef(
        path="/api/v1/alpaca/stocks/snapshots",
        method="GET",
        description="Get snapshots for multiple stocks at once",
        provider="alpaca",
        query_params=[
            QueryParam(name="symbols", type="string", required=True, description="Comma-separated symbols"),
            QueryParam(
                name="feed",
                type="string",
                required=False,
                default="sip",
                valid_values=["sip", "iex"],
                description="Data feed",
            ),
        ],
        response_fields=[
            "snapshots{symbol}.latestTrade",
            "snapshots{symbol}.latestQuote",
            "snapshots{symbol}.dailyBar",
        ],
        tags=["real-time", "snapshot", "stocks"],
        example="GET /api/v1/alpaca/stocks/snapshots?symbols=AAPL,MSFT,GOOGL",
    ),
    # ── Options chain ─────────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/alpaca/options/chain/{underlying}",
        method="GET",
        description="Get full option chain with greeks for an underlying symbol",
        provider="alpaca",
        path_params=[PathParam(name="underlying", type="string", description="Underlying ticker (e.g. AAPL)")],
        query_params=[
            QueryParam(
                name="expiration_date", type="string", required=False, description="Exact expiration (YYYY-MM-DD)"
            ),
            QueryParam(
                name="expiration_date_gte",
                type="string",
                required=False,
                description="Expiration on or after (YYYY-MM-DD)",
            ),
            QueryParam(
                name="expiration_date_lte",
                type="string",
                required=False,
                description="Expiration on or before (YYYY-MM-DD)",
            ),
            QueryParam(name="strike_price_gte", type="float", required=False, description="Minimum strike price"),
            QueryParam(name="strike_price_lte", type="float", required=False, description="Maximum strike price"),
            QueryParam(
                name="option_type",
                type="string",
                required=False,
                valid_values=["call", "put"],
                description="Filter by call or put",
            ),
        ],
        response_fields=[
            "underlying",
            "contracts[].symbol",
            "contracts[].strike_price",
            "contracts[].expiration_date",
            "contracts[].option_type",
            "contracts[].greeks",
        ],
        tags=["options", "chain"],
        example="GET /api/v1/alpaca/options/chain/AAPL?expiration_date_gte=2025-06-01&option_type=call",
    ),
    EndpointDef(
        path="/api/v1/alpaca/options/chain/{underlying}/snapshot",
        method="GET",
        description="Get current option chain snapshot for an underlying (nearest expirations)",
        provider="alpaca",
        path_params=[PathParam(name="underlying", type="string", description="Underlying ticker")],
        response_fields=["underlying", "contracts[].symbol", "contracts[].strike_price", "contracts[].greeks"],
        tags=["options", "chain", "snapshot"],
        example="GET /api/v1/alpaca/options/chain/SPY/snapshot",
    ),
    # ── Options bars / quotes / trades ────────────────────────────────────
    EndpointDef(
        path="/api/v1/alpaca/options/{contract}/bars",
        method="GET",
        description="Get historical bars for an option contract",
        provider="alpaca",
        path_params=[
            PathParam(name="contract", type="string", description="OCC contract symbol (e.g. AAPL240119C00190000)")
        ],
        query_params=[
            QueryParam(
                name="timeframe",
                type="string",
                required=False,
                default="1Day",
                valid_values=["1Min", "5Min", "15Min", "30Min", "1Hour", "1Day"],
                description="Bar aggregation period",
            ),
            QueryParam(name="start", type="datetime", required=False, description="Start time (ISO 8601)"),
            QueryParam(name="end", type="datetime", required=False, description="End time (ISO 8601)"),
            QueryParam(name="limit", type="integer", required=False, default=1000, description="Max bars (max 10000)"),
        ],
        response_fields=[
            "contract",
            "timeframe",
            "bars[].t",
            "bars[].o",
            "bars[].h",
            "bars[].l",
            "bars[].c",
            "bars[].v",
        ],
        tags=["historical", "bars", "options"],
        example="GET /api/v1/alpaca/options/AAPL240119C00190000/bars?timeframe=1Hour",
    ),
    EndpointDef(
        path="/api/v1/alpaca/options/{contract}/quotes",
        method="GET",
        description="Get latest quote for an option contract",
        provider="alpaca",
        path_params=[PathParam(name="contract", type="string", description="OCC contract symbol")],
        response_fields=["ap", "as", "bp", "bs", "t"],
        tags=["real-time", "quotes", "options"],
        example="GET /api/v1/alpaca/options/AAPL240119C00190000/quotes",
    ),
    EndpointDef(
        path="/api/v1/alpaca/options/{contract}/trades",
        method="GET",
        description="Get recent trades for an option contract",
        provider="alpaca",
        path_params=[PathParam(name="contract", type="string", description="OCC contract symbol")],
        query_params=[
            QueryParam(name="start", type="datetime", required=False, description="Start time"),
            QueryParam(name="end", type="datetime", required=False, description="End time"),
            QueryParam(name="limit", type="integer", required=False, default=1000, description="Max trades"),
        ],
        response_fields=["trades[].t", "trades[].p", "trades[].s", "trades[].x"],
        tags=["real-time", "trades", "options"],
        example="GET /api/v1/alpaca/options/AAPL240119C00190000/trades?limit=50",
    ),
    EndpointDef(
        path="/api/v1/alpaca/options/{contract}/snapshot",
        method="GET",
        description="Get market snapshot for an option contract",
        provider="alpaca",
        path_params=[PathParam(name="contract", type="string", description="OCC contract symbol")],
        response_fields=["latestTrade", "latestQuote", "greeks", "impliedVolatility"],
        tags=["real-time", "snapshot", "options"],
        example="GET /api/v1/alpaca/options/AAPL240119C00190000/snapshot",
    ),
    EndpointDef(
        path="/api/v1/alpaca/options/bars",
        method="GET",
        description="Get bars for multiple option contracts",
        provider="alpaca",
        query_params=[
            QueryParam(name="contracts", type="string", required=True, description="Comma-separated OCC symbols"),
            QueryParam(
                name="timeframe",
                type="string",
                required=False,
                default="1Day",
                valid_values=["1Min", "5Min", "15Min", "30Min", "1Hour", "1Day"],
                description="Bar aggregation period",
            ),
            QueryParam(name="start", type="datetime", required=False, description="Start time (ISO 8601)"),
            QueryParam(name="end", type="datetime", required=False, description="End time (ISO 8601)"),
            QueryParam(name="limit", type="integer", required=False, default=1000, description="Max bars per contract"),
        ],
        response_fields=[
            "bars{contract}[].t",
            "bars{contract}[].o",
            "bars{contract}[].h",
            "bars{contract}[].l",
            "bars{contract}[].c",
            "bars{contract}[].v",
        ],
        tags=["historical", "bars", "options"],
        example="GET /api/v1/alpaca/options/bars?contracts=AAPL240119C00190000,AAPL240119P00190000&timeframe=1Day",
    ),
    EndpointDef(
        path="/api/v1/alpaca/options/snapshots",
        method="GET",
        description="Get snapshots for multiple option contracts",
        provider="alpaca",
        query_params=[
            QueryParam(name="contracts", type="string", required=True, description="Comma-separated OCC symbols"),
        ],
        response_fields=[
            "snapshots{contract}.latestTrade",
            "snapshots{contract}.latestQuote",
            "snapshots{contract}.greeks",
        ],
        tags=["real-time", "snapshot", "options"],
        example="GET /api/v1/alpaca/options/snapshots?contracts=AAPL240119C00190000",
    ),
    # ── Crypto ────────────────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/alpaca/crypto/{symbol}/bars",
        method="GET",
        description="Get historical bars for a crypto pair",
        provider="alpaca",
        path_params=[PathParam(name="symbol", type="string", description="Crypto pair (e.g. BTC/USD)")],
        query_params=[
            QueryParam(
                name="timeframe",
                type="string",
                required=False,
                default="1Day",
                valid_values=["1Min", "5Min", "15Min", "30Min", "1Hour", "4Hour", "1Day", "1Week", "1Month"],
                description="Bar aggregation period",
            ),
            QueryParam(name="start", type="datetime", required=False, description="Start time (ISO 8601)"),
            QueryParam(name="end", type="datetime", required=False, description="End time (ISO 8601)"),
            QueryParam(name="limit", type="integer", required=False, default=1000, description="Max bars"),
        ],
        response_fields=["symbol", "timeframe", "bars[].t", "bars[].o", "bars[].h", "bars[].l", "bars[].c", "bars[].v"],
        tags=["historical", "bars", "crypto"],
        example="GET /api/v1/alpaca/crypto/BTC%2FUSD/bars?timeframe=1Hour&limit=100",
    ),
    EndpointDef(
        path="/api/v1/alpaca/crypto/{symbol}/quotes",
        method="GET",
        description="Get latest quote for a crypto pair",
        provider="alpaca",
        path_params=[PathParam(name="symbol", type="string", description="Crypto pair")],
        response_fields=["ap", "as", "bp", "bs", "t"],
        tags=["real-time", "quotes", "crypto"],
        example="GET /api/v1/alpaca/crypto/BTC%2FUSD/quotes",
    ),
    EndpointDef(
        path="/api/v1/alpaca/crypto/{symbol}/trades",
        method="GET",
        description="Get recent trades for a crypto pair",
        provider="alpaca",
        path_params=[PathParam(name="symbol", type="string", description="Crypto pair")],
        query_params=[
            QueryParam(name="start", type="datetime", required=False, description="Start time"),
            QueryParam(name="end", type="datetime", required=False, description="End time"),
            QueryParam(name="limit", type="integer", required=False, default=1000, description="Max trades"),
        ],
        response_fields=["trades[].t", "trades[].p", "trades[].s"],
        tags=["real-time", "trades", "crypto"],
        example="GET /api/v1/alpaca/crypto/ETH%2FUSD/trades?limit=50",
    ),
    EndpointDef(
        path="/api/v1/alpaca/crypto/{symbol}/snapshot",
        method="GET",
        description="Get market snapshot for a crypto pair",
        provider="alpaca",
        path_params=[PathParam(name="symbol", type="string", description="Crypto pair")],
        response_fields=["latestTrade", "latestQuote", "minuteBar", "dailyBar"],
        tags=["real-time", "snapshot", "crypto"],
        example="GET /api/v1/alpaca/crypto/BTC%2FUSD/snapshot",
    ),
    EndpointDef(
        path="/api/v1/alpaca/crypto/bars",
        method="GET",
        description="Get bars for multiple crypto pairs",
        provider="alpaca",
        query_params=[
            QueryParam(
                name="symbols", type="string", required=True, description="Comma-separated pairs (e.g. BTC/USD,ETH/USD)"
            ),
            QueryParam(
                name="timeframe",
                type="string",
                required=False,
                default="1Day",
                valid_values=["1Min", "5Min", "15Min", "30Min", "1Hour", "4Hour", "1Day", "1Week", "1Month"],
                description="Bar aggregation period",
            ),
            QueryParam(name="start", type="datetime", required=False, description="Start time"),
            QueryParam(name="end", type="datetime", required=False, description="End time"),
            QueryParam(name="limit", type="integer", required=False, default=1000, description="Max bars"),
        ],
        response_fields=[
            "bars{symbol}[].t",
            "bars{symbol}[].o",
            "bars{symbol}[].h",
            "bars{symbol}[].l",
            "bars{symbol}[].c",
            "bars{symbol}[].v",
        ],
        tags=["historical", "bars", "crypto"],
        example="GET /api/v1/alpaca/crypto/bars?symbols=BTC/USD,ETH/USD&timeframe=1Hour",
    ),
    EndpointDef(
        path="/api/v1/alpaca/crypto/snapshots",
        method="GET",
        description="Get snapshots for multiple crypto pairs",
        provider="alpaca",
        query_params=[
            QueryParam(name="symbols", type="string", required=True, description="Comma-separated pairs"),
        ],
        response_fields=["snapshots{symbol}.latestTrade", "snapshots{symbol}.latestQuote"],
        tags=["real-time", "snapshot", "crypto"],
        example="GET /api/v1/alpaca/crypto/snapshots?symbols=BTC/USD,ETH/USD",
    ),
    # ── Trading / account ─────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/alpaca/account",
        method="GET",
        description="Get trading account information (buying power, equity, margin)",
        provider="alpaca",
        response_fields=["id", "status", "equity", "buying_power", "cash", "portfolio_value", "pattern_day_trader"],
        tags=["trading", "execution"],
        example="GET /api/v1/alpaca/account",
    ),
    EndpointDef(
        path="/api/v1/alpaca/orders",
        method="GET",
        description="List all orders with optional status filter",
        provider="alpaca",
        query_params=[
            QueryParam(
                name="status",
                type="string",
                required=False,
                default="open",
                valid_values=["open", "closed", "all"],
                description="Order status filter",
            ),
            QueryParam(name="limit", type="integer", required=False, default=50, description="Max orders"),
            QueryParam(
                name="direction",
                type="string",
                required=False,
                default="desc",
                valid_values=["asc", "desc"],
                description="Sort direction",
            ),
        ],
        response_fields=[
            "orders[].id",
            "orders[].symbol",
            "orders[].side",
            "orders[].qty",
            "orders[].filled_avg_price",
            "orders[].status",
        ],
        tags=["trading", "execution"],
        example="GET /api/v1/alpaca/orders?status=open",
    ),
    EndpointDef(
        path="/api/v1/alpaca/orders/{order_id}",
        method="GET",
        description="Get details of a specific order",
        provider="alpaca",
        path_params=[PathParam(name="order_id", type="string", description="Order UUID")],
        response_fields=["id", "symbol", "side", "qty", "type", "time_in_force", "status", "filled_avg_price"],
        tags=["trading", "execution"],
        example="GET /api/v1/alpaca/orders/61e69015-8549-4bef-b9c3-01e75843f47d",
    ),
    EndpointDef(
        path="/api/v1/alpaca/positions",
        method="GET",
        description="Get all open positions",
        provider="alpaca",
        response_fields=[
            "positions[].symbol",
            "positions[].qty",
            "positions[].avg_entry_price",
            "positions[].market_value",
            "positions[].unrealized_pl",
        ],
        tags=["trading", "execution"],
        example="GET /api/v1/alpaca/positions",
    ),
    EndpointDef(
        path="/api/v1/alpaca/positions/{symbol}",
        method="GET",
        description="Get position details for a specific symbol",
        provider="alpaca",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["symbol", "qty", "avg_entry_price", "market_value", "unrealized_pl", "side"],
        tags=["trading", "execution"],
        example="GET /api/v1/alpaca/positions/AAPL",
    ),
    EndpointDef(
        path="/api/v1/alpaca/clock",
        method="GET",
        description="Get current market clock (open/close times, trading status)",
        provider="alpaca",
        response_fields=["timestamp", "is_open", "next_open", "next_close"],
        tags=["trading"],
        example="GET /api/v1/alpaca/clock",
    ),
    EndpointDef(
        path="/api/v1/alpaca/calendar",
        method="GET",
        description="Get market calendar (trading days, early closes)",
        provider="alpaca",
        query_params=[
            QueryParam(name="start", type="string", required=False, description="Start date (YYYY-MM-DD)"),
            QueryParam(name="end", type="string", required=False, description="End date (YYYY-MM-DD)"),
        ],
        response_fields=["calendar[].date", "calendar[].open", "calendar[].close"],
        tags=["trading"],
        example="GET /api/v1/alpaca/calendar?start=2025-01-01&end=2025-12-31",
    ),
    EndpointDef(
        path="/api/v1/alpaca/assets",
        method="GET",
        description="List tradeable assets",
        provider="alpaca",
        query_params=[
            QueryParam(
                name="status",
                type="string",
                required=False,
                default="active",
                valid_values=["active", "inactive"],
                description="Asset status filter",
            ),
            QueryParam(
                name="asset_class",
                type="string",
                required=False,
                default="us_equity",
                valid_values=["us_equity", "crypto"],
                description="Asset class",
            ),
        ],
        response_fields=[
            "assets[].id",
            "assets[].symbol",
            "assets[].name",
            "assets[].tradable",
            "assets[].fractionable",
        ],
        tags=["trading"],
        example="GET /api/v1/alpaca/assets?status=active&asset_class=us_equity",
    ),
    EndpointDef(
        path="/api/v1/alpaca/assets/{symbol}",
        method="GET",
        description="Get details for a specific asset",
        provider="alpaca",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol or asset ID")],
        response_fields=["id", "symbol", "name", "exchange", "tradable", "fractionable", "marginable", "shortable"],
        tags=["trading"],
        example="GET /api/v1/alpaca/assets/AAPL",
    ),
    EndpointDef(
        path="/api/v1/alpaca/watchlists",
        method="GET",
        description="Get all watchlists",
        provider="alpaca",
        response_fields=["watchlists[].id", "watchlists[].name", "watchlists[].assets"],
        tags=["trading"],
        example="GET /api/v1/alpaca/watchlists",
    ),
]


# ---------------------------------------------------------------------------
# Registry: Unusual Whales
# ---------------------------------------------------------------------------

_UW_ENDPOINTS: list[EndpointDef] = [
    # ── Flow ──────────────────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/uw/flow/all",
        method="GET",
        description="Get all recent options flow alerts across all tickers",
        provider="unusual_whales",
        query_params=[
            QueryParam(
                name="limit", type="integer", required=False, default=50, description="Results per page (max 100)"
            ),
            QueryParam(
                name="cursor", type="string", required=False, description="Pagination cursor from previous response"
            ),
        ],
        response_fields=[
            "data[].ticker",
            "data[].strike",
            "data[].expiry",
            "data[].type",
            "data[].sentiment",
            "data[].premium",
            "data[].volume",
            "data[].open_interest",
        ],
        tags=["options", "flow"],
        example="GET /api/v1/uw/flow/all?limit=50",
    ),
    EndpointDef(
        path="/api/v1/uw/flow/{symbol}",
        method="GET",
        description="Get options flow for a specific ticker",
        provider="unusual_whales",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        query_params=[
            QueryParam(
                name="limit", type="integer", required=False, default=50, description="Results per page (max 100)"
            ),
            QueryParam(name="cursor", type="string", required=False, description="Pagination cursor"),
            QueryParam(name="date", type="string", required=False, description="Filter by date (YYYY-MM-DD)"),
        ],
        response_fields=[
            "data[].ticker",
            "data[].strike",
            "data[].expiry",
            "data[].type",
            "data[].sentiment",
            "data[].premium",
        ],
        tags=["options", "flow"],
        example="GET /api/v1/uw/flow/AAPL?limit=25&date=2025-03-10",
    ),
    # ── Dark Pool ─────────────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/uw/darkpool/all",
        method="GET",
        description="Get all recent dark pool / off-exchange prints",
        provider="unusual_whales",
        query_params=[
            QueryParam(
                name="limit", type="integer", required=False, default=50, description="Results per page (max 100)"
            ),
            QueryParam(name="cursor", type="string", required=False, description="Pagination cursor"),
        ],
        response_fields=[
            "data[].ticker",
            "data[].price",
            "data[].size",
            "data[].notional",
            "data[].venue",
            "data[].timestamp",
        ],
        tags=["dark-pool"],
        example="GET /api/v1/uw/darkpool/all?limit=50",
    ),
    EndpointDef(
        path="/api/v1/uw/darkpool/{symbol}",
        method="GET",
        description="Get dark pool prints for a specific ticker",
        provider="unusual_whales",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        query_params=[
            QueryParam(
                name="limit", type="integer", required=False, default=50, description="Results per page (max 100)"
            ),
            QueryParam(name="cursor", type="string", required=False, description="Pagination cursor"),
            QueryParam(name="date", type="string", required=False, description="Filter by date (YYYY-MM-DD)"),
        ],
        response_fields=["data[].ticker", "data[].price", "data[].size", "data[].notional", "data[].venue"],
        tags=["dark-pool"],
        example="GET /api/v1/uw/darkpool/AAPL",
    ),
    # ── Stock analytics ───────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/uw/stock/{symbol}/info",
        method="GET",
        description="Get stock ticker information and overview",
        provider="unusual_whales",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["ticker", "name", "sector", "industry", "market_cap", "description"],
        tags=["fundamentals"],
        example="GET /api/v1/uw/stock/AAPL/info",
    ),
    EndpointDef(
        path="/api/v1/uw/stock/{symbol}/candles",
        method="GET",
        description="Get OHLC candle data for a ticker via Unusual Whales",
        provider="unusual_whales",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        query_params=[
            QueryParam(
                name="timeframe",
                type="string",
                required=False,
                default="1d",
                valid_values=["1m", "5m", "1h", "1d"],
                description="Candle timeframe",
            ),
        ],
        response_fields=["data[].t", "data[].o", "data[].h", "data[].l", "data[].c", "data[].v"],
        tags=["historical", "bars"],
        example="GET /api/v1/uw/stock/AAPL/candles?timeframe=1d",
    ),
    EndpointDef(
        path="/api/v1/uw/stock/{symbol}/option-chains",
        method="GET",
        description="Get option chains for a ticker via Unusual Whales analytics",
        provider="unusual_whales",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=[
            "data[].strike",
            "data[].expiry",
            "data[].type",
            "data[].bid",
            "data[].ask",
            "data[].volume",
            "data[].open_interest",
        ],
        tags=["options", "chain"],
        example="GET /api/v1/uw/stock/AAPL/option-chains",
    ),
    # ── Institutions ──────────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/uw/institutions",
        method="GET",
        description="Get list of all tracked institutions (hedge funds, 13F filers)",
        provider="unusual_whales",
        query_params=[
            QueryParam(name="limit", type="integer", required=False, default=100, description="Max results (max 500)"),
        ],
        response_fields=["data[].name", "data[].cik", "data[].filing_count", "data[].market_value"],
        tags=["institutional"],
        example="GET /api/v1/uw/institutions?limit=100",
    ),
    EndpointDef(
        path="/api/v1/uw/institutions/latest-filings",
        method="GET",
        description="Get the latest institutional filings (13F amendments and new filings)",
        provider="unusual_whales",
        query_params=[
            QueryParam(name="limit", type="integer", required=False, default=50, description="Max results (max 200)"),
        ],
        response_fields=["data[].institution", "data[].filing_date", "data[].form_type", "data[].holdings_count"],
        tags=["institutional"],
        example="GET /api/v1/uw/institutions/latest-filings?limit=25",
    ),
    EndpointDef(
        path="/api/v1/uw/institutions/{institution_id}/activity",
        method="GET",
        description="Get latest trading activity for an institution",
        provider="unusual_whales",
        path_params=[PathParam(name="institution_id", type="string", description="Institution CIK or slug")],
        response_fields=["data[].ticker", "data[].shares", "data[].value", "data[].change_type"],
        tags=["institutional"],
        example="GET /api/v1/uw/institutions/berkshire-hathaway/activity",
    ),
    # ── Insider / Congress ────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/uw/insiders/trades",
        method="GET",
        description="Get recent insider trading transactions",
        provider="unusual_whales",
        query_params=[
            QueryParam(name="limit", type="integer", required=False, default=50, description="Max results"),
        ],
        response_fields=[
            "data[].ticker",
            "data[].insider_name",
            "data[].title",
            "data[].transaction_type",
            "data[].shares",
            "data[].price",
        ],
        tags=["insider"],
        example="GET /api/v1/uw/insiders/trades?limit=25",
    ),
    EndpointDef(
        path="/api/v1/uw/congress/trades",
        method="GET",
        description="Get recent congressional trading activity",
        provider="unusual_whales",
        query_params=[
            QueryParam(name="limit", type="integer", required=False, default=50, description="Max results"),
        ],
        response_fields=[
            "data[].politician",
            "data[].ticker",
            "data[].transaction_type",
            "data[].amount_range",
            "data[].filing_date",
        ],
        tags=["insider"],
        example="GET /api/v1/uw/congress/trades?limit=25",
    ),
    # ── ETF ───────────────────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/uw/etf",
        method="GET",
        description="Get list of tracked ETFs",
        provider="unusual_whales",
        query_params=[
            QueryParam(name="limit", type="integer", required=False, default=100, description="Max results"),
        ],
        response_fields=["data[].ticker", "data[].name", "data[].aum", "data[].expense_ratio"],
        tags=["etf"],
        example="GET /api/v1/uw/etf?limit=50",
    ),
    EndpointDef(
        path="/api/v1/uw/etf/{ticker}",
        method="GET",
        description="Get ETF overview and metadata",
        provider="unusual_whales",
        path_params=[PathParam(name="ticker", type="string", description="ETF ticker symbol")],
        response_fields=["ticker", "name", "aum", "expense_ratio", "holdings_count", "sector_weights"],
        tags=["etf"],
        example="GET /api/v1/uw/etf/SPY",
    ),
    EndpointDef(
        path="/api/v1/uw/etf/{ticker}/holdings",
        method="GET",
        description="Get ETF holdings breakdown",
        provider="unusual_whales",
        path_params=[PathParam(name="ticker", type="string", description="ETF ticker symbol")],
        response_fields=["data[].ticker", "data[].name", "data[].weight", "data[].shares"],
        tags=["etf"],
        example="GET /api/v1/uw/etf/SPY/holdings",
    ),
    # ── Screeners ─────────────────────────────────────────────────────────
    EndpointDef(
        path="/api/v1/uw/screener/options",
        method="GET",
        description="Options flow screener with filtering",
        provider="unusual_whales",
        query_params=[
            QueryParam(name="limit", type="integer", required=False, default=50, description="Max results"),
        ],
        response_fields=["data[].ticker", "data[].premium", "data[].volume", "data[].sentiment"],
        tags=["screener", "options"],
        example="GET /api/v1/uw/screener/options?limit=50",
    ),
    EndpointDef(
        path="/api/v1/uw/screener/stocks",
        method="GET",
        description="Stock screener with fundamental and technical filters",
        provider="unusual_whales",
        query_params=[
            QueryParam(name="limit", type="integer", required=False, default=50, description="Max results"),
        ],
        response_fields=["data[].ticker", "data[].name", "data[].market_cap", "data[].sector"],
        tags=["screener"],
        example="GET /api/v1/uw/screener/stocks?limit=50",
    ),
]


# ---------------------------------------------------------------------------
# Registry: Finnhub (stubs with core params)
# ---------------------------------------------------------------------------

_FINNHUB_ENDPOINTS: list[EndpointDef] = [
    EndpointDef(
        path="/api/v1/finnhub/stock/profile/{symbol}",
        method="GET",
        description="Get company profile (sector, industry, market cap, IPO date)",
        provider="finnhub",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["name", "ticker", "exchange", "finnhubIndustry", "marketCapitalization", "ipo"],
        tags=["fundamentals"],
        example="GET /api/v1/finnhub/stock/profile/AAPL",
    ),
    EndpointDef(
        path="/api/v1/finnhub/stock/metric",
        method="GET",
        description="Get key financial metrics (P/E, EPS, book value, etc.)",
        provider="finnhub",
        query_params=[
            QueryParam(name="symbol", type="string", required=True, description="Ticker symbol"),
            QueryParam(name="metric", type="string", required=False, default="all", description="Metric type"),
        ],
        response_fields=[
            "metric.peBasicExclExtraTTM",
            "metric.epsBasicExclExtraTTM",
            "metric.52WeekHigh",
            "metric.52WeekLow",
        ],
        tags=["fundamentals"],
        example="GET /api/v1/finnhub/stock/metric?symbol=AAPL",
    ),
    EndpointDef(
        path="/api/v1/finnhub/stock/peers",
        method="GET",
        description="Get peer companies for a stock",
        provider="finnhub",
        query_params=[
            QueryParam(name="symbol", type="string", required=True, description="Ticker symbol"),
        ],
        response_fields=["peers[]"],
        tags=["fundamentals"],
        example="GET /api/v1/finnhub/stock/peers?symbol=AAPL",
    ),
    EndpointDef(
        path="/api/v1/finnhub/stock/earnings",
        method="GET",
        description="Get earnings history with surprise data",
        provider="finnhub",
        query_params=[
            QueryParam(name="symbol", type="string", required=True, description="Ticker symbol"),
            QueryParam(name="limit", type="integer", required=False, default=4, description="Number of quarters"),
        ],
        response_fields=[
            "data[].period",
            "data[].actual",
            "data[].estimate",
            "data[].surprise",
            "data[].surprisePercent",
        ],
        tags=["fundamentals"],
        example="GET /api/v1/finnhub/stock/earnings?symbol=AAPL&limit=8",
    ),
    EndpointDef(
        path="/api/v1/finnhub/stock/financials",
        method="GET",
        description="Get reported financial statements",
        provider="finnhub",
        query_params=[
            QueryParam(name="symbol", type="string", required=True, description="Ticker symbol"),
            QueryParam(
                name="statement",
                type="string",
                required=False,
                default="ic",
                valid_values=["ic", "bs", "cf"],
                description="Statement type: income, balance sheet, cash flow",
            ),
            QueryParam(
                name="freq",
                type="string",
                required=False,
                default="quarterly",
                valid_values=["annual", "quarterly"],
                description="Reporting frequency",
            ),
        ],
        response_fields=["financials[].period", "financials[].revenue", "financials[].netIncome", "financials[].eps"],
        tags=["fundamentals"],
        example="GET /api/v1/finnhub/stock/financials?symbol=AAPL&statement=ic&freq=quarterly",
    ),
    EndpointDef(
        path="/api/v1/finnhub/calendar/earnings",
        method="GET",
        description="Get upcoming earnings calendar",
        provider="finnhub",
        query_params=[
            QueryParam(name="from", type="string", required=False, description="Start date (YYYY-MM-DD)"),
            QueryParam(name="to", type="string", required=False, description="End date (YYYY-MM-DD)"),
        ],
        response_fields=[
            "earningsCalendar[].symbol",
            "earningsCalendar[].date",
            "earningsCalendar[].hour",
            "earningsCalendar[].epsEstimate",
        ],
        tags=["fundamentals"],
        example="GET /api/v1/finnhub/calendar/earnings?from=2025-03-10&to=2025-03-14",
    ),
    EndpointDef(
        path="/api/v1/finnhub/news/company/{symbol}",
        method="GET",
        description="Get company-specific news articles",
        provider="finnhub",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        query_params=[
            QueryParam(name="from", type="string", required=False, description="Start date (YYYY-MM-DD)"),
            QueryParam(name="to", type="string", required=False, description="End date (YYYY-MM-DD)"),
        ],
        response_fields=[
            "articles[].headline",
            "articles[].source",
            "articles[].datetime",
            "articles[].summary",
            "articles[].url",
        ],
        tags=["sentiment", "news"],
        example="GET /api/v1/finnhub/news/company/AAPL",
    ),
    EndpointDef(
        path="/api/v1/finnhub/news/market",
        method="GET",
        description="Get general market news",
        provider="finnhub",
        query_params=[
            QueryParam(
                name="category",
                type="string",
                required=False,
                default="general",
                valid_values=["general", "forex", "crypto", "merger"],
                description="News category",
            ),
        ],
        response_fields=["articles[].headline", "articles[].source", "articles[].datetime", "articles[].summary"],
        tags=["sentiment", "news"],
        example="GET /api/v1/finnhub/news/market?category=general",
    ),
    EndpointDef(
        path="/api/v1/finnhub/stock/social-sentiment",
        method="GET",
        description="Get social media sentiment for a stock (Reddit, Twitter)",
        provider="finnhub",
        query_params=[
            QueryParam(name="symbol", type="string", required=True, description="Ticker symbol"),
        ],
        response_fields=["reddit[].mention", "reddit[].positiveScore", "twitter[].mention", "twitter[].positiveScore"],
        tags=["sentiment"],
        example="GET /api/v1/finnhub/stock/social-sentiment?symbol=AAPL",
    ),
    EndpointDef(
        path="/api/v1/finnhub/stock/insider-transactions",
        method="GET",
        description="Get insider transactions for a stock",
        provider="finnhub",
        query_params=[
            QueryParam(name="symbol", type="string", required=True, description="Ticker symbol"),
        ],
        response_fields=[
            "data[].name",
            "data[].share",
            "data[].change",
            "data[].transactionDate",
            "data[].transactionType",
        ],
        tags=["insider"],
        example="GET /api/v1/finnhub/stock/insider-transactions?symbol=AAPL",
    ),
    EndpointDef(
        path="/api/v1/finnhub/stock/congressional-trading",
        method="GET",
        description="Get congressional trading activity for a stock",
        provider="finnhub",
        query_params=[
            QueryParam(name="symbol", type="string", required=True, description="Ticker symbol"),
        ],
        response_fields=["data[].name", "data[].amount", "data[].transactionDate", "data[].transactionType"],
        tags=["insider"],
        example="GET /api/v1/finnhub/stock/congressional-trading?symbol=AAPL",
    ),
]


# ---------------------------------------------------------------------------
# Registry: Alpha Vantage (stubs)
# ---------------------------------------------------------------------------

_ALPHAVANTAGE_ENDPOINTS: list[EndpointDef] = [
    EndpointDef(
        path="/api/v1/alphavantage/company/{symbol}",
        method="GET",
        description="Get company overview and fundamentals",
        provider="alpha_vantage",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["Symbol", "Name", "Sector", "Industry", "MarketCapitalization", "PERatio", "DividendYield"],
        tags=["fundamentals"],
        example="GET /api/v1/alphavantage/company/AAPL",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/income/{symbol}",
        method="GET",
        description="Get income statement data",
        provider="alpha_vantage",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["annualReports[].totalRevenue", "annualReports[].netIncome", "quarterlyReports[]"],
        tags=["fundamentals"],
        example="GET /api/v1/alphavantage/income/AAPL",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/balance/{symbol}",
        method="GET",
        description="Get balance sheet data",
        provider="alpha_vantage",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["annualReports[].totalAssets", "annualReports[].totalLiabilities"],
        tags=["fundamentals"],
        example="GET /api/v1/alphavantage/balance/AAPL",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/earnings/{symbol}",
        method="GET",
        description="Get earnings data",
        provider="alpha_vantage",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["annualEarnings[]", "quarterlyEarnings[].reportedEPS", "quarterlyEarnings[].estimatedEPS"],
        tags=["fundamentals"],
        example="GET /api/v1/alphavantage/earnings/AAPL",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/indicator/{indicator}/{symbol}",
        method="GET",
        description="Get a technical indicator for a symbol",
        provider="alpha_vantage",
        path_params=[
            PathParam(name="indicator", type="string", description="Indicator name (SMA, EMA, RSI, MACD, etc.)"),
            PathParam(name="symbol", type="string", description="Ticker symbol"),
        ],
        query_params=[
            QueryParam(
                name="interval",
                type="string",
                required=False,
                default="daily",
                valid_values=["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"],
                description="Time interval",
            ),
            QueryParam(
                name="time_period",
                type="integer",
                required=False,
                default=14,
                description="Number of data points for calculation",
            ),
            QueryParam(
                name="series_type",
                type="string",
                required=False,
                default="close",
                valid_values=["close", "open", "high", "low"],
                description="Price series to use",
            ),
        ],
        response_fields=["Technical Analysis{date}.value"],
        tags=["technical"],
        example="GET /api/v1/alphavantage/indicator/RSI/AAPL?interval=daily&time_period=14",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/sma/{symbol}",
        method="GET",
        description="Get Simple Moving Average",
        provider="alpha_vantage",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        query_params=[
            QueryParam(
                name="interval",
                type="string",
                required=False,
                default="daily",
                valid_values=["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"],
                description="Time interval",
            ),
            QueryParam(name="time_period", type="integer", required=False, default=20, description="SMA period"),
        ],
        response_fields=["Technical Analysis{date}.SMA"],
        tags=["technical"],
        example="GET /api/v1/alphavantage/sma/AAPL?interval=daily&time_period=50",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/ema/{symbol}",
        method="GET",
        description="Get Exponential Moving Average",
        provider="alpha_vantage",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        query_params=[
            QueryParam(
                name="interval",
                type="string",
                required=False,
                default="daily",
                valid_values=["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"],
                description="Time interval",
            ),
            QueryParam(name="time_period", type="integer", required=False, default=20, description="EMA period"),
        ],
        response_fields=["Technical Analysis{date}.EMA"],
        tags=["technical"],
        example="GET /api/v1/alphavantage/ema/AAPL?interval=daily&time_period=12",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/rsi/{symbol}",
        method="GET",
        description="Get Relative Strength Index",
        provider="alpha_vantage",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        query_params=[
            QueryParam(
                name="interval",
                type="string",
                required=False,
                default="daily",
                valid_values=["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"],
                description="Time interval",
            ),
            QueryParam(name="time_period", type="integer", required=False, default=14, description="RSI period"),
        ],
        response_fields=["Technical Analysis{date}.RSI"],
        tags=["technical"],
        example="GET /api/v1/alphavantage/rsi/AAPL?interval=daily&time_period=14",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/macd/{symbol}",
        method="GET",
        description="Get MACD (Moving Average Convergence Divergence)",
        provider="alpha_vantage",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        query_params=[
            QueryParam(
                name="interval",
                type="string",
                required=False,
                default="daily",
                valid_values=["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"],
                description="Time interval",
            ),
        ],
        response_fields=[
            "Technical Analysis{date}.MACD",
            "Technical Analysis{date}.MACD_Signal",
            "Technical Analysis{date}.MACD_Hist",
        ],
        tags=["technical"],
        example="GET /api/v1/alphavantage/macd/AAPL?interval=daily",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/forex/rate",
        method="GET",
        description="Get real-time exchange rate for a currency pair",
        provider="alpha_vantage",
        query_params=[
            QueryParam(name="from_currency", type="string", required=True, description="Source currency (e.g. USD)"),
            QueryParam(name="to_currency", type="string", required=True, description="Target currency (e.g. EUR)"),
        ],
        response_fields=["from_currency", "to_currency", "exchange_rate", "last_refreshed"],
        tags=["forex"],
        example="GET /api/v1/alphavantage/forex/rate?from_currency=USD&to_currency=EUR",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/economy/gdp",
        method="GET",
        description="Get US GDP data",
        provider="alpha_vantage",
        query_params=[
            QueryParam(
                name="interval",
                type="string",
                required=False,
                default="quarterly",
                valid_values=["quarterly", "annual"],
                description="Reporting interval",
            ),
        ],
        response_fields=["data[].date", "data[].value"],
        tags=["economic"],
        example="GET /api/v1/alphavantage/economy/gdp?interval=quarterly",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/economy/inflation",
        method="GET",
        description="Get US inflation (CPI) data",
        provider="alpha_vantage",
        response_fields=["data[].date", "data[].value"],
        tags=["economic"],
        example="GET /api/v1/alphavantage/economy/inflation",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/economy/interest-rate",
        method="GET",
        description="Get federal funds interest rate data",
        provider="alpha_vantage",
        response_fields=["data[].date", "data[].value"],
        tags=["economic"],
        example="GET /api/v1/alphavantage/economy/interest-rate",
    ),
    EndpointDef(
        path="/api/v1/alphavantage/economy/unemployment",
        method="GET",
        description="Get US unemployment rate data",
        provider="alpha_vantage",
        response_fields=["data[].date", "data[].value"],
        tags=["economic"],
        example="GET /api/v1/alphavantage/economy/unemployment",
    ),
]


# ---------------------------------------------------------------------------
# Registry: SEC EDGAR (stubs)
# ---------------------------------------------------------------------------

_SEC_ENDPOINTS: list[EndpointDef] = [
    EndpointDef(
        path="/api/v1/sec/company/{cik}",
        method="GET",
        description="Get company information by CIK number",
        provider="sec_edgar",
        path_params=[PathParam(name="cik", type="string", description="SEC CIK number (10-digit, zero-padded)")],
        response_fields=["cik", "name", "tickers", "exchanges", "sic", "sicDescription"],
        tags=["sec", "filings", "fundamentals"],
        example="GET /api/v1/sec/company/0000320193",
    ),
    EndpointDef(
        path="/api/v1/sec/company/ticker/{ticker}",
        method="GET",
        description="Look up SEC company info by ticker symbol",
        provider="sec_edgar",
        path_params=[PathParam(name="ticker", type="string", description="Ticker symbol")],
        response_fields=["cik", "name", "tickers", "exchanges"],
        tags=["sec", "filings"],
        example="GET /api/v1/sec/company/ticker/AAPL",
    ),
    EndpointDef(
        path="/api/v1/sec/filings/{cik}",
        method="GET",
        description="Get SEC filings for a company",
        provider="sec_edgar",
        path_params=[PathParam(name="cik", type="string", description="SEC CIK number")],
        query_params=[
            QueryParam(
                name="form_type",
                type="string",
                required=False,
                description="Filter by form type (10-K, 10-Q, 8-K, etc.)",
            ),
            QueryParam(name="limit", type="integer", required=False, default=40, description="Max filings"),
        ],
        response_fields=[
            "filings[].accessionNumber",
            "filings[].filingDate",
            "filings[].form",
            "filings[].primaryDocument",
        ],
        tags=["sec", "filings"],
        example="GET /api/v1/sec/filings/0000320193?form_type=10-K&limit=5",
    ),
    EndpointDef(
        path="/api/v1/sec/facts/{cik}",
        method="GET",
        description="Get all XBRL company facts (structured financial data)",
        provider="sec_edgar",
        path_params=[PathParam(name="cik", type="string", description="SEC CIK number")],
        response_fields=["cik", "entityName", "facts.us-gaap", "facts.dei"],
        tags=["sec", "filings", "fundamentals"],
        example="GET /api/v1/sec/facts/0000320193",
    ),
    EndpointDef(
        path="/api/v1/sec/search",
        method="GET",
        description="Full-text search across SEC filings",
        provider="sec_edgar",
        query_params=[
            QueryParam(name="q", type="string", required=True, description="Search query"),
            QueryParam(name="form_type", type="string", required=False, description="Filter by form type"),
            QueryParam(name="start", type="integer", required=False, default=0, description="Result offset"),
        ],
        response_fields=["hits.total", "hits.hits[].entity_name", "hits.hits[].file_date", "hits.hits[].form_type"],
        tags=["sec", "filings"],
        example="GET /api/v1/sec/search?q=artificial+intelligence&form_type=10-K",
    ),
]


# ---------------------------------------------------------------------------
# Registry: Yahoo Finance (stubs)
# ---------------------------------------------------------------------------

_YF_ENDPOINTS: list[EndpointDef] = [
    EndpointDef(
        path="/api/v1/yf/ticker/{symbol}/info",
        method="GET",
        description="Get comprehensive ticker information (price, fundamentals, stats)",
        provider="yahoo_finance",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["shortName", "sector", "industry", "marketCap", "trailingPE", "forwardPE", "dividendYield"],
        tags=["fundamentals"],
        example="GET /api/v1/yf/ticker/AAPL/info",
    ),
    EndpointDef(
        path="/api/v1/yf/ticker/{symbol}/history",
        method="GET",
        description="Get historical price data via Yahoo Finance",
        provider="yahoo_finance",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        query_params=[
            QueryParam(
                name="period",
                type="string",
                required=False,
                default="1mo",
                valid_values=["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"],
                description="Lookback period",
            ),
            QueryParam(
                name="interval",
                type="string",
                required=False,
                default="1d",
                valid_values=["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"],
                description="Bar interval",
            ),
        ],
        response_fields=["data[].Date", "data[].Open", "data[].High", "data[].Low", "data[].Close", "data[].Volume"],
        tags=["historical", "bars"],
        example="GET /api/v1/yf/ticker/AAPL/history?period=3mo&interval=1d",
    ),
    EndpointDef(
        path="/api/v1/yf/ticker/{symbol}/financials",
        method="GET",
        description="Get financial statements via Yahoo Finance",
        provider="yahoo_finance",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["income_statement", "balance_sheet", "cash_flow"],
        tags=["fundamentals"],
        example="GET /api/v1/yf/ticker/AAPL/financials",
    ),
    EndpointDef(
        path="/api/v1/yf/ticker/{symbol}/options",
        method="GET",
        description="Get available option expirations for a ticker",
        provider="yahoo_finance",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["expirations[]"],
        tags=["options", "chain"],
        example="GET /api/v1/yf/ticker/AAPL/options",
    ),
    EndpointDef(
        path="/api/v1/yf/ticker/{symbol}/options/{expiration}",
        method="GET",
        description="Get option chain for a specific expiration date",
        provider="yahoo_finance",
        path_params=[
            PathParam(name="symbol", type="string", description="Ticker symbol"),
            PathParam(name="expiration", type="string", description="Expiration date (YYYY-MM-DD)"),
        ],
        response_fields=["calls[]", "puts[]"],
        tags=["options", "chain"],
        example="GET /api/v1/yf/ticker/AAPL/options/2025-06-20",
    ),
    EndpointDef(
        path="/api/v1/yf/ticker/{symbol}/recommendations",
        method="GET",
        description="Get analyst recommendations and price targets",
        provider="yahoo_finance",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=[
            "data[].period",
            "data[].strongBuy",
            "data[].buy",
            "data[].hold",
            "data[].sell",
            "data[].strongSell",
        ],
        tags=["fundamentals", "sentiment"],
        example="GET /api/v1/yf/ticker/AAPL/recommendations",
    ),
    EndpointDef(
        path="/api/v1/yf/ticker/{symbol}/holders",
        method="GET",
        description="Get institutional and insider holders",
        provider="yahoo_finance",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["institutionalHolders[]", "mutualFundHolders[]", "insiderHolders[]"],
        tags=["institutional"],
        example="GET /api/v1/yf/ticker/AAPL/holders",
    ),
    EndpointDef(
        path="/api/v1/yf/ticker/{symbol}/earnings",
        method="GET",
        description="Get earnings history and estimates",
        provider="yahoo_finance",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["quarterly[].date", "quarterly[].actual", "quarterly[].estimate"],
        tags=["fundamentals"],
        example="GET /api/v1/yf/ticker/AAPL/earnings",
    ),
    EndpointDef(
        path="/api/v1/yf/ticker/{symbol}/dividends",
        method="GET",
        description="Get dividend history",
        provider="yahoo_finance",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["data[].date", "data[].dividends"],
        tags=["fundamentals"],
        example="GET /api/v1/yf/ticker/AAPL/dividends",
    ),
]


# ---------------------------------------------------------------------------
# Registry: News aggregator (stubs)
# ---------------------------------------------------------------------------

_NEWS_ENDPOINTS: list[EndpointDef] = [
    EndpointDef(
        path="/api/v1/news/articles",
        method="GET",
        description="Get aggregated news articles",
        provider="news",
        query_params=[
            QueryParam(
                name="symbols", type="string", required=False, description="Comma-separated ticker symbols to filter"
            ),
            QueryParam(name="limit", type="integer", required=False, default=25, description="Max articles"),
        ],
        response_fields=[
            "articles[].headline",
            "articles[].source",
            "articles[].datetime",
            "articles[].summary",
            "articles[].symbols",
        ],
        tags=["sentiment", "news"],
        example="GET /api/v1/news/articles?symbols=AAPL,TSLA&limit=10",
    ),
    EndpointDef(
        path="/api/v1/news/sentiment/{symbol}",
        method="GET",
        description="Get aggregated news sentiment for a symbol",
        provider="news",
        path_params=[PathParam(name="symbol", type="string", description="Ticker symbol")],
        response_fields=["symbol", "sentiment_score", "article_count", "positive", "negative", "neutral"],
        tags=["sentiment", "news"],
        example="GET /api/v1/news/sentiment/AAPL",
    ),
]


# ---------------------------------------------------------------------------
# Master registry — all endpoints in one flat list
# ---------------------------------------------------------------------------

ENDPOINT_REGISTRY: list[EndpointDef] = (
    _ALPACA_ENDPOINTS
    + _UW_ENDPOINTS
    + _FINNHUB_ENDPOINTS
    + _ALPHAVANTAGE_ENDPOINTS
    + _SEC_ENDPOINTS
    + _YF_ENDPOINTS
    + _NEWS_ENDPOINTS
)


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------


def _serialize_endpoint(ep: EndpointDef) -> dict[str, Any]:
    """Convert an EndpointDef to a JSON-serializable dict."""
    return {
        "path": ep.path,
        "method": ep.method,
        "description": ep.description,
        "provider": ep.provider,
        "path_params": [{"name": p.name, "type": p.type, "description": p.description} for p in ep.path_params],
        "query_params": [
            {
                "name": q.name,
                "type": q.type,
                "required": q.required,
                "default": q.default,
                "valid_values": q.valid_values,
                "description": q.description,
            }
            for q in ep.query_params
        ],
        "response_fields": ep.response_fields,
        "tags": ep.tags,
        "example": ep.example,
    }


def get_all_serialized() -> list[dict[str, Any]]:
    """Return the full registry as a list of dicts."""
    return [_serialize_endpoint(ep) for ep in ENDPOINT_REGISTRY]


def get_endpoints_for_provider(provider: str) -> list[dict[str, Any]]:
    """Return serialized endpoints for a specific provider."""
    canonical = PROVIDER_ALIASES.get(provider, provider)
    return [_serialize_endpoint(ep) for ep in ENDPOINT_REGISTRY if ep.provider == canonical or ep.provider == provider]


def get_all_tags() -> list[str]:
    """Return sorted unique list of all tags across the registry."""
    tags: set[str] = set()
    for ep in ENDPOINT_REGISTRY:
        tags.update(ep.tags)
    return sorted(tags)


def search_endpoints(
    *,
    query: str | None = None,
    tags: list[str] | None = None,
    provider: str | None = None,
) -> list[dict[str, Any]]:
    """Search endpoints by keyword in path/description and/or by tags."""
    results: list[EndpointDef] = list(ENDPOINT_REGISTRY)

    if provider:
        canonical = PROVIDER_ALIASES.get(provider, provider)
        results = [ep for ep in results if ep.provider in (provider, canonical)]

    if tags:
        tag_set = set(tags)
        results = [ep for ep in results if tag_set.intersection(ep.tags)]

    if query:
        q_lower = query.lower()
        results = [
            ep
            for ep in results
            if q_lower in ep.path.lower() or q_lower in ep.description.lower() or any(q_lower in t for t in ep.tags)
        ]

    return [_serialize_endpoint(ep) for ep in results]


def get_capabilities() -> dict[str, Any]:
    """Return capabilities grouped by use-case category with endpoint counts."""
    caps: dict[str, Any] = {}
    for key, meta in CAPABILITY_TAXONOMY.items():
        tag_set = set(meta["tags"])
        matching = [ep for ep in ENDPOINT_REGISTRY if tag_set.intersection(ep.tags)]
        providers = sorted({ep.provider for ep in matching})
        caps[key] = {
            "label": meta["label"],
            "description": meta["description"],
            "endpoint_count": len(matching),
            "providers": providers,
            "tags": meta["tags"],
        }
    return caps


def get_endpoints_for_client_permissions(
    *,
    allowed_providers: list[str],
    allowed_feeds: list[str],
) -> list[dict[str, Any]]:
    """Filter registry to endpoints accessible by a client's permissions.

    Provider matching uses aliases (e.g. ``uw`` -> ``unusual_whales``).
    If ``allowed_providers`` is empty the full registry is returned.
    """
    if not allowed_providers:
        return get_all_serialized()

    # Build canonical set of permitted providers
    permitted: set[str] = set()
    for p in allowed_providers:
        permitted.add(p)
        canonical = PROVIDER_ALIASES.get(p, p)
        permitted.add(canonical)
        # Reverse aliases
        for alias, canon in PROVIDER_ALIASES.items():
            if canon == p:
                permitted.add(alias)

    return [_serialize_endpoint(ep) for ep in ENDPOINT_REGISTRY if ep.provider in permitted]
