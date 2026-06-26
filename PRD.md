# Data Gateway PRD

> **Version:** 1.0
> **Status:** Approved
> **Last Updated:** 2026-01-14

## Executive Summary

A centralized data gateway service that solves the Alpaca single-WebSocket-per-API-key limitation by multiplexing one upstream connection to multiple downstream client applications. Additionally provides unified REST API proxying for Alpaca, Unusual Whales, News APIs, yfinance, Alpha Vantage, and Finnhub.

---

## Problem Statement

| Issue | Impact |
|-------|--------|
| Alpaca allows **1 WebSocket per API key** | Multiple projects fight for the connection, causing disconnects |
| Redundant API calls across projects | Rate limit exhaustion, wasted bandwidth |
| No shared caching layer | Same data fetched repeatedly |
| Scattered API credentials | Security and maintenance burden |

---

## Solution Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA GATEWAY                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐    ┌─────────────────────────────────────────┐    │
│  │   Alpaca    │───▶│  WebSocket Multiplexer                   │    │
│  │  (1 conn)   │    │  • Connection management                 │    │
│  └─────────────┘    │  • Subscription aggregation              │    │
│                     │  • Heartbeat/reconnection                │    │
│  ┌─────────────┐    └─────────────┬───────────────────────────┘    │
│  │     UW      │                  │                                 │
│  ├─────────────┤    ┌─────────────▼───────────────────────────┐    │
│  │  News API   │───▶│  REST Proxy + Cache                      │    │
│  ├─────────────┤    │  • Request deduplication                 │    │
│  │ AlphaVantage│    │  • TTL-based caching                     │    │
│  ├─────────────┤    │  • Rate limit management                 │    │
│  │  Finnhub    │    └─────────────┬───────────────────────────┘    │
│  └─────────────┘                  │                                 │
│                                   │                                 │
│                     ┌─────────────▼───────────────────────────┐    │
│                     │  Client Manager                          │    │
│                     │  • API key authentication                │    │
│                     │  • Per-client subscriptions              │    │
│                     │  • WebSocket fan-out                     │    │
│                     └─────────────┬───────────────────────────┘    │
│                                   │                                 │
└───────────────────────────────────┼─────────────────────────────────┘
                                    │
         ┌──────────────────────────┼──────────────────────────┐
         │                          │                          │
         ▼                          ▼                          ▼
    ┌─────────┐              ┌─────────┐              ┌─────────┐
    │ Client  │              │ Client  │              │ Client  │
    │  App 1  │              │  App 2  │              │  App 3  │
    └─────────┘              └─────────┘              └─────────┘
```

---

## Core Components

### 1. WebSocket Multiplexer

**Purpose:** Maintain upstream Alpaca WebSocket connections (one per asset class), fan out data to N downstream clients.

> [!IMPORTANT]
> Alpaca uses **separate WebSocket endpoints** for each data type. Per API key:
> - 1 Stock stream (`wss://stream.data.alpaca.markets/v2/sip`)
> - 1 Options stream (`wss://stream.data.alpaca.markets/v1beta1/options`)
> - 1 Crypto stream (`wss://stream.data.alpaca.markets/v1beta3/crypto/us`)
> - 1 News stream (`wss://stream.data.alpaca.markets/v1beta1/news`)

| Feature | Description |
|---------|-------------|
| **Multi-Stream Management** | Separate connection pools for stocks, options, crypto, news |
| **Connection Pooling** | Support multiple Alpaca API keys for load distribution |
| **Subscription Aggregation** | Union of all client subscriptions sent upstream per stream |
| **Smart Reconnection** | Exponential backoff with jitter, auto-resubscribe |
| **Heartbeat Monitoring** | Detect stale connections, trigger reconnect |

**Alpaca Stream Endpoints:**

| Stream | Endpoint | Data Types |
|--------|----------|------------|
| **Stocks (SIP)** | `wss://stream.data.alpaca.markets/v2/sip` | Bars, Trades, Quotes |
| **Stocks (IEX)** | `wss://stream.data.alpaca.markets/v2/iex` | Bars, Trades, Quotes (free tier) |
| **Options** | `wss://stream.data.alpaca.markets/v1beta1/options` | Bars, Trades, Quotes, Greeks |
| **Crypto** | `wss://stream.data.alpaca.markets/v1beta3/crypto/us` | Bars, Trades, Quotes |
| **News** | `wss://stream.data.alpaca.markets/v1beta1/news` | Headlines, content, symbols, sentiment |
| **Forex** | REST API only (no WebSocket) | Latest rates, Historical rates |

**Multi-Stream Architecture:**

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                                  DATA GATEWAY                                     │
│                                                                                   │
│  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐          │
│  │  Stock Multiplexer │  │ Options Multiplexer│  │ Crypto Multiplexer │          │
│  │                    │  │                    │  │                    │          │
│  │  Key 1 ─▶ WS Conn  │  │  Key 1 ─▶ WS Conn  │  │  Key 1 ─▶ WS Conn  │          │
│  │  Key 2 ─▶ WS Conn  │  │  Key 2 ─▶ WS Conn  │  │                    │          │
│  │                    │  │                    │  │  Pairs: BTC/USD    │          │
│  │  Symbols: AAPL,SPY │  │  AAPL250117C00200  │  │         ETH/USD    │          │
│  └─────────┬──────────┘  └─────────┬──────────┘  └─────────┬──────────┘          │
│            │                       │                       │                      │
│            └───────────────────────┼───────────────────────┘                      │
│                                    ▼                                              │
│                      ┌─────────────────────────┐     ┌──────────────────┐        │
│                      │     Message Router      │     │  Forex REST Proxy│        │
│                      │                         │     │  (polling-based) │        │
│                      │  Routes by client       │     └────────┬─────────┘        │
│                      │  subscription list      │              │                   │
│                      └───────────┬─────────────┘              │                   │
│                                  │                            │                   │
│                                  └──────────┬─────────────────┘                   │
│                                             │                                     │
└─────────────────────────────────────────────┼─────────────────────────────────────┘
                                              │
                    ┌─────────────────────────┼─────────────────────────┐
                    ▼                         ▼                         ▼
               ┌─────────┐              ┌─────────┐              ┌─────────┐
               │Cerberus │              │ Orion   │              │ 3Roses  │
               │(stocks) │              │(options)│              │(crypto) │
               └─────────┘              └─────────┘              └─────────┘
```

**Supported Data Feeds:**

| Asset Class | Data Types |
|-------------|------------|
| **Stocks** | Bars (1m-1M), Trades, Quotes |
| **Options** | Bars, Trades, Quotes, Greeks |
| **Crypto** | Bars, Trades, Quotes |
| **News** | Headlines, articles, symbols, sentiment |
| **Forex** | Latest rates, Historical rates (REST only) |

**Protocol:**
```
Client → Gateway: {"action": "subscribe", "symbols": ["AAPL", "TSLA"], "feeds": ["bars", "quotes"]}
Gateway → Client: {"type": "bar", "symbol": "AAPL", "data": {...}}
```

**Upstream Connection State Machine:**

```mermaid
stateDiagram-v2
    [*] --> Disconnected

    Disconnected --> Connecting: connect()
    Connecting --> Authenticating: socket opened
    Connecting --> Disconnected: connection failed

    Authenticating --> Subscribing: auth success
    Authenticating --> Disconnected: auth failed

    Subscribing --> Connected: subscriptions confirmed
    Subscribing --> Disconnected: subscribe failed

    Connected --> Connected: data received / heartbeat
    Connected --> Reconnecting: connection lost / heartbeat timeout

    Reconnecting --> Connecting: backoff elapsed
    Reconnecting --> Disconnected: max retries exceeded

    note right of Reconnecting
        Exponential backoff:
        1s → 2s → 4s → 8s → 16s (max)
        Jitter: ±20%
    end note
```

**Reconnection Algorithm:**

```python
async def reconnect_with_backoff(self):
    base_delay = 1.0
    max_delay = 16.0
    max_retries = 10

    for attempt in range(max_retries):
        delay = min(base_delay * (2 ** attempt), max_delay)
        jitter = delay * 0.2 * (random.random() * 2 - 1)  # ±20%
        await asyncio.sleep(delay + jitter)

        try:
            await self.connect()
            await self.authenticate()
            await self.resubscribe_all()
            logger.info("Reconnected", attempt=attempt)
            return
        except Exception as e:
            logger.warning("Reconnect failed", attempt=attempt, error=str(e))

    logger.error("Max retries exceeded, giving up")
    raise ConnectionError("Unable to reconnect after max retries")
```

---

### 2. REST API Proxy

**Purpose:** Unified endpoint for all REST API calls with caching and rate limiting.

| Provider | Endpoints | Cache TTL |
|----------|-----------|-----------|
| **Alpaca** | Historical bars, trades, quotes, snapshots, account | 1-60s depending on endpoint |
| **Unusual Whales** | All SDK endpoints (flow, dark pool, institutions, etc.) | 30-300s |
| **News API (NewsAPI.org)** | Articles, events, sentiment | 60s |
| **yfinance** | Tickers, fundamentals, financials, earnings, options chains | 300s |
| **Alpha Vantage** | _Stub for future: fundamentals, earnings_ | 300s |
| **Finnhub** | _Stub for future: insider trades, recommendations_ | 300s |

**Request Flow:**
```
Client Request → Auth Check → Cache Lookup → [Cache Hit] → Return
                                          → [Cache Miss] → Upstream API → Cache Store → Return
```

---

### 3. Client Authentication

**API Key System:**

| Component | Implementation |
|-----------|----------------|
| **Key Generation** | `secrets.token_urlsafe(32)` → 43 char keys |
| **Key Storage** | SHA-256 hashed in `config/clients.yaml` |
| **Key Rotation** | CLI tool: `python -m gateway.cli rotate-key <client_id>` |
| **Rate Limiting** | Per-client configurable limits |

**Client Config Schema:**
```json
{
  "clients": {
    "cerberus": {
      "key_hash": "sha256:...",
      "rate_limit_rpm": 600,
      "allowed_providers": ["alpaca", "uw", "news"],
      "ws_subscriptions_max": 500
    }
  }
}
```

---

### 4. Caching Layer

**Technology:** In-memory with Redis optional for multi-instance deployments.

| Cache Type | Storage | Use Case |
|------------|---------|----------|
| **Hot Cache** | In-memory (LRU) | Sub-100ms lookups, recent bars/quotes |
| **Warm Cache** | Redis (optional) | Shared state across gateway replicas |

**Cache Strategy:**
- REST responses: TTL based on data freshness requirements
- WebSocket data: Ring buffer of last N messages per symbol
- Request deduplication: Coalesce identical in-flight requests

---

## Data Architecture

### Canonical Data Schemas

All data flowing through the gateway adheres to canonical schemas, regardless of source provider. This ensures clients receive consistent data formats.

**Core Market Data Schemas:**

```python
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

@dataclass
class Bar:
    """OHLCV bar data - normalized across all providers"""
    symbol: str
    timestamp: datetime           # UTC, bar close time
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    vwap: Decimal | None          # Volume-weighted average price
    trade_count: int | None       # Number of trades in bar
    provider: str                 # Source: alpaca, yfinance
    timeframe: str                # 1Min, 5Min, 1Hour, 1Day

@dataclass
class Quote:
    """Best bid/ask quote - normalized"""
    symbol: str
    timestamp: datetime           # UTC
    bid_price: Decimal
    bid_size: int
    ask_price: Decimal
    ask_size: int
    provider: str

@dataclass
class Trade:
    """Individual trade - normalized"""
    symbol: str
    timestamp: datetime           # UTC, execution time
    price: Decimal
    size: int
    trade_id: str
    conditions: list[str]         # Trade condition codes
    exchange: str | None
    provider: str

@dataclass
class OptionContract:
    """Option contract data - normalized"""
    contract_symbol: str          # OCC format
    underlying: str
    expiration: date
    strike: Decimal
    option_type: str              # call, put
    bid: Decimal
    ask: Decimal
    last: Decimal
    volume: int
    open_interest: int
    delta: Decimal | None
    gamma: Decimal | None
    theta: Decimal | None
    vega: Decimal | None
    iv: Decimal | None
    provider: str
    timestamp: datetime

@dataclass
class NewsArticle:
    """News article - normalized"""
    article_id: str
    headline: str
    summary: str | None
    content: str | None
    url: str
    source: str
    author: str | None
    published_at: datetime
    symbols: list[str]            # Related tickers
    sentiment: float | None       # -1 to 1
    provider: str
```

---

### Data Normalization Layer

Provider responses are automatically normalized to canonical schemas:

| Provider | Raw Format | Transformation |
|----------|------------|----------------|
| **Alpaca** | `{"t": "2026-01-14T21:30:00Z", "o": 185.5, ...}` | Map single-letter keys to full names |
| **yfinance** | `{"Date": "2026-01-14", "Open": 185.5, ...}` | Parse date, rename capitalized keys |
| **UW** | Provider-specific JSON | Map to canonical option/flow schemas |
| **News** | Various JSON formats | Extract headline, summary, symbols |

**Normalization Rules:**

```python
class DataNormalizer:
    def normalize_bar(self, raw: dict, provider: str) -> Bar:
        if provider == "alpaca":
            return Bar(
                symbol=raw["S"],
                timestamp=datetime.fromisoformat(raw["t"].replace("Z", "+00:00")),
                open=Decimal(str(raw["o"])),
                high=Decimal(str(raw["h"])),
                low=Decimal(str(raw["l"])),
                close=Decimal(str(raw["c"])),
                volume=int(raw["v"]),
                vwap=Decimal(str(raw.get("vw"))) if raw.get("vw") else None,
                trade_count=raw.get("n"),
                provider="alpaca",
                timeframe=raw.get("timeframe", "1Min")
            )
        elif provider == "yfinance":
            return Bar(
                symbol=raw["symbol"],
                timestamp=datetime.fromisoformat(raw["Date"]),
                open=Decimal(str(raw["Open"])),
                high=Decimal(str(raw["High"])),
                low=Decimal(str(raw["Low"])),
                close=Decimal(str(raw["Close"])),
                volume=int(raw["Volume"]),
                vwap=None,  # yfinance doesn't provide VWAP
                trade_count=None,
                provider="yfinance",
                timeframe=raw.get("interval", "1d")
            )
```

---

### Data Validation

All incoming data is validated before delivery to clients:

**Validation Rules:**

| Field | Rule | On Failure |
|-------|------|------------|
| `timestamp` | Must be valid ISO8601, not in future | Reject, log GW-E7001 |
| `open/high/low/close` | Must be > 0 | Reject, log GW-E7002 |
| `high >= low` | High must be >= Low | Reject, log GW-E7003 |
| `high >= open, close` | High must be >= Open and Close | Reject, log GW-E7004 |
| `low <= open, close` | Low must be <= Open and Close | Reject, log GW-E7005 |
| `volume` | Must be >= 0 | Reject, log GW-E7006 |
| `symbol` | Must match known pattern | Reject, log GW-E7007 |

**Validator Implementation:**

```python
class DataValidator:
    def validate_bar(self, bar: Bar) -> ValidationResult:
        errors = []

        # Price validation
        if bar.close <= 0:
            errors.append(("GW-E7002", f"Invalid close price: {bar.close}"))
        if bar.high < bar.low:
            errors.append(("GW-E7003", f"High ({bar.high}) < Low ({bar.low})"))
        if bar.high < bar.open or bar.high < bar.close:
            errors.append(("GW-E7004", f"High ({bar.high}) < Open/Close"))
        if bar.low > bar.open or bar.low > bar.close:
            errors.append(("GW-E7005", f"Low ({bar.low}) > Open/Close"))

        # Volume validation
        if bar.volume < 0:
            errors.append(("GW-E7006", f"Negative volume: {bar.volume}"))

        # Timestamp validation
        if bar.timestamp > datetime.now(UTC):
            errors.append(("GW-E7001", f"Future timestamp: {bar.timestamp}"))

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            bar=bar if len(errors) == 0 else None
        )
```

---

### Symbology Service

Gateway provides symbol resolution and format conversion:

**Symbol Formats:**

| Type | Format | Example |
|------|--------|---------|
| Stock | Uppercase ticker | `AAPL` |
| Option (OCC) | 21 characters | `AAPL250117C00200000` |
| Option (Human) | Readable | `AAPL 2025-01-17 $200 C` |
| Crypto | Pair format | `BTC/USD` |
| Forex | ISO pair | `EUR/USD` |

**Symbol Resolution Endpoint:**

```
GET /api/v1/symbology/resolve?symbol=AAPL%20Jan17%20200C

Response:
{
  "success": true,
  "data": {
    "input": "AAPL Jan17 200C",
    "normalized": {
      "occ": "AAPL250117C00200000",
      "human": "AAPL 2025-01-17 $200 Call",
      "underlying": "AAPL",
      "expiration": "2025-01-17",
      "strike": 200.00,
      "type": "call"
    },
    "provider_formats": {
      "alpaca": "AAPL250117C00200000",
      "uw": "AAPL_250117C200"
    }
  }
}
```

**Symbol Validation:**

```python
class SymbolResolver:
    STOCK_PATTERN = re.compile(r'^[A-Z]{1,5}$')
    OCC_PATTERN = re.compile(r'^([A-Z]{1,6})(\d{6})([CP])(\d{8})$')

    def resolve(self, symbol: str) -> ResolvedSymbol:
        # Try stock first
        if self.STOCK_PATTERN.match(symbol):
            return ResolvedSymbol(type="stock", symbol=symbol)

        # Try OCC option
        if match := self.OCC_PATTERN.match(symbol):
            return ResolvedSymbol(
                type="option",
                underlying=match.group(1),
                expiration=self._parse_date(match.group(2)),
                option_type="call" if match.group(3) == "C" else "put",
                strike=Decimal(match.group(4)) / 1000
            )

        # Try human-readable option
        return self._parse_human_option(symbol)
```

---

### Data Lineage & Provenance

All data includes provenance metadata for traceability:

**Metadata Envelope:**

```json
{
  "type": "data",
  "feed": "stock_bars",
  "symbol": "AAPL",
  "data": {
    "open": 185.50,
    "high": 185.75,
    "low": 185.40,
    "close": 185.60,
    "volume": 125000
  },
  "meta": {
    "provider": "alpaca",
    "provider_timestamp": "2026-01-14T21:30:00.123Z",
    "gateway_received_at": "2026-01-14T21:30:00.125Z",
    "gateway_processed_at": "2026-01-14T21:30:00.126Z",
    "gateway_sent_at": "2026-01-14T21:30:00.127Z",
    "latency_ms": {
      "provider_to_gateway": 2,
      "gateway_processing": 1,
      "total": 4
    },
    "sequence": 12345,
    "cached": false,
    "cache_key": null,
    "normalized": true,
    "validated": true
  }
}
```

**REST Response Metadata:**

```json
{
  "success": true,
  "data": [...],
  "meta": {
    "request_id": "req-abc123",
    "provider": "yfinance",
    "cached": true,
    "cache_age_ms": 15000,
    "cache_ttl_remaining_ms": 285000,
    "record_count": 100,
    "data_range": {
      "start": "2026-01-01T00:00:00Z",
      "end": "2026-01-14T00:00:00Z"
    }
  }
}
```

---

### Data Deduplication

Gateway prevents duplicate message delivery:

**Deduplication Strategy:**

```python
class MessageDeduplicator:
    def __init__(self, window_size: int = 1000):
        # Per-symbol LRU cache of seen message hashes
        self.seen: dict[str, LRUCache] = {}

    def _hash(self, symbol: str, timestamp: str, data: dict) -> str:
        """Create unique hash for deduplication"""
        content = f"{symbol}:{timestamp}:{json.dumps(data, sort_keys=True)}"
        return hashlib.md5(content.encode()).hexdigest()

    def is_duplicate(self, symbol: str, message: dict) -> bool:
        key = self._hash(symbol, message["timestamp"], message["data"])

        if symbol not in self.seen:
            self.seen[symbol] = LRUCache(maxsize=1000)

        if key in self.seen[symbol]:
            logger.debug("Duplicate detected", symbol=symbol, code="GW-I7010")
            return True

        self.seen[symbol][key] = True
        return False
```

**Deduplication Scope:**

| Scenario | Behavior |
|----------|----------|
| Same bar from retry | Deduplicated, single delivery |
| Same bar to multiple clients | Each client gets one copy |
| Reconnection replay | Deduplicated if within window |
| Different timeframe, same close time | NOT deduplicated (different data) |

---

### Data Freshness Policy

**Cache Freshness Rules:**

| Data Type | Max Cache Age | Stale-If-Error | Use Case |
|-----------|---------------|----------------|----------|
| **Real-time bars** | 60s | No | Active trading |
| **Quotes** | 1s | No | Order routing |
| **Trades** | 5s | No | Tape reading |
| **Historical bars** | 24h | Yes (serve stale) | Backtesting |
| **Options chain** | 60s | Yes (5 min max) | Strategy analysis |
| **Fundamentals** | 300s | Yes (24h max) | Research |
| **News** | 60s | Yes (1h max) | Alerts |

**Freshness Headers:**

```
# Request fresh data only (bypass cache)
X-Gateway-Cache: bypass

# Accept stale data if upstream fails
Accept-Stale: true
Accept-Stale-Max-Age: 300

# Response freshness indicators
X-Gateway-Cache: HIT
X-Gateway-Cache-Age: 15000
X-Gateway-Stale: false
X-Gateway-Data-Age: 2000
```

**Stale Data Handling:**

```python
async def fetch_with_freshness(self, key: str, max_age: int) -> Response:
    cached = await self.cache.get(key)

    try:
        fresh = await self.fetch_upstream(key)
        await self.cache.set(key, fresh)
        return fresh
    except UpstreamError as e:
        if cached and self.accept_stale:
            cache_age = time.time() - cached.timestamp
            if cache_age < self.stale_max_age:
                return cached.with_header("X-Gateway-Stale", "true")
        raise
```

---

### Temporal Data Handling

**Timestamp Standards:**

| Rule | Specification |
|------|---------------|
| Format | ISO 8601 with timezone |
| Timezone | All timestamps in UTC (suffix `Z`) |
| Precision | Milliseconds for real-time, seconds for historical |
| Bar timestamp | Close time of the bar |

**Market Sessions:**

| Session | Hours (Eastern) | Data Available |
|---------|-----------------|----------------|
| Pre-market | 04:00 - 09:30 | Trades, Quotes (limited) |
| Regular | 09:30 - 16:00 | All data types |
| After-hours | 16:00 - 20:00 | Trades, Quotes (limited) |
| Closed | 20:00 - 04:00 | None (historical only) |

**Session Filtering:**

```
GET /api/v1/alpaca/stocks/AAPL/bars?session=regular
GET /api/v1/alpaca/stocks/AAPL/bars?session=extended
GET /api/v1/alpaca/stocks/AAPL/bars?session=all
```

**Timezone Handling:**

```python
class TimestampNormalizer:
    def normalize(self, ts: str, source_tz: str = "UTC") -> datetime:
        """Convert any timestamp to UTC datetime"""
        if ts.endswith("Z"):
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))

        # Parse with source timezone, convert to UTC
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = pytz.timezone(source_tz).localize(dt)
        return dt.astimezone(pytz.UTC)
```

---

### Bar Aggregation Rules

When aggregating fine-grained bars to coarser timeframes:

**OHLCV Aggregation:**

| Field | Aggregation Rule |
|-------|------------------|
| Open | First bar's open |
| High | Maximum of all highs |
| Low | Minimum of all lows |
| Close | Last bar's close |
| Volume | Sum of all volumes |
| VWAP | Recalculate: sum(price × volume) / sum(volume) |
| Trade Count | Sum of all trade counts |

**Example:**

```
Input: 3 × 1-min bars
[09:30] O=100, H=102, L=99, C=101, V=1000
[09:31] O=101, H=103, L=100, C=102, V=1500
[09:32] O=102, H=104, L=101, C=103, V=1200

Output: 1 × 3-min bar
[09:32] O=100, H=104, L=99, C=103, V=3700
```

**Gap Handling:**

| Scenario | Behavior |
|----------|----------|
| Missing bars in range | Aggregate available bars, set `complete: false` |
| All bars missing | Return error, do not fabricate data |
| Partial data (no volume) | Aggregate available fields, nullify missing |

**Response with Gap Indicator:**

```json
{
  "data": {...},
  "meta": {
    "aggregated": true,
    "source_timeframe": "1Min",
    "target_timeframe": "5Min",
    "complete": false,
    "missing_bars": 2,
    "expected_bars": 5,
    "coverage": 0.6
  }
}
```

**Adjusted vs. Unadjusted Prices:**

| Parameter | Behavior |
|-----------|----------|
| `?adjusted=false` (default) | Raw exchange prices |
| `?adjusted=true` | Split and dividend adjusted |

---

## Quant Research Support

### Point-in-Time (PIT) Data Guarantees

> [!CAUTION]
> Backtesting requires knowing what data was available at decision time, not what data exists today with the benefit of hindsight. Using non-PIT data introduces look-ahead bias.

**PIT Support by Provider:**

| Provider | Data Type | PIT Support | Notes |
|----------|-----------|-------------|-------|
| **Alpaca** | Real-time bars | ✅ Inherent | Data delivered at market time |
| **Alpaca** | Historical bars | ✅ Yes | Original data preserved |
| **Alpaca** | Options chains | ⚠️ Limited | Current chain, no historical |
| **yfinance** | Historical bars | ❌ No | Retroactively adjusted |
| **yfinance** | Fundamentals | ❌ No | May contain restated values |
| **UW** | Flow data | ✅ Yes | Timestamped at execution |
| **News** | Articles | ✅ Yes | Publication timestamp |

**Adjusted Prices Warning:**

```python
# BAD: Introduces look-ahead bias
# Adjusted prices incorporate future splits/dividends
bars = await gateway.get_bars("AAPL", start="2020-01-01", adjusted=True)

# GOOD: Use unadjusted + separate adjustment factors
bars = await gateway.get_bars("AAPL", start="2020-01-01", adjusted=False)
adj_factors = await gateway.get_adjustment_factors("AAPL", start="2020-01-01")
# Apply adjustments with as-of logic in your backtest
```

---

### As-Of Query Support

Query historical data as it existed at a specific point in time:

**Endpoints with As-Of Support:**

```
GET /api/v1/alpaca/stocks/AAPL/bars?as_of=2024-01-15T16:00:00Z
GET /api/v1/alpaca/options/chain/AAPL?as_of=2024-01-15T16:00:00Z
```

**As-Of Behavior:**

| Provider | As-Of Support | Behavior |
|----------|---------------|----------|
| **Alpaca** | ✅ Historical bars | Returns data available at that time |
| **Alpaca** | ⚠️ Options chains | Not supported (current only) |
| **yfinance** | ❌ | Returns current data regardless |
| **UW** | ⚠️ Limited | Flow data timestamped, chain not supported |

**Response Metadata:**

```json
{
  "data": [...],
  "meta": {
    "as_of": "2024-01-15T16:00:00Z",
    "pit_guaranteed": true,
    "data_version": "original"  // or "restated"
  }
}
```

---

### Survivorship Bias Handling

**Delisted Ticker Support:**

| Provider | Delisted Access | Notes |
|----------|-----------------|-------|
| **Alpaca** | ✅ Yes | Historical data preserved |
| **yfinance** | ⚠️ Partial | May fail for old delistings |
| **UW** | ⚠️ Limited | Recent data only |

**Querying Delisted Tickers:**

```
GET /api/v1/alpaca/stocks/LUMN/bars?start=2022-01-01&end=2023-01-01
```

Response includes delisting info:
```json
{
  "data": [...],
  "meta": {
    "symbol": "LUMN",
    "symbol_status": "delisted",
    "delisting_date": "2023-10-02",
    "delisting_reason": "merger"
  }
}
```

**Symbol Changes Endpoint:**

```
GET /api/v1/symbology/changes?start=2023-01-01&end=2024-01-01

Response:
{
  "changes": [
    {
      "date": "2023-10-02",
      "old_symbol": "LUMN",
      "new_symbol": null,
      "action": "delisted",
      "reason": "Acquired by Apollo"
    },
    {
      "date": "2023-07-03",
      "old_symbol": "META",
      "new_symbol": "META",
      "action": "renamed",
      "reason": "Facebook rebranding"
    }
  ]
}
```

> [!IMPORTANT]
> The gateway does NOT provide historical index constituents. For research requiring point-in-time S&P 500 membership, use external sources like Compustat or CRSP.

---

### Historical Replay Mode

Simulate real-time data arrival for strategy backtesting:

**Create Replay Session:**

```
POST /api/v1/replay/sessions
{
  "name": "backtest-2024-q1",
  "symbols": ["AAPL", "MSFT", "GOOGL"],
  "feeds": ["bars", "quotes", "trades"],
  "start": "2024-01-15T09:30:00Z",
  "end": "2024-01-15T16:00:00Z",
  "speed": 10.0,           // 10x real-time
  "include_premarket": false
}

Response:
{
  "session_id": "replay-abc123",
  "ws_endpoint": "ws://gateway:8080/ws/replay/replay-abc123",
  "estimated_messages": 450000,
  "estimated_duration_seconds": 2340  // 6.5 hours / 10x = 39 min
}
```

**Replay WebSocket Messages:**

```json
{
  "type": "data",
  "feed": "stock_bars",
  "symbol": "AAPL",
  "data": {
    "open": 185.50,
    "high": 185.75,
    "low": 185.40,
    "close": 185.60,
    "volume": 125000
  },
  "meta": {
    "market_timestamp": "2024-01-15T10:30:00.123Z",
    "replay_wall_clock": "2026-01-14T14:06:00.012Z",
    "replay_speed": 10.0,
    "sequence": 12345,
    "session_progress": 0.15  // 15% complete
  }
}
```

**Replay Control:**

```json
// Pause
{"action": "pause", "session_id": "replay-abc123"}

// Resume at different speed
{"action": "resume", "session_id": "replay-abc123", "speed": 1.0}

// Seek to timestamp
{"action": "seek", "session_id": "replay-abc123", "timestamp": "2024-01-15T14:00:00Z"}

// Stop
{"action": "stop", "session_id": "replay-abc123"}
```

---

### Tick Data Support

Individual trade and quote data for high-frequency analysis:

**Trade Tape (Tick-by-Tick):**

```json
// Subscribe
{"action": "subscribe", "feed": "stock_trades", "symbols": ["AAPL"]}

// Received message
{
  "type": "data",
  "feed": "stock_trades",
  "symbol": "AAPL",
  "data": {
    "price": 185.50,
    "size": 100,
    "timestamp": "2024-01-15T10:30:00.123456789Z",
    "trade_id": "T123456789",
    "conditions": ["@", "F"],
    "exchange": "Q",
    "tape": "C"
  }
}
```

**Trade Condition Codes:**

| Code | Meaning |
|------|---------|
| `@` | Regular sale |
| `F` | Intermarket sweep |
| `I` | Odd lot |
| `T` | Extended hours |
| `W` | Average price |
| `Z` | Sold (out of sequence) |

**Quote Updates (NBBO):**

```json
{
  "type": "data",
  "feed": "stock_quotes",
  "symbol": "AAPL",
  "data": {
    "bid_price": 185.49,
    "bid_size": 200,
    "bid_exchange": "Q",
    "ask_price": 185.51,
    "ask_size": 300,
    "ask_exchange": "P",
    "timestamp": "2024-01-15T10:30:00.123456789Z",
    "conditions": ["R"]  // Regular
  }
}
```

**Data Granularity Reference:**

| Feed | Granularity | Typical Volume | Use Case |
|------|-------------|----------------|----------|
| `stock_bars` | 1-min aggregated | ~390/day | Strategy signals |
| `stock_quotes` | Real-time NBBO | ~100K/day | Spread analysis |
| `stock_trades` | Individual ticks | ~50K/day | Microstructure research |
| `option_quotes` | Real-time | Varies | Options trading |

---

### Corporate Actions

**Corporate Actions Endpoint:**

```
GET /api/v1/corporate-actions/AAPL?start=2020-01-01&end=2024-01-01
```

```json
{
  "symbol": "AAPL",
  "actions": [
    {
      "type": "split",
      "ex_date": "2020-08-31",
      "record_date": "2020-08-24",
      "ratio": 4.0,
      "description": "4-for-1 stock split"
    },
    {
      "type": "dividend",
      "ex_date": "2024-02-09",
      "record_date": "2024-02-12",
      "pay_date": "2024-02-15",
      "amount": 0.24,
      "currency": "USD",
      "frequency": "quarterly"
    },
    {
      "type": "spinoff",
      "ex_date": "2023-04-03",
      "parent_symbol": "GE",
      "child_symbol": "GEV",
      "ratio": 0.25,
      "description": "GE Vernova spinoff"
    }
  ]
}
```

**Adjustment Factors Endpoint:**

```
GET /api/v1/adjustment-factors/AAPL?start=2020-01-01&end=2024-01-01
```

```json
{
  "symbol": "AAPL",
  "factors": [
    {
      "date": "2020-08-30",
      "cumulative_factor": 1.0,
      "split_factor": 1.0,
      "dividend_factor": 1.0
    },
    {
      "date": "2020-08-31",
      "cumulative_factor": 0.25,
      "split_factor": 0.25,
      "dividend_factor": 1.0,
      "event": "4:1 split"
    }
  ]
}
```

**Applying Adjustments:**

```python
# Get unadjusted prices + factors
bars = await gateway.get_bars("AAPL", start="2020-01-01", adjusted=False)
factors = await gateway.get_adjustment_factors("AAPL", start="2020-01-01")

# Apply adjustments as-of each bar's date (proper PIT adjustment)
for bar in bars:
    factor = factors.get_as_of(bar.timestamp)
    adjusted_close = bar.close * factor.cumulative_factor
```

---

### Bulk Data Endpoints

Efficient retrieval of large datasets for research:

**Batch Historical Bars:**

```
POST /api/v1/bulk/bars
{
  "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", ...],  // Up to 500
  "start": "2023-01-01",
  "end": "2024-01-01",
  "timeframe": "1Day",
  "adjusted": false,
  "format": "jsonl"  // or "parquet"
}

Response:
{
  "job_id": "bulk-abc123",
  "status": "accepted",
  "estimated_records": 125000,
  "estimated_size_mb": 45
}
```

**Check Job Status:**

```
GET /api/v1/bulk/jobs/bulk-abc123

{
  "job_id": "bulk-abc123",
  "status": "running",  // pending, running, complete, failed
  "progress": 0.65,
  "symbols_complete": 325,
  "symbols_total": 500,
  "records_fetched": 81250,
  "started_at": "2026-01-14T14:00:00Z",
  "eta_seconds": 120
}
```

**Download Results:**

```
# Streaming JSONL
GET /api/v1/bulk/jobs/bulk-abc123/download?format=jsonl

{"symbol": "AAPL", "timestamp": "2023-01-03", "open": 130.28, ...}
{"symbol": "AAPL", "timestamp": "2023-01-04", "open": 126.89, ...}
...

# Parquet (for large datasets)
GET /api/v1/bulk/jobs/bulk-abc123/download?format=parquet
# Returns binary Parquet file
```

**Bulk Options Chains:**

```
POST /api/v1/bulk/options/chains
{
  "underlyings": ["AAPL", "MSFT", "SPY"],
  "date": "2024-01-15",
  "expiration_range": {
    "min_dte": 0,
    "max_dte": 60
  },
  "moneyness_range": {
    "min_delta": 0.1,
    "max_delta": 0.9
  }
}
```

---

### Trading Calendar

**Market Hours Endpoint:**

```
GET /api/v1/calendar/market-hours?date=2024-01-15

{
  "date": "2024-01-15",
  "market": "NYSE",
  "status": "open",
  "sessions": {
    "premarket": {"start": "04:00", "end": "09:30"},
    "regular": {"start": "09:30", "end": "16:00"},
    "afterhours": {"start": "16:00", "end": "20:00"}
  },
  "timezone": "America/New_York",
  "is_holiday": false,
  "is_early_close": false
}
```

**Trading Days Range:**

```
GET /api/v1/calendar/trading-days?start=2024-01-01&end=2024-01-31

{
  "trading_days": [
    "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
    "2024-01-08", ...
  ],
  "holidays": [
    {"date": "2024-01-01", "name": "New Year's Day"},
    {"date": "2024-01-15", "name": "Martin Luther King Jr. Day"}
  ],
  "early_closes": []
}
```

**Earnings Calendar:**

```
GET /api/v1/calendar/earnings?symbols=AAPL,MSFT&start=2024-01-01&end=2024-03-31

{
  "earnings": [
    {
      "symbol": "AAPL",
      "date": "2024-02-01",
      "time": "after_close",
      "eps_estimate": 2.10,
      "revenue_estimate": 117000000000
    },
    {
      "symbol": "MSFT",
      "date": "2024-01-30",
      "time": "after_close",
      "eps_estimate": 2.78,
      "revenue_estimate": 61000000000
    }
  ]
}
```

---

### Research Best Practices

> [!WARNING]
> **Common Pitfalls to Avoid**

| Pitfall | How to Avoid |
|---------|--------------|
| **Look-ahead bias** | Use unadjusted prices + as-of adjustment factors |
| **Survivorship bias** | Include delisted tickers in historical universe |
| **Data snooping** | Use out-of-sample testing periods |
| **Fill assumptions** | Account for slippage, don't assume mid-price fills |
| **Corporate action gaps** | Check for splits/dividends on large price gaps |

**Recommended Research Workflow:**

```python
# 1. Get historical universe (including delisted)
symbols = await gateway.get_symbols_as_of("SP500", "2020-01-01")

# 2. Fetch unadjusted data
bars = await gateway.bulk_get_bars(
    symbols=symbols,
    start="2020-01-01",
    end="2024-01-01",
    adjusted=False
)

# 3. Get adjustment factors separately
factors = await gateway.bulk_get_adjustment_factors(
    symbols=symbols,
    start="2020-01-01"
)

# 4. Check data quality
quality = await gateway.get_quality_report(symbols, "2020-01-01", "2024-01-01")
if quality.gaps > 0.01:
    logger.warning(f"Data has {quality.gaps:.1%} missing bars")

# 5. Apply adjustments point-in-time during backtest
for date in trading_days:
    prices = bars.as_of(date)
    adj = factors.as_of(date)
    adjusted_prices = prices * adj
```

---

## ML Integration (Stateless)

> [!NOTE]
> This section covers stateless ML enhancements built into the gateway. Feature stores, experiment tracking, and dataset versioning require external storage (build separately).

### ML Export Formats

Bulk endpoints support ML-friendly export formats:

**Supported Formats:**

| Format | Content-Type | Use Case | Size Efficiency |
|--------|-------------|----------|-----------------|
| JSON | `application/json` | API compatibility | 1x (baseline) |
| JSONL | `application/x-ndjson` | Streaming, line-by-line | 1x |
| Parquet | `application/vnd.apache.parquet` | Training datasets | 0.2x (compressed) |
| Arrow IPC | `application/vnd.apache.arrow.stream` | In-memory analytics | 0.3x |
| CSV | `text/csv` | Legacy compatibility | 1.1x |

**Request Format Parameter:**

```
GET /api/v1/bulk/jobs/{job_id}/download?format=parquet

# Or request via Accept header
GET /api/v1/bulk/jobs/{job_id}/download
Accept: application/vnd.apache.parquet
```

**Parquet Schema:**

```python
# Output schema for bar data
schema = pa.schema([
    ('symbol', pa.string()),
    ('timestamp', pa.timestamp('us', tz='UTC')),
    ('open', pa.float64()),
    ('high', pa.float64()),
    ('low', pa.float64()),
    ('close', pa.float64()),
    ('volume', pa.int64()),
    ('vwap', pa.float64()),
    ('trade_count', pa.int32()),
    ('provider', pa.string()),
])
```

**Partitioning Options:**

```
POST /api/v1/bulk/bars
{
  "symbols": ["AAPL", "MSFT", ...],
  "start": "2023-01-01",
  "end": "2024-01-01",
  "format": "parquet",
  "partitioning": {
    "by": ["symbol", "date"],   // Partition columns
    "compression": "snappy"     // snappy, gzip, zstd
  }
}
```

---

### Model Integration Patterns

#### Pattern 1: Direct WebSocket Subscription

Lowest latency for real-time inference:

```python
import asyncio
from gateway.client import GatewayClient

class RealTimePredictor:
    def __init__(self, model, symbols):
        self.model = model
        self.symbols = symbols
        self.gateway = GatewayClient("ws://gateway:8080/ws", api_key="gw_...")

    async def run(self):
        await self.gateway.connect()
        await self.gateway.subscribe(symbols=self.symbols, feeds=["bars"])

        async for message in self.gateway.stream():
            if message["type"] == "data":
                bar = message["data"]
                features = self.extract_features(bar)
                prediction = self.model.predict(features)
                await self.publish_prediction(bar["symbol"], prediction)

    def extract_features(self, bar):
        # Transform bar to feature vector
        return [bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]]
```

**Latency Profile:**
- Gateway delivery: < 10ms
- Feature extraction: < 5ms
- Model inference: < 20ms
- **Total: < 35ms**

#### Pattern 2: Event-Driven via Message Queue

For decoupled, scalable inference:

```
┌──────────┐     ┌─────────┐     ┌──────────────┐     ┌───────┐
│ Gateway  │────►│  Kafka  │────►│ Feature      │────►│ Model │
│          │     │         │     │ Pipeline     │     │       │
└──────────┘     └─────────┘     └──────────────┘     └───────┘
     │                                                      │
     │                                                      ▼
     │                                               ┌────────────┐
     └──────────────────────────────────────────────►│ Predictions│
                                                     │ Topic      │
                                                     └────────────┘
```

```python
# Gateway producer
async def gateway_to_kafka():
    async for bar in gateway.stream():
        await kafka.send("market.bars", bar)

# Model consumer
async def model_consumer():
    async for bar in kafka.consume("market.bars"):
        features = pipeline.transform(bar)
        prediction = model.predict(features)
        await kafka.send("predictions", prediction)
```

**Latency Profile:**
- Gateway → Kafka: < 5ms
- Kafka → Consumer: < 10ms
- Feature + Inference: < 30ms
- **Total: < 45ms**

#### Pattern 3: Scheduled Batch Inference

For daily/hourly predictions:

```python
import schedule
from gateway.client import GatewayClient

def batch_inference():
    # Fetch latest bars for all symbols
    bars = gateway.get_bars(
        symbols=UNIVERSE,
        timeframe="1Min",
        limit=1
    )

    # Bulk feature extraction
    features = feature_pipeline.transform_batch(bars)

    # Batch prediction
    predictions = model.predict_batch(features)

    # Store/publish predictions
    store_predictions(predictions)

# Schedule every minute
schedule.every(1).minute.do(batch_inference)
```

**Best For:** Position sizing, daily rebalancing, overnight signals

---

### On-Demand Dataset Splitting

Bulk exports support time-series splits:

**Temporal Split (Recommended):**

```
POST /api/v1/bulk/bars
{
  "symbols": ["AAPL", "MSFT"],
  "start": "2023-01-01",
  "end": "2024-01-01",
  "format": "parquet",
  "split": {
    "strategy": "temporal",
    "train_end": "2023-06-30",
    "validation_end": "2023-09-30"
    // test_end = request end date
  }
}

Response:
{
  "job_id": "bulk-abc123",
  "splits": {
    "train": {
      "path": "train/",
      "start": "2023-01-01",
      "end": "2023-06-30",
      "records": 100000
    },
    "validation": {
      "path": "validation/",
      "start": "2023-07-01",
      "end": "2023-09-30",
      "records": 50000
    },
    "test": {
      "path": "test/",
      "start": "2023-10-01",
      "end": "2023-12-31",
      "records": 50000
    }
  }
}
```

**Walk-Forward Split:**

```
POST /api/v1/bulk/bars
{
  ...
  "split": {
    "strategy": "walk_forward",
    "train_window": "180d",
    "test_window": "30d",
    "step": "30d",
    "embargo": "1d"   // Gap between train/test to prevent leakage
  }
}

Response:
{
  "splits": [
    {"fold": 1, "train": "2023-01-01/2023-06-30", "test": "2023-07-02/2023-07-31"},
    {"fold": 2, "train": "2023-02-01/2023-07-31", "test": "2023-08-02/2023-08-31"},
    {"fold": 3, "train": "2023-03-01/2023-08-31", "test": "2023-09-02/2023-09-30"},
    ...
  ]
}
```

---

### Label Computation

Compute labels/targets at export time (no storage required):

**Built-in Label Functions:**

| Label | Formula | Parameters |
|-------|---------|------------|
| `return` | (close[t+n] - close[t]) / close[t] | `lookahead` |
| `log_return` | log(close[t+n] / close[t]) | `lookahead` |
| `direction` | sign(return) | `lookahead` |
| `volatility` | std(returns) over window | `window` |
| `max_return` | max(returns) over window | `lookahead` |
| `min_return` | min(returns) over window | `lookahead` |
| `hit_target` | 1 if return > threshold else 0 | `lookahead`, `threshold` |

**Request with Labels:**

```
POST /api/v1/bulk/bars
{
  "symbols": ["AAPL", "MSFT"],
  "start": "2023-01-01",
  "end": "2024-01-01",
  "format": "parquet",
  "labels": [
    {"name": "return_5m", "type": "return", "lookahead": "5m"},
    {"name": "return_1h", "type": "return", "lookahead": "1h"},
    {"name": "direction_5m", "type": "direction", "lookahead": "5m"},
    {"name": "volatility_1h", "type": "volatility", "window": "1h"},
    {"name": "hit_1pct", "type": "hit_target", "lookahead": "1d", "threshold": 0.01}
  ]
}
```

**Output Schema with Labels:**

```python
schema = pa.schema([
    # Original bar fields
    ('symbol', pa.string()),
    ('timestamp', pa.timestamp('us', tz='UTC')),
    ('open', pa.float64()),
    ('close', pa.float64()),
    ...
    # Computed labels
    ('return_5m', pa.float64()),
    ('return_1h', pa.float64()),
    ('direction_5m', pa.int8()),      # -1, 0, 1
    ('volatility_1h', pa.float64()),
    ('hit_1pct', pa.int8()),          # 0, 1
])
```

> [!CAUTION]
> Labels use future data. The last `lookahead` rows will have null labels. Only use labels for training, never inference.

---

### ML Latency Requirements

**End-to-End Inference Latency:**

| Stage | p50 | p99 | Max | Notes |
|-------|-----|-----|-----|-------|
| Gateway delivery | < 5ms | < 20ms | < 50ms | WebSocket message |
| Feature extraction | < 5ms | < 15ms | < 30ms | Simple indicators |
| Model inference | < 10ms | < 30ms | < 100ms | Depends on model |
| **Total pipeline** | < 20ms | < 65ms | < 180ms | Target |

**Throughput Requirements:**

| Scenario | Target Throughput |
|----------|-------------------|
| Single symbol, single model | 1,000 inferences/sec |
| 500 symbols, single model | 500 inferences/sec |
| 500 symbols, ensemble (5 models) | 100 inferences/sec |

**Batch Processing:**

| Operation | Throughput |
|-----------|------------|
| Bulk export (Parquet) | 100,000 rows/sec |
| Label computation | 50,000 rows/sec |
| Streaming download | 10 MB/sec |

---

### ML Client SDK Patterns

#### Async Data Loader

Efficient data loading for training:

```python
from gateway.ml import AsyncDataLoader

loader = AsyncDataLoader(
    gateway_url="http://gateway:8080",
    api_key="gw_...",
    batch_size=1024,
    prefetch_batches=4,
    shuffle=True
)

# Training loop
for epoch in range(100):
    async for batch in loader.iter_batches(dataset_path):
        features = batch["features"]
        labels = batch["labels"]
        loss = model.train_step(features, labels)
```

#### GPU-Friendly Batching

For PyTorch/TensorFlow:

```python
import torch
from gateway.ml import GatewayDataset, collate_fn

dataset = GatewayDataset(
    gateway_url="http://gateway:8080",
    symbols=["AAPL", "MSFT"],
    start="2023-01-01",
    end="2024-01-01",
    features=["open", "high", "low", "close", "volume"],
    labels=["return_5m"]
)

loader = torch.utils.data.DataLoader(
    dataset,
    batch_size=256,
    num_workers=4,
    collate_fn=collate_fn,
    pin_memory=True  # Fast GPU transfer
)

for batch in loader:
    features = batch["features"].to("cuda")
    labels = batch["labels"].to("cuda")
    predictions = model(features)
    loss = criterion(predictions, labels)
```

#### Streaming Inference

For real-time prediction:

```python
from gateway.ml import StreamingInference

async def run_inference():
    inference = StreamingInference(
        gateway_url="ws://gateway:8080/ws",
        model=trained_model,
        feature_extractor=feature_pipeline,
        symbols=UNIVERSE,
        feeds=["bars"]
    )

    async for prediction in inference.run():
        # prediction: {"symbol": "AAPL", "prediction": 0.65, "timestamp": "..."}
        await publish(prediction)
```

#### Feature Window Buffer

For indicators requiring lookback:

```python
from gateway.ml import FeatureWindowBuffer

buffer = FeatureWindowBuffer(
    symbols=["AAPL", "MSFT"],
    window_size=20,  # 20 bars lookback
    features=["close", "volume"]
)

async for bar in gateway.stream():
    buffer.add(bar)

    if buffer.is_ready(bar["symbol"]):
        window = buffer.get_window(bar["symbol"])
        # window shape: (20, 2) - last 20 bars, 2 features
        features = compute_indicators(window)
        prediction = model.predict(features)
```

---

## API Specification

### WebSocket API

**Endpoint:** `ws://gateway:8080/ws`

#### Request/Response Correlation

All client requests support an optional `request_id` for correlating responses:

```json
{"action": "subscribe", "request_id": "req-123", ...}
// Response includes the same request_id
{"type": "ack", "request_id": "req-123", "status": "ok", ...}
```

#### Authentication

```json
// Request
{"action": "auth", "request_id": "auth-001", "key": "gw_xxxxxxxxxxxxxx"}

// Success Response
{
  "type": "auth_result",
  "request_id": "auth-001",
  "status": "ok",
  "client_id": "cerberus",
  "message": "Authenticated successfully"
}

// Error Response
{
  "type": "auth_result",
  "request_id": "auth-001",
  "status": "error",
  "error_code": "GW-E2001",
  "message": "Invalid API key"
}
```

#### Subscribe

```json
// Request
{
  "action": "subscribe",
  "request_id": "sub-456",
  "provider": "alpaca",
  "feed": "stock_bars",
  "symbols": ["AAPL", "MSFT", "TSLA"],
  "timeframe": "1Min"
}

// Success Response (all symbols)
{
  "type": "subscription_ack",
  "request_id": "sub-456",
  "status": "ok",
  "subscribed": ["AAPL", "MSFT", "TSLA"],
  "failed": []
}

// Partial Success Response
{
  "type": "subscription_ack",
  "request_id": "sub-456",
  "status": "partial",
  "subscribed": ["AAPL", "MSFT"],
  "failed": [
    {"symbol": "TSLA", "error_code": "GW-E3001", "reason": "Symbol temporarily unavailable"}
  ]
}
```

#### Unsubscribe

```json
// Request
{
  "action": "unsubscribe",
  "request_id": "unsub-789",
  "provider": "alpaca",
  "feed": "stock_bars",
  "symbols": ["TSLA"]
}

// Response
{
  "type": "unsubscription_ack",
  "request_id": "unsub-789",
  "status": "ok",
  "unsubscribed": ["TSLA"]
}
```

#### News Subscription

```json
// Subscribe to news for specific tickers
{
  "action": "subscribe",
  "request_id": "news-001",
  "provider": "alpaca",
  "feed": "news",
  "symbols": ["AAPL", "TSLA"]
}

// Subscribe to all news (wildcard)
{
  "action": "subscribe",
  "request_id": "news-002",
  "provider": "alpaca",
  "feed": "news",
  "symbols": ["*"]
}
```

#### Data Messages

```json
{
  "type": "data",
  "provider": "alpaca",
  "feed": "stock_bars",
  "symbol": "AAPL",
  "seq": 12345,
  "timestamp": "2026-01-14T21:30:00Z",
  "data": {
    "open": 185.50,
    "high": 185.75,
    "low": 185.40,
    "close": 185.60,
    "volume": 125000
  }
}
```

#### WebSocket Message Type Catalog

| Type | Direction | Description |
|------|-----------|-------------|
| `auth_result` | Server→Client | Authentication response |
| `subscription_ack` | Server→Client | Subscribe confirmation |
| `unsubscription_ack` | Server→Client | Unsubscribe confirmation |
| `data` | Server→Client | Market data message |
| `error` | Server→Client | Error notification |
| `system` | Server→Client | System events (shutdown, buffer_warning) |
| `heartbeat` | Bidirectional | Keep-alive ping/pong |

#### Heartbeat Protocol

- **Server sends:** `{"type": "heartbeat", "ts": 1705248000}` every 30 seconds
- **Client should respond:** `{"action": "heartbeat"}` within 10 seconds
- **No response after 3 heartbeats:** Server disconnects client

---

### REST API

**Base URL:** `http://gateway:8080/api/v1`

**Required Headers:**
```
X-Gateway-Key: gw_xxxxxxxxxxxxxx
Content-Type: application/json
```

#### Response Envelope

All REST responses follow a consistent envelope format:

**Success Response:**
```json
{
  "success": true,
  "data": { ... },
  "meta": {
    "cached": true,
    "cache_age_ms": 1500,
    "request_id": "req-abc123"
  }
}
```

**Error Response:**
```json
{
  "success": false,
  "error": {
    "code": "GW-E3001",
    "message": "Invalid symbol: INVALID123",
    "details": {
      "symbol": "INVALID123",
      "provider": "alpaca"
    }
  },
  "meta": {
    "request_id": "req-abc123"
  }
}
```

#### Rate Limit Headers

All responses include rate limit information:
```
X-RateLimit-Limit: 600
X-RateLimit-Remaining: 542
X-RateLimit-Reset: 1705248000
X-RateLimit-Reset-After: 45
```

#### Cache Control Headers

```
X-Gateway-Cache: HIT
X-Gateway-Cache-Age: 1500
X-Gateway-Cache-TTL: 58500
```

Request cache bypass: `X-Gateway-Cache: bypass`

---

#### Alpaca Stock Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/alpaca/stocks/{symbol}/bars` | Historical stock bars |
| GET | `/alpaca/stocks/{symbol}/trades` | Historical trades |
| GET | `/alpaca/stocks/{symbol}/quotes` | Historical quotes |
| GET | `/alpaca/stocks/{symbol}/snapshot` | Current snapshot |

**Query Parameters for `/bars`, `/trades`, `/quotes`:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `timeframe` | string | Yes (bars only) | — | `1Min`, `5Min`, `15Min`, `1Hour`, `1Day` |
| `start` | ISO8601 | No | 24h ago | Start timestamp |
| `end` | ISO8601 | No | now | End timestamp |
| `limit` | int | No | 1000 | Max records (1-10000) |
| `feed` | string | No | `sip` | `sip` or `iex` |

**Example:**
```
GET /api/v1/alpaca/stocks/AAPL/bars?timeframe=1Min&start=2026-01-14T09:30:00Z&limit=100
```

---

#### Alpaca Options Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/alpaca/options/{contract}/bars` | Option contract bars |
| GET | `/alpaca/options/{contract}/quotes` | Option contract quotes |
| GET | `/alpaca/options/chain/{underlying}` | Full option chain + greeks |
| GET | `/alpaca/options/chain/{underlying}/snapshot` | Current chain snapshot |

**Query Parameters for option chain:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `expiration_date` | YYYY-MM-DD | No | all | Filter by expiration |
| `expiration_date_gte` | YYYY-MM-DD | No | — | Expiration on or after |
| `expiration_date_lte` | YYYY-MM-DD | No | — | Expiration on or before |
| `strike_price_gte` | float | No | — | Strike at or above |
| `strike_price_lte` | float | No | — | Strike at or below |
| `type` | string | No | all | `call` or `put` |

---

#### Alpaca Crypto Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/alpaca/crypto/{pair}/bars` | Crypto bars (e.g., BTC/USD) |
| GET | `/alpaca/crypto/{pair}/trades` | Crypto trades |
| GET | `/alpaca/crypto/{pair}/quotes` | Crypto quotes |
| GET | `/alpaca/crypto/{pair}/snapshot` | Current snapshot |

---

#### Alpaca Forex Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/alpaca/forex/rates` | Get latest FX rates |
| GET | `/alpaca/forex/rates/historical` | Historical FX rates |

**Query Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `pairs` | string | Yes | — | Comma-separated: `EUR/USD,GBP/USD` |
| `start` | ISO8601 | No (historical) | — | Start timestamp |
| `end` | ISO8601 | No (historical) | — | End timestamp |
| `timeframe` | string | No | `1Day` | `1Min`, `1Hour`, `1Day` |

> [!NOTE]
> Forex data is REST-only (no WebSocket). Gateway polls upstream on first request and caches for TTL.

---

#### Alpaca Trading Endpoints

Account, position, and order-management routes that proxy Alpaca's live/paper
trading API. Mutating endpoints require a client role of `trader`, `admin`, or
`super_admin`.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/alpaca/account` | Account info |
| GET | `/alpaca/positions` | Open positions |
| GET | `/alpaca/orders` | List orders |
| POST | `/alpaca/orders` | Submit an order (query params, not JSON body) |
| DELETE | `/alpaca/orders/{order_id}` | Cancel an order |
| GET | `/alpaca/clock` | Market clock |
| GET | `/alpaca/calendar` | Trading calendar |

**Idempotency contract (orders):** `client_order_id` is optional. If omitted,
the gateway auto-generates a `dg-<uuid>` key and returns it in the response
meta. Alpaca natively dedupes `submit_order` by `client_order_id`, so a caller
that receives a `504` (timeout) can safely retry with the same key — the 504
response detail echoes the key under `idempotency_context`. Supplying an empty
or whitespace-only `client_order_id` is rejected with `400 GW-E4006` (a silent
fallback would mint a fresh key per retry and defeat dedup); keys longer than
Alpaca's 128-char limit are likewise rejected with `GW-E4006`.

**Per-call timeouts (split read/write):** read calls (`get_account`,
`get_orders`, `get_position`, `get_clock`, `get_calendar`, …) use a 15s
wall-clock ceiling (`alpaca_trading_write_call_timeout_seconds` controls the
write side; reads use `alpaca_trading_call_timeout_seconds`); write calls
(`create_order`, `replace_order`, `cancel_order`, `cancel_all_orders`,
`close_position`, `close_all_positions`) get a longer 25s ceiling because
opening-bell broker latency can exceed 15s on writes and surfacing a 504 forces
the caller into the idempotency-retry contract. An HTTP-level safety net
(`alpaca_trading_http_timeout_seconds`, default 30s) releases the executor
thread on either path.

**Error codes:** `GW-E4006` (400, invalid `client_order_id`); `GW-E5004` (504,
timed out waiting for Alpaca — idempotency context attached so callers can
verify or retry); `GW-E5005` (503, trading-call backpressure — the in-flight
cap `alpaca_trading_max_inflight` was reached and the call fast-fails instead of
queueing). Any 5xx raised while placing/replacing/closing an order re-attaches
the idempotency context so the retry key is never lost.

---

#### Unusual Whales Endpoints

> [!IMPORTANT]
> The authoritative provider endpoint contract is generated from live routes in `PROVIDER_ENDPOINT_CONTRACT.md`.
> Regenerate with: `python scripts/generate_provider_contract.py`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/uw/flow/{symbol}` | Options flow for ticker |
| GET | `/uw/flow/all` | All recent flow |
| GET | `/uw/darkpool/{symbol}` | Dark pool activity |
| GET | `/uw/darkpool/all` | All dark pool prints |
| GET | `/uw/institutions/{symbol}` | 13F institutional holdings |
| GET | `/uw/congress/{symbol}` | Congressional trades |
| GET | `/uw/insiders/{symbol}` | Insider transactions |
| GET | `/uw/...` | All SDK endpoints proxied |

**Pagination (cursor-based):**

```
GET /api/v1/uw/flow/AAPL?limit=50&cursor=eyJsYXN0X...
```

**Response includes pagination info:**
```json
{
  "success": true,
  "data": [...],
  "pagination": {
    "next_cursor": "eyJsYXN0X...",
    "has_more": true,
    "total_count": 1250
  }
}
```

---

#### News Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/news/articles` | Search news articles |
| GET | `/news/articles/{id}` | Not supported by NewsAPI.org (returns 501) |
| GET | `/news/sentiment/{symbol}` | Aggregated sentiment |

**Query Parameters for `/news/articles`:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `symbols` | string | No | — | Comma-separated: `AAPL,TSLA` |
| `keywords` | string | No | — | Search terms |
| `start` | ISO8601 | No | 24h ago | Start timestamp |
| `end` | ISO8601 | No | now | End timestamp |
| `limit` | int | No | 50 | Max articles (1-200) |
| `cursor` | string | No | — | Pagination cursor |
| `sort` | string | No | `desc` | `asc` or `desc` by date |

**Example:**
```
GET /api/v1/news/articles?symbols=AAPL,TSLA&limit=20&sort=desc
```

---

#### yfinance Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/yf/ticker/{symbol}` | Full ticker info (price, volume, market cap) |
| GET | `/yf/ticker/{symbol}/info` | Company info (sector, industry, description) |
| GET | `/yf/ticker/{symbol}/financials` | Income statement, balance sheet, cash flow |
| GET | `/yf/ticker/{symbol}/earnings` | Quarterly and annual earnings |
| GET | `/yf/ticker/{symbol}/history` | Historical OHLCV data |
| GET | `/yf/ticker/{symbol}/options` | Available option expirations |
| GET | `/yf/ticker/{symbol}/options/{expiration}` | Option chain for expiration |
| GET | `/yf/ticker/{symbol}/recommendations` | Analyst recommendations |
| GET | `/yf/ticker/{symbol}/holders` | Institutional and insider holders |
| GET | `/yf/ticker/{symbol}/calendar` | Earnings and dividend calendar |

**Query Parameters for `/history`:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `period` | string | No | `1mo` | `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `2y`, `5y`, `10y`, `ytd`, `max` |
| `interval` | string | No | `1d` | `1m`, `2m`, `5m`, `15m`, `30m`, `60m`, `90m`, `1h`, `1d`, `5d`, `1wk`, `1mo`, `3mo` |
| `start` | YYYY-MM-DD | No | — | Start date (overrides period) |
| `end` | YYYY-MM-DD | No | — | End date |

**Example:**
```
GET /api/v1/yf/ticker/AAPL/history?period=1mo&interval=1d
GET /api/v1/yf/ticker/AAPL/financials
GET /api/v1/yf/ticker/AAPL/options/2026-01-17
```

> [!NOTE]
> yfinance is a scraper-based library with no official API. Responses are cached for 300s to minimize scraping load. Heavy usage may trigger rate limiting from Yahoo Finance.

---

#### API Reference Summary

| Provider | Endpoints | Auth | Pagination |
|----------|-----------|------|------------|
| **Alpaca** | Stocks, Options, Crypto, Forex | X-Gateway-Key | Offset-based |
| **Unusual Whales** | Flow, Dark Pool, Institutions | X-Gateway-Key | Cursor-based |
| **News** | Articles, Sentiment | X-Gateway-Key | Cursor-based |
| **yfinance** | Tickers, Fundamentals, Financials | X-Gateway-Key | — |
| **Alpha Vantage** | Time series, indicators, forex, economic | X-Gateway-Key | — |
| **Finnhub** | Fundamentals, earnings, alternative data | X-Gateway-Key | — |

---

## Configuration

### Environment Variables

```bash
# Alpaca (supports multiple keys for load balancing)
ALPACA_API_KEY_1=xxxxx
ALPACA_SECRET_KEY_1=xxxxx
ALPACA_API_KEY_2=xxxxx  # Optional secondary
ALPACA_SECRET_KEY_2=xxxxx

# Unusual Whales
UW_API_KEY=xxxxx

# News API (NewsAPI.org)
NEWS_API_KEY=xxxxx

# Alpha Vantage (stub)
ALPHAVANTAGE_API_KEY=xxxxx

# Finnhub (stub)
FINNHUB_API_KEY=xxxxx

# Gateway Settings
GATEWAY_HOST=0.0.0.0
GATEWAY_PORT=8080
GATEWAY_LOG_LEVEL=INFO
GATEWAY_CACHE_DEFAULT_TTL=300

# Redis (optional)
REDIS_URL=redis://localhost:6379
```

---

## Project Structure

```
data-gateway/
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── README.md
├── CHANGELOG.md
├── PRD.md
│
├── config/
│   ├── clients.yaml          # API keys + permissions
│   └── providers.yaml        # Provider routing + capabilities
│
├── gateway/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry
│   ├── cli.py                # Admin CLI (key management)
│   │
│   ├── core/
│   │   ├── auth.py           # API key validation
│   │   ├── cache.py          # Caching layer
│   │   ├── config.py         # Settings management
│   │   └── logging.py        # Structured logging
│   │
│   ├── websocket/
│   │   ├── manager.py        # Client connection manager
│   │   ├── multiplexer.py    # Alpaca WS multiplexer
│   │   └── protocol.py       # Message parsing/routing
│   │
│   ├── providers/
│   │   ├── base.py           # Provider abstract base
│   │   ├── alpaca.py         # Alpaca REST + WS
│   │   ├── uw.py             # Unusual Whales SDK wrapper
│   │   ├── news.py           # NewsAPI.org client
│   │   ├── alphavantage.py   # Stub
│   │   └── finnhub.py        # Stub
│   │
│   └── api/
│       ├── router.py         # Main API router
│       ├── alpaca.py         # Alpaca REST endpoints
│       ├── uw.py             # UW REST endpoints
│       └── news.py           # News REST endpoints
│
└── tests/
    ├── conftest.py
    ├── test_auth.py
    ├── test_cache.py
    ├── test_websocket.py
    └── test_providers/
```

---

## Dependencies

```toml
[tool.poetry.dependencies]
python = "^3.11"
fastapi = "^0.115.0"
uvicorn = {extras = ["standard"], version = "^0.32.0"}
websockets = "^13.0"
alpaca-py = "^0.30.0"
unusualwhales = {git = "https://github.com/JasperDale420/unusualwhales_python_client.git"}
eventregistry = "^9.1"
yfinance = "^0.2.40"
httpx = "^0.27.0"
pydantic = "^2.0"
pydantic-settings = "^2.0"
redis = "^5.0"
cachetools = "^5.3"

[tool.poetry.group.dev.dependencies]
pytest = "^8.0"
pytest-asyncio = "^0.24"
pytest-cov = "^5.0"
ruff = "^0.5"
mypy = "^1.10"
```

---

## Deployment

### Docker Compose

```yaml
version: "3.9"

services:
  gateway:
    build: .
    ports:
      - "8080:8080"
    env_file:
      - .env
    volumes:
      - ./config:/app/config:ro
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    restart: unless-stopped

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes

volumes:
  redis_data:
```

---

## Provider Extensibility Architecture

> [!TIP]
> The gateway is designed for plug-and-play data source integration. Adding a new provider requires implementing a standard interface and registering it in configuration — no core gateway changes needed.

### Provider Plugin Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Gateway Core                               │
│                                                                      │
│  ┌─────────────┐    ┌─────────────────────┐    ┌─────────────────┐  │
│  │ Provider    │    │    Provider         │    │   Unified API   │  │
│  │ Registry    │───►│    Router           │───►│   Layer         │  │
│  │             │    │                     │    │                 │  │
│  └──────┬──────┘    └─────────────────────┘    └─────────────────┘  │
│         │                                                            │
└─────────┼────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Provider Plugins                              │
│                                                                      │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────────────┐ │
│  │  Alpaca   │  │ yfinance  │  │    UW     │  │  Your Provider   │ │
│  │  Plugin   │  │  Plugin   │  │  Plugin   │  │     Plugin       │ │
│  └───────────┘  └───────────┘  └───────────┘  └───────────────────┘ │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

### Provider Interface Contract

Every provider must implement the `DataProvider` protocol:

```python
from abc import ABC, abstractmethod
from typing import AsyncIterator
from datetime import datetime

class DataProvider(ABC):
    """Base interface for all data providers."""

    # ─────────────────────────────────────────────────────────────────
    # Required: Provider Identity
    # ─────────────────────────────────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique provider identifier (e.g., 'alpaca', 'polygon')."""
        pass

    @property
    @abstractmethod
    def supported_feeds(self) -> list[str]:
        """List of supported feed types: ['bars', 'quotes', 'trades', 'options']."""
        pass

    @property
    @abstractmethod
    def capabilities(self) -> ProviderCapabilities:
        """Provider capabilities and limitations."""
        pass

    # ─────────────────────────────────────────────────────────────────
    # Required: Lifecycle
    # ─────────────────────────────────────────────────────────────────

    @abstractmethod
    async def initialize(self, config: dict) -> None:
        """Called once at gateway startup. Load credentials, warm up connections."""
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """Clean shutdown. Close connections, flush buffers."""
        pass

    @abstractmethod
    async def health_check(self) -> HealthStatus:
        """Return current health status for monitoring."""
        pass

    # ─────────────────────────────────────────────────────────────────
    # Optional: WebSocket Streaming (implement if supports real-time)
    # ─────────────────────────────────────────────────────────────────

    async def subscribe(self, symbols: list[str], feeds: list[str]) -> None:
        """Subscribe to real-time data. Optional for REST-only providers."""
        raise NotImplementedError("Provider does not support streaming")

    async def unsubscribe(self, symbols: list[str], feeds: list[str]) -> None:
        """Unsubscribe from real-time data."""
        raise NotImplementedError("Provider does not support streaming")

    async def stream(self) -> AsyncIterator[NormalizedMessage]:
        """Yield normalized messages from upstream. Must be async generator."""
        raise NotImplementedError("Provider does not support streaming")

    # ─────────────────────────────────────────────────────────────────
    # Optional: REST API (implement if supports historical data)
    # ─────────────────────────────────────────────────────────────────

    async def get_bars(
        self,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
        **kwargs
    ) -> list[NormalizedBar]:
        """Fetch historical bars. Return normalized data."""
        raise NotImplementedError("Provider does not support historical bars")

    async def get_quotes(self, symbols: list[str]) -> list[NormalizedQuote]:
        """Fetch current quotes."""
        raise NotImplementedError("Provider does not support quotes")

    async def get_trades(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime
    ) -> list[NormalizedTrade]:
        """Fetch historical trades."""
        raise NotImplementedError("Provider does not support trades")
```

**ProviderCapabilities Dataclass:**

```python
@dataclass
class ProviderCapabilities:
    """Declares what the provider can do."""

    # Data types
    supports_bars: bool = False
    supports_quotes: bool = False
    supports_trades: bool = False
    supports_options: bool = False
    supports_news: bool = False

    # Modes
    supports_streaming: bool = False
    supports_historical: bool = False

    # Limits
    max_symbols_per_request: int = 100
    max_historical_range_days: int = 365
    rate_limit_requests_per_minute: int = 600

    # Features
    supports_adjusted_prices: bool = False
    supports_extended_hours: bool = False

    # Timeframes (for bars)
    supported_timeframes: list[str] = field(default_factory=lambda: ["1Min", "1Hour", "1Day"])
```

---

### Provider Registration (Configuration-Based)

Providers are registered via configuration, not code:

**config/providers.yaml:**

```yaml
providers:
  # ─────────────────────────────────────────────────────────────────
  # Built-in Providers
  # ─────────────────────────────────────────────────────────────────
  alpaca:
    enabled: true
    module: gateway.providers.alpaca
    class: AlpacaProvider
    priority: 1  # Lower = higher priority for fallback
    config:
      api_key_env: ALPACA_API_KEY
      secret_key_env: ALPACA_SECRET_KEY
      paper: false
      feed: sip  # or iex
    capabilities:
      streaming: true
      historical: true
      bars: true
      quotes: true
      trades: true
      options: true

  yfinance:
    enabled: true
    module: gateway.providers.yfinance
    class: YFinanceProvider
    priority: 2
    config:
      cache_ttl: 300
    capabilities:
      streaming: false
      historical: true
      bars: true

  unusual_whales:
    enabled: true
    module: gateway.providers.uw
    class: UnusualWhalesProvider
    priority: 1
    config:
      api_key_env: UW_API_KEY
    capabilities:
      streaming: false
      historical: true
      flow: true
      darkpool: true

  # ─────────────────────────────────────────────────────────────────
  # Example: Adding a New Provider
  # ─────────────────────────────────────────────────────────────────
  polygon:
    enabled: false  # Disabled until implemented
    module: gateway.providers.polygon
    class: PolygonProvider
    priority: 2
    config:
      api_key_env: POLYGON_API_KEY
    capabilities:
      streaming: true
      historical: true
      bars: true
      quotes: true
      trades: true
```

**Runtime Registration:**

```python
# gateway/core/registry.py
class ProviderRegistry:
    def __init__(self):
        self.providers: dict[str, DataProvider] = {}

    async def load_from_config(self, config_path: str):
        """Load and initialize all enabled providers from config."""
        config = yaml.safe_load(open(config_path))

        for name, provider_config in config["providers"].items():
            if not provider_config.get("enabled", True):
                logger.info(f"Skipping disabled provider: {name}")
                continue

            # Dynamic import
            module = importlib.import_module(provider_config["module"])
            provider_class = getattr(module, provider_config["class"])

            # Instantiate and initialize
            provider = provider_class()
            await provider.initialize(provider_config.get("config", {}))

            self.providers[name] = provider
            logger.info(f"Registered provider: {name}")

    def get(self, name: str) -> DataProvider:
        return self.providers.get(name)

    def list_providers(self) -> list[str]:
        return list(self.providers.keys())
```

---

### Adding a New Provider: Step-by-Step

**Example: Adding Polygon.io as a data source**

#### Step 1: Create Provider File

```python
# gateway/providers/polygon.py

from gateway.core.provider import DataProvider, ProviderCapabilities
from gateway.schemas import NormalizedBar, NormalizedQuote

class PolygonProvider(DataProvider):
    """Polygon.io data provider implementation."""

    @property
    def name(self) -> str:
        return "polygon"

    @property
    def supported_feeds(self) -> list[str]:
        return ["bars", "quotes", "trades"]

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supports_bars=True,
            supports_quotes=True,
            supports_trades=True,
            supports_options=True,
            supports_streaming=True,
            supports_historical=True,
            max_symbols_per_request=1000,
            rate_limit_requests_per_minute=100,
            supported_timeframes=["1Min", "5Min", "1Hour", "1Day"]
        )

    async def initialize(self, config: dict) -> None:
        self.api_key = os.environ[config["api_key_env"]]
        self.client = PolygonClient(self.api_key)
        self.ws = None

    async def shutdown(self) -> None:
        if self.ws:
            await self.ws.close()

    async def health_check(self) -> HealthStatus:
        try:
            await self.client.get_ticker("AAPL")
            return HealthStatus(healthy=True)
        except Exception as e:
            return HealthStatus(healthy=False, error=str(e))

    # ─────────────────────────────────────────────────────────────────
    # REST Implementation
    # ─────────────────────────────────────────────────────────────────

    async def get_bars(
        self,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
        **kwargs
    ) -> list[NormalizedBar]:
        results = []

        for symbol in symbols:
            raw_bars = await self.client.get_aggs(
                ticker=symbol,
                multiplier=1,
                timespan=self._convert_timeframe(timeframe),
                from_=start.strftime("%Y-%m-%d"),
                to=end.strftime("%Y-%m-%d")
            )

            # Normalize to gateway schema
            for bar in raw_bars.results:
                results.append(NormalizedBar(
                    symbol=symbol,
                    timestamp=datetime.fromtimestamp(bar.t / 1000, tz=UTC),
                    open=Decimal(str(bar.o)),
                    high=Decimal(str(bar.h)),
                    low=Decimal(str(bar.l)),
                    close=Decimal(str(bar.c)),
                    volume=bar.v,
                    vwap=Decimal(str(bar.vw)) if bar.vw else None,
                    trade_count=bar.n,
                    provider="polygon"
                ))

        return results

    # ─────────────────────────────────────────────────────────────────
    # WebSocket Implementation
    # ─────────────────────────────────────────────────────────────────

    async def subscribe(self, symbols: list[str], feeds: list[str]) -> None:
        if not self.ws:
            self.ws = await self._connect_websocket()

        channels = []
        for feed in feeds:
            prefix = {"bars": "A", "quotes": "Q", "trades": "T"}[feed]
            channels.extend([f"{prefix}.{s}" for s in symbols])

        await self.ws.send(json.dumps({"action": "subscribe", "params": ",".join(channels)}))

    async def stream(self) -> AsyncIterator[NormalizedMessage]:
        async for msg in self.ws:
            data = json.loads(msg)
            yield self._normalize_message(data)

    def _normalize_message(self, raw: dict) -> NormalizedMessage:
        # Convert Polygon format to gateway normalized format
        ...
```

#### Step 2: Add to Configuration

```yaml
# config/providers.yaml
providers:
  polygon:
    enabled: true
    module: gateway.providers.polygon
    class: PolygonProvider
    priority: 2
    config:
      api_key_env: POLYGON_API_KEY
```

#### Step 3: Set Environment Variable

```bash
export POLYGON_API_KEY=your_api_key_here
```

#### Step 4: Restart Gateway

```bash
docker-compose restart gateway
```

#### Step 5: Verify

```bash
# Check provider registered
curl http://localhost:8080/api/v1/admin/providers

# Test endpoint
curl http://localhost:8080/api/v1/polygon/stocks/AAPL/bars
```

---

### Provider Lifecycle Hooks

Providers can implement optional hooks for advanced scenarios:

```python
class DataProvider(ABC):
    # ... base methods ...

    # ─────────────────────────────────────────────────────────────────
    # Optional Lifecycle Hooks
    # ─────────────────────────────────────────────────────────────────

    async def on_client_connect(self, client_id: str) -> None:
        """Called when a new client connects. Use for per-client setup."""
        pass

    async def on_client_disconnect(self, client_id: str) -> None:
        """Called when a client disconnects. Use for cleanup."""
        pass

    async def on_subscription_change(
        self,
        added: list[str],
        removed: list[str]
    ) -> None:
        """Called when aggregate subscriptions change."""
        pass

    async def on_upstream_error(self, error: Exception) -> ErrorAction:
        """Called on upstream error. Return RETRY, FAILOVER, or PROPAGATE."""
        return ErrorAction.RETRY

    async def on_rate_limit(self) -> None:
        """Called when rate limited by upstream. Implement backoff."""
        await asyncio.sleep(60)

    async def on_config_reload(self, new_config: dict) -> None:
        """Called on hot config reload. Update internal state."""
        pass
```

---

### Provider Fallback & Priority

Configure provider fallback for resilience:

```yaml
# Route configuration
routes:
  stocks:
    providers:
      - alpaca      # Primary
      - polygon     # Fallback 1
      - yfinance    # Fallback 2 (REST only)
    fallback_on:
      - connection_error
      - timeout
      - rate_limit

  options:
    providers:
      - alpaca      # Primary, only source for options streaming
    fallback_on: []  # No fallback for options

  flow:
    providers:
      - unusual_whales  # Exclusive source
```

**Fallback Behavior:**

```python
async def get_bars_with_fallback(self, symbols: list[str], ...):
    for provider_name in self.get_route("stocks").providers:
        provider = self.registry.get(provider_name)

        try:
            return await provider.get_bars(symbols, ...)
        except (ConnectionError, TimeoutError) as e:
            logger.warning(f"Provider {provider_name} failed, trying next", error=e)
            continue

    raise AllProvidersFailed("No providers available for stocks")
```

---

### Provider Admin API

Manage providers at runtime:

```
# List all providers
GET /api/v1/admin/providers

{
  "providers": [
    {
      "name": "alpaca",
      "status": "healthy",
      "capabilities": {...},
      "config": {"feed": "sip"},
      "stats": {
        "requests_total": 50000,
        "errors_total": 12,
        "avg_latency_ms": 45
      }
    },
    ...
  ]
}

# Get single provider status
GET /api/v1/admin/providers/alpaca

# Disable provider temporarily
POST /api/v1/admin/providers/alpaca/disable

# Re-enable provider
POST /api/v1/admin/providers/alpaca/enable

# Reload provider config (hot reload)
POST /api/v1/admin/providers/alpaca/reload
```

---

### Provider Testing Requirements

New providers must pass these tests before enabling:

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| `test_initialization` | Provider initializes with valid config | No exceptions |
| `test_health_check` | Health check returns valid status | Returns HealthStatus |
| `test_get_bars` | Fetches historical bars | Returns normalized bars |
| `test_normalization` | Data matches gateway schema | All fields present, types correct |
| `test_error_handling` | Handles upstream errors gracefully | Returns provider error, not crash |
| `test_rate_limit` | Respects rate limits | No 429s from upstream |
| `test_streaming` (if applicable) | Subscribe/stream/unsubscribe cycle | Messages flow correctly |

**Test Template:**

```python
# tests/providers/test_polygon.py

import pytest
from gateway.providers.polygon import PolygonProvider

@pytest.fixture
async def provider():
    p = PolygonProvider()
    await p.initialize({"api_key_env": "POLYGON_API_KEY"})
    yield p
    await p.shutdown()

@pytest.mark.asyncio
async def test_get_bars_returns_normalized_data(provider):
    bars = await provider.get_bars(
        symbols=["AAPL"],
        timeframe="1Day",
        start=datetime(2024, 1, 1),
        end=datetime(2024, 1, 10)
    )

    assert len(bars) > 0
    assert all(isinstance(b, NormalizedBar) for b in bars)
    assert all(b.provider == "polygon" for b in bars)
    assert all(b.open > 0 for b in bars)

@pytest.mark.asyncio
async def test_health_check(provider):
    status = await provider.health_check()
    assert status.healthy is True
```

---

## Backend Engineering Specifications

### Concurrency Model

**Architecture:**
```
┌─────────────────────────────────────────────────────────┐
│                    Main Event Loop                      │
│                    (uvicorn/asyncio)                    │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────────┐  ┌─────────────────┐              │
│  │ Stock Stream    │  │ Options Stream  │  ...         │
│  │ Task            │  │ Task            │              │
│  └────────┬────────┘  └────────┬────────┘              │
│           │                    │                        │
│           └────────┬───────────┘                        │
│                    ▼                                    │
│           ┌─────────────────┐                           │
│           │ Message Router  │                           │
│           │ (async queue)   │                           │
│           └────────┬────────┘                           │
│                    │                                    │
│     ┌──────────────┼──────────────┐                    │
│     ▼              ▼              ▼                    │
│  ┌──────┐      ┌──────┐      ┌──────┐                  │
│  │Client│      │Client│      │Client│                  │
│  │Task 1│      │Task 2│      │Task 3│                  │
│  └──────┘      └──────┘      └──────┘                  │
│                                                         │
│  ┌─────────────────────────────────────────┐           │
│  │ ThreadPoolExecutor (CPU-bound work)      │           │
│  │ - JSON serialization (large payloads)    │           │
│  │ - Compression (if enabled)               │           │
│  └─────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────┘
```

**Task Management:**

| Component | Type | Count |
|-----------|------|-------|
| Upstream WebSocket | `asyncio.Task` | 1 per stream per key |
| Client WebSocket | `asyncio.Task` | 1 per connected client |
| Message Router | `asyncio.Task` | 1 global |
| Grace Period Timers | `asyncio.Task` | 1 per pending unsubscribe |
| REST Handler | Uvicorn workers | 1 (single process default) |

**Shared State Protection:**

```python
# All shared state uses asyncio.Lock (NOT threading.Lock)
class SubscriptionManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._subscriptions: dict[str, SymbolSubscription] = {}

    async def subscribe(self, ...):
        async with self._lock:
            # Modify shared state safely
```

**CPU-Bound Offloading:**

```python
# Large JSON serialization goes to thread pool
executor = ThreadPoolExecutor(max_workers=4)

async def serialize_large_response(data: dict) -> str:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, json.dumps, data)
```

---

### Memory Limits

| Resource | Limit | Enforcement |
|----------|-------|-------------|
| **Total process memory** | 512MB target, 1GB hard limit | Monitor via Prometheus, alert at 80% |
| **In-memory cache** | 256MB or 100,000 items | LRU eviction when exceeded |
| **Per-client send buffer** | 5,000 messages | Disconnect on overflow (GW-E4003) |
| **Per-symbol history ring buffer** | 1,000 messages | Circular overwrite |
| **REST response cache per endpoint** | 10,000 entries | LRU eviction |

**Memory Pressure Handling:**

```python
class MemoryWatchdog:
    WARNING_THRESHOLD = 0.7   # 70% of limit
    CRITICAL_THRESHOLD = 0.9  # 90% of limit

    async def check(self):
        usage = self.get_memory_usage()
        if usage > self.CRITICAL_THRESHOLD:
            # Emergency: clear all caches
            await self.cache.clear()
            logger.error("Memory critical, caches cleared", code="GW-E5010")
        elif usage > self.WARNING_THRESHOLD:
            # Warning: aggressive eviction
            await self.cache.evict_oldest(percent=30)
            logger.warning("Memory pressure, evicting cache", code="GW-W5011")
```

---

### Connection Limits

| Setting | Value | Rationale |
|---------|-------|-----------|
| **Handshake timeout** | 10s | Prevent hanging connections |
| **Auth timeout** | 10s | Disconnect unauthenticated clients |
| **Idle timeout** | 5min | Free resources for inactive clients |
| **Max connection duration** | 24h | Force daily reconnect for cleanup |
| **Max connections per client ID** | 5 | Prevent runaway reconnect loops |
| **Max total clients** | 100 | Resource protection |
| **WebSocket message size limit** | 1MB | Prevent memory attacks |

**Idle Detection:**

```python
class ClientConnection:
    IDLE_TIMEOUT = 300  # 5 minutes

    async def check_idle(self):
        if time.time() - self.last_activity > self.IDLE_TIMEOUT:
            await self.send_system_message({
                "type": "system",
                "event": "idle_warning",
                "timeout_seconds": 60
            })
            await asyncio.sleep(60)
            if time.time() - self.last_activity > self.IDLE_TIMEOUT + 60:
                await self.disconnect("idle_timeout")
```

---

### Circuit Breaker

Upstream connections use circuit breaker pattern to prevent cascading failures:

```python
class CircuitBreaker:
    # States
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Probing recovery

    # Thresholds
    failure_threshold = 5      # Consecutive failures to trip
    recovery_timeout = 60      # Seconds before half-open
    success_threshold = 3      # Successes in half-open to close

    def __init__(self, name: str):
        self.name = name
        self.state = self.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None

    async def call(self, func, *args, **kwargs):
        if self.state == self.OPEN:
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = self.HALF_OPEN
                self.success_count = 0
            else:
                raise CircuitOpenError(f"Circuit {self.name} is open")

        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        if self.state == self.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.state = self.CLOSED
                logger.info(f"Circuit {self.name} closed", code="GW-I1010")
        self.failure_count = 0

    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = self.OPEN
            logger.error(f"Circuit {self.name} opened", code="GW-E1011")
```

**Per-Component Circuit Breakers:**

| Component | Failure Threshold | Recovery Timeout |
|-----------|-------------------|------------------|
| Alpaca Stock WS | 5 failures | 60s |
| Alpaca Options WS | 5 failures | 60s |
| Alpaca REST | 10 failures | 30s |
| UW REST | 10 failures | 30s |
| News REST | 10 failures | 30s |

---

### Startup Sequence

```
┌─────────────────────────────────────────────────────────┐
│ 1. Load Configuration                                   │
│    - Parse environment variables                        │
│    - Load clients.yaml                                  │
│    - Validate all required config present               │
│    └─► FAIL: Exit with code 1, log GW-E0001            │
├─────────────────────────────────────────────────────────┤
│ 2. Initialize Cache                                     │
│    - Create in-memory LRU cache                         │
│    - Connect to Redis if REDIS_URL set                  │
│    └─► Redis FAIL: Warn GW-W5002, continue without     │
├─────────────────────────────────────────────────────────┤
│ 3. Connect Upstream Streams                             │
│    - Attempt connection to each Alpaca stream           │
│    - Retry with backoff for 60 seconds                  │
│    └─► ALL FAIL: Exit with code 2, log GW-E1020        │
│    └─► PARTIAL: Continue with available streams         │
├─────────────────────────────────────────────────────────┤
│ 4. Start REST Proxy                                     │
│    - Initialize HTTP client pools                       │
│    - Verify upstream connectivity with health check     │
├─────────────────────────────────────────────────────────┤
│ 5. Start WebSocket Server                               │
│    - Begin accepting client connections                 │
├─────────────────────────────────────────────────────────┤
│ 6. Expose Health Endpoints                              │
│    - /health returns 200                                │
│    - /health/ready returns 200 if upstream connected    │
├─────────────────────────────────────────────────────────┤
│ 7. RUNNING                                              │
│    - Log startup complete with GW-I0010                 │
│    - Emit gateway_startup_complete metric               │
└─────────────────────────────────────────────────────────┘
```

**Config Hot Reload:**

```python
# SIGHUP triggers config reload
import signal

def setup_signal_handlers():
    signal.signal(signal.SIGHUP, handle_sighup)

async def handle_sighup(signum, frame):
    logger.info("SIGHUP received, reloading config", code="GW-I0020")
    try:
        new_config = load_clients_yaml()
        validate_config(new_config)
        await apply_config(new_config)
        logger.info("Config reloaded successfully", code="GW-I0021")
    except Exception as e:
        logger.error("Config reload failed, keeping old config",
                     code="GW-E0021", error=str(e))
```

---

### REST Backpressure

**Concurrency Control:**

```python
class RESTProxy:
    def __init__(self):
        # Limit concurrent upstream requests per provider
        self.semaphores = {
            "alpaca": asyncio.Semaphore(50),
            "uw": asyncio.Semaphore(30),
            "news": asyncio.Semaphore(20),
        }

        # Request queue with max depth
        self.queue_depth = 100
        self.pending_requests = 0

    async def proxy_request(self, provider: str, request):
        if self.pending_requests >= self.queue_depth:
            raise HTTPException(503, "Gateway overloaded, try again later")

        self.pending_requests += 1
        try:
            async with self.semaphores[provider]:
                return await self._forward_request(provider, request)
        finally:
            self.pending_requests -= 1
```

**Timeout Configuration:**

| Stage | Timeout | Action on Timeout |
|-------|---------|-------------------|
| Connection | 5s | Return 504 |
| Request | 30s | Return 504 |
| Total | 60s | Return 504 |

---

## Quality Assurance & Testing

### Test Coverage Matrix

| Component | Unit | Integration | E2E | Load | Chaos | Priority |
|-----------|------|-------------|-----|------|-------|----------|
| **WebSocket Auth** | ✅ | ✅ | ✅ | ✅ | — | P0 |
| **WS Multiplexer** | ✅ | ✅ | ✅ | ✅ | ✅ | P0 |
| **Subscription Manager** | ✅ | ✅ | ✅ | ✅ | — | P0 |
| **REST Proxy** | ✅ | ✅ | ✅ | ✅ | ✅ | P0 |
| **Cache Layer** | ✅ | ✅ | — | — | ✅ | P1 |
| **Rate Limiter** | ✅ | ✅ | ✅ | ✅ | — | P1 |
| **Circuit Breaker** | ✅ | ✅ | — | — | ✅ | P1 |
| **Data Normalizer** | ✅ | ✅ | — | — | — | P1 |
| **Data Validator** | ✅ | ✅ | — | — | — | P1 |
| **Symbol Resolver** | ✅ | ✅ | — | — | — | P2 |
| **Replay Mode** | ✅ | ✅ | ✅ | ✅ | — | P2 |
| **Bulk Downloads** | ✅ | ✅ | ✅ | ✅ | — | P2 |

**Coverage Targets:**

| Test Type | Line Coverage | Branch Coverage | Notes |
|-----------|---------------|-----------------|-------|
| Unit | 80% | 70% | Core business logic |
| Integration | 70% | 60% | Component boundaries |
| E2E | 50% | — | Critical paths only |

---

### Acceptance Criteria

#### WebSocket Authentication

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AUTH-01 | Valid auth | Unauthenticated WS | Valid key sent | `auth_result.status` = "ok" |
| AUTH-02 | Invalid key | Unauthenticated WS | Invalid key sent | `auth_result.status` = "error", code = GW-E2001 |
| AUTH-03 | Timeout | Unauthenticated WS | 10s passes | Connection closed |
| AUTH-04 | Already authed | Authenticated WS | Auth sent again | Error: "already authenticated" |
| AUTH-05 | Expired key | Unauthenticated WS | Expired key sent | `auth_result.status` = "error", code = GW-E2002 |
| AUTH-06 | Revoked key | Unauthenticated WS | Revoked key sent | `auth_result.status` = "error", code = GW-E2003 |

#### Subscription Management

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| SUB-01 | Single symbol | Authenticated client | Subscribe to AAPL | `subscription_ack` with AAPL |
| SUB-02 | Multiple symbols | Authenticated client | Subscribe to 100 symbols | `subscription_ack` with all 100 |
| SUB-03 | Invalid symbol | Authenticated client | Subscribe to "INVALID$" | Error code GW-E8001 |
| SUB-04 | Duplicate sub | Already subscribed | Subscribe again | No-op, success |
| SUB-05 | Max symbols | At 5000 symbols | Subscribe to 1 more | Error code GW-E1010 |
| SUB-06 | Unsubscribe | Subscribed to AAPL | Unsubscribe AAPL | Grace period starts (30s) |
| SUB-07 | Resubscribe in grace | In grace period | Subscribe same symbol | Cancel grace, immediate active |

#### Data Delivery

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| DATA-01 | Bar delivery | Subscribed to bars | Upstream sends bar | Client receives normalized bar |
| DATA-02 | Quote delivery | Subscribed to quotes | Upstream sends quote | Client receives normalized quote |
| DATA-03 | Deduplication | Same bar sent twice | — | Client receives only once |
| DATA-04 | Sequence order | Multiple messages | — | `sequence` numbers are monotonic |
| DATA-05 | Backpressure | Client slow | Buffer fills | Warning, then disconnect |

#### REST API

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| REST-01 | Cache hit | Data in cache | GET bars | `X-Gateway-Cache: HIT`, < 50ms |
| REST-02 | Cache miss | Data not in cache | GET bars | Fetch from upstream, cache it |
| REST-03 | Rate limited | At rate limit | Any request | 429 with `Retry-After` header |
| REST-04 | Invalid auth | Bad API key | Any request | 401 Unauthorized |
| REST-05 | Provider down | Upstream timeout | GET bars | GW-E1005 after circuit breaker |

---

### Test Data Requirements

#### Mock Data Sets

| Dataset | Purpose | Records | Location |
|---------|---------|---------|----------|
| `alpaca_bars_valid.json` | Happy path bars | 1000 | `tests/fixtures/` |
| `alpaca_bars_invalid.json` | Validation testing | 50 | `tests/fixtures/` |
| `alpaca_quotes.json` | Quote testing | 500 | `tests/fixtures/` |
| `alpaca_trades.json` | Trade testing | 1000 | `tests/fixtures/` |
| `option_chains.json` | Options endpoints | 500 contracts | `tests/fixtures/` |
| `uw_flow_samples.json` | UW flow testing | 200 | `tests/fixtures/` |
| `symbols_1000.txt` | Load testing | 1000 | `tests/fixtures/` |
| `symbols_5000.txt` | Stress testing | 5000 | `tests/fixtures/` |

#### Edge Case Data

| File | Contents |
|------|----------|
| `invalid_high_low.json` | Bar with high < low |
| `crossed_quote.json` | Quote with bid > ask |
| `zero_volume.json` | Trade with size = 0 |
| `future_timestamp.json` | Timestamp in future |
| `special_chars.json` | Symbols with invalid chars |
| `extreme_prices.json` | Prices at 0, negative, very large |

#### Sample Valid Bar

```json
{
  "S": "AAPL",
  "t": "2026-01-14T14:30:00Z",
  "o": 185.50,
  "h": 186.00,
  "l": 185.25,
  "c": 185.75,
  "v": 125000,
  "vw": 185.60,
  "n": 450
}
```

#### Sample Invalid Bar (High < Low)

```json
{
  "S": "AAPL",
  "t": "2026-01-14T14:30:00Z",
  "o": 185.50,
  "h": 184.00,
  "l": 186.00,
  "c": 185.75,
  "v": 125000
}
```

---

### Test Hooks & Error Injection

> [!WARNING]
> Debug endpoints are disabled in production via `GATEWAY_DEBUG_MODE=false`

#### Debug Endpoints (Development Only)

| Endpoint | Purpose | Parameters |
|----------|---------|------------|
| `POST /debug/disconnect-upstream` | Force upstream disconnect | `{"stream": "stocks"}` |
| `POST /debug/inject-latency` | Add latency to responses | `{"ms": 500, "duration": 60}` |
| `POST /debug/fill-cache` | Fill cache to trigger eviction | `{"percent": 90}` |
| `POST /debug/trigger-rate-limit` | Simulate rate limit hit | `{"client": "test"}` |
| `POST /debug/circuit-open` | Force circuit breaker open | `{"component": "alpaca"}` |
| `POST /debug/circuit-reset` | Reset circuit breaker | `{"component": "alpaca"}` |
| `POST /debug/inject-error` | Return error for next N requests | `{"code": "GW-E1001", "count": 5}` |

#### Environment Variables for Testing

```bash
# Enable test hooks
GATEWAY_DEBUG_MODE=true

# Inject random failures (1% of requests)
GATEWAY_INJECT_FAILURE_RATE=0.01

# Add latency to all responses
GATEWAY_SIMULATE_LATENCY_MS=100

# Lower timeouts for faster tests
GATEWAY_AUTH_TIMEOUT_MS=1000
GATEWAY_UPSTREAM_TIMEOUT_MS=2000
```

---

### Performance Benchmarks

#### Latency SLAs

| Operation | p50 | p95 | p99 | Max | Notes |
|-----------|-----|-----|-----|-----|-------|
| WS auth handshake | < 20ms | < 50ms | < 100ms | < 200ms | From connect to auth_result |
| WS message delivery | < 10ms | < 25ms | < 50ms | < 100ms | Upstream to client |
| REST (cache HIT) | < 5ms | < 15ms | < 30ms | < 50ms | In-memory cache |
| REST (cache MISS) | < 100ms | < 250ms | < 500ms | < 1s | Upstream fetch + cache |
| Subscribe request | < 10ms | < 25ms | < 50ms | < 100ms | Ack response |
| Bulk download start | < 500ms | < 1s | < 2s | < 5s | Job accepted |

#### Load Test Pass Criteria

| Metric | Threshold | Action if Fail |
|--------|-----------|----------------|
| Error rate | < 0.1% | 🛑 Block release |
| p99 latency (WS) | < 100ms | 🛑 Block release |
| p99 latency (REST) | < 500ms | 🛑 Block release |
| Memory growth | < 50MB/hour | 🛑 Block release |
| Connection drops | < 1/hour | ⚠️ Investigate |
| CPU usage | < 70% average | ⚠️ Investigate |
| Message loss | 0% | 🛑 Block release |

#### Load Test Scenarios

```yaml
# locustfile.py scenarios
scenarios:
  # Normal load
  - name: "baseline"
    clients: 10
    symbols_per_client: 500
    duration: 1h
    actions:
      - subscribe: 80%
      - unsubscribe: 10%
      - rest_query: 10%

  # Burst test
  - name: "connection_burst"
    clients: 50
    ramp_up: 10s  # All connect in 10s
    hold_duration: 5m

  # Symbol churn
  - name: "subscription_churn"
    clients: 10
    subscribe_rate: 100/min
    unsubscribe_rate: 100/min
    duration: 30m

  # Reconnection storm
  - name: "reconnection_storm"
    clients: 20
    disconnect_all: true
    reconnect_delay: 0s
    measure: time_to_full_recovery
```

#### Stress Test Scenarios

| Scenario | Configuration | Pass Criteria |
|----------|---------------|---------------|
| Max clients | 100 simultaneous | No OOM, < 10s recovery |
| Max symbols | Single client, 10000 symbols | Graceful reject at 5000 |
| Max messages | 50,000 msg/sec throughput | No drops, < 100ms p99 |
| Sustained load | 10 clients, 1000 symbols, 24h | Stable memory, no leaks |

---

### Regression Testing

#### Smoke Tests (Every Commit, < 2 min)

```yaml
smoke_tests:
  - name: server_starts
    command: "docker-compose up -d && sleep 5"
    verify: "curl -f http://localhost:8080/health"

  - name: health_ready
    command: "curl http://localhost:8080/health/ready"
    verify: "status == 200"

  - name: auth_works
    command: "python tests/smoke/test_auth.py"
    verify: "exit_code == 0"

  - name: subscribe_works
    command: "python tests/smoke/test_subscribe.py"
    verify: "received bar within 60s"

  - name: rest_works
    command: "curl -H 'X-Gateway-Key: test' http://localhost:8080/api/v1/alpaca/stocks/AAPL/bars"
    verify: "status == 200, success == true"
```

#### Full Regression (Nightly, < 30 min)

```yaml
regression_suite:
  unit_tests:
    command: "pytest tests/unit -v --cov=gateway --cov-report=xml"
    coverage_threshold: 80%

  integration_tests:
    command: "pytest tests/integration -v"
    dependencies: ["mock_alpaca", "redis"]

  e2e_tests:
    command: "pytest tests/e2e -v"
    dependencies: ["full_stack"]

  load_tests:
    command: "locust -f tests/load/locustfile.py --headless -u 10 -r 1 -t 5m"
    thresholds:
      p99_response_time: 100ms
      failure_rate: 0.1%

  security_scan:
    command: "pip-audit && safety check"
    fail_on: "high,critical"
```

#### Release Gating Criteria

| Gate | Criteria | Enforcement |
|------|----------|-------------|
| Unit tests | 100% pass | CI blocks merge |
| Integration tests | 100% pass | CI blocks merge |
| E2E tests | 100% pass | CI blocks merge |
| Code coverage | > 80% | CI blocks merge |
| Load test | All SLAs met | Manual gate |
| Security scan | No high/critical | CI blocks merge |
| Manual QA | Sign-off | Required for prod |

---

### API Contract Testing

#### Schema Validation

All responses validated against JSON schemas:

```yaml
# schemas/bar_response.yaml
type: object
required: [success, data, meta]
properties:
  success:
    type: boolean
    const: true
  data:
    type: array
    items:
      $ref: '#/definitions/Bar'
  meta:
    type: object
    required: [request_id, provider]
    properties:
      request_id:
        type: string
      provider:
        type: string
      cached:
        type: boolean

definitions:
  Bar:
    type: object
    required: [symbol, timestamp, open, high, low, close, volume]
    properties:
      symbol:
        type: string
        pattern: "^[A-Z]{1,5}$"
      timestamp:
        type: string
        format: date-time
      open:
        type: number
        minimum: 0
      high:
        type: number
      low:
        type: number
      close:
        type: number
      volume:
        type: integer
        minimum: 0
```

#### Contract Test Example

```python
import jsonschema
from gateway.schemas import BAR_RESPONSE_SCHEMA

@pytest.mark.contract
def test_bar_response_matches_schema():
    response = client.get("/api/v1/alpaca/stocks/AAPL/bars")
    jsonschema.validate(response.json(), schema=BAR_RESPONSE_SCHEMA)

@pytest.mark.contract
def test_error_response_matches_schema():
    response = client.get("/api/v1/alpaca/stocks/INVALID$/bars")
    jsonschema.validate(response.json(), schema=ERROR_RESPONSE_SCHEMA)
```

#### Breaking Change Detection

```bash
# Compare OpenAPI specs before release
oasdiff changelog openapi-current.yaml openapi-new.yaml

# Fail on breaking changes
oasdiff breaking openapi-current.yaml openapi-new.yaml --fail-on WARN
```

---

### Edge Cases & Boundary Conditions

#### Symbol Edge Cases

| Case | Input | Expected | Test ID |
|------|-------|----------|---------|
| Empty | `""` | GW-E8001 | EDGE-SYM-01 |
| Too long | `"ABCDEFGHIJ"` | GW-E8001 | EDGE-SYM-02 |
| Lowercase | `"aapl"` | Normalize to AAPL | EDGE-SYM-03 |
| Special chars | `"AAPL$"` | GW-E8001 | EDGE-SYM-04 |
| Numbers only | `"12345"` | GW-E8001 | EDGE-SYM-05 |
| Delisted | `"LUMN"` | Return historical, flag | EDGE-SYM-06 |
| Unicode | `"ÅPL"` | GW-E8001 | EDGE-SYM-07 |

#### Pagination Edge Cases

| Case | Input | Expected | Test ID |
|------|-------|----------|---------|
| Limit = 0 | `?limit=0` | Default to 1000 | EDGE-PAG-01 |
| Limit negative | `?limit=-1` | GW-E8002 | EDGE-PAG-02 |
| Limit > max | `?limit=50000` | Cap at 10000 | EDGE-PAG-03 |
| Invalid cursor | `?cursor=garbage` | GW-E8002 | EDGE-PAG-04 |
| Expired cursor | `?cursor=old...` | Start fresh | EDGE-PAG-05 |
| Page beyond data | `?page=99999` | Empty array | EDGE-PAG-06 |

#### Time Range Edge Cases

| Case | Input | Expected | Test ID |
|------|-------|----------|---------|
| Start > End | `start=2024-01-15&end=2024-01-01` | GW-E8003 | EDGE-TIME-01 |
| Future date | `end=2030-01-01` | Cap at now | EDGE-TIME-02 |
| Weekend | `start=2024-01-13` (Sat) | Empty, success | EDGE-TIME-03 |
| > 5 year range | 10 year span | GW-E8002 | EDGE-TIME-04 |
| Invalid format | `start=notadate` | GW-E8003 | EDGE-TIME-05 |
| Timezone mix | `start=2024-01-01T00:00:00+05:00` | Normalize UTC | EDGE-TIME-06 |

#### Connection Edge Cases

| Case | Trigger | Expected | Test ID |
|------|---------|----------|---------|
| Abrupt disconnect | Kill client socket | Cleanup subscriptions | EDGE-CONN-01 |
| Reconnect same IP | Connect after disconnect | New session | EDGE-CONN-02 |
| Max connections | 1001st connection | Reject 429 | EDGE-CONN-03 |
| Half-open | Client ACKs but no data | Timeout after 60s | EDGE-CONN-04 |

---

### Test Environments

| Environment | Purpose | Data Source | Upstreams | Access |
|-------------|---------|-------------|-----------|--------|
| **Local** | Developer testing | Mock files | Mock WS server | localhost |
| **CI** | Automated tests | Mock files | Mock WS server | GitHub Actions |
| **Staging** | Pre-release | Delayed (15 min) | Alpaca paper | VPN |
| **QA** | Manual testing | Delayed (15 min) | Alpaca paper | VPN |
| **Production** | Live | Real-time | Alpaca SIP | VPN + Auth |

#### Mock Server Setup

```bash
# Start mock Alpaca WebSocket server
docker run -p 8081:8081 gateway-mock-alpaca:latest

# Configure gateway to use mock
export ALPACA_WS_URL=ws://localhost:8081/v2/sip
export ALPACA_REST_URL=http://localhost:8081

# Run tests
pytest tests/integration -v
```

#### Mock Server Capabilities

| Feature | Support | Notes |
|---------|---------|-------|
| Bar messages | ✅ | Configurable rate |
| Quote messages | ✅ | Configurable spread |
| Trade messages | ✅ | Configurable size |
| Auth simulation | ✅ | Accept/reject modes |
| Disconnect simulation | ✅ | Trigger via API |
| Latency injection | ✅ | Configurable delay |
| Error responses | ✅ | Configure error codes |

---

### Bug Severity Classification

| Severity | Definition | Response SLA | Examples |
|----------|------------|--------------|----------|
| **S1 - Critical** | Data loss, complete outage, security breach | 4 hour fix | All clients disconnected, wrong prices, auth bypass |
| **S2 - Major** | Feature broken for all users | 24 hour fix | Auth fails, one provider down, rate limit broken |
| **S3 - Minor** | Feature degraded but usable | 7 day fix | Slow response, incorrect metadata, UI glitch |
| **S4 - Trivial** | Cosmetic or enhancement | Backlog | Log formatting, typos, minor UX |

#### Bug Report Template

```markdown
## Bug Report

**Severity:** S2
**Component:** REST Proxy
**Environment:** Staging
**Reporter:** @engineer
**Date:** 2026-01-14

### Description
REST endpoint returns 500 instead of proper timeout error

### Steps to Reproduce
1. Configure upstream timeout to 1ms
2. Call `GET /api/v1/alpaca/stocks/AAPL/bars`
3. Observe response

### Expected Behavior
- Status: 504 Gateway Timeout
- Body: `{"success": false, "error": {"code": "GW-E1005", ...}}`

### Actual Behavior
- Status: 500 Internal Server Error
- Body: `{"detail": "Internal server error"}`

### Logs
```
2026-01-14T14:30:00.123Z ERROR upstream_timeout component=rest_proxy ...
```

### Screenshots/Recordings
(attach if applicable)

### Impact
- Clients cannot distinguish timeout from other errors
- Retry logic fails

### Workaround
None
```

#### Defect Lifecycle

```
┌─────────┐    ┌──────────┐    ┌────────────┐    ┌──────────┐
│   New   │───►│ Triaged  │───►│ In Progress│───►│ Resolved │
└─────────┘    └──────────┘    └────────────┘    └──────────┘
                    │                │                  │
                    │                │                  ▼
                    │                │           ┌──────────┐
                    ▼                ▼           │ Verified │
              ┌──────────┐    ┌──────────┐      └──────────┘
              │ Deferred │    │ Blocked  │            │
              └──────────┘    └──────────┘            ▼
                                               ┌──────────┐
                                               │  Closed  │
                                               └──────────┘
```

---

### Test Naming Convention

```python
# Pattern: test_<component>_<action>_<scenario>_<expected>

# Unit tests
def test_validator_validate_bar_high_less_than_low_raises_error():
    ...

def test_cache_get_expired_entry_returns_none():
    ...

# Integration tests
def test_websocket_subscribe_valid_symbol_returns_ack():
    ...

def test_rest_proxy_upstream_timeout_returns_504():
    ...

# E2E tests
def test_e2e_full_flow_connect_auth_subscribe_receive_data():
    ...
```

---

### Flaky Test Policy

| Consecutive Failures | Action |
|---------------------|--------|
| 1 | Log, continue |
| 2 | Mark for investigation |
| 3 | Quarantine test |
| 5 | Disable test, create bug |

**Quarantine Process:**

```python
@pytest.mark.quarantine(reason="Flaky due to timing", issue="GH-123")
def test_sometimes_fails():
    ...
```

Quarantined tests:
- Excluded from CI blocking
- Run separately in nightly
- Tracked in issue tracker
- Monthly review for fix or removal

---

## SRE Operations

### Service Level Objectives (SLOs)

| SLI | Target | Window | Error Budget |
|-----|--------|--------|--------------|
| **Availability** | 99.9% | 30d rolling | 43.2 min/month |
| **WebSocket Latency (p99)** | < 100ms | 7d rolling | — |
| **WebSocket Latency (p50)** | < 20ms | 7d rolling | — |
| **REST Latency (p99)** | < 500ms | 7d rolling | — |
| **Message Delivery Rate** | 99.95% | 24h rolling | — |
| **Upstream Connection Uptime** | 99.5% | 7d rolling | — |

**SLI Definitions:**

```promql
# Availability (success rate)
sum(rate(gateway_requests_total{status!~"5.."}[5m]))
  / sum(rate(gateway_requests_total[5m]))

# WebSocket Latency p99
histogram_quantile(0.99, rate(gateway_ws_message_latency_seconds_bucket[5m]))

# Message Delivery Rate
sum(rate(gateway_messages_delivered_total[1h]))
  / sum(rate(gateway_messages_received_total[1h]))
```

---

### Alerting Configuration

```yaml
groups:
  - name: data-gateway
    rules:
      # Critical Alerts (P1 - Page immediately)
      - alert: GatewayDown
        expr: up{job="gateway"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Data Gateway is down"
          runbook: "docs/runbooks/gateway-down.md"

      - alert: AllUpstreamsDisconnected
        expr: gateway_upstream_connected_total == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "All upstream WebSocket connections lost"
          runbook: "docs/runbooks/upstream-disconnected.md"

      - alert: HighErrorRate
        expr: rate(gateway_errors_total[5m]) / rate(gateway_requests_total[5m]) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Error rate > 5%"
          runbook: "docs/runbooks/high-error-rate.md"

      # Warning Alerts (P2/P3 - Investigate soon)
      - alert: MemoryPressure
        expr: gateway_memory_usage_bytes / gateway_memory_limit_bytes > 0.8
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Memory usage > 80%"
          runbook: "docs/runbooks/memory-pressure.md"

      - alert: UpstreamLatencyHigh
        expr: histogram_quantile(0.99, rate(gateway_upstream_latency_seconds_bucket[5m])) > 1
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Upstream p99 latency > 1s"

      - alert: ClientBufferBackpressure
        expr: gateway_client_buffer_usage > 0.7
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Client buffers > 70% full"

      - alert: CircuitBreakerOpen
        expr: gateway_circuit_breaker_state{state="open"} == 1
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Circuit breaker tripped for {{ $labels.component }}"

      - alert: CacheHitRateLow
        expr: rate(gateway_cache_hits_total[15m]) / rate(gateway_cache_requests_total[15m]) < 0.5
        for: 30m
        labels:
          severity: info
        annotations:
          summary: "Cache hit rate < 50%"
```

---

### Deployment Strategy

#### Rolling Update Procedure

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. Pre-flight Checks                                                │
│    - Verify new image exists and is tested                          │
│    - Confirm no active incidents                                    │
│    - Notify team in #deployments channel                            │
├─────────────────────────────────────────────────────────────────────┤
│ 2. Deploy New Container                                             │
│    - docker-compose up -d --no-deps gateway                         │
│    - New container starts alongside old (if using orchestrator)     │
├─────────────────────────────────────────────────────────────────────┤
│ 3. Health Check                                                     │
│    - Wait for /health/ready to return 200                           │
│    - Verify upstream connections established                        │
│    - Check Prometheus metrics are flowing                           │
├─────────────────────────────────────────────────────────────────────┤
│ 4. Traffic Migration                                                │
│    - Clients automatically reconnect on connection close            │
│    - Monitor error rates during migration                           │
├─────────────────────────────────────────────────────────────────────┤
│ 5. Old Container Drain                                              │
│    - SIGTERM sent to old container                                  │
│    - 60s grace period for existing connections                      │
│    - Old container exits after drain                                │
├─────────────────────────────────────────────────────────────────────┤
│ 6. Post-Deploy Verification                                         │
│    - Confirm all clients reconnected                                │
│    - Check error rates returned to baseline                         │
│    - Monitor for 15 minutes before declaring success                │
└─────────────────────────────────────────────────────────────────────┘
```

#### Client Impact During Deployment

```json
// Clients receive before disconnect:
{
  "type": "system",
  "event": "server_restarting",
  "message": "Gateway restarting, please reconnect in 30 seconds",
  "reconnect_after_seconds": 30
}
```

**Client Expectations:**
- Reconnect within 30-60 seconds
- Re-authenticate after reconnect
- Re-subscribe to all feeds (subscriptions are session-bound)

#### Rollback Procedure

```bash
# Immediate rollback (< 5 minutes)
docker-compose down
docker tag gateway:latest gateway:failed
docker tag gateway:previous gateway:latest
docker-compose up -d

# Verify
curl http://localhost:8080/health/ready
```

---

### Disaster Recovery

| Scenario | Detection | Impact | Recovery | RTO |
|----------|-----------|--------|----------|-----|
| **Gateway OOM** | Container restart, alert | Brief disconnect | Automatic restart | < 2 min |
| **All upstreams down** | `AllUpstreamsDisconnected` alert | No data flow | Wait for upstream | N/A |
| **Single API key revoked** | `GW-E2003` errors | Partial data | Failover to other keys | Automatic |
| **All API keys revoked** | `AllUpstreamsDisconnected` | Complete outage | Contact Alpaca, restore keys | Hours |
| **Host failure** | Health check fails | Complete outage | Deploy to new host | < 30 min |
| **Config corruption** | Startup failure | Can't start | Restore from backup | < 10 min |
| **Redis failure** | `GW-E5002` alert | Cache degradation | Automatic fallback to memory | Automatic |
| **Network partition** | Circuit breakers trip | Partial outage | Wait for network | N/A |

#### Backup Strategy

| Data | Backup Method | Frequency | Retention |
|------|---------------|-----------|-----------|
| `clients.yaml` | Git version control | Every change | Forever |
| Docker image | Container registry | Every deploy | 30 days |
| Environment variables | Secrets manager | On change | Versioned |

> [!NOTE]
> The gateway is stateless. Subscriptions are ephemeral (session-bound). No database to back up.

---

### Docker Resource Limits

```yaml
version: "3.9"

services:
  gateway:
    build: .
    ports:
      - "8080:8080"
    env_file:
      - .env
    volumes:
      - ./config:/app/config:ro
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: '2.0'
        reservations:
          memory: 256M
          cpus: '0.5'
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
    logging:
      driver: json-file
      options:
        max-size: "100m"
        max-file: "3"
    ulimits:
      nofile:
        soft: 65536
        hard: 65536

  redis:
    image: redis:7-alpine
    deploy:
      resources:
        limits:
          memory: 256M
    restart: unless-stopped
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes --maxmemory 200mb --maxmemory-policy allkeys-lru

volumes:
  redis_data:
```

---

### Log Management

| Environment | Destination | Rotation | Retention |
|-------------|-------------|----------|-----------|
| Local dev | stdout | None | Session |
| Docker | json-file driver | 100MB × 3 files | 300MB max |
| Production | stdout → Loki/CloudWatch | Aggregator handles | 30 days |

**Required Fields for All Logs:**

| Field | Purpose | Example |
|-------|---------|---------|
| `timestamp` | When | `2026-01-14T14:30:00.123Z` |
| `level` | Severity | `ERROR`, `WARN`, `INFO` |
| `event` | What happened | `upstream_connection_failed` |
| `error_code` | Quick lookup | `GW-E1001` |
| `correlation_id` | Request tracing | `req-abc123` |
| `client_id` | Who affected | `cerberus` |
| `component` | Where | `alpaca.stock_multiplexer` |

**Log Shipping Configuration (Promtail):**

```yaml
# promtail-config.yaml
scrape_configs:
  - job_name: gateway
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
    relabel_configs:
      - source_labels: ['__meta_docker_container_name']
        regex: '/gateway'
        action: keep
    pipeline_stages:
      - json:
          expressions:
            level: level
            error_code: error_code
            client_id: context.client_id
      - labels:
          level:
          error_code:
```

---

### Capacity Planning

#### Current Estimates (10 Clients)

| Metric | Per Client | Total (10) | Notes |
|--------|------------|------------|-------|
| **Symbols watched** | 500 | ~3,000 unique | After deduplication |
| **Messages/sec (bars)** | 8 | 50-80 | 1-min bars at market hours |
| **Messages/sec (quotes)** | 100 | 500-1,000 | Varies by volatility |
| **Messages/sec (peak)** | 500 | 5,000 | News events, high volatility |
| **Bandwidth out** | 100 KB/s | 1 MB/s | JSON, uncompressed |
| **Memory per client** | 20 MB | 200 MB | Buffers + metadata |
| **CPU per client** | 5% | 50% | Single core |

#### Scaling Triggers

| Resource | Warning | Action |
|----------|---------|--------|
| **Memory** | > 70% for 15 min | Enable Redis, increase limit |
| **CPU** | > 60% for 15 min | Add worker process |
| **Clients** | > 50 | Horizontal scaling (multiple gateways) |
| **Message queue** | > 1000 depth | Investigate slow clients |
| **Symbols** | > 5000 unique | Additional Alpaca API keys |

#### Horizontal Scaling Architecture

```
                    ┌─────────────────────┐
                    │   Load Balancer     │
                    │   (sticky sessions) │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        ┌──────────┐    ┌──────────┐    ┌──────────┐
        │Gateway 1 │    │Gateway 2 │    │Gateway 3 │
        │          │    │          │    │          │
        │Key 1     │    │Key 2     │    │Key 3     │
        │Clients   │    │Clients   │    │Clients   │
        │1-30      │    │31-60     │    │61-90     │
        └──────────┘    └──────────┘    └──────────┘
              │                │                │
              └────────────────┴────────────────┘
                               │
                        ┌──────┴──────┐
                        │ Shared Redis│
                        │ (cache only)│
                        └─────────────┘
```

---

### On-Call Playbook

#### Incident Severity Levels

| Level | Definition | Response SLA | Examples |
|-------|------------|--------------|----------|
| **P1 - Critical** | All clients affected, data loss risk | 15 min ack | Gateway down, all upstreams failed |
| **P2 - Major** | Some clients affected | 1 hour | One API key failed, high error rate |
| **P3 - Minor** | Degraded but functional | 4 hours | Cache miss spike, slow responses |
| **P4 - Low** | No user impact | Next business day | Log noise, minor config issue |

#### Escalation Path

```
Alert fires
    │
    ▼
PagerDuty notifies on-call
    │
    ├─► Ack within 15 min (P1) / 1 hour (P2)
    │
    ▼ (No ack)
Escalate to secondary on-call
    │
    ▼ (P1 > 30 min unresolved)
Page engineering lead
    │
    ▼ (P1 > 1 hour unresolved)
Incident commander engaged
```

#### Common Runbook Quick Actions

| Alert | First Action | If That Fails |
|-------|--------------|---------------|
| `GatewayDown` | Check Docker: `docker-compose logs gateway` | Restart: `docker-compose restart gateway` |
| `AllUpstreamsDisconnected` | Check Alpaca status page | Verify API keys, rotate if needed |
| `HighErrorRate` | Check logs for error_code pattern | Identify affected clients, isolate |
| `MemoryPressure` | Clear cache: `curl -X POST /api/v1/admin/cache/clear` | Restart with increased limit |
| `CircuitBreakerOpen` | Wait for auto-recovery (60s) | Force reset: `curl -X POST /api/v1/admin/circuits/reset` |

#### Post-Incident Checklist

- [ ] Incident timeline documented
- [ ] Root cause identified
- [ ] Customer communication sent (if needed)
- [ ] Monitoring gap addressed
- [ ] Runbook updated with learnings
- [ ] Follow-up tasks created

---

## Implementation Phases

### Phase 1: Core Infrastructure (MVP)
- [ ] Project scaffolding (FastAPI, Docker)
- [ ] Client authentication system
- [ ] In-memory caching layer
- [ ] Health check endpoints

### Phase 2: Alpaca Integration
- [ ] WebSocket multiplexer (stocks + options)
- [ ] REST proxy for historical data
- [ ] Multi-API-key load balancing
- [ ] Subscription management

### Phase 3: Additional Providers
- [ ] Unusual Whales SDK integration
- [x] News API (NewsAPI.org) integration (partial)
- [ ] Alpha Vantage stub
- [ ] Finnhub stub

### Phase 4: Production Hardening
- [ ] Redis cache backend
- [ ] Prometheus metrics
- [ ] Structured logging (JSON)
- [ ] Rate limiting per client
- [ ] Connection pooling optimization

---

## Success Metrics

| Metric | Target |
|--------|--------|
| WebSocket message latency | < 50ms gateway overhead |
| REST cache hit rate | > 60% |
| Uptime | 99.9% |
| Max concurrent clients | 10+ |
| Memory footprint | < 512MB baseline |

## Security Architecture

### Secrets Management

> [!CAUTION]
> Never store credentials in environment variables, config files, or source code for production deployments.

**Secret Sources (Priority Order):**

| Priority | Source | Use Case |
|----------|--------|----------|
| 1 | HashiCorp Vault | Production |
| 2 | AWS Secrets Manager / GCP Secret Manager | Cloud deployments |
| 3 | SOPS-encrypted files | Staging |
| 4 | Docker Secrets | Container orchestration |
| 5 | Environment variables | Development only |

**Required Secrets:**

| Secret | Classification | Rotation | Notes |
|--------|----------------|----------|-------|
| `ALPACA_API_KEY_*` | Restricted | 90 days | Multiple for failover |
| `ALPACA_SECRET_KEY_*` | Restricted | 90 days | Paired with API key |
| `UW_API_KEY` | Restricted | 90 days | Unusual Whales access |
| `GATEWAY_SIGNING_KEY` | Confidential | 30 days | JWT/HMAC signing |
| `REDIS_PASSWORD` | Confidential | 90 days | If Redis auth enabled |
| `ADMIN_API_KEY` | Confidential | 30 days | Admin endpoint access |

**Vault Integration:**

```python
# Production secret loading
from hvac import Client as VaultClient

class SecretManager:
    def __init__(self):
        self.vault = VaultClient(url=os.environ["VAULT_ADDR"])
        self.vault.auth.kubernetes.login(role="data-gateway")

    def get_alpaca_credentials(self, key_num: int) -> tuple[str, str]:
        secret = self.vault.secrets.kv.v2.read_secret_version(
            path=f"data-gateway/alpaca-key-{key_num}"
        )
        return secret["data"]["api_key"], secret["data"]["secret_key"]
```

**Docker Secrets Configuration:**

```yaml
services:
  gateway:
    secrets:
      - alpaca_api_key_1
      - alpaca_secret_key_1
    environment:
      - ALPACA_API_KEY_1_FILE=/run/secrets/alpaca_api_key_1
      - ALPACA_SECRET_KEY_1_FILE=/run/secrets/alpaca_secret_key_1

secrets:
  alpaca_api_key_1:
    external: true  # Managed by orchestrator
  alpaca_secret_key_1:
    external: true
```

---

### Transport Security (TLS)

**Protocol Requirements:**

| Environment | WebSocket | REST | Minimum TLS |
|-------------|-----------|------|-------------|
| Production | `wss://` | `https://` | TLS 1.3 |
| Staging | `wss://` | `https://` | TLS 1.2 |
| Development | `ws://` | `http://` | None (localhost only) |

**TLS Configuration:**

```yaml
# nginx.conf or Caddy equivalent
ssl_protocols TLSv1.3 TLSv1.2;
ssl_ciphers ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384;
ssl_prefer_server_ciphers on;
ssl_session_timeout 1d;
ssl_session_cache shared:SSL:50m;
ssl_stapling on;
ssl_stapling_verify on;
```

**Allowed Cipher Suites (TLS 1.3):**

```
TLS_AES_256_GCM_SHA384
TLS_CHACHA20_POLY1305_SHA256
TLS_AES_128_GCM_SHA256
```

**Certificate Management:**

| Aspect | Requirement |
|--------|-------------|
| Provider | Let's Encrypt (public) or Internal CA (private) |
| Renewal | Auto-renewal at 30 days before expiry |
| Key size | RSA 4096-bit or ECDSA P-384 |
| HSTS | `max-age=31536000; includeSubDomains` |

**mTLS for Service-to-Service (Optional):**

```yaml
tls:
  client_auth: require
  client_ca_file: /etc/ssl/certs/client-ca.pem
  allowed_cns:
    - cerberus.empire.local
    - orion.empire.local
```

---

### Authentication Hardening

**API Key Structure:**

```
gw_<client_id>_<32_random_bytes_base64url>

Example: gw_cerberus_xK9mP2nQ5rT8vW1yZ3aB6cD4eF7gH0jL
```

**Key Properties:**

| Property | Value |
|----------|-------|
| Total length | 48-64 characters |
| Entropy | 256 bits (32 random bytes) |
| Encoding | Base64URL (no +, /, =) |
| Storage | SHA-256 hash only |
| Expiration | Configurable (default: 1 year) |

**Key Lifecycle Management:**

```
# Create new key
POST /api/v1/admin/keys
Authorization: Bearer <admin_token>
{
  "client_id": "cerberus",
  "permissions": {...},
  "expires_in_days": 365
}

Response:
{
  "key_id": "key_abc123",
  "key": "gw_cerberus_xK9mP2nQ...",  // Only shown once!
  "expires_at": "2027-01-14T00:00:00Z"
}
```

```
# Rotate key (invalidates old, creates new)
POST /api/v1/admin/keys/{key_id}/rotate

# Revoke key immediately
DELETE /api/v1/admin/keys/{key_id}

# List keys (no secrets shown)
GET /api/v1/admin/keys
```

**Scoped Permissions:**

```json
{
  "key_id": "key_abc123",
  "client_id": "cerberus",
  "permissions": {
    "providers": ["alpaca", "uw"],
    "feeds": ["bars", "quotes", "trades", "flow"],
    "symbols": ["*"],
    "max_symbols": 1000,
    "rate_limit_requests_per_minute": 600,
    "allowed_ips": ["10.0.0.0/8"],
    "features": {
      "replay": true,
      "bulk_download": true,
      "admin": false
    }
  },
  "expires_at": "2027-01-14T00:00:00Z",
  "created_at": "2026-01-14T14:30:00Z",
  "last_used_at": "2026-01-14T22:15:00Z"
}
```

**Session Tokens (Short-Lived):**

```
# Exchange API key for session token
POST /api/v1/auth/token
X-Gateway-Key: gw_cerberus_xK9mP2nQ...

Response:
{
  "access_token": "eyJhbGciOiJFUzI1NiIs...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "rt_xyz789..."
}

# Use token for requests
GET /api/v1/alpaca/stocks/AAPL/bars
Authorization: Bearer eyJhbGciOiJFUzI1NiIs...
```

---

### Authorization (RBAC)

**Role Definitions:**

| Role | Description | Assignment |
|------|-------------|------------|
| `client` | Standard data access | Default for all keys |
| `admin` | Key management, diagnostics | Explicit grant |
| `super_admin` | Full control including shutdown | Infrastructure team only |

**Permission Matrix:**

| Endpoint Pattern | client | admin | super_admin |
|------------------|--------|-------|-------------|
| `/api/v1/alpaca/*` | ✅ | ✅ | ✅ |
| `/api/v1/uw/*` | ✅ | ✅ | ✅ |
| `/api/v1/bulk/*` | ✅ | ✅ | ✅ |
| `/api/v1/replay/*` | ✅ | ✅ | ✅ |
| `/api/v1/admin/keys` | ❌ | ✅ | ✅ |
| `/api/v1/admin/cache/*` | ❌ | ✅ | ✅ |
| `/api/v1/admin/circuits/*` | ❌ | ✅ | ✅ |
| `/api/v1/admin/logs/*` | ❌ | ✅ | ✅ |
| `/api/v1/admin/config/*` | ❌ | ❌ | ✅ |
| `/api/v1/admin/shutdown` | ❌ | ❌ | ✅ |

**Resource Isolation:**

- Clients can only view their own subscriptions
- No cross-client data leakage
- Subscription lists not exposed via API
- Connection metadata isolated per client

---

### Input Validation

**Symbol Validation:**

| Type | Pattern | Example |
|------|---------|---------|
| Stock | `^[A-Z]{1,5}$` | `AAPL` |
| Option (OCC) | `^[A-Z]{1,6}\d{6}[CP]\d{8}$` | `AAPL250117C00200000` |
| Crypto | `^[A-Z]{2,5}/[A-Z]{3,4}$` | `BTC/USD` |
| Forex | `^[A-Z]{3}/[A-Z]{3}$` | `EUR/USD` |

**Parameter Limits:**

| Parameter | Max Value | Default | Validation |
|-----------|-----------|---------|------------|
| `symbols` array | 500 | — | Reject if exceeded |
| `limit` | 10000 | 1000 | Cap at max |
| `start`/`end` range | 5 years | 24h | Reject if exceeded |
| `timeframe` | enum | 1Min | Reject invalid |
| `speed` (replay) | 100.0 | 1.0 | Cap at max |

**Request Size Limits:**

| Endpoint Type | Max Body | Max URL | Enforcement |
|---------------|----------|---------|-------------|
| REST POST | 1 MB | 8 KB | 413 Payload Too Large |
| REST GET | — | 8 KB | 414 URI Too Long |
| WebSocket | 64 KB | — | Disconnect |

**Forbidden Characters:**

Reject input containing: `` <script> ' " ; -- /* */ \x00 ``

**Validation Error Response:**

```json
{
  "success": false,
  "error": {
    "code": "GW-E8001",
    "message": "Invalid symbol format",
    "details": {
      "field": "symbol",
      "value": "INVALID$",
      "expected_pattern": "^[A-Z]{1,5}$"
    }
  }
}
```

**Validation Error Codes:**

| Code | Description |
|------|-------------|
| `GW-E8001` | Invalid symbol format |
| `GW-E8002` | Parameter exceeds limit |
| `GW-E8003` | Invalid date/time format |
| `GW-E8004` | Invalid enum value |
| `GW-E8005` | Request body too large |
| `GW-E8006` | Forbidden characters detected |
| `GW-E8007` | Required parameter missing |

---

### Rate Limiting

**Limit Hierarchy:**

| Level | Limit | Window | Burst | Enforcement |
|-------|-------|--------|-------|-------------|
| **Global** | 10,000 req/min | 1 min | 500 | 503 Service Unavailable |
| **Per-IP** | 1,000 req/min | 1 min | 100 | 429 Too Many Requests |
| **Per-Client** | 600 req/min | 1 min | 60 | 429 Too Many Requests |
| **Per-Endpoint** | Varies | — | — | 429 Too Many Requests |

**Endpoint-Specific Limits:**

| Endpoint | Limit | Reason |
|----------|-------|--------|
| `/api/v1/bulk/*` | 10/hour | Resource intensive |
| `/api/v1/replay/*` | 5 concurrent | Memory intensive |
| `/api/v1/admin/*` | 60/min | Prevent abuse |
| `/api/v1/auth/token` | 10/min | Prevent brute force |

**WebSocket-Specific Limits:**

| Action | Limit | Enforcement |
|--------|-------|-------------|
| Subscribe requests | 100/min | Reject, error message |
| Symbols per subscribe | 100 | Partial success |
| Total symbols per client | 5000 | Reject new subscriptions |
| Messages sent to client | 10,000/sec | Backpressure, then disconnect |

**Rate Limit Response:**

```json
{
  "success": false,
  "error": {
    "code": "GW-E9001",
    "message": "Rate limit exceeded",
    "details": {
      "limit": 600,
      "window": "1m",
      "retry_after_seconds": 45
    }
  }
}
```

**Rate Limit Headers:**

```
X-RateLimit-Limit: 600
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1705248045
X-RateLimit-Reset-After: 45
Retry-After: 45
```

---

### Audit Logging

**Audited Events:**

| Event | Level | Trigger |
|-------|-------|---------|
| `auth_success` | INFO | Successful authentication |
| `auth_failure` | WARN | Failed authentication |
| `auth_timeout` | WARN | Auth not completed in 10s |
| `key_created` | INFO | New API key generated |
| `key_rotated` | INFO | API key rotated |
| `key_revoked` | WARN | API key deleted |
| `admin_action` | INFO | Any admin endpoint called |
| `config_changed` | WARN | Configuration modified |
| `rate_limited` | WARN | Client hit rate limit |
| `ip_blocked` | WARN | IP address blocked |
| `permission_denied` | WARN | Unauthorized access attempt |

**Audit Log Format:**

```json
{
  "timestamp": "2026-01-14T14:30:00.123Z",
  "event": "auth_success",
  "level": "INFO",
  "actor": {
    "client_id": "cerberus",
    "key_id": "key_abc123",
    "ip": "10.0.1.50",
    "user_agent": "Python/3.11 httpx/0.27"
  },
  "resource": {
    "type": "session",
    "id": "sess-xyz789"
  },
  "action": "authenticate",
  "result": "success",
  "metadata": {
    "auth_method": "api_key",
    "connection_type": "websocket"
  },
  "correlation_id": "req-abc123"
}
```

**Failed Auth Log:**

```json
{
  "timestamp": "2026-01-14T14:30:00.123Z",
  "event": "auth_failure",
  "level": "WARN",
  "actor": {
    "ip": "203.0.113.50",
    "user_agent": "curl/7.88"
  },
  "action": "authenticate",
  "result": "failure",
  "metadata": {
    "reason": "invalid_key",
    "key_prefix": "gw_unknown_",
    "attempt_count": 5
  },
  "correlation_id": "req-def456"
}
```

**Audit Log Retention:**

| Log Type | Retention | Storage |
|----------|-----------|---------|
| Audit logs | 1 year | Immutable/append-only |
| Application logs | 30 days | Standard |
| Debug logs | 7 days | Development only |

---

### DDoS Protection

**Connection Limits:**

| Limit | Value | Enforcement |
|-------|-------|-------------|
| Max connections per IP | 10 | Reject new connections |
| Max total connections | 1000 | Reject new connections |
| Connection rate per IP | 5/sec | 429 response |
| Slow client threshold | 10 KB/s | Disconnect after warning |

**Auth Failure Blocking:**

| Threshold | Action | Duration |
|-----------|--------|----------|
| 10 failures in 5 min | Warn in logs | — |
| 25 failures in 5 min | Temporary block | 5 minutes |
| 50 failures in 5 min | Extended block | 1 hour |
| 100 failures in 5 min | Permanent block | Until manual review |

**IP Blocklist:**

```json
// Managed via admin API
POST /api/v1/admin/security/blocklist
{
  "ip": "203.0.113.50",
  "reason": "Brute force attempt",
  "duration_hours": 24
}

GET /api/v1/admin/security/blocklist
{
  "blocked": [
    {
      "ip": "203.0.113.50",
      "reason": "Brute force attempt",
      "blocked_at": "2026-01-14T14:30:00Z",
      "expires_at": "2026-01-15T14:30:00Z"
    }
  ]
}
```

**External DDoS Protection:**

| Layer | Service | Configuration |
|-------|---------|---------------|
| L3/L4 | AWS Shield / Cloudflare | Always-on |
| L7 | WAF rules | Block known patterns |
| Application | Gateway rate limiting | Per-client |

---

### Data Classification

**Classification Levels:**

| Level | Definition | Examples |
|-------|------------|----------|
| **Public** | No restrictions | Market prices, OHLCV data |
| **Internal** | No external exposure | Client IDs, subscription lists |
| **Confidential** | Encrypted, masked in logs | Gateway API keys, configs |
| **Restricted** | Vault only, never logged | Upstream credentials |

**Handling Rules:**

| Data Type | Logging | Caching | Storage |
|-----------|---------|---------|---------|
| Market prices | Full detail | Yes | None (ephemeral) |
| Client ID | Full | Yes | Config file |
| Gateway key | Last 4 chars only | Never | Hashed in DB |
| Upstream key | Never | Never | Vault only |
| IP addresses | Full (30 days) | No | Logs |
| Error messages | Sanitized | No | Logs |

**Log Masking:**

```python
# GOOD - masked key suffix
logger.info(
    "Client authenticated",
    client_id="cerberus",
    key_suffix="...x7f9"
)

# BAD - never log full keys
logger.info("Auth", key="gw_cerberus_abc123...")  # NEVER DO THIS

# GOOD - sanitized error
logger.error("Query failed", symbol="AAPL", error="Timeout after 30s")

# BAD - may leak sensitive data
logger.error("Failed", request=request_body)  # NEVER LOG RAW INPUT
```

**Credential Exposure Response:**

If a credential is accidentally exposed:
1. Rotate immediately via `/api/v1/admin/keys/{id}/rotate`
2. Log incident as `credential_exposure` event
3. Investigate access logs for unauthorized use
4. Notify security team within 1 hour

---

### Security Headers

**REST API Headers:**

```
Strict-Transport-Security: max-age=31536000; includeSubDomains
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
X-XSS-Protection: 1; mode=block
Content-Security-Policy: default-src 'none'
Cache-Control: no-store
Referrer-Policy: no-referrer
```

**CORS Configuration:**

```python
CORS_CONFIG = {
    "allow_origins": [
        "https://dashboard.empire.local",
        "https://cerberus.empire.local"
    ],
    "allow_methods": ["GET", "POST", "DELETE"],
    "allow_headers": ["X-Gateway-Key", "Authorization", "Content-Type"],
    "allow_credentials": True,
    "max_age": 3600
}
```

---

### Dependency Security

**Vulnerability Scanning:**

| Tool | Frequency | Action |
|------|-----------|--------|
| `pip-audit` | Every build | Block on high/critical |
| `safety` | Daily | Alert on new CVEs |
| Snyk / Dependabot | Continuous | Auto-PR for patches |

**Pinned Dependencies:**

```toml
# Always pin exact versions in production
alpaca-py = "0.30.1"  # Not ^0.30.0
websockets = "13.0.1"
pydantic = "2.5.3"
```

**Security Update SLA:**

| Severity | Response Time |
|----------|---------------|
| Critical | 24 hours |
| High | 7 days |
| Medium | 30 days |
| Low | Next release |

---

## Logging & Diagnostics

### Logging Philosophy

> [!IMPORTANT]
> Every error must be diagnosable from logs alone. Include error codes, context, and actionable information.

### Structured Log Format

All logs are JSON-formatted for easy parsing and aggregation:

```json
{
  "timestamp": "2026-01-14T14:20:00.123Z",
  "level": "ERROR",
  "event": "upstream_connection_failed",
  "error_code": "GW-E1001",
  "component": "alpaca.stock_multiplexer",
  "correlation_id": "req-abc123",
  "context": {
    "stream": "stocks",
    "key_id": "key_1",
    "endpoint": "wss://stream.data.alpaca.markets/v2/sip",
    "attempt": 3,
    "max_retries": 10
  },
  "error": {
    "type": "ConnectionError",
    "message": "WebSocket handshake failed",
    "upstream_code": 503,
    "upstream_message": "Service Unavailable"
  },
  "stack_trace": "..."
}
```

### Error Code Registry

All gateway errors use the format `GW-XNNNN` where:
- `E` = Error, `W` = Warning, `I` = Info
- First digit = Component (1=Connection, 2=Auth, 3=Subscription, 4=Client, 5=Cache, 6=REST)

| Error Code | Level | Component | Description | Action |
|------------|-------|-----------|-------------|--------|
| **Connection Errors (1xxx)** |
| `GW-E1001` | ERROR | Upstream | WebSocket connection failed | Check network, verify endpoint |
| `GW-E1002` | ERROR | Upstream | Handshake timeout (>10s) | Check Alpaca status |
| `GW-E1003` | ERROR | Upstream | Connection dropped unexpectedly | Auto-reconnect triggered |
| `GW-W1004` | WARN | Upstream | Heartbeat missed | Monitor for E1003 |
| `GW-I1005` | INFO | Upstream | Reconnection successful | No action |
| `GW-E1006` | ERROR | Upstream | Max retries exceeded | Manual intervention, check credentials |
| **Authentication Errors (2xxx)** |
| `GW-E2001` | ERROR | Client | Invalid API key | Client should check key |
| `GW-E2002` | ERROR | Client | API key expired/revoked | Generate new key |
| `GW-E2003` | ERROR | Upstream | Alpaca auth rejected | Check env credentials |
| `GW-W2004` | WARN | Client | Auth timeout (no auth in 10s) | Client disconnected |
| **Subscription Errors (3xxx)** |
| `GW-E3001` | ERROR | Subscription | Invalid symbol | Remove from request |
| `GW-E3002` | ERROR | Subscription | Symbol limit exceeded | Reduce subscriptions |
| `GW-E3003` | ERROR | Subscription | Upstream rejected subscription | Check symbol validity |
| `GW-W3004` | WARN | Subscription | Subscription already exists | Idempotent, no action |
| `GW-I3005` | INFO | Subscription | Grace period unsubscribe | Normal cleanup |
| **Client Errors (4xxx)** |
| `GW-E4001` | ERROR | Client | Message parse failed | Check client protocol |
| `GW-W4002` | WARN | Client | Buffer warning threshold | Client consuming slowly |
| `GW-E4003` | ERROR | Client | Buffer overflow, disconnecting | Client must reconnect |
| `GW-E4004` | ERROR | Client | Rate limit exceeded | Client should backoff |
| `GW-I4005` | INFO | Client | Client connected | Normal |
| `GW-I4006` | INFO | Client | Client disconnected | Normal |
| **Cache Errors (5xxx)** |
| `GW-W5001` | WARN | Cache | Cache miss, fetching upstream | Normal |
| `GW-E5002` | ERROR | Cache | Redis connection failed | Falling back to memory |
| `GW-I5003` | INFO | Cache | Cache invalidated | Normal |
| **REST Proxy Errors (6xxx)** |
| `GW-E6001` | ERROR | REST | Upstream timeout | Retry or fail to client |
| `GW-E6002` | ERROR | REST | Upstream rate limited (429) | Backoff, retry |
| `GW-E6003` | ERROR | REST | Upstream error (5xx) | Fail to client |
| `GW-W6004` | WARN | REST | Slow response (>2s) | Monitor performance |

### Log Levels

| Level | Usage |
|-------|-------|
| `ERROR` | Operation failed, requires attention or will impact clients |
| `WARN` | Recoverable issue, should be monitored |
| `INFO` | Normal state transitions (connect, subscribe, disconnect) |
| `DEBUG` | Detailed tracing (disabled in production) |

**Upstream-failure log severity convention:** when a provider call fails, the
gateway picks the log level by HTTP status class — upstream `4xx` responses
(client-correctable: bad symbol, invalid params, rate limit) are logged at
`WARN` without a traceback, while `5xx` responses and unexpected exceptions are
logged at `ERROR` with `exc_info=True`. This keeps caller-error noise out of the
error stream so genuine server-side failures stay visible. Implemented in
`gateway/api/deps.py` and `gateway/api/alpaca/common.py`.

**HTTP-status → error-code mapping:** unhandled `HTTPException`s are rendered
through `gateway/api/errors.py`, which maps the HTTP status to a default
`GW-XNNNN` code where the first digit tracks the status class — e.g. `400 →
GW-E4000`, `404 → GW-E4004`, `429 → GW-E4001`, `503 → GW-E5003`, `504 →
GW-E5004`. Routes that raise a structured `detail={"code": ...}` (such as the
Alpaca trading endpoints, see below) keep their explicit code.

### Correlation IDs

Every client request generates a `correlation_id` that propagates through:
- WebSocket messages
- REST API calls
- Upstream requests
- All related log entries

```python
# Example: Tracing a subscription through logs
correlation_id = "req-abc123"
# Logs will show:
# [req-abc123] Client subscribed to AAPL
# [req-abc123] Upstream subscription sent to key_1
# [req-abc123] Upstream confirmed AAPL subscription
# [req-abc123] Subscription active, starting delivery
```

### Diagnostic Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Liveness check |
| `GET /health/ready` | Readiness (upstream connected?) |
| `GET /api/v1/status` | Full status (clients, upstreams, subscriptions) |
| `GET /api/v1/admin/logs/recent?level=ERROR&limit=100` | Recent error logs |
| `GET /api/v1/admin/errors/summary` | Error code counts (last hour) |

### Error Summary Response

`GET /api/v1/admin/errors/summary`
```json
{
  "period": "last_hour",
  "total_errors": 12,
  "by_code": {
    "GW-E1003": 5,
    "GW-E3001": 4,
    "GW-W4002": 3
  },
  "by_component": {
    "upstream": 5,
    "subscription": 4,
    "client": 3
  }
}
```

### Log Retention

| Environment | Retention | Storage |
|-------------|-----------|---------|
| Development | 24 hours | Local file |
| Production | 30 days | Stdout → Log aggregator (Loki, CloudWatch, etc.) |

---

## Error Handling

### Upstream Error Propagation

When upstream providers return errors, the gateway propagates them to affected clients:

**Error Message Format:**
```json
{
  "type": "error",
  "provider": "alpaca",
  "code": "RATE_LIMITED",
  "message": "Upstream rate limit exceeded, retry after delay",
  "retry_after_ms": 5000,
  "symbols": ["AAPL", "TSLA"]
}
```

**Error Codes:**

| Code | Meaning | Client Action |
|------|---------|---------------|
| `RATE_LIMITED` | Upstream 429 | Backoff, retry after `retry_after_ms` |
| `UPSTREAM_UNAVAILABLE` | Upstream 5xx or timeout | Retry with exponential backoff |
| `INVALID_SYMBOL` | Symbol not found | Remove from subscription |
| `AUTH_FAILED` | Upstream auth rejected | Alert, check credentials |
| `PARTIAL_FAILURE` | Some symbols failed | Check `symbols` array for affected |
| `SUBSCRIPTION_LIMIT` | Max subscriptions exceeded | Reduce symbol count |

### Partial Failure Handling

For batch requests (e.g., subscribing to 50 symbols):
- **Succeed partial:** Accept valid symbols, reject invalid
- **Report failures:** Return list of failed symbols with reasons
- **No all-or-nothing:** Don't fail entire request for one bad symbol

```json
{
  "type": "subscription_result",
  "succeeded": ["AAPL", "MSFT", "GOOGL"],
  "failed": [
    {"symbol": "INVALID123", "reason": "Symbol not found"},
    {"symbol": "DELISTED", "reason": "Symbol delisted"}
  ]
}
```

---

## Backpressure & Flow Control

### Problem
Slow clients (network issues, busy processing) can't consume messages fast enough, causing gateway memory to grow unbounded.

### Per-Client Buffer Management

| Buffer State | Threshold | Action |
|--------------|-----------|--------|
| **Healthy** | < 1,000 msgs | Normal operation |
| **Warning** | 1,000 - 5,000 msgs | Log warning, emit `buffer_warning` to client |
| **Critical** | > 5,000 msgs | Disconnect client with `BUFFER_OVERFLOW` error |

**Buffer Warning Message:**
```json
{
  "type": "system",
  "event": "buffer_warning",
  "pending_messages": 2500,
  "message": "Client consuming slowly, risk of disconnect"
}
```

### Message Priority (During Backpressure)

When buffer enters warning state, prioritize:
1. **Critical:** Errors, system messages, disconnects
2. **High:** Quotes (most time-sensitive)
3. **Medium:** Trades
4. **Low:** Bars (can be reconstructed from trades)

In critical state, only Critical priority messages are delivered before disconnect.

### Client Responsibility
Clients should:
- Process messages asynchronously (don't block the WS read loop)
- Implement local buffering if needed
- Monitor for `buffer_warning` events

---

## Client Reconnection Semantics

### Session Lifecycle

| Event | Behavior |
|-------|----------|
| **New connection** | Client must authenticate, then subscribe |
| **Auth timeout** | 10 seconds to send auth, else disconnect |
| **Disconnect (client-initiated)** | Subscriptions cleared immediately |
| **Disconnect (network/error)** | Subscriptions cleared immediately |
| **Reconnect** | Client must re-authenticate and re-subscribe |

### v1 Design: Session-Bound Subscriptions

- Subscriptions are **not persisted** across disconnections
- No "resume" capability in v1
- Clients are responsible for tracking their subscriptions and replaying on reconnect

**Rationale:** Simpler implementation, avoids stale subscription state, clear contract.

### Reconnection Response

On successful auth after reconnect:
```json
{
  "type": "system",
  "event": "authenticated",
  "client_id": "cerberus",
  "message": "Authenticated. Previous subscriptions cleared. Please re-subscribe."
}
```

### Future Enhancement (v2)
- Client-provided session ID for subscription recovery
- Server-side subscription persistence with TTL
- Missed message replay from ring buffer

---

## Multi-Key Load Balancing

### Strategy: Sticky Subscription Assignment

Each Alpaca API key manages a subset of symbols. Once a symbol is assigned to a key, it stays there (sticky) to avoid resubscription churn.

```
Key 1: AAPL, MSFT, GOOGL, META (first 500 symbols)
Key 2: TSLA, NVDA, AMD, ... (next 500 symbols)
```

### Assignment Algorithm

1. **New subscription request** for symbol X
2. **Check existing:** Is X already assigned to a key? → Use that key
3. **Find least-loaded key:** Assign X to key with fewest subscriptions
4. **Rebalance threshold:** If imbalance > 30%, trigger background rebalance

### Failover Behavior

| Event | Action |
|-------|--------|
| Key hits rate limit (429) | Mark key unhealthy for 60s, failover affected symbols to healthy key |
| Key auth fails | Mark key dead, redistribute all symbols |
| Key recovers | Gradually migrate symbols back (10/second) |

### Key Health Tracking

```python
class KeyHealth:
    rate_limit_until: datetime | None  # When 429 expires
    consecutive_errors: int            # For circuit breaker
    is_healthy: bool                   # Computed property
    subscription_count: int            # Current load
```

### Configuration

```yaml
alpaca:
  keys:
    - key_id: ${ALPACA_API_KEY_1}
      secret: ${ALPACA_SECRET_KEY_1}
      max_subscriptions: 500
    - key_id: ${ALPACA_API_KEY_2}
      secret: ${ALPACA_SECRET_KEY_2}
      max_subscriptions: 500
  rebalance_threshold: 0.3  # 30% imbalance triggers rebalance
  failover_cooldown_seconds: 60
```

**Load Balancer Flowchart:**

```mermaid
flowchart TD
    A[Select key for symbol X] --> B{X already assigned\nto a key?}
    B -->|Yes| C{Is assigned\nkey healthy?}
    C -->|Yes| D[Return assigned key]
    C -->|No| E[Failover: find healthy key]
    B -->|No| F[Get all healthy keys]
    F --> G{Any healthy\nkeys available?}
    G -->|No| H[Return error: no capacity]
    G -->|Yes| I[Sort by subscription count ASC]
    I --> J[Select least-loaded key]
    J --> K{Key at max\nsubscriptions?}
    K -->|Yes| L[Try next key]
    K -->|No| M[Assign X to key]
    E --> N{Healthy key\nfound?}
    N -->|Yes| O[Migrate X to new key]
    N -->|No| H

    M --> P[Return selected key]
    O --> P
    D --> P
```

**Key Selection Algorithm:**

```python
class LoadBalancer:
    def __init__(self, keys: list[AlpacaKey]):
        self.keys = {k.id: k for k in keys}
        self.symbol_assignments: dict[str, str] = {}  # symbol -> key_id

    def select_key(self, symbol: str) -> AlpacaKey:
        if symbol in self.symbol_assignments:
            key_id = self.symbol_assignments[symbol]
            key = self.keys[key_id]
            if key.is_healthy:
                return key
            else:
                return self._failover(symbol, key)

        return self._assign_new(symbol)

    def _assign_new(self, symbol: str) -> AlpacaKey:
        healthy_keys = [k for k in self.keys.values() if k.is_healthy]
        if not healthy_keys:
            raise NoCapacityError("No healthy keys available")

        available = [k for k in healthy_keys if k.subscription_count < k.max_subscriptions]
        if not available:
            raise NoCapacityError("All keys at max subscriptions")

        key = min(available, key=lambda k: k.subscription_count)
        self.symbol_assignments[symbol] = key.id
        key.subscription_count += 1
        return key

    def _failover(self, symbol: str, failed_key: AlpacaKey) -> AlpacaKey:
        logger.warning("Failover triggered", symbol=symbol, failed_key=failed_key.id)
        new_key = self._assign_new(symbol)
        failed_key.subscription_count -= 1
        return new_key

    def check_rebalance(self) -> bool:
        counts = [k.subscription_count for k in self.keys.values() if k.is_healthy]
        if len(counts) < 2:
            return False
        imbalance = (max(counts) - min(counts)) / max(counts)
        return imbalance > 0.3  # 30% threshold
```

---

## Subscription Lifecycle

### Reference Counting

Each symbol subscription is reference-counted across clients:

```python
subscriptions = {
    "AAPL": {
        "clients": {"cerberus", "orion"},  # Set of client IDs
        "upstream_key": "key_1",
        "feed_types": {"bars", "quotes"},
        "subscribed_at": datetime
    }
}
```

### Unsubscribe Behavior

| Scenario | Action |
|----------|--------|
| Client unsubscribes from AAPL, others still subscribed | Decrement ref count only |
| Last client unsubscribes from AAPL | Start grace period timer |
| Grace period expires (60s) | Unsubscribe upstream |
| New subscription during grace period | Cancel unsubscribe, increment ref count |

**Rationale for grace period:** Prevents thrashing when clients briefly disconnect/reconnect.

### Multi-Timeframe Subscriptions

If Client A wants 1-min bars and Client B wants 5-min bars:
- Gateway subscribes to **1-min** upstream (finest granularity)
- Gateway aggregates to 5-min locally for Client B

```json
{
  "action": "subscribe",
  "provider": "alpaca",
  "feed": "stock_bars",
  "symbols": ["AAPL"],
  "timeframe": "5Min"  // Gateway handles aggregation
}
```

**Subscription Flow Diagram:**

```mermaid
flowchart TD
    A[Client subscribes to AAPL] --> B{AAPL already\nsubscribed upstream?}
    B -->|Yes| C[Add client to ref set]
    B -->|No| D{In grace period?}
    D -->|Yes| E[Cancel pending unsub]
    D -->|No| F[Select key via load balancer]
    E --> C
    F --> G[Subscribe upstream]
    G --> H{Success?}
    H -->|Yes| C
    H -->|No| I[Return error to client]
    C --> J[Start delivering data]

    K[Client unsubscribes from AAPL] --> L[Remove client from ref set]
    L --> M{ref count == 0?}
    M -->|No| N[Continue for other clients]
    M -->|Yes| O[Start grace timer 60s]
    O --> P{Timer expires?}
    P -->|Yes| Q[Unsubscribe upstream]
    P -->|New sub arrives| R[Cancel timer, add client]
```

**Subscription Manager Algorithm:**

```python
class SubscriptionManager:
    def __init__(self):
        self.subscriptions: dict[str, SymbolSubscription] = {}
        self.grace_timers: dict[str, asyncio.Task] = {}

    async def subscribe(self, client_id: str, symbol: str, feeds: set[str]) -> Result:
        if symbol in self.grace_timers:
            self.grace_timers[symbol].cancel()
            del self.grace_timers[symbol]

        if symbol in self.subscriptions:
            sub = self.subscriptions[symbol]
            sub.clients.add(client_id)
            sub.feed_types |= feeds
            return Result.ok()

        key = self.load_balancer.select_key(symbol)
        try:
            await key.subscribe(symbol, feeds)
        except UpstreamError as e:
            return Result.error(e)

        self.subscriptions[symbol] = SymbolSubscription(
            clients={client_id},
            upstream_key=key.id,
            feed_types=feeds,
            subscribed_at=datetime.utcnow()
        )
        return Result.ok()

    async def unsubscribe(self, client_id: str, symbol: str):
        if symbol not in self.subscriptions:
            return

        sub = self.subscriptions[symbol]
        sub.clients.discard(client_id)

        if len(sub.clients) == 0:
            self.grace_timers[symbol] = asyncio.create_task(
                self._grace_period_unsub(symbol, delay=60.0)
            )

    async def _grace_period_unsub(self, symbol: str, delay: float):
        await asyncio.sleep(delay)
        if symbol in self.subscriptions and len(self.subscriptions[symbol].clients) == 0:
            key = self.keys[self.subscriptions[symbol].upstream_key]
            await key.unsubscribe(symbol)
            del self.subscriptions[symbol]
        if symbol in self.grace_timers:
            del self.grace_timers[symbol]
```

---

## Message Ordering & Delivery Guarantees

### Ordering Guarantees

| Guarantee | Scope |
|-----------|-------|
| **Per-symbol ordering** | ✅ Guaranteed (messages for AAPL arrive in order) |
| **Cross-symbol ordering** | ❌ Not guaranteed (AAPL and TSLA may interleave) |
| **Delivery** | At-most-once (no retries, no persistence) |

### Sequence Numbers

Each message includes a sequence number for gap detection:

```json
{
  "provider": "alpaca",
  "feed": "stock_bars",
  "symbol": "AAPL",
  "seq": 12345,
  "timestamp": "2026-01-14T21:30:00Z",
  "data": {...}
}
```

- Sequence numbers are **per-symbol, per-feed**
- Reset to 0 on gateway restart
- Clients can detect gaps: `if new_seq != last_seq + 1`

### Gap Detection Response

Clients detecting gaps should:
1. Log the gap for diagnostics
2. Optionally request historical backfill via REST API
3. Continue processing (gaps are expected during reconnects)

---

## Graceful Shutdown

### Shutdown Sequence

1. **Stop accepting** new WebSocket connections
2. **Notify clients** with shutdown warning:
   ```json
   {"type": "system", "event": "shutdown", "timeout_seconds": 30}
   ```
3. **Drain period** (30 seconds): Continue delivering queued messages
4. **Unsubscribe upstream** from all symbols
5. **Close client connections** with 1001 (Going Away) code
6. **Close upstream connections**
7. **Flush metrics** to Prometheus
8. **Exit**

### Health Check During Shutdown

```
GET /health → 503 Service Unavailable
{
  "status": "shutting_down",
  "drain_remaining_seconds": 25
}
```

### Docker Integration

```yaml
services:
  gateway:
    stop_grace_period: 45s  # Allow drain + buffer
    stop_signal: SIGTERM
```

---

## Client Introspection API

### Status Endpoint

`GET /api/v1/status` — Returns gateway and client state

```json
{
  "gateway": {
    "uptime_seconds": 86400,
    "version": "1.0.0"
  },
  "upstream": {
    "alpaca": {
      "status": "connected",
      "keys_active": 2,
      "total_subscriptions": 847
    },
    "uw": {"status": "healthy"},
    "news": {"status": "healthy"}
  },
  "clients": {
    "connected": 3,
    "total_subscriptions": 1250
  }
}
```

### Client Subscriptions Endpoint

`GET /api/v1/subscriptions` — Returns calling client's active subscriptions

```json
{
  "client_id": "cerberus",
  "subscriptions": [
    {"provider": "alpaca", "feed": "stock_bars", "symbols": ["AAPL", "TSLA"], "timeframe": "1Min"},
    {"provider": "alpaca", "feed": "stock_quotes", "symbols": ["SPY"]}
  ],
  "total_symbols": 3
}
```

---

## Cache Control

### Cache Bypass

Clients can force fresh data with header:
```
X-Gateway-Cache: bypass
```

### Cache Invalidation Events

| Event | Invalidation |
|-------|--------------|
| Market open/close | Clear all snapshot caches |
| Option expiration | Clear affected option chains |
| Corporate action | Clear affected symbol caches |
| Manual trigger | `POST /api/v1/admin/cache/clear` |

### Cache TTL by Endpoint

| Endpoint | TTL | Rationale |
|----------|-----|-----------|
| Stock bars (historical) | 60s | Immutable once closed |
| Stock snapshot | 5s | Real-time sensitive |
| Option chain | 30s | Greeks change frequently |
| UW flow data | 60s | Moderate freshness needed |
| News articles | 300s | Rarely updates |

---



| Question | Decision | Rationale |
|----------|----------|-----------|
| **WebSocket compression** | No (skip for v1) | Messages are small (~200-500 bytes), compression adds latency overhead, only beneficial for WAN with 100+ symbols |
| **Redis** | Optional (in-memory default) | Single gateway instance sufficient for 3-10 clients; Redis available for future horizontal scaling |
| **Metrics format** | Prometheus | Industry standard, minimal code with `prometheus-fastapi-instrumentator`, integrates with Grafana |

### Prometheus Metrics Exposed

```
# WebSocket
gateway_ws_clients_connected          # Current connected clients
gateway_ws_subscriptions_total        # Total active subscriptions
gateway_ws_messages_sent_total        # Messages fanned out
gateway_ws_upstream_reconnects_total  # Alpaca reconnection count

# REST API
gateway_rest_requests_total{provider="alpaca",endpoint="/bars"}
gateway_rest_latency_seconds{provider="uw"}
gateway_cache_hits_total
gateway_cache_misses_total

# System
gateway_upstream_connections{provider="alpaca"}
gateway_memory_bytes
```
