# Data Gateway API Reference

The Data Gateway is a unified financial data gateway providing access to 7+ data providers through REST APIs and real-time WebSocket streams.

## Quick Start

**Base URL:** `http://localhost:8080`

**Authentication:** Most endpoints require `X-Gateway-Key`. Health endpoints (`/health/*`) are public.

**API Discovery:**

```bash
# Get full API catalog
curl -H "X-Gateway-Key: <your-gateway-api-key>" http://localhost:8080/catalog/

# WebSocket streams
curl -H "X-Gateway-Key: <your-gateway-api-key>" http://localhost:8080/catalog/streams

# REST providers
curl -H "X-Gateway-Key: <your-gateway-api-key>" http://localhost:8080/catalog/providers

# OpenAPI docs
open http://localhost:8080/docs
```

**Stub data:** Some endpoints return stub/mock data only when `GATEWAY_ALLOW_STUB_DATA=true`. If disabled (default), those endpoints return `501 Not Implemented` until a real data loader is configured.

**Legacy aliases (deprecated):** `/symbology/*`, `/corporate-actions/*`, `/adjustment-factors/*` (forward to `/api/v1/*`).

---

## WebSocket Streaming

### Connection

Connect to the Gateway WebSocket endpoint:

```
ws://localhost:8080/ws
```

### Authentication

Send an auth message immediately after connecting:

```json
{
  "action": "auth",
  "key": "<your-gateway-api-key>"
}
```

**Access control notes:**

- Provider access is enforced via `permissions.providers` in `config/clients.yaml`.
- Feed access is enforced via `permissions.feeds`.
- Admin endpoints require a client role of `admin` or `super_admin`.

### Subscribe to Data Feeds

```json
{
  "action": "subscribe",
  "feeds": ["stock_bars"],
  "symbols": ["AAPL", "MSFT", "GOOGL"]
}
```

```json
{
  "action": "subscribe",
  "feeds": ["news"],
  "symbols": ["*"]
}
```

Legacy payloads may still send `feed` instead of `feeds`.

### Available Feeds

| Feed | Description | Stream |
|------|-------------|--------|
| `stock_bars` | Stock minute bars | stocks_sip |
| `stock_quotes` | Stock NBBO quotes | stocks_sip |
| `stock_trades` | Stock trade executions | stocks_sip |
| `stock_dailyBars` | Stock daily bars (updated each minute) | stocks_sip |
| `stock_updatedBars` | Stock updated bars for late trades | stocks_sip |
| `stock_lulds` | Limit Up/Limit Down price bands | stocks_sip |
| `stock_statuses` | Trading halt/resume status updates | stocks_sip |
| `stock_imbalances` | Auction imbalance data | stocks_sip |
| `option_bars` | Options minute bars | options_opra |
| `option_quotes` | Options quotes | options_opra |
| `option_trades` | Options trade executions | options_opra |
| `crypto_bars` | Crypto minute bars | crypto |
| `crypto_quotes` | Crypto quotes | crypto |
| `crypto_trades` | Crypto trade executions | crypto |
| `crypto_dailyBars` | Crypto daily bars | crypto |
| `crypto_updatedBars` | Crypto updated bars | crypto |
| `crypto_orderbooks` | Crypto Level 2 orderbooks | crypto |
| `news` | Real-time news articles | news |

### Symbol Formats

| Stream | Format | Examples |
|--------|--------|----------|
| Stocks | Ticker symbol | `AAPL`, `MSFT`, `SPY` |
| Options | OCC format | `AAPL240119C00190000` |
| Crypto | Pair format | `BTC/USD`, `ETH/USD` |
| News | Ticker or `*` | `AAPL`, `*` (all) |

---

## REST API Providers

Provider endpoint contracts are generated from live routes in `PROVIDER_ENDPOINT_CONTRACT.md`.

## Bulk Data

Bulk data requests are asynchronous jobs. Create a job and poll for status or download results.

**Endpoints:**

- `POST /api/v1/bulk/bars`
- `POST /api/v1/bulk/options/chains`
- `POST /api/v1/bulk/adjustment-factors`
- `GET /api/v1/bulk/jobs/{job_id}`
- `GET /api/v1/bulk/jobs/{job_id}/download?format=jsonl|json`
- `DELETE /api/v1/bulk/jobs/{job_id}`
- `GET /api/v1/bulk/jobs`

**Bulk options notes:**

- Uses Alpaca options snapshots when the Alpaca provider is configured.
- `expiration_range`: `{"min_dte": 0, "max_dte": 45}` (days to expiration).
- `moneyness_range`: `{"min_delta": 0.2, "max_delta": 0.6}` (absolute delta).

**Bulk adjustment factors notes:**

- Uses the adjustment factors service (currently stubbed unless `GATEWAY_ALLOW_STUB_DATA=true`).

**Bulk bars notes:**

- Uses Alpaca historical bars when the Alpaca provider is configured.

### Alpaca Markets (`/alpaca`)

Stock, options, and crypto market data + trading execution.

**Documentation:** <https://docs.alpaca.markets/>

| Category | Endpoints |
|----------|-----------|
| Stocks | `/stocks/{symbol}/bars`, `/stocks/{symbol}/quotes`, `/stocks/{symbol}/trades`, `/stocks/{symbol}/snapshot` |
| Options | `/options/{symbol}/bars`, `/options/{symbol}/quotes`, `/options/chain/{underlying}` |
| Crypto | `/crypto/{symbol}/bars`, `/crypto/{symbol}/quotes`, `/crypto/snapshots` |
| Trading | `/account`, `/orders`, `/positions`, `/watchlists`, `/calendar`, `/clock`, `/assets` |

---

### Unusual Whales (`/uw`)

Options flow, dark pool, institutional, and alternative data.

**Documentation:** <https://docs.unusualwhales.com/>

- Authoritative routes are generated from FastAPI and listed in `PROVIDER_ENDPOINT_CONTRACT.md` under `unusual_whales`.

---

### SEC EDGAR (`/sec`)

SEC filings, company facts, and insider data.

**Documentation:** <https://www.sec.gov/developer>

- Authoritative routes are generated from FastAPI and listed in `PROVIDER_ENDPOINT_CONTRACT.md` under `sec`.

---

### Finnhub (`/finnhub`)

Fundamentals, earnings, news, and alternative data.

**Documentation:** <https://finnhub.io/docs/api>

- Authoritative routes are generated from FastAPI and listed in `PROVIDER_ENDPOINT_CONTRACT.md` under `finnhub`.

---

### Alpha Vantage (`/alphavantage`)

Technical indicators, forex, and economic data.

**Documentation:** <https://www.alphavantage.co/documentation/>

- Authoritative routes are generated from FastAPI and listed in `PROVIDER_ENDPOINT_CONTRACT.md` under `alphavantage`.

---

### Yahoo Finance (`/yf`)

Free stock quotes, financials, and analysis.

- Authoritative routes are generated from FastAPI and listed in `PROVIDER_ENDPOINT_CONTRACT.md` under `yfinance`.

---

### News Aggregator (`/news`)

Provider: NewsAPI.org.

| Category | Endpoints |
|----------|-----------|
| Articles | `/articles` |
| Sentiment | `/sentiment/{symbol}` |

Notes:

- NewsAPI.org does not expose a get-by-id endpoint. `/articles/{article_id}` is not supported and will return `501`.

---

### Trading Calendar (`/calendar`)

Market hours, trading days, and earnings calendar.

| Endpoint | Description |
|----------|-------------|
| `/api/v1/calendar/market-hours` | Market hours for a date |
| `/api/v1/calendar/trading-days` | Trading days in a range |
| `/api/v1/calendar/earnings` | Earnings calendar for symbols |
| `/api/v1/calendar/is-open` | Market open status |
| `/api/v1/calendar/next-trading-day` | Next trading day |

Notes:

- Market hours/trading days use Alpaca when configured; otherwise fall back to static calendar logic.
- Earnings calendar uses Finnhub when configured; otherwise returns 501 unless `GATEWAY_ALLOW_STUB_DATA=true`.

---

### Corporate Actions (`/api/v1/corporate-actions`) & Adjustment Factors (`/api/v1/adjustment-factors`)

Corporate actions history and adjustment factors for backtesting.

| Endpoint | Description |
|----------|-------------|
| `/api/v1/corporate-actions/{symbol}` | All actions (splits, dividends, etc.) |
| `/api/v1/corporate-actions/{symbol}/splits` | Splits only |
| `/api/v1/corporate-actions/{symbol}/dividends` | Dividends only |
| `/api/v1/adjustment-factors/{symbol}` | Adjustment factors |
| `/api/v1/adjustment-factors/adjust-prices` | Adjust prices using factors |

Notes:

- Corporate actions use Alpaca when configured; otherwise return 501 unless `GATEWAY_ALLOW_STUB_DATA=true`.

---

### Historical Replay (`/api/v1/replay`)

Create, control, and stream historical replay sessions for backtesting.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/replay/sessions` | `POST` | Create a replay session |
| `/api/v1/replay/sessions` | `GET` | List all sessions (supports `limit` + `offset`) |
| `/api/v1/replay/sessions/{session_id}` | `GET` | Get session status and progress |
| `/api/v1/replay/sessions/{session_id}/control` | `POST` | Control: `pause`, `resume`, `seek`, `stop` |
| `/api/v1/replay/sessions/{session_id}` | `DELETE` | Delete a session |
| `ws://host:8080/api/v1/replay/sessions/{session_id}/ws` | WebSocket | Stream replayed data |

**Create session request:**

```json
{
  "name": "My backtest",
  "symbols": ["AAPL", "MSFT"],
  "feeds": ["bars", "trades"],
  "start": "2024-01-15T09:30:00",
  "end": "2024-01-15T16:00:00",
  "speed": 10.0,
  "include_premarket": false
}
```

**WebSocket control actions** (send JSON over the replay WS):

- `{"action": "pause"}` — pause playback
- `{"action": "resume", "speed": 5.0}` — resume at new speed
- `{"action": "seek", "timestamp": "2024-01-15T12:00:00"}` — jump to timestamp
- `{"action": "stop"}` — end session

Notes:

- Sessions are scoped to the client that created them.
- Replay WebSocket connections require `X-Gateway-Key` in the handshake.
- Speed range: `0 < speed ≤ 100` (1.0 = real-time).

---

### Symbology (`/api/v1/symbology`)

Symbol resolution, validation, and cross-provider format conversion.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/symbology/resolve` | `GET` | Resolve symbol to normalized components |
| `/api/v1/symbology/validate` | `GET` | Validate symbol format |
| `/api/v1/symbology/batch` | `POST` | Batch resolve up to 500 symbols |
| `/api/v1/symbology/convert` | `GET` | Convert symbol to provider-specific format |

**Supported formats:**

| Type | Example | Resolved Fields |
|------|---------|-----------------|
| Stock | `AAPL` | symbol, type=`equity` |
| OCC option | `AAPL250117C00200000` | underlying, expiration, strike, option_type |
| Human option | `AAPL 2025-01-17 $200 C` | Same as OCC |
| Crypto | `BTC/USD` | symbol, type=`crypto` |
| Forex | `EUR/USD` | symbol, type=`forex` |

**Batch request:**

```json
{
  "symbols": ["AAPL", "BTC/USD", "AAPL250117C00200000"]
}
```

Notes:

- Legacy alias at `/symbology/*` (excluded from OpenAPI schema).
- Provider conversion targets: `alpaca`, `uw`, `yfinance`.

---

### Data Quality (`/quality`)

Per-symbol data quality analysis and monitoring.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/quality/symbol/{symbol}` | `GET` | Quality metrics for a symbol on a date |
| `/quality/summary` | `GET` | Aggregate quality across symbols |
| `/quality/analyze` | `POST` | Analyze raw bar/quote/trade data |

**Quality issue codes:**

| Code | Description |
|------|-------------|
| `Q001` | Missing bars in sequence |
| `Q002` | Crossed quote (bid > ask) |
| `Q003` | Stale quote (>60s unchanged) |
| `Q004` | Zero volume bar during market hours |
| `Q005` | Price outside normal range (>20% move) |
| `Q006` | Timestamp out of sequence |

Notes:

- Quality endpoints return `501` unless `GATEWAY_ALLOW_STUB_DATA=true` (symbol/summary endpoints use stub data).
- The `/quality/analyze` endpoint accepts raw data and returns real analysis.

---

### API Catalog (`/catalog`)

Runtime API discovery — returns the full endpoint catalog, available streams, providers, and feed types.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/catalog/` | `GET` | Full API summary (providers + streams + feeds) |
| `/catalog/streams` | `GET` | WebSocket streams and channels |
| `/catalog/providers` | `GET` | REST providers with categorized endpoints |
| `/catalog/feeds` | `GET` | Available feed types for subscriptions |

Notes:

- All catalog endpoints require authentication (`X-Gateway-Key`).
- The provider catalog is static (compiled from route definitions), not fetched live from upstream.

---

## Error Codes

| Code | Description |
|------|-------------|
| `GW-E2001` | Invalid API key |
| `GW-E2002` | Invalid message format |
| `GW-E2003` | Invalid action type |
| `GW-E2004` | Authentication timeout |
| `GW-E3001` | Unknown action |

---

## Rate Limits

Rate limits are applied per-provider based on upstream API limits.

| Provider | Requests/min |
|----------|--------------|
| Alpaca | 200 |
| Unusual Whales | 60 |
| SEC EDGAR | 10 |
| Finnhub | 60 |
| Alpha Vantage | 5 |
| Yahoo Finance | 2000 |

---

## Health & Monitoring

```bash
# Health check
curl http://localhost:8080/health

# Ready check (includes dependencies)
curl http://localhost:8080/health/ready

# Status with metrics
curl http://localhost:8080/health/status

# Prometheus metrics
curl -H "X-Gateway-Key: <your-gateway-api-key>" http://localhost:8080/metrics
```
