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

Provider endpoint contracts are generated from live routes in [`PROVIDER_ENDPOINT_CONTRACT.md`](../PROVIDER_ENDPOINT_CONTRACT.md).

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

### Alpaca Markets (`/api/v1/alpaca`)

Stock, options, and crypto market data + trading execution.

**Documentation:** <https://docs.alpaca.markets/>

All Alpaca routes are mounted under the canonical prefix `/api/v1/alpaca`
(`ALPACA_ROUTER_PREFIX` in `gateway/api/alpaca/common.py`). Paths below are
relative to that prefix.

| Category | Endpoints |
|----------|-----------|
| Stocks | `GET /stocks/{symbol}/bars`, `GET /stocks/{symbol}/quotes`, `GET /stocks/{symbol}/trades`, `GET /stocks/{symbol}/snapshot`, `GET /stocks/bars/latest`, `GET /stocks/trades/latest`, `GET /stocks/quotes`, `GET /stocks/snapshots`, `GET /stocks/auctions` |
| Options | `GET /options/chain/{underlying}`, `GET /options/chain/{underlying}/snapshot`, `GET /options/{contract}/bars`, `GET /options/{contract}/quotes`, `GET /options/quotes`, `GET /options/trades`, `GET /options/snapshots/{underlying}` |
| Crypto | `GET /crypto/{pair}/bars`, `GET /crypto/{pair}/trades`, `GET /crypto/{pair}/quotes/historical`, `GET /crypto/{pair}/orderbook`, `GET /crypto/snapshots`, `GET /crypto/bars/latest`, `GET /crypto/quotes/latest`, `GET /crypto/trades/latest` |
| Trading — orders | `POST /orders`, `GET /orders`, `GET /orders/{order_id}`, `GET /orders:by_client_order_id`, `PATCH /orders/{order_id}`, `DELETE /orders/{order_id}`, `DELETE /orders` |
| Trading — positions | `GET /positions`, `GET /positions/{symbol}`, `DELETE /positions/{symbol}`, `DELETE /positions` |
| Trading — account | `GET /account`, `GET /account/configurations`, `PATCH /account/configurations`, `GET /account/activities`, `GET /portfolio/history` |
| Trading — reference | `GET /assets`, `GET /assets/{symbol}`, `GET /clock`, `GET /calendar`, `GET /watchlists` (+ CRUD) |

**Role enforcement:** Trading and account endpoints
(`/account`, `/orders`, `/positions`, `/portfolio`, `/watchlists`, `/assets`,
`/clock`, `/calendar`) require a client role of `trader`, `admin`, or
`super_admin`; other roles get `403 GW-E2008`.

#### Order Idempotency & Retry Contract

`POST /api/v1/alpaca/orders` and `PATCH /api/v1/alpaca/orders/{order_id}`
implement an idempotency contract so callers can safely retry on transient
5xx failures:

- **client_order_id handling** — if the caller omits `client_order_id`, the
  gateway auto-generates a `dg-<uuid4hex>` key. If the caller supplies one it
  is used verbatim. Empty, whitespace-only, or over-128-char keys are rejected
  with `400 GW-E4006` (there is no silent fallback — that would break
  Alpaca-side dedup on retry).
- **On success** — the effective key is returned in `meta.client_order_id`,
  with `meta.client_order_id_source` set to `"gateway"` or `"caller"`.
- **On 5xx** (504 timeout, 503 backpressure, or any other upstream 5xx) — the
  error `detail` carries `client_order_id`, `client_order_id_source`,
  `retry_with: "client_order_id"`, and a human `retry_hint`. Because Alpaca
  natively dedupes `submit_order` / `replace_order_by_id` by
  `client_order_id`, a caller seeing a 5xx can either retry the same request
  with the same key (Alpaca returns the existing order rather than placing a
  second) or `GET /api/v1/alpaca/orders:by_client_order_id?client_order_id=<key>`
  to check whether the order/replacement landed. For `PATCH` the original
  order transitions to `replaced` status when the replacement applies — do not
  retry PATCH naively.

`DELETE /api/v1/alpaca/positions/{symbol}` (`close_position`) cannot use
`client_order_id` (Alpaca's `ClosePositionRequest` does not accept one).
Instead, on a 5xx the error `detail` carries `symbol`,
`retry_with: "get_position"`, and a `retry_hint`. The caller resolves "did the
close land?" via `GET /api/v1/alpaca/positions/{symbol}`: a `404` means the
close succeeded (or the position was already gone — do not retry); a `200`
with position data means it is safe to retry the close. `close_position` also
rejects `qty < 0` with `400 GW-E4006`.

#### Trading Timeout Semantics

A `504 GW-E5004` on a **write** operation does not mean the write failed — the
asyncio task is cancelled while the executor thread may still be completing the
Alpaca call, so the order/replace/close **may have landed at the broker**.
Always reconcile via the retry contract above rather than blindly re-issuing.

| Operation class | Wall-clock budget | Setting |
|-----------------|-------------------|---------|
| Reads (`get_account`, `get_orders`, `get_position`, `get_clock`, `get_calendar`, `get_portfolio_history`, `get_assets`) | 15s | `GATEWAY_ALPACA_TRADING_CALL_TIMEOUT_SECONDS` |
| Writes (`create_order`, `replace_order`, `cancel_order`, `cancel_all_orders`, `close_position`, `close_all_positions`) | 25s | `GATEWAY_ALPACA_TRADING_WRITE_CALL_TIMEOUT_SECONDS` |
| HTTP net safety net (SDK session) | 30s | `GATEWAY_ALPACA_TRADING_HTTP_TIMEOUT_SECONDS` |

When more than `GATEWAY_ALPACA_TRADING_MAX_INFLIGHT` (default 24) trading calls
are already in flight, new calls fast-fail with `503 GW-E5005` (backpressure)
instead of queueing.

---

### Unusual Whales (`/uw`)

Options flow, dark pool, institutional, and alternative data.

**Documentation:** <https://docs.unusualwhales.com/>

- Authoritative routes are generated from FastAPI and listed in [`PROVIDER_ENDPOINT_CONTRACT.md`](../PROVIDER_ENDPOINT_CONTRACT.md) under `unusual_whales`.

---

### SEC EDGAR (`/sec`)

SEC filings, company facts, and insider data.

**Documentation:** <https://www.sec.gov/developer>

- Authoritative routes are generated from FastAPI and listed in [`PROVIDER_ENDPOINT_CONTRACT.md`](../PROVIDER_ENDPOINT_CONTRACT.md) under `sec`.

---

### Finnhub (`/finnhub`)

Fundamentals, earnings, news, and alternative data.

**Documentation:** <https://finnhub.io/docs/api>

- Authoritative routes are generated from FastAPI and listed in [`PROVIDER_ENDPOINT_CONTRACT.md`](../PROVIDER_ENDPOINT_CONTRACT.md) under `finnhub`.

---

### Alpha Vantage (`/alphavantage`)

Technical indicators, forex, and economic data.

**Documentation:** <https://www.alphavantage.co/documentation/>

- Authoritative routes are generated from FastAPI and listed in [`PROVIDER_ENDPOINT_CONTRACT.md`](../PROVIDER_ENDPOINT_CONTRACT.md) under `alphavantage`.

---

### Yahoo Finance (`/yf`)

Free stock quotes, financials, and analysis.

- Authoritative routes are generated from FastAPI and listed in [`PROVIDER_ENDPOINT_CONTRACT.md`](../PROVIDER_ENDPOINT_CONTRACT.md) under `yfinance`.

---

### News Aggregator (`/api/v1/news`)

Provider: NewsAPI.org.

| Category | Endpoints |
|----------|-----------|
| Articles | `/api/v1/news/articles` |
| Sentiment | `/api/v1/news/sentiment/{symbol}` |

Notes:
- NewsAPI.org does not expose a get-by-id endpoint. `/api/v1/news/articles/{article_id}` is not supported and will return `501`.

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

## Error Envelope

Every REST error is normalized into a stable envelope by
`gateway_http_exception_handler` (`gateway/api/errors.py`), registered globally
for all `HTTPException`s:

```json
{
  "success": false,
  "error": {
    "code": "GW-E4006",
    "message": "client_order_id length 200 exceeds Alpaca's 128-char limit.",
    "details": { "...": "any extra fields from the handler" }
  },
  "detail": "..."
}
```

- `error.code` — stable gateway error code (table below). When a handler does
  not supply one, it defaults from the HTTP status (e.g. `400 → GW-E4000`,
  `404 → GW-E4004`, `429 → GW-E4001`, `500 → GW-E5000`, `503 → GW-E5003`,
  `504 → GW-E5004`).
- `error.details` — present when the handler attached extra fields beyond
  `code` and `message` (e.g. `client_order_id`, `retry_with`, `retry_hint`,
  `symbol`, `retry_after`).
- `detail` — retained for backward compatibility with callers that read the
  raw FastAPI `detail`.

WebSocket errors are returned as `{"type": "error", "error_code": "...", "message": "..."}`
frames (not the REST envelope).

## Error Codes

### Authentication & authorization (2xxx)

| Code | Status | Description |
|------|--------|-------------|
| `GW-E2001` | 401 | Missing `X-Gateway-Key` header (REST) / invalid API key (WebSocket) |
| `GW-E2002` | 401 | Invalid API key (REST) / invalid message format (WebSocket auth) |
| `GW-E2003` | 403 | Forbidden (REST default) / invalid action type (WebSocket) |
| `GW-E2004` | — | WebSocket authentication timeout |
| `GW-E2005` | 403 | Admin access required (admin endpoints / `/api/v1/status`) |
| `GW-E2006` | 403 | Provider access denied (client lacks provider permission) |
| `GW-E2007` | — | Feed access denied (WebSocket subscribe) |
| `GW-E2008` | 403 | Trading access required (Alpaca trading/account endpoints need `trader`+ role) |

### Routing & request (3xxx, 4xxx)

| Code | Status | Description |
|------|--------|-------------|
| `GW-E3001` | — | Unknown WebSocket action |
| `GW-E3002` / `GW-E3003` | — | Stream subscription errors (WebSocket) |
| `GW-E4000` | 400 | Bad request (default) |
| `GW-E4001` | 429 | Global rate limit exceeded |
| `GW-E4002` | 429 | Provider rate limit exceeded (includes `provider`, `retry_after`) |
| `GW-E4004` | 404 | Not found (default) |
| `GW-E4005` | — | Admin resource error |
| `GW-E4006` | 400 | Invalid `client_order_id` (empty / whitespace / >128 chars) **or** `close_position` `qty < 0` |
| `GW-E4007` | 400 | Option contract passed to a stock endpoint (use the options route) |
| `GW-E4009` | 409 | Conflict (default) |
| `GW-E4029` | 429 | Endpoint-specific rate limit (bulk/replay) |

### Upstream & server (5xxx)

| Code | Status | Description |
|------|--------|-------------|
| `GW-E5000` | 500 | Internal server error (default) |
| `GW-E5002` | 502 | Upstream provider error (default) |
| `GW-E5003` | 503 | Service unavailable (default) |
| `GW-E5004` | 504 | Trading-call timeout — write **may** have landed; reconcile via the retry contract |
| `GW-E5005` | 503 | Trading backpressure — too many in-flight calls; retry shortly |
| `GW-E5007` | 5xx | Non-timeout upstream trading error (carries idempotency context) |

### Validation & data quality (7xxx, 8xxx)

| Code | Status | Description |
|------|--------|-------------|
| `GW-E7001`–`GW-E7007` | — | Streaming event schema/validation failures |
| `GW-E8001` | 400/422 | Invalid symbol format |
| `GW-E8002` | 400 | Parameter exceeds limit (e.g. too many symbols) |
| `GW-E8003` | 400 | Invalid datetime format |
| `GW-E8004` | 400 | Invalid enum value |
| `GW-E8005` | 400/413 | Request body too large |
| `GW-E8006` | 400 | Forbidden characters |
| `GW-E8007` | 400 | Required parameter missing |

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
