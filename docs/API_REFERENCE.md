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
