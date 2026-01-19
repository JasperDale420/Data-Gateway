# Data Gateway API Reference

The Data Gateway is a unified financial data gateway providing access to 7+ data providers through REST APIs and real-time WebSocket streams.

## Quick Start

**Base URL:** `http://localhost:8080`

**API Discovery:**

```bash
# Get full API catalog
curl http://localhost:8080/catalog/

# WebSocket streams
curl http://localhost:8080/catalog/streams

# REST providers
curl http://localhost:8080/catalog/providers

# OpenAPI docs
open http://localhost:8080/docs
```

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

### Subscribe to Data Feeds

```json
{
  "action": "subscribe",
  "feed": "stock_bars",
  "symbols": ["AAPL", "MSFT", "GOOGL"]
}
```

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

| Category | Endpoints |
|----------|-----------|
| Options Flow | `/flow`, `/flow/alerts`, `/flow/historical`, `/flow/ticker/{ticker}` |
| Dark Pool | `/darkpool`, `/darkpool/ticker/{ticker}` |
| Institutional | `/institution`, `/institution/{name}`, `/institution/{name}/holdings` |
| Insider | `/insider/transactions`, `/insider/ticker/{ticker}` |
| Analytics | `/stock/{ticker}/overview`, `/stock/{ticker}/options/volume`, `/stock/{ticker}/options/greeks` |
| ETF | `/etf`, `/etf/{ticker}`, `/etf/{ticker}/holdings` |
| Screeners | `/screener/options`, `/screener/stocks` |

---

### SEC EDGAR (`/sec`)

SEC filings, company facts, and insider data.

**Documentation:** <https://www.sec.gov/developer>

| Category | Endpoints |
|----------|-----------|
| Company | `/company/{cik}`, `/company/ticker/{ticker}` |
| Filings | `/filings/{cik}`, `/filings/{cik}/{form_type}`, `/search` |
| Institutional | `/13f/{cik}`, `/insiders/{cik}` |
| XBRL | `/facts/{cik}`, `/concept/{cik}/{concept}`, `/frames/{concept}/{period}` |

---

### Finnhub (`/finnhub`)

Fundamentals, earnings, news, and alternative data.

**Documentation:** <https://finnhub.io/docs/api>

| Category | Endpoints |
|----------|-----------|
| Company | `/stock/profile/{symbol}`, `/stock/metric`, `/stock/peers`, `/stock/executive` |
| Financials | `/stock/financials`, `/stock/financials-reported`, `/stock/revenue-estimate` |
| Earnings | `/stock/earnings`, `/stock/eps-estimate`, `/calendar/earnings` |
| News | `/news/company/{symbol}`, `/news/market`, `/news/sentiment` |
| Alternative | `/stock/social-sentiment`, `/stock/insider-transactions`, `/stock/congressional-trading` |

---

### Alpha Vantage (`/alphavantage`)

Technical indicators, forex, and economic data.

**Documentation:** <https://www.alphavantage.co/documentation/>

| Category | Endpoints |
|----------|-----------|
| Overview | `/company/{symbol}`, `/income/{symbol}`, `/balance/{symbol}`, `/cashflow/{symbol}` |
| Technical | `/indicator/{indicator}/{symbol}`, `/sma/{symbol}`, `/ema/{symbol}`, `/rsi/{symbol}`, `/macd/{symbol}` |
| Forex | `/forex/rate`, `/forex/intraday`, `/forex/daily` |
| Economy | `/economy/gdp`, `/economy/inflation`, `/economy/interest-rate`, `/economy/unemployment` |

---

### Yahoo Finance (`/yf`)

Free stock quotes, financials, and analysis.

| Category | Endpoints |
|----------|-----------|
| Quotes | `/ticker/{symbol}`, `/ticker/{symbol}/info`, `/ticker/{symbol}/history` |
| Financials | `/ticker/{symbol}/financials`, `/ticker/{symbol}/earnings`, `/ticker/{symbol}/dividends` |
| Options | `/ticker/{symbol}/options`, `/ticker/{symbol}/options/{expiration}` |
| Analysis | `/ticker/{symbol}/recommendations`, `/ticker/{symbol}/holders`, `/ticker/{symbol}/sustainability` |

---

### News Aggregator (`/news`)

Consolidated news from multiple sources.

| Category | Endpoints |
|----------|-----------|
| Articles | `/articles`, `/articles/{article_id}` |
| Sentiment | `/sentiment/{symbol}` |

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
curl http://localhost:8080/metrics
```
