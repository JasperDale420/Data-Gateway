# Data Gateway Consumer Guide

Practical reference for Empire trading systems consuming data through the Data Gateway (port 8080). Organized by what you need to do, not by provider internals.

**Base URL:** `http://localhost:8080`
**Auth Header:** `X-Gateway-Key: <your-api-key>`
**Response Format:** `{"success": true, "data": {...}, "meta": {"provider": "...", "cached": bool}}`

---

## 1. Quick Reference by Use Case

### I need historical price bars

| Provider | Endpoint | Timeframes | Max Limit |
|----------|----------|------------|-----------|
| Alpaca | `GET /api/v1/alpaca/stocks/{symbol}/bars` | 1Min..1Month | 10,000 |
| Finnhub | `GET /api/v1/finnhub/bars/{symbol}` | 1,5,15,30,60,D,W,M | 10,000 |
| Alpha Vantage | `GET /api/v1/alphavantage/intraday/{symbol}` | 1min,5min,15min,30min,60min | 500 |
| Alpha Vantage | `GET /api/v1/alphavantage/daily/{symbol}` | daily (adjusted) | 5,000 |
| Alpha Vantage | `GET /api/v1/alphavantage/weekly/{symbol}` | weekly | -- |
| Alpha Vantage | `GET /api/v1/alphavantage/monthly/{symbol}` | monthly | -- |
| yFinance | `GET /api/v1/yf/{ticker}/history` | 1m..3mo (via `interval`) | -- |

**Best for backtesting:** Alpaca (highest limits, SIP/IEX feeds, timezone-aware).
**Best for daily OHLCV:** Alpha Vantage daily (adjusted for splits/dividends).
**Best for quick free data:** yFinance (no API key needed from provider, but still needs Gateway key).

### I need a real-time quote / snapshot

| Provider | Endpoint | Notes |
|----------|----------|-------|
| Alpaca | `GET /api/v1/alpaca/stocks/{symbol}/quotes` | Latest NBBO quote |
| Alpaca | `GET /api/v1/alpaca/stocks/{symbol}/snapshot` | Quote + latest 1-min bar |
| Alpaca | `GET /api/v1/alpaca/stocks/snapshots?symbols=AAPL,MSFT` | Multi-symbol snapshots |
| Finnhub | `GET /api/v1/finnhub/quote/{symbol}` | Real-time quote (30s cache) |
| Alpha Vantage | `GET /api/v1/alphavantage/quote/{symbol}` | Global quote |
| yFinance | `GET /api/v1/yf/{ticker}/info` | Ticker info + price (5min cache) |

### I need an options chain

| Provider | Endpoint | Notes |
|----------|----------|-------|
| Alpaca | `GET /api/v1/alpaca/options/chain/{underlying}` | Full chain with greeks. Filter: `expiration_date`, `type` (call/put), `strike_price_gte/lte` |
| Alpaca | `GET /api/v1/alpaca/options/chain/{underlying}/snapshot` | Current snapshot of all contracts |
| UW | `GET /api/v1/uw/stock/{symbol}/option-chains` | Option chains via Unusual Whales |
| UW | `GET /api/v1/uw/stock/{symbol}/option-contracts` | Detailed contract data |
| yFinance | `GET /api/v1/yf/{ticker}/options/{expiration}` | Chain by expiration date |
| yFinance | `GET /api/v1/yf/{ticker}/options-expirations` | List available expirations first |

### I need options flow / unusual activity

| Provider | Endpoint | Notes |
|----------|----------|-------|
| UW | `GET /api/v1/uw/flow/all` | All recent options flow (paginated, 30s cache) |
| UW | `GET /api/v1/uw/flow/{symbol}` | Per-ticker flow, filterable by date |
| UW | `GET /api/v1/uw/darkpool/all` | Dark pool trades |
| UW | `GET /api/v1/uw/darkpool/{symbol}` | Per-ticker dark pool |
| UW | `GET /api/v1/uw/stock/{symbol}/flow-recent` | Recent flow for a ticker |
| UW | `GET /api/v1/uw/stock/{symbol}/flow-per-strike-intraday` | Intraday flow by strike |

### I need options greeks / GEX / volatility

| Provider | Endpoint | Notes |
|----------|----------|-------|
| UW | `GET /api/v1/uw/gex/{symbol}` | Gamma exposure (60s cache) |
| UW | `GET /api/v1/uw/gex/{symbol}/strike` | GEX by strike price |
| UW | `GET /api/v1/uw/gex/{symbol}/expiry` | GEX by expiration |
| UW | `GET /api/v1/uw/{symbol}/iv-term-structure` | IV term structure (300s cache) |
| UW | `GET /api/v1/uw/{symbol}/realized-vol` | Realized volatility |
| UW | `GET /api/v1/uw/{symbol}/vol-stats` | Volatility statistics |
| UW | `GET /api/v1/uw/{symbol}/iv-surface` | IV surface |
| UW | `GET /api/v1/uw/{symbol}/iv-rank` | IV rank (300s cache) |
| UW | `GET /api/v1/uw/{symbol}/max-pain` | Max pain by expiry |
| UW | `GET /api/v1/uw/{symbol}/net-premium` | Net premium ticks (60s cache) |
| UW | `GET /api/v1/uw/{symbol}/oi-change` | Open interest change |
| UW | `GET /api/v1/uw/stock/{symbol}/greeks-by-strike/{expiry}` | Greeks by strike for expiry |
| UW | `GET /api/v1/uw/stock/{symbol}/greek-exposure-by-strike-expiry/{expiry}` | Greek exposure by strike/expiry |
| UW | `GET /api/v1/uw/stock/{symbol}/greek-flow-by-expiry/{expiry}` | Greek flow by expiry |
| UW | `GET /api/v1/uw/stock/{symbol}/risk-reversal-skew/{expiry}` | Risk reversal skew |
| UW | `GET /api/v1/uw/stock/{symbol}/spot-exposures` | Spot GEX exposures |

### I need company fundamentals

| Provider | Endpoint | Notes |
|----------|----------|-------|
| Finnhub | `GET /api/v1/finnhub/fundamentals/profile/{symbol}` | Company profile (3600s cache) |
| Finnhub | `GET /api/v1/finnhub/fundamentals/financials/{symbol}` | P/E, EPS, beta |
| Finnhub | `GET /api/v1/finnhub/fundamentals/metrics/{symbol}` | All/margin/valuation/price/profitability |
| Finnhub | `GET /api/v1/finnhub/fundamentals/peers/{symbol}` | Peer companies |
| Finnhub | `GET /api/v1/finnhub/fundamentals/executives/{symbol}` | Executive list |
| Alpha Vantage | `GET /api/v1/alphavantage/fundamentals/overview/{symbol}` | Company overview |
| Alpha Vantage | `GET /api/v1/alphavantage/fundamentals/income-statement/{symbol}` | Income statement |
| Alpha Vantage | `GET /api/v1/alphavantage/fundamentals/balance-sheet/{symbol}` | Balance sheet |
| Alpha Vantage | `GET /api/v1/alphavantage/fundamentals/cash-flow/{symbol}` | Cash flow |
| Alpha Vantage | `GET /api/v1/alphavantage/fundamentals/earnings/{symbol}` | Earnings history |
| yFinance | `GET /api/v1/yf/{ticker}/financials` | Income/balance/cashflow |
| yFinance | `GET /api/v1/yf/{ticker}/company-info` | Company details |
| yFinance | `GET /api/v1/yf/{ticker}/holders` | Institutional holders |
| yFinance | `GET /api/v1/yf/{ticker}/major-holders` | Major holder breakdown |

### I need earnings data

| Provider | Endpoint | Notes |
|----------|----------|-------|
| Finnhub | `GET /api/v1/finnhub/earnings/calendar` | Upcoming earnings calendar |
| Finnhub | `GET /api/v1/finnhub/earnings/estimates/{symbol}` | EPS estimates (quarterly/annual) |
| Finnhub | `GET /api/v1/finnhub/earnings/revenue-estimates/{symbol}` | Revenue estimates |
| Finnhub | `GET /api/v1/finnhub/earnings/recommendations/{symbol}` | Analyst recommendations |
| Finnhub | `GET /api/v1/finnhub/earnings/price-target/{symbol}` | Price target consensus |
| UW | `GET /api/v1/uw/earnings/premarket` | Premarket earnings (300s cache) |
| UW | `GET /api/v1/uw/earnings/afterhours` | After-hours earnings |
| UW | `GET /api/v1/uw/earnings/{symbol}` | Historical earnings per ticker |
| yFinance | `GET /api/v1/yf/{ticker}/earnings` | Earnings history |
| yFinance | `GET /api/v1/yf/{ticker}/recommendations` | Analyst recommendations |

### I need SEC filings / XBRL

| Provider | Endpoint | Notes |
|----------|----------|-------|
| SEC | `GET /api/v1/sec/company/{cik}` | Company info by CIK |
| SEC | `GET /api/v1/sec/company/ticker/{ticker}` | Company info by ticker |
| SEC | `GET /api/v1/sec/filings/{cik}` | All filings (filter: `form_type`, `limit`) |
| SEC | `GET /api/v1/sec/filings/{cik}/{form_type}` | Filings by type (10-K, 10-Q, 8-K) |
| SEC | `GET /api/v1/sec/13f/{cik}` | 13F institutional holdings |
| SEC | `GET /api/v1/sec/insiders/{cik}` | Insider trades (Form 3/4/5) |
| SEC | `GET /api/v1/sec/facts/{cik}` | XBRL company facts |
| SEC | `GET /api/v1/sec/concept/{cik}/{concept}` | Single XBRL concept history (e.g., Revenues) |
| SEC | `GET /api/v1/sec/frames/{concept}/{period}` | Cross-company XBRL for a period (CY2023Q1) |
| SEC | `GET /api/v1/sec/search?q=...` | Full-text filing search |

### I need insider / institutional / congress trading

| Provider | Endpoint | Notes |
|----------|----------|-------|
| UW | `GET /api/v1/uw/insider/transactions` | All recent insider transactions (300s cache) |
| UW | `GET /api/v1/uw/insider/sector-flow` | Insider flow by sector |
| UW | `GET /api/v1/uw/insider/ticker-flow` | Insider flow by ticker |
| UW | `GET /api/v1/uw/insider/{symbol}/insiders` | Per-ticker insiders (3600s cache) |
| UW | `GET /api/v1/uw/politicians/recent-trades` | Congressional trades (300s cache) |
| UW | `GET /api/v1/uw/politicians/people` | Politician list (3600s cache) |
| UW | `GET /api/v1/uw/politicians/{politician_id}/portfolios` | Politician portfolios |
| UW | `GET /api/v1/uw/politicians/{symbol}/holders` | Political holders per ticker |
| UW | `GET /api/v1/uw/institutions` | All institutions (3600s cache) |
| UW | `GET /api/v1/uw/institutions/latest-filings` | Latest institutional filings |
| UW | `GET /api/v1/uw/institutions/{id}/holdings` | Institution holdings |
| UW | `GET /api/v1/uw/institutions/{symbol}/ownership` | Ownership per ticker |
| Finnhub | `GET /api/v1/finnhub/fundamentals/ownership/{symbol}` | Institutional ownership |
| Finnhub | `GET /api/v1/finnhub/fundamentals/insider-transactions/{symbol}` | Insider transactions |
| Finnhub | `GET /api/v1/finnhub/alternative/congress-trading` | Congress trading (STOCK Act) |
| SEC | `GET /api/v1/sec/insiders/{cik}` | SEC insider filings (Form 3/4/5) |
| SEC | `GET /api/v1/sec/13f/{cik}` | 13F institutional holdings |

### I need news / sentiment

| Provider | Endpoint | Notes |
|----------|----------|-------|
| News | `GET /api/v1/news/articles` | Search articles (symbols, keywords, date range, 60s cache) |
| News | `GET /api/v1/news/articles/{article_id}` | Single article by ID |
| News | `GET /api/v1/news/sentiment/{symbol}` | Aggregated sentiment per ticker |
| Alpaca | `GET /api/v1/alpaca/news/articles` | Alpaca news (symbol filter, 30s cache) |
| Finnhub | `GET /api/v1/finnhub/news/company/{symbol}` | Company-specific news |
| Finnhub | `GET /api/v1/finnhub/news/market` | Market news (general/forex/crypto/merger) |
| Finnhub | `GET /api/v1/finnhub/analysis/social-sentiment/{symbol}` | Social media sentiment (1800s cache) |
| Finnhub | `GET /api/v1/finnhub/analysis/insider-sentiment/{symbol}` | Insider sentiment |
| yFinance | `GET /api/v1/yf/{ticker}/news` | Yahoo Finance news |

### I need technical indicators

| Provider | Endpoint | Notes |
|----------|----------|-------|
| Alpha Vantage | `GET /api/v1/alphavantage/indicators/sma/{symbol}` | SMA |
| Alpha Vantage | `GET /api/v1/alphavantage/indicators/ema/{symbol}` | EMA |
| Alpha Vantage | `GET /api/v1/alphavantage/indicators/rsi/{symbol}` | RSI |
| Alpha Vantage | `GET /api/v1/alphavantage/indicators/macd/{symbol}` | MACD |
| Alpha Vantage | `GET /api/v1/alphavantage/indicators/bbands/{symbol}` | Bollinger Bands |
| Alpha Vantage | `GET /api/v1/alphavantage/indicators/stoch/{symbol}` | Stochastic |
| Alpha Vantage | `GET /api/v1/alphavantage/indicators/adx/{symbol}` | ADX |
| Alpha Vantage | `GET /api/v1/alphavantage/indicators/cci/{symbol}` | CCI |
| Alpha Vantage | `GET /api/v1/alphavantage/indicators/atr/{symbol}` | ATR |
| Alpha Vantage | `GET /api/v1/alphavantage/indicators/obv/{symbol}` | OBV |
| Alpha Vantage | `GET /api/v1/alphavantage/indicators/{function}/{symbol}` | Any Alpha Vantage indicator |
| Finnhub | `GET /api/v1/finnhub/analysis/support-resistance/{symbol}` | Support/resistance levels |
| Finnhub | `GET /api/v1/finnhub/analysis/patterns/{symbol}` | Chart pattern recognition |

Common parameters for AV indicators: `interval` (1min-monthly), `time_period` (default 14), `series_type` (close/open/high/low), `max_points`.

### I need macro / economic data

| Provider | Endpoint | Notes |
|----------|----------|-------|
| Alpha Vantage | `GET /api/v1/alphavantage/economic/real-gdp` | Real GDP (annual/quarterly) |
| Alpha Vantage | `GET /api/v1/alphavantage/economic/cpi` | Consumer Price Index |
| Alpha Vantage | `GET /api/v1/alphavantage/economic/inflation` | Inflation rate |
| Alpha Vantage | `GET /api/v1/alphavantage/economic/treasury-yield` | Treasury yields |
| Alpha Vantage | `GET /api/v1/alphavantage/economic/federal-funds-rate` | Fed funds rate |
| Alpha Vantage | `GET /api/v1/alphavantage/economic/unemployment` | Unemployment rate |
| Alpha Vantage | `GET /api/v1/alphavantage/economic/nonfarm-payroll` | Nonfarm payrolls |
| Alpha Vantage | `GET /api/v1/alphavantage/economic/retail-sales` | Retail sales |
| Alpha Vantage | `GET /api/v1/alphavantage/economic/durables` | Durable goods orders |

### I need crypto data

| Provider | Endpoint | Notes |
|----------|----------|-------|
| Alpaca | `GET /api/v1/alpaca/crypto/{pair}/bars` | Bars (pair format: BTC/USD) |
| Alpaca | `GET /api/v1/alpaca/crypto/{pair}/trades` | Historical trades |
| Alpaca | `GET /api/v1/alpaca/crypto/quotes/latest?symbols=BTC/USD,ETH/USD` | Latest quotes |
| Alpaca | `GET /api/v1/alpaca/crypto/snapshots?symbols=BTC/USD` | Current snapshots |
| Alpaca | `GET /api/v1/alpaca/crypto/{pair}/orderbook` | Order book |

### I need forex rates

| Provider | Endpoint | Notes |
|----------|----------|-------|
| Alpaca | `GET /api/v1/alpaca/forex/rates?pairs=USD/EUR,USD/GBP` | Latest rates (10s cache) |
| Alpaca | `GET /api/v1/alpaca/forex/rates/historical` | Historical rates (300s cache) |

### I need to place / manage orders (Alpaca brokerage)

| Action | Method | Endpoint |
|--------|--------|----------|
| Create order | POST | `/api/v1/alpaca/orders` |
| List orders | GET | `/api/v1/alpaca/orders` |
| Get order | GET | `/api/v1/alpaca/orders/{order_id}` |
| Get by client ID | GET | `/api/v1/alpaca/orders:by_client_order_id?client_order_id=...` |
| Modify order | PATCH | `/api/v1/alpaca/orders/{order_id}` |
| Cancel order | DELETE | `/api/v1/alpaca/orders/{order_id}` |
| Cancel all | DELETE | `/api/v1/alpaca/orders` |
| List positions | GET | `/api/v1/alpaca/positions` |
| Get position | GET | `/api/v1/alpaca/positions/{symbol}` |
| Close position | DELETE | `/api/v1/alpaca/positions/{symbol}` |
| Close all | DELETE | `/api/v1/alpaca/positions` |
| Account info | GET | `/api/v1/alpaca/account` |
| Portfolio history | GET | `/api/v1/alpaca/portfolio/history` |
| Market clock | GET | `/api/v1/alpaca/clock` |
| Trading calendar | GET | `/api/v1/alpaca/calendar` |
| Asset info | GET | `/api/v1/alpaca/assets/{symbol}` |

**Requires role:** `trader`, `admin`, or `super_admin`.

### I need real-time streaming (WebSocket)

Connect to `ws://localhost:8080/ws` and authenticate:

```json
{"action": "auth", "key": "your-api-key"}
```

Response: `{"type": "auth_result", "status": "ok", "client_id": "...", "message": "Authenticated successfully"}`

Subscribe to feeds:

```json
{"action": "subscribe", "provider": "alpaca", "feeds": ["stock_bars", "stock_quotes"], "symbols": ["AAPL", "MSFT"]}
```

Available feeds: `stock_bars`, `stock_quotes`, `stock_trades`, `option_bars`, `option_quotes`, `option_trades`, `news`.

Unsubscribe:

```json
{"action": "unsubscribe", "provider": "alpaca", "feeds": ["stock_bars"], "symbols": ["AAPL"]}
```

Check status:

```json
{"action": "status"}
```

Heartbeat: Server sends `{"type": "heartbeat", "ts": ...}` every 30s. Disconnects after 3 missed heartbeats.

---

## 2. Provider Comparison Matrix

### Data Coverage

| Data Type | Alpaca | UW | Finnhub | Alpha Vantage | yFinance | SEC | News |
|-----------|--------|----|---------|---------------|----------|-----|------|
| Stock Bars (OHLCV) | Yes | Yes | Yes | Yes | Yes | -- | -- |
| Real-time Quotes | Yes | -- | Yes | Yes | -- | -- | -- |
| Trades | Yes | -- | -- | -- | -- | -- | -- |
| Options Chain | Yes | Yes | -- | -- | Yes | -- | -- |
| Options Flow | -- | Yes | -- | -- | -- | -- | -- |
| Greeks/GEX/IV | -- | Yes | -- | -- | -- | -- | -- |
| Dark Pool | -- | Yes | -- | -- | -- | -- | -- |
| Short Interest | -- | Yes | -- | -- | -- | -- | -- |
| Company Fundamentals | -- | -- | Yes | Yes | Yes | -- | -- |
| Financial Statements | -- | -- | Yes | Yes | Yes | -- | -- |
| Earnings Calendar | -- | Yes | Yes | Yes | Yes | -- | -- |
| Analyst Estimates | -- | -- | Yes | -- | Yes | -- | -- |
| Insider Trading | -- | Yes | Yes | -- | -- | Yes | -- |
| Institutional Holdings | -- | Yes | Yes | -- | Yes | Yes | -- |
| Congress Trading | -- | Yes | Yes | -- | -- | -- | -- |
| SEC Filings | -- | -- | -- | -- | -- | Yes | -- |
| XBRL Data | -- | -- | -- | -- | -- | Yes | -- |
| News Articles | Yes | -- | Yes | -- | Yes | -- | Yes |
| Sentiment | -- | -- | Yes | -- | -- | -- | Yes |
| Technical Indicators | -- | -- | Yes | Yes | -- | -- | -- |
| Economic Data | -- | -- | -- | Yes | -- | -- | -- |
| Crypto | Yes | -- | -- | -- | -- | -- | -- |
| Forex | Yes | -- | -- | -- | -- | -- | -- |
| Order Management | Yes | -- | -- | -- | -- | -- | -- |
| Streaming (WebSocket) | Yes | -- | -- | -- | -- | -- | -- |
| ETF Data | -- | Yes | -- | -- | -- | -- | -- |
| Seasonality | -- | Yes | -- | -- | -- | -- | -- |
| Pattern Recognition | -- | -- | Yes | -- | -- | -- | -- |

### Cache TTLs

| Provider | Typical TTL | Notes |
|----------|------------|-------|
| Alpaca quotes | 0s | No cache, real-time |
| Alpaca assets/calendar | 600-3600s | Slow-changing metadata |
| Alpaca news | 30s | |
| UW flow/earnings | 60-300s | Varies by data freshness |
| UW institutions/politicians | 3600s | Infrequent updates |
| Finnhub quotes | 30s | |
| Finnhub fundamentals | 3600s | |
| Finnhub sentiment | 1800s | |
| Alpha Vantage | 300-3600s | Economic data cached longer |
| yFinance | 300s | 5-minute cache on most endpoints |
| SEC | 3600s | Filings update infrequently |
| SEC search | 300s | Shorter TTL for search |
| News articles | 60s | |

---

## 3. Authentication & Permissions

### API Key Authentication

All requests require an `X-Gateway-Key` header:

```
X-Gateway-Key: gw_your_api_key_here
```

Keys are defined in the clients YAML config. Both plaintext keys (dev) and SHA-256 hashed keys (production) are supported.

### Client Permissions Model

Each client has these permission attributes:

| Field | Default | Description |
|-------|---------|-------------|
| `providers` | `[]` (all) | Allowed providers. Empty = unrestricted. Values: `alpaca`, `unusual_whales`, `finnhub`, `alphavantage`, `yfinance`, `sec`, `news` |
| `feeds` | `[]` (all) | Allowed WebSocket feed types. Values: `bars`, `quotes`, `trades`, `options`, `news` |
| `max_symbols` | 100 | Max symbols per request (comma-separated lists) |
| `rate_limit` | 60 | Requests per minute per client |
| `ws_subscriptions_max` | 500 | Max active WebSocket subscriptions |

### Role-Based Access

| Role | Access |
|------|--------|
| `client` | All data endpoints (read-only) |
| `trader` | Data + Alpaca trading endpoints (orders, positions, account) |
| `admin` | Data + trading + admin endpoints |
| `super_admin` | Everything |

Trading endpoints (`/api/v1/alpaca/orders`, `/api/v1/alpaca/positions`, `/api/v1/alpaca/account`, etc.) require `trader` role or above. Admin endpoints (`/api/v1/admin/*`) require `admin` or `super_admin`.

### Provider Permission Enforcement

If `providers` is set on a client, access is restricted to only those providers. The path is parsed to extract the provider (e.g., `/api/v1/finnhub/...` extracts `finnhub`), and the request is rejected with `403 GW-E2006` if the client lacks access.

Alias mapping: `uw` maps to `unusual_whales`, `yf` maps to `yfinance` in client configs.

### Symbol Limits

The Gateway enforces `max_symbols` on comma-separated list parameters: `symbols`, `contracts`, `pairs`, `isins`, `underlyings`. Exceeding the limit returns `400 GW-E8002`.

---

## 4. Common Request Patterns

All examples use Python `httpx`. The Gateway runs at `http://localhost:8080`.

### Fetch daily bars for backtesting

```python
import httpx
from datetime import datetime, timezone

GATEWAY = "http://localhost:8080"
HEADERS = {"X-Gateway-Key": "gw_your_key"}

async def get_daily_bars(symbol: str, start: str, end: str) -> list[dict]:
    """Fetch daily OHLCV bars from Alpaca via the Gateway.

    Args:
        symbol: Ticker symbol (e.g., "AAPL")
        start: ISO8601 start (e.g., "2025-01-01T00:00:00Z")
        end: ISO8601 end (e.g., "2025-12-31T23:59:59Z")
    """
    async with httpx.AsyncClient(base_url=GATEWAY, headers=HEADERS, timeout=30) as client:
        resp = await client.get(
            f"/api/v1/alpaca/stocks/{symbol}/bars",
            params={
                "timeframe": "1Day",
                "start": start,
                "end": end,
                "limit": 10000,
                "feed": "sip",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["data"]["bars"]
```

### Get the current options chain with greeks

```python
async def get_options_chain(underlying: str, expiration: str | None = None) -> dict:
    """Fetch the full options chain from Alpaca.

    Args:
        underlying: Underlying ticker (e.g., "SPY")
        expiration: Optional expiration date filter (YYYY-MM-DD)
    """
    async with httpx.AsyncClient(base_url=GATEWAY, headers=HEADERS, timeout=30) as client:
        params = {"limit": 1000}
        if expiration:
            params["expiration_date"] = expiration

        resp = await client.get(
            f"/api/v1/alpaca/options/chain/{underlying}",
            params=params,
        )
        resp.raise_for_status()
        return resp.json()["data"]
```

### Submit a bracket order

```python
async def submit_bracket_order(
    symbol: str,
    qty: int,
    side: str,
    limit_price: float,
    take_profit: float,
    stop_loss: float,
) -> dict:
    """Submit a bracket order through the Gateway.

    Requires 'trader' role or above.
    """
    async with httpx.AsyncClient(base_url=GATEWAY, headers=HEADERS, timeout=30) as client:
        resp = await client.post(
            "/api/v1/alpaca/orders",
            json={
                "symbol": symbol,
                "qty": qty,
                "side": side,
                "type": "limit",
                "time_in_force": "day",
                "limit_price": limit_price,
                "order_class": "bracket",
                "take_profit": {"limit_price": take_profit},
                "stop_loss": {"stop_price": stop_loss},
            },
        )
        resp.raise_for_status()
        return resp.json()["data"]
```

### Stream real-time bars via WebSocket

```python
import asyncio
import json
import websockets

async def stream_bars(symbols: list[str]):
    """Connect to Gateway WebSocket and stream real-time stock bars."""
    uri = "ws://localhost:8080/ws"

    async with websockets.connect(uri) as ws:
        # 1. Authenticate
        await ws.send(json.dumps({
            "action": "auth",
            "key": "gw_your_key",
        }))
        auth_resp = json.loads(await ws.recv())
        assert auth_resp["status"] == "ok", f"Auth failed: {auth_resp}"

        # 2. Subscribe to stock bars
        await ws.send(json.dumps({
            "action": "subscribe",
            "provider": "alpaca",
            "feeds": ["stock_bars"],
            "symbols": symbols,
        }))
        sub_resp = json.loads(await ws.recv())
        print(f"Subscribed: {sub_resp.get('subscribed', [])}")

        # 3. Receive data
        while True:
            msg = json.loads(await ws.recv())
            if msg.get("type") == "heartbeat":
                await ws.send(json.dumps({"action": "heartbeat"}))
                continue
            print(f"Bar: {msg}")
```

### Look up SEC filings by ticker

```python
async def get_10k_filings(ticker: str, limit: int = 10) -> list[dict]:
    """Get recent 10-K filings for a company by ticker symbol."""
    async with httpx.AsyncClient(base_url=GATEWAY, headers=HEADERS, timeout=30) as client:
        # Step 1: Resolve ticker to CIK
        resp = await client.get(f"/api/v1/sec/company/ticker/{ticker}")
        resp.raise_for_status()
        company = resp.json()["data"]
        cik = company["cik"]

        # Step 2: Fetch 10-K filings
        resp = await client.get(
            f"/api/v1/sec/filings/{cik}/10-K",
            params={"limit": limit},
        )
        resp.raise_for_status()
        return resp.json()["data"]["filings"]
```

---

## 5. Rate Limits & Best Practices

### Rate Limit Layers

The Gateway enforces rate limits at two levels:

**1. Client rate limit (per API key)**
- Default: 60 requests/minute (sliding window)
- Configurable per client in YAML config
- Response headers on every request:
  - `X-RateLimit-Limit` -- max requests per minute
  - `X-RateLimit-Remaining` -- requests left in window
  - `X-RateLimit-Reset` -- unix timestamp of reset
  - `X-RateLimit-Reset-After` -- seconds until reset
- Exceeding returns `429` with `GW-E4001` and `Retry-After` header

**2. Provider rate limit (per upstream provider)**
- Protects against hitting provider API quotas
- Queuing behavior: requests wait up to 30s for a slot (default `block=True`)
- Only returns `429 GW-E4002` if the wait deadline is exceeded
- Each provider has its own rate limit pool

### Best Practices

**Use cached endpoints when possible.**
Most endpoints have built-in caching (see TTL table above). The `meta.cached` field in responses tells you if data came from cache. For rapidly-changing data like quotes, use the WebSocket stream instead of polling.

**Batch symbol requests.**
Use comma-separated multi-symbol endpoints instead of making N individual calls:

```
# Good: 1 request
GET /api/v1/alpaca/stocks/snapshots?symbols=AAPL,MSFT,GOOGL,AMZN

# Bad: 4 requests
GET /api/v1/alpaca/stocks/AAPL/snapshot
GET /api/v1/alpaca/stocks/MSFT/snapshot
GET /api/v1/alpaca/stocks/GOOGL/snapshot
GET /api/v1/alpaca/stocks/AMZN/snapshot
```

**Respect the `max_symbols` limit.**
Default is 100 symbols per request. If you need more, split into batches.

**Check `Retry-After` on 429s.**
The header tells you exactly how long to wait. Do not retry immediately.

**Use the right provider for the job.**

| Need | Best Provider | Why |
|------|--------------|-----|
| Backtesting bars | Alpaca | Highest limits, SIP feed, timezone-aware |
| Real-time quotes | Alpaca / Finnhub | Low latency, minimal cache |
| Options flow/greeks | UW | Deepest options analytics |
| Fundamentals | Finnhub + AV | Complementary data sets |
| SEC/XBRL | SEC | Official source |
| Quick prototyping | yFinance | Wide coverage, no provider key needed |
| Economic indicators | Alpha Vantage | Only provider with macro data |
| Technical indicators | Alpha Vantage | 50+ built-in indicators |
| News/sentiment | News + Finnhub | NewsAPI for articles, Finnhub for social |
| Order execution | Alpaca | Only brokerage provider |

**Handle timezone-aware datetimes.**
Alpaca rejects naive timestamps. Always pass timezone-aware ISO8601 strings:

```python
from datetime import datetime, timezone
start = datetime(2025, 1, 1, tzinfo=timezone.utc).isoformat()  # "2025-01-01T00:00:00+00:00"
```

**Use `feed=sip` for Alpaca when possible.**
SIP (Securities Information Processor) includes all exchanges. IEX is free-tier only. Default is `sip`.

---

## 6. What's Available Per Trading System

### 3Roses (Day Trading)

Gap-and-go, micro-pullback, VWAP strategies need:

- [x] **Intraday bars** -- Alpaca `stocks/{symbol}/bars` with `timeframe=1Min` or `5Min`
- [x] **Real-time quotes** -- Alpaca `stocks/{symbol}/quotes` or WebSocket `stock_quotes`
- [x] **Real-time bar stream** -- WebSocket `stock_bars` feed
- [x] **Pre-market data** -- Alpaca bars with `feed=sip` include extended hours
- [x] **Volume analysis** -- Alpaca bars include volume; UW `stock/{symbol}/volume-price-levels`
- [x] **Order execution** -- Alpaca orders API (bracket orders, stop-loss)
- [x] **Position management** -- Alpaca positions API
- [x] **Market clock** -- Alpaca `clock` endpoint
- [x] **News catalyst** -- News `articles` with symbol filter, Alpaca `news/articles`
- [ ] **Level 2 / depth of book** -- Not available through Gateway (Alpaca direct only)

### Cerberus (Multi-Strategy Algo)

10 strategies with 5-axis regime detection need:

- [x] **Historical bars (multi-timeframe)** -- Alpaca bars (1Min through 1Month)
- [x] **Options chain + greeks** -- Alpaca chain, UW greeks/GEX/IV
- [x] **Options flow** -- UW `flow/all`, `flow/{symbol}`, dark pool
- [x] **Volatility surface** -- UW `{symbol}/iv-surface`, `{symbol}/iv-term-structure`
- [x] **Technical indicators** -- Alpha Vantage indicators (SMA, EMA, RSI, MACD, BBANDS, ATR, etc.)
- [x] **Support/resistance** -- Finnhub `analysis/support-resistance/{symbol}`
- [x] **Pattern recognition** -- Finnhub `analysis/patterns/{symbol}`
- [x] **Earnings calendar** -- Finnhub `earnings/calendar`, UW `earnings/premarket`
- [x] **Fundamentals** -- Finnhub profile/financials/metrics
- [x] **Sentiment** -- Finnhub social sentiment, News sentiment
- [x] **Order execution** -- Alpaca orders API
- [x] **Macro regime** -- Alpha Vantage economic data (GDP, CPI, yields, unemployment)
- [x] **Short interest** -- UW `shorts/{symbol}/interest`, `shorts/{symbol}/volume`

### Kairos (Options Swing Trading)

LLM-driven options strategies need:

- [x] **Options chain** -- Alpaca chain with greeks, UW option chains
- [x] **IV rank / IV percentile** -- UW `{symbol}/iv-rank`
- [x] **IV term structure** -- UW `{symbol}/iv-term-structure`
- [x] **GEX / Greek exposure** -- UW `gex/{symbol}`, spot exposures
- [x] **Max pain** -- UW `{symbol}/max-pain`
- [x] **Net premium flow** -- UW `{symbol}/net-premium`
- [x] **OI change** -- UW `{symbol}/oi-change`
- [x] **Risk reversal skew** -- UW `stock/{symbol}/risk-reversal-skew/{expiry}`
- [x] **Earnings calendar** -- UW/Finnhub earnings endpoints
- [x] **Analyst estimates** -- Finnhub EPS/revenue estimates, price target
- [x] **News/sentiment** -- News articles + Finnhub sentiment
- [x] **Historical bars** -- Alpaca daily/weekly bars for context
- [x] **Order execution** -- Alpaca orders API (options orders)

### WhaleHunter (Flow Analysis)

Options flow + dark pool pattern mining need:

- [x] **Options flow** -- UW `flow/all`, `flow/{symbol}` (primary data source)
- [x] **Dark pool trades** -- UW `darkpool/all`, `darkpool/{symbol}`
- [x] **GEX data** -- UW `gex/{symbol}`, by strike, by expiry
- [x] **OI / volume analysis** -- UW `stock/{symbol}/volume-oi-by-expiry`, `option-volume-by-price`
- [x] **Flow per strike** -- UW `stock/{symbol}/flow-per-strike-intraday`
- [x] **Greek flow** -- UW `stock/{symbol}/greek-flow-by-expiry/{expiry}`
- [x] **Spot exposures** -- UW `stock/{symbol}/spot-exposures`, by expiry/strike
- [x] **Insider trades** -- UW insider endpoints
- [x] **Institutional activity** -- UW institutions endpoints
- [x] **Short data** -- UW shorts endpoints (interest, FTDs, volume)
- [x] **Screener** -- UW `screener/options` for hottest chains

### Orion (Real-Time Data Lake)

Signal engine + data lake need:

- [x] **Streaming data** -- WebSocket `stock_bars`, `stock_quotes`, `stock_trades`
- [x] **Historical backfill** -- Alpaca bars/trades with high limits (10,000)
- [x] **Multi-symbol snapshots** -- Alpaca `stocks/snapshots`
- [x] **Options streaming** -- WebSocket `option_bars`, `option_quotes`, `option_trades`
- [x] **News streaming** -- WebSocket `news` feed
- [x] **Crypto data** -- Alpaca crypto endpoints
- [x] **Forex rates** -- Alpaca forex endpoints
- [x] **Market-wide data** -- UW `market/tide`, screeners

### TheOracle / TheOracleMeta (GARP Analysis)

Quantitative GARP analysis needs:

- [x] **Company fundamentals** -- Finnhub profile/financials, AV overview
- [x] **Financial statements** -- AV income-statement, balance-sheet, cash-flow
- [x] **Earnings data** -- Finnhub earnings, AV earnings
- [x] **Analyst estimates** -- Finnhub EPS/revenue/EBIT/EBITDA estimates
- [x] **Price target** -- Finnhub price target
- [x] **Valuation metrics** -- Finnhub `fundamentals/metrics/{symbol}?category=valuation`
- [x] **SEC filings** -- SEC filings, XBRL facts/concepts
- [x] **Historical prices** -- Alpaca or AV daily bars
- [x] **Peer comparison** -- Finnhub `fundamentals/peers/{symbol}`
- [x] **Institutional ownership** -- UW/Finnhub ownership data
- [x] **ESG/sustainability** -- yFinance `{ticker}/sustainability`

### Atlas (Research Loop)

Self-learning research loop needs:

- [x] **Historical bars** -- Alpaca bars (all timeframes for factor generation)
- [x] **Fundamentals** -- Finnhub + AV for factor inputs
- [x] **Economic data** -- AV economic indicators for macro factors
- [x] **SEC XBRL** -- SEC facts/concepts for financial data
- [x] **Earnings** -- Finnhub/UW earnings for event studies
- [x] **Options data** -- UW for options-derived signals (GEX, flow, IV)
- [x] **Sentiment** -- Finnhub/News for NLP-based factors

---

## Appendix: Error Codes

| Code | HTTP Status | Meaning |
|------|-------------|---------|
| GW-E2001 | 401 | Missing or invalid API key |
| GW-E2002 | 401 | Invalid API key |
| GW-E2003 | 401 | Wrong WebSocket auth action |
| GW-E2004 | 401 | WebSocket auth timeout |
| GW-E2005 | 403 | Admin access required |
| GW-E2006 | 403 | Provider access denied |
| GW-E2007 | 403 | WebSocket feed access denied |
| GW-E2008 | 403 | Trading access required |
| GW-E3001 | 400 | Unknown WebSocket action |
| GW-E4001 | 429 | Client rate limit exceeded |
| GW-E4002 | 429 | Provider rate limit exceeded |
| GW-E8001 | 400 | Invalid WebSocket message format |
| GW-E8002 | 400 | Max symbols/subscriptions exceeded |
| GW-E8005 | 400 | WebSocket message exceeds size limit |
