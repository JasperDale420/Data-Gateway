# Data-Gateway Consumer Audit

> Generated: 2026-03-10
> Scope: All 11 downstream systems in the Empire monorepo

This document catalogs every Data-Gateway endpoint consumed by downstream trading systems,
identifies direct API bypasses, highlights gaps and overlaps, and provides actionable
recommendations for consolidation.

---

## Table of Contents

1. [Data-Gateway Endpoint Inventory](#1-data-gateway-endpoint-inventory)
2. [Per-System Endpoint Usage](#2-per-system-endpoint-usage)
3. [Gap Analysis](#3-gap-analysis)
4. [Direct API Bypass List](#4-direct-api-bypass-list)
5. [Overlap Analysis](#5-overlap-analysis)
6. [Recommendations](#6-recommendations)

---

## 1. Data-Gateway Endpoint Inventory

Data-Gateway exposes endpoints under these router prefixes:

| Prefix | Provider | Route Count | Category |
|--------|----------|-------------|----------|
| `/api/v1/alpaca` | Alpaca | ~55 | Stocks, options, crypto, forex, trading, news, screener, watchlists, metadata |
| `/api/v1/uw` | Unusual Whales | ~85 | Flow, darkpool, options, earnings, market, stock analytics, ETFs, insiders, screener, contracts, intelligence, calendar |
| `/api/v1/finnhub` | Finnhub | ~35 | Fundamentals, earnings, analysis, ETFs, forex, crypto, news, quotes, funds |
| `/api/v1/alphavantage` | Alpha Vantage | ~25 | Time series, fundamentals, indicators, forex, crypto, calendars |
| `/api/v1/yf` | yFinance | ~16 | Ticker info, history, financials, options, holders, recommendations, earnings, news |
| `/api/v1/sec` | SEC EDGAR | ~10 | Company, filings, 13F, insiders, facts, concepts, frames, search |
| `/api/v1/news` | Multi-provider | 3 | Articles, article by ID, sentiment |
| `/api/v1/market` | Multi-provider | 4 | Bars, quotes, trades, news (aggregated) |
| `/api/v1/calendar` | Multi-provider | 5 | Market hours, trading days, earnings, is-open, next-trading-day |
| `/api/v1/corporate-actions` | Multi-provider | 3+2 | Splits, dividends, adjustment factors |
| `/api/v1/symbology` | Internal | 4 | Resolve, validate, batch, convert |
| `/api/v1/backfill` | Internal | 7 | Submit, list, get, delete, cancel, feeds |
| `/api/v1/bulk` | Internal | 8 | Bulk data operations |
| `/api/v1/replay` | Internal | 5 | Historical replay sessions |
| `/health` | Internal | 3 | Health, readiness, status |
| `/catalog` | Internal | 5 | Streams, feeds, providers |
| Admin | Internal | ~15 | Logs, errors, rate limits, providers, cache, circuits, security |

**Total: ~290+ endpoints**

---

## 2. Per-System Endpoint Usage

### 2.1 3Roses (Day Trading)

**Client files:**
- `src/data/gateway_feed.py` (async data feed)
- `src/exec/gateway_broker.py` (async broker)
- `src/watchlist/gateway_client.py` (sync watchlist scanner)

| Endpoint | Method | Parameters | Module |
|----------|--------|------------|--------|
| `/api/v1/alpaca/assets` | GET | `status=active, asset_class=us_equity` | gateway_feed, gateway_client |
| `/api/v1/alpaca/stocks/snapshots` | GET | `symbols=CSV` | gateway_feed, gateway_client |
| `/api/v1/alpaca/stocks/{symbol}/bars` | GET | `timeframe, start, end, limit` | gateway_feed, gateway_client |
| `/api/v1/alpaca/stocks/{symbol}/snapshot` | GET | — | gateway_feed |
| `/api/v1/alpaca/news` | GET | `symbols, start, end, limit` | gateway_feed, gateway_client |
| `/api/v1/alpaca/account` | GET | — | gateway_broker |
| `/api/v1/alpaca/orders` | POST | `symbol, qty, side, order_type, time_in_force, limit_price, stop_price` | gateway_broker |
| `/api/v1/alpaca/orders` | GET | `status=closed/open, limit` | gateway_broker |
| `/api/v1/alpaca/orders/{order_id}` | PATCH | `qty, limit_price, stop_price, trail` | gateway_broker |
| `/api/v1/alpaca/orders/{order_id}` | DELETE | — | gateway_broker |
| `/api/v1/alpaca/orders` | DELETE | — (cancel all) | gateway_broker |
| `/api/v1/alpaca/positions` | GET | — | gateway_client |
| `/api/v1/alpaca/positions` | DELETE | — (flatten all) | gateway_broker |
| `/api/v1/alpaca/positions/{symbol}` | DELETE | — | gateway_broker |
| WebSocket `/ws` | WS | Real-time bar streaming | gateway_feed |

**Provider coverage:** Alpaca only (15 endpoints + WS)
**Missing:** No UW flow, no Finnhub fundamentals, no SEC data, no technical indicators

---

### 2.2 Cerberus (Multi-Strategy Algo Trading)

**Client files:**
- `src/data/api_client.py` (unified gateway client)
- `src/data/gateway_stream.py` (WebSocket streaming)

| Endpoint | Method | Parameters | Module |
|----------|--------|------------|--------|
| `/api/v1/alpaca/stocks/{symbol}/bars` | GET | `timeframe, start, end, limit` | api_client |
| `/api/v1/alpaca/stocks/{symbol}/trades` | GET | `start, end, limit` | api_client |
| `/api/v1/alpaca/orders` | GET | `status, limit` | api_client |
| `/api/v1/alpaca/orders` | POST | `symbol, qty, side, order_type, time_in_force, ...` | api_client |
| `/api/v1/uw/flow/{ticker}` | GET | `limit, date` | api_client |
| `/api/v1/uw/gex/{ticker}` | GET | — | api_client |
| `/api/v1/alpaca/screener/most-actives` | GET | `by=volume/trades` | api_client |
| `/api/v1/alpaca/screener/movers` | GET | `market_type` | api_client |
| WebSocket `/ws` | WS | Real-time streaming | gateway_stream |

**Provider coverage:** Alpaca + UW (9 endpoints + WS)
**Note:** Has vendored `unusualwhales_python_client` in `src/vendors/` but routes UW requests through gateway via `api_client.py`

---

### 2.3 Orion (Real-Time Signal Engine)

**Client files:**
- `src/orion/connectors/uw_flow_connector.py`
- `src/orion/connectors/uw_iv_connector.py`
- `src/orion/connectors/uw_greeks_connector.py`
- `src/orion/connectors/uw_earnings_connector.py`
- `src/orion/connectors/alpaca_options_connector.py` (BYPASS)
- `src/orion/connectors/alpaca_option_greeks_connector.py` (BYPASS)
- `src/orion/unusualwhales/client.py` (vendored, BYPASS)

| Endpoint | Method | Parameters | Module |
|----------|--------|------------|--------|
| `/api/v1/uw/{ticker}/iv-rank` | GET | — | uw_iv_connector |
| `/api/v1/uw/market/tide` | GET | — | uw_flow_connector |
| `/api/v1/uw/{ticker}/max-pain` | GET | — | uw_greeks_connector |
| `/api/v1/uw/{ticker}/spot-exposures` | GET | — | uw_greeks_connector |
| `/api/v1/uw/earnings/premarket` | GET | — | uw_earnings_connector |
| `/api/v1/uw/earnings/afterhours` | GET | — | uw_earnings_connector |
| `/api/v1/uw/earnings/{ticker}` | GET | — | uw_earnings_connector |

**Provider coverage:** UW only via gateway (7 endpoints)
**Direct bypasses:** 2 connectors call Alpaca directly (see Section 4)

---

### 2.4 Atlas (Self-Learning Research Loop)

**Client files:**
- `atlas/adapters/gateway_client.py`
- `atlas/adapters/heber_catalog_client.py` (Heber catalog, not gateway)

| Endpoint | Method | Parameters | Module |
|----------|--------|------------|--------|
| `/api/v1/alpaca/stocks/{symbol}/bars` | GET | `timeframe, start, end, limit` | gateway_client |
| `/api/v1/alpaca/stocks/{symbol}/snapshot` | GET | — | gateway_client |
| `/api/v1/alpaca/options/chain/{underlying}` | GET | `expiration_date, strike_price_gte/lte` | gateway_client |
| `/api/v1/uw/flow/{symbol}` | GET | `limit` | gateway_client |
| `/api/v1/uw/flow/all` | GET | `limit` | gateway_client |
| `/api/v1/uw/darkpool/{symbol}` | GET | `limit` | gateway_client |
| `/api/v1/uw/darkpool/all` | GET | `limit` | gateway_client |
| `/api/v1/calendar/earnings` | GET | `start, end` | gateway_client |
| `/api/v1/backfill` | POST | job submission | gateway_client |
| `/api/v1/backfill` | GET | list jobs | gateway_client |
| `/api/v1/backfill/{job_id}` | DELETE | cancel job | gateway_client |

**Provider coverage:** Alpaca + UW + Calendar + Backfill (11 endpoints)
**Note:** Most comprehensive gateway consumer; also reads from Heber lakehouse directly

---

### 2.5 trading-bot (Crypto/Equity Execution)

**Client files:**
- `src/core/gateway_client.py`

| Endpoint | Method | Parameters | Module |
|----------|--------|------------|--------|
| `/api/v1/providers` | GET | — | gateway_client |
| `/api/v1/alpaca/stocks/{symbol}/bars` | GET | `timeframe, start, end, limit` | gateway_client |
| `/api/v1/alpaca/stocks/{symbol}/quotes` | GET | `start, end, limit` | gateway_client |
| `/api/v1/alpaca/stocks/{symbol}/trades` | GET | `start, end, limit` | gateway_client |
| `/api/v1/alpaca/account` | GET | — | gateway_client |
| `/api/v1/uw/flow/{symbol}` | GET | `limit` | gateway_client |
| WebSocket `/ws` | WS | Real-time streaming | gateway_client |

**Provider coverage:** Alpaca + UW + Catalog (7 endpoints + WS)

---

### 2.6 options-bot (Options Automation)

**Client files:**
- `src/core/gateway_client.py`

| Endpoint | Method | Parameters | Module |
|----------|--------|------------|--------|
| `/api/v1/alpaca/options/chain/{underlying}` | GET | `expiration_date, type` | gateway_client |
| `/api/v1/alpaca/options/quotes` | GET | `symbols` | gateway_client |
| `/api/v1/alpaca/stocks/{symbol}/quotes` | GET | — | gateway_client |
| `/api/v1/alpaca/account` | GET | — | gateway_client |
| `/api/v1/alpaca/orders` | POST | `symbol, qty, side, order_type, ...` | gateway_client |

**Provider coverage:** Alpaca only (5 endpoints)

---

### 2.7 Kairos (LLM Options Swing Trading)

**Client files:** None using gateway

| Endpoint | Method | Parameters | Module |
|----------|--------|------------|--------|
| (none) | — | — | — |

**Provider coverage:** Zero gateway endpoints
**Status:** Uses `alpaca-py` SDK directly for all trading and data operations. Has `ai_gateway_url` but that points to AI-Gateway (LLM proxy), not Data-Gateway.

---

### 2.8 TheOracle (GARP Quantitative Analysis)

**Client files:** None using gateway

| Endpoint | Method | Parameters | Module |
|----------|--------|------------|--------|
| (none) | — | — | — |

**Provider coverage:** Zero gateway endpoints
**Status:** Uses `alpaca-py` SDK and `yfinance` directly via ingestion services

---

### 2.9 TheOracleMeta (Oracle-GARP v3.1 Agentic)

**Client files:** None using gateway

| Endpoint | Method | Parameters | Module |
|----------|--------|------------|--------|
| (none) | — | — | — |

**Provider coverage:** Zero gateway endpoints
**Status:** Uses `yfinance` and `alpaca-py` SDK directly

---

### 2.10 whalehunter (Flow Analysis + Pattern Mining)

**Client files:** None using gateway

| Endpoint | Method | Parameters | Module |
|----------|--------|------------|--------|
| (none) | — | — | — |

**Provider coverage:** Zero gateway endpoints
**Status:** Calls `https://api.unusualwhales.com/api` directly AND uses Alpaca SDK directly (`StockHistoricalDataClient`, `TradingClient`, `OptionHistoricalDataClient`)

---

### 2.11 EmpireUI (React Dashboard)

**Client files:** None using gateway directly

| Endpoint | Method | Parameters | Module |
|----------|--------|------------|--------|
| (none) | — | — | — |

**Provider coverage:** Zero gateway endpoints
**Status:** Has service registry references only; fetches data from individual system APIs, not Data-Gateway

---

## 3. Gap Analysis

Endpoints available in Data-Gateway but **unused by ANY downstream system**.

### 3.1 Alpaca Endpoints (Unused)

| Endpoint | Available Since | Potential Consumers |
|----------|----------------|---------------------|
| `/api/v1/alpaca/stocks/{symbol}/quotes` | Launch | 3Roses (real-time pricing), Cerberus |
| `/api/v1/alpaca/stocks/bars/latest` | Launch | All trading systems |
| `/api/v1/alpaca/stocks/trades/latest` | Launch | 3Roses, Cerberus |
| `/api/v1/alpaca/stocks/quotes` | Launch | All (multi-symbol quotes) |
| `/api/v1/alpaca/stocks/auctions` | Launch | 3Roses (opening auction data) |
| `/api/v1/alpaca/options/{contract}/bars` | Launch | options-bot, Atlas, Kairos |
| `/api/v1/alpaca/options/{contract}/quotes` | Launch | options-bot |
| `/api/v1/alpaca/options/{contract}/quotes/historical` | Launch | Atlas (research) |
| `/api/v1/alpaca/options/trades` | Launch | whalehunter |
| `/api/v1/alpaca/options/trades/latest` | Launch | options-bot, Kairos |
| `/api/v1/alpaca/options/snapshots/{underlying}` | Launch | Orion (currently bypasses) |
| `/api/v1/alpaca/options/chain/{underlying}/snapshot` | Launch | options-bot |
| `/api/v1/alpaca/crypto/*` (8 endpoints) | Launch | trading-bot |
| `/api/v1/alpaca/forex/*` (2 endpoints) | Launch | — |
| `/api/v1/alpaca/portfolio/history` | Launch | 3Roses, Cerberus (P&L tracking) |
| `/api/v1/alpaca/assets/{symbol}` | Launch | — |
| `/api/v1/alpaca/clock` | Launch | All (market hours check) |
| `/api/v1/alpaca/calendar` | Launch | All (trading calendar) |
| `/api/v1/alpaca/account/configurations` | Launch | — |
| `/api/v1/alpaca/account/activities` | Launch | — |
| `/api/v1/alpaca/watchlists/*` (7 endpoints) | Launch | — |
| `/api/v1/alpaca/orders/{order_id}` | GET | 3Roses, Cerberus (order lookup) |
| `/api/v1/alpaca/orders:by_client_order_id` | GET | — |
| `/api/v1/alpaca/positions/{symbol}` | GET | 3Roses, Cerberus |
| `/api/v1/alpaca/meta/conditions` | Launch | — |
| `/api/v1/alpaca/meta/exchanges` | Launch | — |
| `/api/v1/alpaca/logos/{symbol}` | Launch | EmpireUI |
| `/api/v1/alpaca/fixed-income/prices` | Launch | — |
| `/api/v1/alpaca/corporate-actions/{symbol}` | Launch | Atlas, Heber |

### 3.2 Unusual Whales Endpoints (Unused)

| Endpoint | Potential Consumers |
|----------|---------------------|
| `/api/v1/uw/{symbol}/net-premium` | Cerberus, whalehunter |
| `/api/v1/uw/{symbol}/oi-change` | options-bot, Atlas |
| `/api/v1/uw/{symbol}/option-volume` | whalehunter |
| `/api/v1/uw/{symbol}/volume-levels` | 3Roses, Cerberus |
| `/api/v1/uw/{symbol}/nope` | Cerberus (delta-adjusted) |
| `/api/v1/uw/{symbol}/pc-ratio` | All (put/call ratio) |
| `/api/v1/uw/{symbol}/greek-flow-expiry` | Kairos, options-bot |
| `/api/v1/uw/{symbol}/short-interest` | Atlas (research) |
| `/api/v1/uw/{symbol}/ftds` | Atlas, whalehunter |
| `/api/v1/uw/{symbol}/short-volume` | whalehunter |
| `/api/v1/uw/darkpool/{symbol}/levels` | whalehunter |
| `/api/v1/uw/congress/*` (3 endpoints) | Atlas (research) |
| `/api/v1/uw/insider/*` (4 endpoints) | Atlas, whalehunter |
| `/api/v1/uw/institutions/{symbol}` | Atlas |
| `/api/v1/uw/insiders/{symbol}` | whalehunter |
| `/api/v1/uw/screener/stocks` | Cerberus |
| `/api/v1/uw/screener/options` | options-bot |
| `/api/v1/uw/screener/contracts` | whalehunter |
| `/api/v1/uw/screener/analysts` | Atlas |
| `/api/v1/uw/alerts/*` (3 endpoints) | — |
| `/api/v1/uw/etf/*` (9 endpoints) | — |
| `/api/v1/uw/stock/{symbol}/*` (20+ endpoints) | Partial overlap with existing UW endpoints |
| `/api/v1/uw/market/*` (economic, sector, imbalances, etc.) | All trading systems |
| `/api/v1/uw/seasonality/*` (2 endpoints) | Atlas (research) |
| `/api/v1/uw/market/spike` | 3Roses, Cerberus (volatility) |
| `/api/v1/uw/contract/*` (volume profile) | whalehunter |
| `/api/v1/uw/news/headlines` | All |
| `/api/v1/uw/option-contract/*` (4 endpoints) | options-bot, whalehunter |

### 3.3 Finnhub Endpoints (Completely Unused)

**Zero Finnhub endpoints are consumed by any downstream system.** All 35 endpoints are unused:

- Company profile, financials, peers, metrics, executives, ownership
- Earnings, recommendations, estimates (EPS, revenue, EBIT, EBITDA), price targets
- Insider sentiment, upgrade/downgrade, social sentiment, support/resistance, patterns
- ETF profiles, holdings, sector/country weights, index constituents
- Forex rates, exchanges, symbols, candles
- Crypto exchanges, symbols, candles, profiles
- Mutual fund profiles, holdings, sector
- FDA calendar, congress trading, lobbying, USA spending
- News (company, market)
- Quotes, bars

### 3.4 Alpha Vantage Endpoints (Completely Unused)

**Zero Alpha Vantage endpoints are consumed by any downstream system.** All 25 endpoints are unused:

- Time series (quote, intraday, daily, weekly, monthly, search)
- Fundamentals (overview, earnings, income statement, balance sheet, cash flow)
- Technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands, Stochastic, ADX, CCI, ATR, OBV, generic)
- Forex (rate, daily)
- Crypto (rating, daily)
- Calendars (earnings, IPO, listing status)

### 3.5 yFinance Endpoints (Completely Unused)

**Zero yFinance endpoints are consumed via gateway.** All 16 endpoints are unused, despite TheOracle and TheOracleMeta using yfinance directly via the Python library:

- Ticker info, financials, earnings, history, options, recommendations
- Holders, major holders, calendar, dividends, splits, actions, news, sustainability

### 3.6 SEC Endpoints (Completely Unused)

**Zero SEC endpoints are consumed.** All 10 endpoints are unused:

- Company lookup (by CIK, by ticker)
- Filings (by CIK, by form type)
- 13F holdings, insider transactions
- XBRL facts, concepts, frames
- Full-text search

### 3.7 Infrastructure Endpoints (Mostly Unused)

| Category | Endpoints | Used By |
|----------|-----------|---------|
| News aggregation (`/api/v1/news/*`) | 3 | None |
| Market aggregation (`/api/v1/market/*`) | 4 | None |
| Calendar (`/api/v1/calendar/*`) | 5 | Atlas (earnings only) |
| Corporate actions | 5 | None |
| Symbology | 4 | None |
| Backfill | 7 | Atlas (3 of 7) |
| Bulk | 8 | None |
| Replay | 5 | None |

---

## 4. Direct API Bypass List

Systems that call external APIs directly instead of routing through Data-Gateway.

### 4.1 whalehunter (FULL BYPASS)

| External API | Endpoint | File |
|-------------|----------|------|
| `https://api.unusualwhales.com/api` | Flow, darkpool, options analytics | Multiple files in `whalehunter/` |
| Alpaca SDK (`StockHistoricalDataClient`) | Stock bars, trades, snapshots | Via `alpaca-py` SDK |
| Alpaca SDK (`TradingClient`) | Orders, positions | Via `alpaca-py` SDK |
| Alpaca SDK (`OptionHistoricalDataClient`) | Options bars, chains | Via `alpaca-py` SDK |

**Impact:** Duplicate API key management; not benefiting from gateway caching, rate limiting, or circuit breaking. Every UW call whalehunter makes is available through `/api/v1/uw/*`.

### 4.2 Kairos (FULL BYPASS)

| External API | Endpoint | File |
|-------------|----------|------|
| Alpaca SDK (`TradingClient`) | Orders, positions, account | Via `alpaca-py` SDK |
| Alpaca SDK (data client) | Stock data, options | Via `alpaca-py` SDK |

**Impact:** Kairos manages its own Alpaca credentials. All data and trading endpoints are available through `/api/v1/alpaca/*`.

### 4.3 TheOracle (FULL BYPASS)

| External API | Endpoint | File |
|-------------|----------|------|
| Alpaca SDK | Stock data, trading | Via `alpaca-py` SDK |
| yFinance library | Ticker info, history, financials | Via `yfinance` Python library |

**Impact:** yFinance data is available at `/api/v1/yf/*`; Alpaca data at `/api/v1/alpaca/*`.

### 4.4 TheOracleMeta (FULL BYPASS)

| External API | Endpoint | File |
|-------------|----------|------|
| yFinance library | Ticker info, history | Via `yfinance` Python library |
| Alpaca SDK | Stock data | Via `alpaca-py` SDK |

**Impact:** Same as TheOracle.

### 4.5 Orion (PARTIAL BYPASS)

| External API | Endpoint | File |
|-------------|----------|------|
| `https://data.alpaca.markets/v1beta1/options/snapshots` | Options snapshots | `alpaca_options_connector.py` |
| `https://data.alpaca.markets` | Options greeks | `alpaca_option_greeks_connector.py` |
| `https://api.unusualwhales.com` | Vendored UW client | `src/orion/unusualwhales/client.py` |

**Impact:** Options snapshots are available at `/api/v1/alpaca/options/snapshots/{underlying}`. The vendored UW client appears to be legacy/unused alongside the gateway connectors.

### 4.6 Cerberus (MINOR BYPASS)

| External API | Endpoint | File |
|-------------|----------|------|
| Vendored `unusualwhales_python_client` | UW SDK | `src/vendors/unusualwhales_python_client/` |

**Impact:** Low risk. The vendored client exists but `api_client.py` routes UW calls through gateway. Vendored SDK may be dead code.

---

## 5. Overlap Analysis

Same data fetched by multiple systems, creating redundant API calls.

### 5.1 High-Overlap Endpoints

| Endpoint | Systems Using It | Redundancy Risk |
|----------|-----------------|-----------------|
| `/api/v1/alpaca/stocks/{symbol}/bars` | 3Roses, Cerberus, Atlas, trading-bot | **HIGH** - Same bars requested by 4 systems; gateway caching mitigates if running |
| `/api/v1/alpaca/account` | 3Roses, trading-bot, options-bot | MEDIUM - Account state; each system needs fresh data |
| `/api/v1/alpaca/orders` (GET) | 3Roses, Cerberus | MEDIUM - Both read order status |
| `/api/v1/alpaca/orders` (POST) | 3Roses, Cerberus, options-bot | LOW - Different symbols/strategies |
| `/api/v1/alpaca/news` | 3Roses (two clients) | LOW - Same system, different modules |
| `/api/v1/alpaca/assets` | 3Roses (two clients) | LOW - Universe listing, infrequent |
| `/api/v1/alpaca/stocks/snapshots` | 3Roses (two clients) | MEDIUM - Both gateway_feed and gateway_client fetch snapshots |
| `/api/v1/uw/flow/{symbol}` | Cerberus, Atlas, trading-bot | MEDIUM - Three systems fetch same flow data |
| `/api/v1/alpaca/stocks/{symbol}/snapshot` | 3Roses, Atlas | LOW - Different symbols typically |

### 5.2 Cross-System Data Duplication

**Stock price bars** are the most duplicated data type:
- 3Roses fetches 1-min bars for day trading watchlist + live runner
- Cerberus fetches multi-timeframe bars for 10 strategies
- Atlas fetches daily bars for research hypotheses
- trading-bot fetches bars for execution decisions
- TheOracle and whalehunter fetch the SAME bars but bypass the gateway entirely

**Options flow** is fetched by three gateway consumers (Cerberus, Atlas, trading-bot) and one bypass consumer (whalehunter), all hitting the same upstream UW API.

### 5.3 Internal Duplication Within 3Roses

3Roses has three separate gateway client implementations:
1. `src/data/gateway_feed.py` — async, used by live runner
2. `src/exec/gateway_broker.py` — async, used for order execution
3. `src/watchlist/gateway_client.py` — sync, used by premarket scanner

The feed and watchlist clients both call `/api/v1/alpaca/assets`, `/api/v1/alpaca/stocks/snapshots`, `/api/v1/alpaca/stocks/{symbol}/bars`, and `/api/v1/alpaca/news` independently.

---

## 6. Recommendations

### 6.1 Priority 1: Migrate Direct Bypass Systems

| System | Action | Effort | Impact |
|--------|--------|--------|--------|
| **whalehunter** | Replace direct UW API calls with `/api/v1/uw/*` endpoints; replace Alpaca SDK with `/api/v1/alpaca/*` | Medium | Eliminates duplicate API key management; gains caching, rate limiting, circuit breaking |
| **Kairos** | Replace `alpaca-py` SDK usage with gateway client | Medium | Centralizes credential management; consistent error handling |
| **TheOracle** | Replace `yfinance` + Alpaca SDK with gateway calls to `/api/v1/yf/*` and `/api/v1/alpaca/*` | Medium | Consistent data pipeline; enables Heber ingestion |
| **TheOracleMeta** | Same as TheOracle | Low | Shares TheOracle patterns |
| **Orion** | Replace 2 direct Alpaca connectors with `/api/v1/alpaca/options/snapshots/{underlying}` | Low | Already partially on gateway; just 2 connectors to fix |

### 6.2 Priority 2: Adopt Unused High-Value Endpoints

| System | Endpoint to Adopt | Benefit |
|--------|-------------------|---------|
| **3Roses** | `/api/v1/alpaca/clock` | Replace hardcoded market hours checks with live API |
| **3Roses** | `/api/v1/alpaca/portfolio/history` | Add P&L tracking without manual calculation |
| **3Roses** | `/api/v1/uw/flow/{symbol}` | Add options flow as a signal input for gap-and-go |
| **3Roses** | `/api/v1/uw/{symbol}/volume-levels` | Volume profile for VWAP strategy enhancement |
| **Cerberus** | `/api/v1/uw/{symbol}/net-premium` | Net premium tilt for strategy regime detection |
| **Cerberus** | `/api/v1/uw/{symbol}/pc-ratio` | Put/call ratio as sentiment signal |
| **Cerberus** | `/api/v1/finnhub/support-resistance/{symbol}` | Technical levels for strategy boundaries |
| **Atlas** | `/api/v1/sec/filings/{cik}` | SEC filing data for fundamental research |
| **Atlas** | `/api/v1/uw/{symbol}/short-interest` | Short interest as research signal |
| **Atlas** | `/api/v1/finnhub/recommendations/{symbol}` | Analyst consensus for hypothesis generation |
| **Atlas** | `/api/v1/alphavantage/indicator/*` | Pre-computed technical indicators (SMA, RSI, MACD) |
| **options-bot** | `/api/v1/uw/screener/options` | Options screener for opportunity discovery |
| **options-bot** | `/api/v1/uw/{symbol}/greek-flow-expiry` | Greek flow by expiry for position management |
| **options-bot** | `/api/v1/alpaca/options/{contract}/quotes` | Real-time contract quotes |
| **options-bot** | `/api/v1/alpaca/options/chain/{underlying}/snapshot` | Full chain snapshot with greeks |

### 6.3 Priority 3: Reduce Duplication

| Action | Systems Affected | Implementation |
|--------|-----------------|----------------|
| Shared bar cache service | 3Roses, Cerberus, Atlas, trading-bot | Implement a Redis-backed bar cache in Heber that all systems read from; gateway already caches but systems re-request independently |
| Unified flow subscriber | Cerberus, Atlas, trading-bot | Single WebSocket subscriber that publishes flow events to Redis Streams; downstream systems consume from stream |
| Consolidate 3Roses gateway clients | 3Roses | Merge `gateway_feed.py` snapshot/asset logic with `gateway_client.py` into a shared internal module |

### 6.4 Priority 4: Retire Dead Code

| Item | Location | Action |
|------|----------|--------|
| Vendored `unusualwhales_python_client` | `Cerberus/src/vendors/` | Remove if confirmed unused by `api_client.py` |
| Vendored `unusualwhales` client | `Orion/src/orion/unusualwhales/` | Remove after connectors are migrated to gateway |
| Direct Alpaca option connectors | `Orion/src/orion/connectors/alpaca_options_connector.py`, `alpaca_option_greeks_connector.py` | Replace with gateway calls, then delete |

### 6.5 Gateway Enhancement Suggestions

Based on consumer patterns observed:

1. **Multi-symbol bars endpoint** — Systems frequently loop over symbols calling `/stocks/{symbol}/bars` one at a time. A batch endpoint like `/api/v1/alpaca/stocks/bars?symbols=AAPL,TSLA,NVDA` would reduce HTTP overhead.

2. **WebSocket topic filtering** — Both 3Roses and Cerberus connect to `/ws` but only need specific symbols. Ensure topic-based subscription filtering is efficient.

3. **Aggregated market snapshot** — A single call returning bars + snapshot + news for a watchlist would replace the 3 separate calls 3Roses makes per symbol during premarket scanning.

4. **Provider status in catalog** — Expose which providers are currently healthy/enabled so downstream systems can gracefully degrade (the `/catalog/providers` endpoint exists but is unused).

---

## Summary Statistics

| Metric | Count |
|--------|-------|
| Total gateway endpoints available | ~290 |
| Endpoints used by at least one system | ~25 |
| **Endpoint utilization rate** | **~8.6%** |
| Systems fully on gateway | 6 (3Roses, Cerberus, Atlas, trading-bot, options-bot, Orion partial) |
| Systems fully bypassing gateway | 4 (Kairos, TheOracle, TheOracleMeta, whalehunter) |
| Systems not fetching data | 1 (EmpireUI) |
| Providers with zero gateway consumers | 4 (Finnhub, Alpha Vantage, yFinance, SEC) |
| Direct API bypass instances | 11 distinct bypass patterns across 5 systems |
