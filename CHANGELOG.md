# Changelog

All notable changes to this project will be documented in this file.

## [0.5.23] - 2026-02-06

### Added

- **Wave 2 stream/sink perf coverage**: Added `tests/perf/test_perf_stream_sink.py` with dedicated `pytest -m perf` tests for:
  - stream fanout in-flight semaphore bound validation
  - sink publish backpressure/task-growth profiling under blocked sink I/O

### Changed

- **Benchmark deep-dive progress tracking**: Updated `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` to mark Wave 2 stream/sink perf coverage as COMPLETE, include validated perf run results (`4 passed`), and narrow remaining benchmark scope to replay/bulk memory coverage, runtime sink in-flight bounding, and CI perf guardrails.
- **Top-level next-run benchmark scope refinement**: Updated `PERFORMANCE_AUDIT.md` item 14 to reflect that sink/fanout perf coverage is now in place and remaining BENCH work is replay/bulk memory paths plus CI threshold/artifact enforcement.

## [0.5.22] - 2026-02-06

### Added

- **Initial BENCH-1 perf harness**: Added `tests/perf/test_perf_baseline.py` with dedicated `pytest -m perf` baseline tests for envelope serialization and metrics path normalization hot paths.

### Changed

- **Pytest perf marker split**: Updated `pyproject.toml` to register a `perf` marker and exclude perf tests from default runs (`-m 'not perf'`), enabling explicit benchmark execution without slowing functional CI suites.
- **Benchmark baseline stabilization**: Updated failing perf-sensitive tests to match current contracts:
  - cache-header tests now target public health routes in `tests/test_middleware_streaming.py` and `tests/test_optimization.py`
  - replay tests now pass required `client_id` to `ReplaySession` and `ReplaySessionManager.create_session` in `tests/test_replay.py`
- **Benchmark audit progress tracking**: Updated `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark BENCH-1 baseline stabilization as complete and shift future scope to BENCH Wave 2/3 (coverage expansion + CI perf guardrails).

## [0.5.21] - 2026-02-06

### Added

- **Benchmark/profiling readiness deep-dive performance audit**: Added `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` covering CI perf-gating gaps, pytest benchmark-readiness, targeted failing perf-sensitive test slices, and fresh microbench baselines across middleware/stream/sink/replay-adjacent core paths.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to add explicit COMPLETE coverage for benchmark/profiling readiness and shifted next-run scope to BENCH Wave 1 implementation tasks.

## [0.5.20] - 2026-02-06

### Added

- **Core infrastructure deep-dive performance audit**: Added `PERFORMANCE_AUDIT_CORE_INFRA_DEEP_DIVE.md` covering the remaining core infrastructure set (`gateway/core/{adjustments,auth,balancer,circuit_breaker,connections,corporate_actions,data_sink,dedup,metrics,multiplexer,normalizer,rate_limiter,redis_sink,provider}.py`, 3380 LOC) with prioritized low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to add explicit COMPLETE coverage for remaining core infrastructure modules and expanded next-run priorities with implementation-focused CORE-INFRA Wave 1 work.

## [0.5.19] - 2026-02-06

### Added

- **Tests deep-dive performance audit**: Added `PERFORMANCE_AUDIT_TESTS_DEEP_DIVE.md` covering the full `tests/` suite (28 files, 303 tests, 4491 LOC) with measured runtime hotspots from `pytest -q --durations=25` and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `tests/` as COMPLETE and shift remaining next-run scope to implementation waves plus benchmark harness creation.

## [0.5.18] - 2026-02-06

### Added

- **Core modules deep-dive performance audit**: Added `PERFORMANCE_AUDIT_CORE_MODULES_DEEP_DIVE.md` covering `gateway/core/security.py`, `gateway/core/quality.py`, `gateway/core/calendar.py`, `gateway/core/symbology.py`, `gateway/core/validator.py`, plus `scripts/live_provider_smoke.py` and `scripts/generate_provider_contract.py`, with quantified hotspots and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark the sampled core module set and runtime scripts as COMPLETE, and moved next-run priorities to implementation waves, full `tests/` execution-path audit, and benchmark harnessing.

## [0.5.17] - 2026-02-06

### Added

- **News provider deep-dive performance audit**: Added `PERFORMANCE_AUDIT_NEWS_PROVIDER_DEEP_DIVE.md` covering `gateway/providers/news.py` (333 LOC, full provider pass) with quantified hotspots and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/providers/news.py` as COMPLETE and replaced the remaining provider deep-pass item with implementation-focused News provider Wave 1 follow-up.

## [0.5.16] - 2026-02-06

### Added

- **Alpha Vantage provider deep-dive performance audit**: Added `PERFORMANCE_AUDIT_ALPHAVANTAGE_PROVIDER_DEEP_DIVE.md` covering `gateway/providers/alphavantage.py` (1082 LOC, full provider pass) with quantified hotspots and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/providers/alphavantage.py` as COMPLETE and narrowed remaining provider deep-audit scope to `gateway/providers/news.py`.

## [0.5.15] - 2026-02-06

### Added

- **Alpaca provider deep-dive performance audit**: Added `PERFORMANCE_AUDIT_ALPACA_PROVIDER_DEEP_DIVE.md` covering `gateway/providers/alpaca.py` (2153 LOC, full provider pass) with quantified hotspots and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/providers/alpaca.py` as COMPLETE and narrowed remaining provider deep-audit scope to `gateway/providers/alphavantage.py` and `gateway/providers/news.py`.

## [0.5.14] - 2026-02-06

### Added

- **Non-provider router deep-dive performance audit**: Added `PERFORMANCE_AUDIT_NON_PROVIDER_ROUTERS_DEEP_DIVE.md` covering `gateway/api/bulk.py`, `gateway/api/calendar.py`, `gateway/api/corporate.py`, `gateway/api/news.py`, `gateway/api/quality.py`, `gateway/api/replay.py`, `gateway/api/symbology.py`, and `gateway/api/metrics.py` (34 endpoints) with quantified hotspots and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark the non-provider router group as COMPLETE and narrowed future scope toward implementation waves, remaining provider deep passes, and benchmark/profiling validation.

## [0.5.13] - 2026-02-06

### Added

- **Alpaca deep-dive performance audit**: Added `PERFORMANCE_AUDIT_ALPACA_DEEP_DIVE.md` covering all `gateway/api/alpaca/*` modules (14 files, 60 endpoints) with quantified hotspots, low-risk optimization recommendations, implementation waves, and audited-vs-future tracking.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/api/alpaca/*` as COMPLETE and explicitly list the remaining non-provider API modules and partial providers requiring future deep audits.

## [0.5.12] - 2026-02-05

### Added

- **Finnhub + control-plane deep-dive performance audit**: Added `PERFORMANCE_AUDIT_FINNHUB_CONTROL_PLANE_DEEP_DIVE.md` covering `gateway/api/finnhub/*`, `gateway/api/admin.py`, `gateway/api/catalog.py`, `gateway/api/health.py`, and `gateway/providers/finnhub.py` with quantified hotspots and low-risk implementation waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark Finnhub routers/provider and admin/catalog/health routers as COMPLETE, and narrowed pending route-level audit scope to `gateway/api/alpaca/*`.

## [0.5.11] - 2026-02-05

### Added

- **SEC deep-dive performance audit**: Added `PERFORMANCE_AUDIT_SEC_DEEP_DIVE.md` with endpoint/provider hotspot metrics, prioritized low-risk findings, implementation waves, and audited-vs-future tracking.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/api/sec.py` and `gateway/providers/sec.py` as COMPLETE and revised next-run priorities toward implementation waves and remaining sampled router groups.

## [0.5.10] - 2026-02-05

### Added

- **yfinance deep-dive performance audit**: Added `PERFORMANCE_AUDIT_YF_DEEP_DIVE.md` with endpoint/provider hotspot metrics, prioritized low-risk findings, implementation waves, and audited-vs-future tracking.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/api/yf.py` and `gateway/providers/yfinance.py` as COMPLETE and revised next-run priorities to include `gateway/api/sec.py` deep audit.

## [0.5.9] - 2026-02-05

### Added

- **Alpha Vantage deep-dive performance audit**: Added `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md` with route-level hotspot metrics, prioritized low-risk findings, implementation waves, and audited-vs-future file tracking.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/api/alphavantage/*` as COMPLETE and revised next-run priorities to implementation follow-up plus `gateway/api/yf.py` deep audit.

## [0.5.8] - 2026-02-05

### Added

- **UW deep-dive performance audit**: Added `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` with prioritized low-risk findings, evidence anchors, route/provider hotspot metrics, phased implementation plan, and file-level audit coverage.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/providers/uw.py` and `gateway/api/uw/*` as COMPLETE after dedicated deep pass, and replaced next-run UW audit tasks with implementation-focused follow-ups.

## [0.5.7] - 2026-02-05

### Added

- **Repository-wide performance audit and execution backlog**: Added `PERFORMANCE_AUDIT.md` with prioritized low-risk optimization findings, implementation waves, verification plan, and a coverage tracker showing audited modules vs future-run audit targets.

## [0.5.6] - 2026-02-05

### Added

- **Provider alignment audit report**: Added `PROVIDER_ALIGNMENT_AUDIT.md` with route inventory, doc-drift analysis, and error-contract findings for UW/Finnhub/Alpha Vantage/SEC/yfinance.
- **Generated provider contract artifact**: Added `scripts/generate_provider_contract.py` and generated `PROVIDER_ENDPOINT_CONTRACT.md` from live FastAPI routes.

### Fixed

- **Middleware streaming safety (TD-031)**:
  - `EventEnvelopeMiddleware` now skips envelope wrapping for unknown-length, streamed, and oversized responses.
  - `CacheMiddleware` now skips caching for streamed event payloads (`text/event-stream`, `application/x-ndjson`) to avoid body buffering.
  - `main.py` now passes `cache_max_body_bytes` into `EventEnvelopeMiddleware` so both cache and envelope logic use the same body-size guard.
- **Middleware regression coverage**:
  - Added tests validating bypass behavior for streaming/large payloads and preserving envelope wrapping for small JSON payloads.
- **Calendar trading-day loop syntax**: Fixed indentation in `TradingCalendar.get_trading_days()` that prevented module import and blocked test execution.
- **Retired duplicate Alpaca stream handlers (TD-014 follow-up)**: Removed unused legacy modules `gateway/providers/alpaca_stream.py`, `gateway/providers/alpaca_options_stream.py`, `gateway/providers/alpaca_crypto_stream.py`, and `gateway/providers/alpaca_news_stream.py` to keep `gateway/core/stream.py` as the single streaming implementation.
- **Standardized HTTP error contract (TD-033)**: Added global HTTPException normalization so API errors consistently return `success=false` with stable `error.code`/`error.message`.
- **Provider docs and PRD contract alignment (TD-032/TD-034)**: Updated API docs to reference generated route contract and added PRD reference to generated endpoint contract.
- **Contract drift guard in CI**: Added CI step to enforce `python scripts/generate_provider_contract.py --check`.
- **Integration auth fixture drift**: Centralized test API-key fixtures in `tests/conftest.py` and removed hardcoded keys from auth/integration/smoke tests to prevent `401` regressions when client keys change.
- **WebSocket disconnect busy-loop**: Stopped `_message_loop` from spinning on post-disconnect `RuntimeError` by treating disconnect-runtime errors as terminal and exiting cleanly.
- **Pytest asyncio loop-scope pinning**: Set `asyncio_default_fixture_loop_scope = "function"` to remove deprecation warnings and lock predictable async fixture behavior across pytest-asyncio upgrades.
- **Clarification for bundled commit scope**: Added `COMMIT_6077c9f_BREAKDOWN.md` to document and categorize the full set of files that landed in `6077c9f` without rewriting commit history.
- **Pre-commit reliability restoration**: Fixed current `ruff`/`mypy` blockers in bulk/calendar/corporate/replay modules and allowlisted known high-entropy OpenAPI schema field names so `pre-commit run --all-files` passes cleanly again.
- **Release-readiness CI workflow**: Added `.github/workflows/release-readiness.yml` to run `pre-commit` plus the targeted auth/integration/smoke/websocket pytest suite on push/PR to `master`.
- **Live provider smoke tooling**: Added `scripts/live_provider_smoke.py`, `LIVE_PROVIDER_SMOKE_CHECKLIST.md`, and generated `LIVE_PROVIDER_SMOKE_REPORT.md` for repeatable runtime checks against Alpaca/Finnhub/AlphaVantage/UW/SEC.
- **Typed provider registry access (step-down of `Any`)**: Introduced provider Protocol types and replaced `Any` casts in bulk/calendar/corporate API paths for stronger mypy guarantees on registry-loaded providers.
- **Audit release-readiness closure**: Added a release-readiness section to `AUDIT_TECHNICAL_DEBT.md` with static debt completion status, regression test status, live provider smoke outcomes, and release-gate recommendation.

## [0.5.5] - 2026-02-04

### Fixed

- **Mypy error in AlpacaProvider**: Fixed `exercise_options_position` return type handling - SDK method returns `None`, code now correctly ignores void return instead of calling `_model_to_dict` on it
- **Import sorting in uw_poller.py**: Fixed ruff I001 import block formatting
- **Type parameter style in yf.py**: Migrated from `TypeVar("T")` to PEP 695 type parameter syntax (`async def _dedupe[T](...)`)

### Added

- **Redis sink debug logging**: Successful Redis publishes now log at debug level with `redis_sink_published` event containing topic, message_id, and event_id for full traceability
- **Prometheus metrics for data pipeline**:
  - `gateway_envelopes_created_total{provider, feed}` - tracks EventEnvelope creation rate by provider and feed
  - `gateway_sink_publish_total{sink, topic, status}` - tracks data sink publish operations with success/error status
- **Publish deduplication gate**: Added Redis-based deduplication in `DataSinkRegistry.publish_all()` to prevent duplicate events in Heber Bronze layer
  - Checks `dedup:publish:{event_id}` before publishing; skips if already sent
  - 24h TTL on dedup keys; fail-open on cache errors
  - ~1-2ms latency per event for data integrity
- **Expanded FEED_MAPPING**: Added 21 new feed type mappings for UW endpoints
  - Market sentiment: `tide`, `market_tide`, `sector_tide`
  - Alternative data: `etf`, `holdings`, `flows`, `shorts`, `short_interest`, `ftd`, `screener`
  - Political/institutional: `insiders`, `institutions`, `politicians`
  - Analytics: `volatility`, `iv_rank`, `seasonality`, `max_pain`
- **Extended event ID unique fields**: Added feed-specific unique field extraction for etf, shorts, screener, market_tide, insiders, institutions, politicians, analytics feeds
- **Normalized schemas for alternative data**: Added three new schemas for UW alternative data feeds:
  - `NormalizedInsiderTrade` - SEC Form 4 insider trading data
  - `NormalizedInstitutionHolding` - 13F institutional holdings
  - `NormalizedPoliticianTrade` - Congressional trade disclosures
- **Normalized schemas for forex and fundamentals**:
  - `NormalizedForexRate` - Currency pair bid/ask/OHLC data
  - `NormalizedFundamentals` - Company financial metrics (PE, market cap, margins, etc.)

### Refactored

- **envelope.py `_extract_unique_fields`**: Converted from if/elif chain to mapping-based lookup with `FEED_UNIQUE_FIELDS` dict to reduce cognitive complexity

---

## [0.5.4] - 2026-01-29

### Fixed

- **WebSocket Connection Cleanup**: Aggressive connection cleanup on shutdown to prevent "connection limit exceeded" errors on restart
  - `UpstreamConnection.stop()`: Now sends explicit close frame with timeout, forces socket abort if stuck
  - `StreamMultiplexer.stop()`: Concurrent connection closure with 10s timeout for all streams
  - `lifespan`: Multiplexer shutdown now happens FIRST (before drain period) to release Alpaca connection slots immediately
  - Added detailed shutdown logging for debugging connection issues
- **Redis Docker Networking**: Fixed Redis connection errors in Docker by overriding `GATEWAY_CACHE_REDIS_URL`, `GATEWAY_DATA_SINK_REDIS_URL`, and `REDIS_URL` in `docker-compose.yml` to use container hostname (`redis://redis:6379/0`) instead of localhost

---

## [0.5.3] - 2026-01-21

### Added

- **Heber Data Sink Integration**: All Gateway data now publishes to Redis Streams for Heber lakehouse ingestion
  - Added `GATEWAY_DATA_SINK_ENABLED`, `GATEWAY_DATA_SINK_REDIS_URL`, `GATEWAY_DATA_SINK_MAX_STREAM_LEN` config
  - Enabled Redis service in `docker-compose.yml` with health checks
  - WebSocket stream data (bars, quotes, trades, news) publishes to `gateway.stream.*` topics
  - REST API responses publish to `gateway.rest.*` topics via `EventEnvelopeMiddleware`

---

## [0.5.2] - 2026-01-20

### Fixed

- **UW SDK StockEarningsTime enum**: Added missing `POSTMARKET` value to handle `"postmarket"` responses from Unusual Whales API that previously caused `ValueError`
- **UW Provider `_extract_data`**: Fixed `KeyError: 0` when handling single-object responses (e.g., `TickerInfo`, `MarketTide`) by checking `isinstance(data, list)` before iterating
- **SuccessResponse schema**: Fixed `ResponseValidationError` by changing `data` field from `dict` to `dict | list | None` to support paginated list responses and null responses
- **Catalog endpoints**: Removed `response_model=SuccessResponse` from catalog discovery endpoints (`/catalog/*`) which return custom discovery structures
- **IV Rank error message**: Enhanced 404 response to include context about possible causes (market hours, data availability, subscription tier)

### Added

- **Endpoint validation test suite**: Added `test_endpoint_validation.py` with 34 tests covering all API routes to catch schema mismatches early

### Changed

- **UW Provider logging**: Added debug logging to `_get_data_safe()` for empty response handling diagnostics

---

## [0.5.1] - 2026-01-19

### Added

- **API Catalog & Discovery**: Runtime API discovery via `/catalog/` endpoints
  - `GET /catalog/` — API summary and discovery entry point
  - `GET /catalog/streams` — WebSocket stream metadata (stocks, options, crypto, news)
  - `GET /catalog/streams/{id}` — Individual stream details with channels and examples
  - `GET /catalog/feeds` — Gateway feed name mappings (18 feed types)
  - `GET /catalog/providers` — REST API provider catalog (7 providers)
  - `GET /catalog/providers/{id}` — Individual provider endpoint listings
- **Extended WebSocket Feeds**: Added support for additional Alpaca channels
  - Stock: `dailyBars`, `updatedBars`, `lulds`, `statuses`, `imbalances`
  - Crypto: `dailyBars`, `updatedBars`, `orderbooks`
- **Documentation**: Created `API_REFERENCE.md` with comprehensive endpoint reference
- **Security Middleware**: `SecurityHeadersMiddleware` for security headers
- **Global Rate Limiting**: `GlobalRateLimitMiddleware` per PRD 7.5.1-2

### Changed

- **README.md**: Added API Discovery and WebSocket Streaming sections
- **Graceful Shutdown**: Extended shutdown with drain period per PRD 6.5/11.3.4
- **SIGHUP Handler**: Hot config reload support per PRD 6.5.4

---

## [0.5.0] - 2026-01-18

### Added

- **Alpaca Trading API via SDK**: Migrated from httpx to `alpaca-py` SDK
  - `GET /api/v1/alpaca/account` — Account information
  - `POST /api/v1/alpaca/orders` — Create orders (market, limit, stop, stop_limit)
  - `GET /api/v1/alpaca/orders` — List orders with filters
  - `GET /api/v1/alpaca/orders/{id}` — Get specific order
  - `GET /api/v1/alpaca/orders:by_client_order_id` — Get by client ID
  - `PATCH /api/v1/alpaca/orders/{id}` — Replace/modify order (NEW)
  - `DELETE /api/v1/alpaca/orders/{id}` — Cancel order
  - `DELETE /api/v1/alpaca/orders` — Cancel all orders
  - `GET /api/v1/alpaca/positions` — All open positions
  - `GET /api/v1/alpaca/positions/{symbol}` — Position for symbol
  - `DELETE /api/v1/alpaca/positions/{symbol}` — Close position
  - `DELETE /api/v1/alpaca/positions` — Close all positions
  - `GET /api/v1/alpaca/portfolio/history` — Portfolio history
  - `GET /api/v1/alpaca/assets` — Available assets
  - `GET /api/v1/alpaca/assets/{symbol}` — Asset info
  - `GET /api/v1/alpaca/clock` — Market clock
  - `GET /api/v1/alpaca/calendar` — Trading calendar
  - `GET /api/v1/alpaca/account/configurations` — Account config (NEW)
  - `PATCH /api/v1/alpaca/account/configurations` — Update config (NEW)
  - `GET /api/v1/alpaca/account/activities` — Account activities (NEW)
  - `GET /api/v1/alpaca/watchlists` — List watchlists (NEW)
  - `POST /api/v1/alpaca/watchlists` — Create watchlist (NEW)
  - `GET /api/v1/alpaca/watchlists/{id}` — Get watchlist (NEW)
  - `PUT /api/v1/alpaca/watchlists/{id}` — Update watchlist (NEW)
  - `DELETE /api/v1/alpaca/watchlists/{id}` — Delete watchlist (NEW)
  - `POST /api/v1/alpaca/watchlists/{id}/assets` — Add asset (NEW)
  - `DELETE /api/v1/alpaca/watchlists/{id}/assets/{symbol}` — Remove asset (NEW)
- **Market Data API Expansion**:
  - `GET /api/v1/alpaca/stocks/bars/latest` — Latest bars (NEW)
  - `GET /api/v1/alpaca/stocks/trades/latest` — Latest trades (NEW)
  - `GET /api/v1/alpaca/stocks/quotes` — Historical quotes (NEW)
  - `GET /api/v1/alpaca/stocks/snapshots` — Snapshots (NEW)
  - `GET /api/v1/alpaca/stocks/auctions` — Auctions (NEW)
  - `GET /api/v1/alpaca/options/trades` — Options trades (NEW)
  - `GET /api/v1/alpaca/options/trades/latest` — Latest trades (NEW)
  - `GET /api/v1/alpaca/options/snapshots/{underlying}` — Snapshots (NEW)
  - `GET /api/v1/alpaca/crypto/bars/latest` — Latest bars (NEW)
  - `GET /api/v1/alpaca/crypto/trades/latest` — Latest trades (NEW)
  - `GET /api/v1/alpaca/logos/{symbol}` — Company logo (NEW)
  - `GET /api/v1/alpaca/fixed-income/prices` — Fixed income prices (NEW)
- **Pydantic Response Models**: Added 60+ typed response schemas for OpenAPI documentation
  - Stock, Options, Crypto, Forex, News, Screener response types
  - Trading API response types (Account, Order, Position, etc.)
- **Unusual Whales API Full Coverage**: 106 endpoints (100% SDK parity)
  - Phase 1: News headlines, Politician people/trades/portfolios/holders
  - Phase 2: Economic/FDA/Market calendars, Market imbalances/options volume/insider trades/sector stats, Market tide by ETF
  - Phase 3: Institution list/activity/holdings/sectors/ownership/filings, Insider transactions/sector flow/ticker flow/insiders
  - Phase 4: Stock info/candles/state, OI per strike/expiry, Greeks/Greek exposure by strike-expiry, ATM options, Flow per strike intraday, Risk reversal skew, Spot exposures, Options volume, Greek flow by expiry, Sector tickers, Stock insider trades
  - Phase 5: ETF info/inflow-outflow/ticker-exposure/country-weights, Screener analysts, Alerts all/configuration
- **Paper/Live trading support**: Uses `APCA_API_BASE_URL` env var
- **New dependency**: `alpaca-py>=0.28`

---

## [0.4.0] - 2026-01-16

### Added

- **WebSocket Multiplexer**: Full upstream connection management for Alpaca streams
  - `StreamMultiplexer`: Manages all upstream WebSocket connections, routes messages to clients
  - `UpstreamConnection`: Single WebSocket connection with auth, subscribe, heartbeat, reconnection
  - `SubscriptionManager`: Tracks client subscriptions, computes aggregate upstream subscriptions
  - `AlpacaStreamType`: Enum for stocks (SIP/IEX), options, crypto, news streams
- **Dynamic subscribe/unsubscribe**: Clients can add/remove symbols in real-time
- **Subscription aggregation**: Multiple clients share single upstream connection per stream type
- **Reconnection with backoff**: Exponential backoff 1s→16s with ±20% jitter per PRD
- **Stream configuration**: `GATEWAY_STREAM_USE_IEX`, `GATEWAY_STREAM_RECONNECT_*` settings
- **Multiplexer dependency**: `get_multiplexer()`/`set_multiplexer()` for DI

### Changed

- `websocket.py`: Subscribe/unsubscribe handlers now wire to `StreamMultiplexer`
- `main.py`: Initializes `StreamMultiplexer` on startup if Alpaca credentials are set

---

## [0.3.0] - 2026-01-14

### Added

- **UnusualWhalesProvider**: Flow, darkpool, market tide, institutions, congress, insiders
- **UW API** (PRD-aligned `/api/v1/uw/*`):
  - `/uw/flow/all`, `/uw/flow/{symbol}`
  - `/uw/darkpool/all`, `/uw/darkpool/{symbol}`
  - `/uw/institutions/{symbol}`, `/uw/congress/{symbol}`, `/uw/insiders/{symbol}`
  - `/uw/market/tide`
- **Cursor pagination**: `next_cursor`, `has_more`, `total_count` per PRD
- **News API stub**: `/api/v1/news/*` returns 501 (EventRegistry pending)
- **Provider stubs**: AlphaVantageProvider, FinnhubProvider
- **Schemas**: `NormalizedFlowAlert`, `NormalizedDarkpoolTrade`, `NormalizedMarketTide`
- **Phase 1 completion**:
  - Per-client rate limits from `permissions.rate_limit`
  - WebSocket subscription limit via `permissions.ws_subscriptions_max`
  - `MessageRingBuffer` for WebSocket message history per symbol
  - `RequestDeduplicator` to coalesce identical in-flight requests
- **Phase 2 options endpoints**:
  - `GET /api/v1/alpaca/options/chain/{underlying}` - full option chain with greeks
  - `GET /api/v1/alpaca/options/chain/{underlying}/snapshot` - chain snapshot
  - `GET /api/v1/alpaca/options/{contract}/bars` - historical option bars
  - `GET /api/v1/alpaca/options/{contract}/quotes` - latest option quotes
- **NormalizedOptionContract** schema with greeks support
- **WebSocket heartbeat monitoring**: 30s timeout with auto-reconnect
- **YFinanceProvider**: Fundamentals, financials, history, options, recommendations
- **yfinance API** (10 endpoints at `/api/v1/yf/*`):
  - `/yf/ticker/{symbol}` - full ticker info
  - `/yf/ticker/{symbol}/info` - company info
  - `/yf/ticker/{symbol}/financials` - income, balance, cash flow
  - `/yf/ticker/{symbol}/earnings` - quarterly/annual earnings
  - `/yf/ticker/{symbol}/history` - historical OHLCV
  - `/yf/ticker/{symbol}/options` - option expirations
  - `/yf/ticker/{symbol}/options/{exp}` - option chain
  - `/yf/ticker/{symbol}/recommendations` - analyst recs
  - `/yf/ticker/{symbol}/holders` - institutional/insider
  - `/yf/ticker/{symbol}/calendar` - earnings/dividend calendar
- **SECProvider**: Filings, 13F, insider trades via data.sec.gov (free API)
- **SEC API** (7 endpoints at `/api/v1/sec/*`):
  - `/sec/company/{cik}` - company info by CIK
  - `/sec/company/ticker/{ticker}` - CIK lookup by ticker
  - `/sec/filings/{cik}` - all filings
  - `/sec/filings/{cik}/{form_type}` - filings by type
  - `/sec/13f/{cik}` - 13F institutional holdings
  - `/sec/insiders/{cik}` - insider trades (Form 3/4/5)
  - `/sec/facts/{cik}` - XBRL company facts
- **Phase 2 completion - Crypto REST** (4 endpoints at `/api/v1/alpaca/crypto/*`):
  - `GET /alpaca/crypto/{pair}/bars` - historical crypto bars
  - `GET /alpaca/crypto/{pair}/trades` - historical crypto trades
  - `GET /alpaca/crypto/{pair}/quotes` - latest crypto quote
  - `GET /alpaca/crypto/{pair}/snapshot` - current snapshot
- **Phase 2 completion - Forex REST** (2 endpoints at `/api/v1/alpaca/forex/*`):
  - `GET /alpaca/forex/rates` - latest FX rates
  - `GET /alpaca/forex/rates/historical` - historical FX rates
- **Phase 2 WebSocket stream handlers**:
  - `AlpacaOptionsStreamHandler` - options WS with heartbeat monitoring
  - `AlpacaCryptoStreamHandler` - crypto WS with heartbeat monitoring
  - `AlpacaNewsStreamHandler` - news WS with heartbeat monitoring

## [0.2.0] - 2026-01-14

### Added

- **Provider framework**: `DataProvider` base class with capabilities and lifecycle hooks
- **Provider registry**: Dynamic provider loading from `config/providers.yaml`
- **AlpacaProvider**: Full REST API support for bars, quotes, trades
- **AlpacaStreamHandler**: WebSocket streaming with reconnection logic
- **REST API**: Alpaca endpoints at `/api/v1/alpaca/stocks/*` (PRD-aligned)
- **SubscriptionManager**: Reference counting with 30s grace period
- **KeyLoadBalancer**: Round-robin key selection with health tracking
- **RateLimitMiddleware**: `X-RateLimit-*` headers per PRD spec
- **CacheMiddleware**: `X-Gateway-Cache` headers with HIT/MISS tracking
- **REST authentication**: `X-Gateway-Key` header requirement
- **WebSocket heartbeat**: 30s interval, disconnect after 3 missed (PRD-aligned)
- **Message format**: `provider`, `feed`, `error_code` fields (PRD-aligned)
- **Admin endpoints**: `/api/v1/status`, `/admin/logs/recent`, `/admin/errors/summary`
- **CLI tool**: `python -m gateway.cli` for key management (generate, rotate, list)
- **Key hashing**: SHA-256 hashed keys for production (`key_hash` field)
- **Schema fields**: Added `timeframe` to Bar, `trade_id` to Trade
- **Test suite**: 34 tests covering all core components

## [0.1.0] - 2026-01-14

### Added

- **Project scaffolding**: FastAPI application with uvicorn
- **Docker support**: Multi-stage Dockerfile with non-root user
- **Configuration**: pydantic-settings with environment variable loading
- **Client authentication**: YAML-based client keys with permissions
- **In-memory cache**: TTLCache with hit/miss statistics
- **Connection manager**: WebSocket connection tracking
- **Health endpoints**: `/health`, `/health/ready`, `/health/status`
- **WebSocket endpoint**: `/ws` with auth handshake and timeout
- **Structured logging**: structlog with JSON output
- **Test suite**: pytest fixtures and unit tests for core components
