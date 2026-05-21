# CLAUDE.md

Data-Gateway is the unified REST/WebSocket proxy for the Empire monorepo. It normalizes data from 7 providers (Alpaca, UnusualWhales, Finnhub, Alpha Vantage, yfinance, SEC, News) into canonical schemas and publishes EventEnvelopes to Redis Streams for Heber ingestion. Port 8080. Python 3.12, FastAPI, uv.

## Commands

```bash
uv sync --extra local --extra dev    # install all deps (local SDK + dev tools)
uv run pytest                        # run all tests (excludes perf by default)
uv run pytest tests/test_foo.py      # single test file
uv run pytest -k "test_name"         # single test by name
uv run pytest -m perf                # performance/benchmark tests only
ruff check .                         # lint
ruff format .                        # auto-format
mypy .                               # type check
uv run python -m gateway.cli generate-key     # generate API key
uv run python -m gateway.cli add-client <id>  # add client to clients.yaml
uv run python -m gateway.cli list-clients     # list configured clients
uv run uvicorn gateway.main:app --host 0.0.0.0 --port 8080  # start server
```

> **Important**: `uv sync` without `--extra local` will uninstall `unusualwhales-python-client`,
> `empire-core`, and `empire-schemas` (they live in `[project.optional-dependencies].local`).
> Always use `--extra local --extra dev` for local development and CI.

### Docker

```bash
# Build from monorepo root (context needs empire-core + vendor SDK)
docker build -f Data-Gateway/Dockerfile -t data-gateway .
docker compose -f Data-Gateway/docker-compose.yml up
```

## Architecture

### Package Layout

```
gateway/
  main.py              # FastAPI app factory, lifespan (startup/shutdown), stream-to-sink dispatch
  config.py            # Pydantic Settings (GATEWAY_* env prefix)
  cli.py               # Key management CLI (generate-key, add-client, rotate-key, revoke-client)
  schemas/             # Pydantic models for all normalized data types
    market_data.py     # NormalizedBar, NormalizedQuote, NormalizedTrade, StockSnapshot, etc.
    flow.py            # Options flow schemas
    options.py         # Option chain / greeks schemas
    corporate.py       # Splits, dividends
    fundamentals.py    # Fundamental data schemas
    institutional.py   # Institutional holdings
    news.py            # News article schemas
    responses.py       # API response wrappers
    base.py            # WebSocket protocol messages (Auth, Subscribe, Unsubscribe)
  providers/           # Provider implementations (each extends DataProvider ABC)
    alpaca/            # Alpaca (stocks, options, crypto, forex, news, corporate, trading)
    uw/                # UnusualWhales (flow, darkpool, institutional, earnings, market, options)
    finnhub.py         # Finnhub (quotes, fundamentals, news, ETFs, crypto, forex)
    alphavantage.py    # Alpha Vantage (bars, quotes, treasury yields)
    yfinance.py        # yfinance (historical bars)
    sec.py             # SEC EDGAR (filings)
    news.py            # News aggregation
  core/                # Infrastructure layer
    provider.py        # DataProvider ABC + ProviderCapabilities dataclass
    registry.py        # ProviderRegistry (dynamic loading from providers.yaml, priority routing)
    envelope.py        # EventEnvelope model, wrap_event(), fast_wrap_streaming_event(), compute_event_id()
    stream.py          # StreamMultiplexer (Alpaca WebSocket → fanout to clients + sink)
    cache.py           # InMemoryCache (TTLCache) + RedisCache
    redis_sink.py      # RedisStreamsSink (publishes to heber:events stream)
    data_sink.py       # DataSink ABC + DataSinkRegistry (circuit breaker, dedup)
    backfill.py        # BackfillEngine (historical data fetch → sink publish, chunked by date)
    connections.py     # ConnectionManager (WebSocket client tracking)
    auth.py            # ClientAuthenticator (API key validation from clients.yaml)
    circuit_breaker.py # Circuit breaker for sink failures
    rate_limiter.py    # Per-provider rate limiting
    validator.py       # Schema validation for streaming events
    quality.py         # Data quality checks
    dedup.py           # Request deduplication
    shutdown.py        # ShutdownCoordinator (8-step graceful shutdown)
    uw_poller.py       # Background poller for UW flow/darkpool/market_tide (5min interval)
    quotes_poller.py   # Background quote polling
    treasury_poller.py # Treasury yield polling (Alpha Vantage)
    option_capture.py  # Alpaca option chain snapshot capture service
    symbology.py       # Symbol normalization and mapping
    security.py        # Input validation, security headers
    metrics.py         # Prometheus metrics
    http_client.py     # Shared httpx client factory
  api/                 # FastAPI routers
    alpaca/            # /api/alpaca/* (stocks, options, crypto, forex, news, corporate, trading)
    uw/                # /api/uw/* (flow, darkpool, institutional, earnings, market, options)
    finnhub/           # /api/finnhub/* (quotes, fundamentals, news, ETFs, crypto, forex)
    alphavantage/      # /api/alphavantage/* (time series, quotes)
    yf.py              # /api/yf/* (yfinance bars)
    sec.py             # /api/sec/* (SEC filings)
    health.py          # /health (liveness), /health/ready (readiness)
    admin.py           # /api/admin/* (provider status, config, error buffer)
    market.py          # /api/market/* (market-wide data)
    news.py            # /api/news/* (aggregated news)
    websocket.py       # /ws (WebSocket endpoint with auth + heartbeat)
    backfill.py        # /api/backfill/* (historical backfill jobs)
    bulk.py            # /api/bulk/* (batch operations)
    replay.py          # /api/replay/* (event replay)
    calendar.py        # /api/calendar/* (trading calendar)
    symbology.py       # /api/symbology/* (symbol resolution)
    corporate.py       # /api/corporate/* + /api/adjustments/*
    catalog.py         # /api/catalog/* (data catalog)
    quality.py         # /api/quality/* (data quality metrics)
    metrics.py         # /metrics (Prometheus)
    middleware.py      # CORS, RateLimit, GlobalRateLimit, Cache, EventEnvelope, InputValidation, SecurityHeaders, RequestMetrics
    deps.py            # FastAPI dependency injection (registry, cache, auth, connections, sink)
    errors.py          # Structured error handler
  types/
    provider_protocols.py  # Typed Protocol classes for provider subsets
config/
  providers.yaml       # Provider module/class/priority/capabilities
  clients.yaml         # API key definitions per client (cerberus, 3roses, orion, atlas, orbit, test)
  perf_baseline.json   # Performance baselines
  perf_budgets.json    # Performance budgets
  prometheus_alerts.yml
```

### Provider Abstraction

All providers extend `gateway.core.provider.DataProvider` ABC:
- **Required**: `name`, `supported_feeds`, `capabilities`, `initialize()`, `shutdown()`, `health_check()`
- **Optional REST**: `get_bars()`, `get_quotes()`, `get_trades()` — return `NormalizedBar`/`NormalizedQuote`/`NormalizedTrade`
- **Optional Streaming**: `subscribe()`, `unsubscribe()`, `stream()`

Providers are loaded dynamically at startup via `ProviderRegistry.load_from_config()` from `config/providers.yaml`. Each provider has a priority; routes map data types to prioritized provider lists with fallback policies.

### Normalization Schemas

Defined in `gateway/schemas/market_data.py`:
- **NormalizedBar**: symbol, timestamp, OHLCV, vwap, trade_count, provider, timeframe
- **NormalizedQuote**: symbol, timestamp, bid/ask price+size, exchange, conditions, tape, provider
- **NormalizedTrade**: symbol, timestamp, price, size, trade_id, exchange, conditions, tape, taker_side, provider

All use `Decimal` for prices/sizes. Timestamps are timezone-aware `datetime`.

### EventEnvelope Production

Every outbound event is wrapped in an `EventEnvelope` (`gateway/core/envelope.py`) before publishing:
- **Fields**: event_id (BLAKE2b idempotency hash), provider, feed, source, instrument_type, instrument_key, symbol, ts_event, ts_ingest, schema_version, lineage, quality_flags, payload
- **Instrument keys**: `equity:AAPL`, `option:OCC:AAPL250117C00200000`, `crypto:BTC-USD`, `forex:EUR-USD`
- **Two paths**: `wrap_event()` (REST/batch, full validation) and `fast_wrap_streaming_event()` (WebSocket, skips Pydantic, uses random event_id for speed)
- **Feed-specific dedup**: `FEED_UNIQUE_FIELDS` maps each feed type to fields used in event_id hashing

#### Gotcha: `_infer_instrument_type` and per-underlying analytics

`_infer_instrument_type` flags any payload with `strike` or `expiry` fields as
`instrument_type=option`. For options-flow / option-contract data this is
correct, but for **per-underlying analytics that include an expiry**
(`iv_term_structure`, `iv_surface`-style feeds) it produces malformed
`option:{symbol}` keys (no OCC suffix). Heber's writer-side validator rejects
these and 100% of records drop on Bronze→Silver normalization.

When adding a poller for a per-underlying feed that carries expiry fields,
pass `instrument_type_override="equity"` and
`instrument_key_override=f"equity:{ticker.upper()}"` to `wrap_event()`.
`_poll_eod_iv_term_structure` in `gateway/core/uw_poller.py` is the
reference example.

### Redis / Caching / Data Sink

- **In-memory cache**: `InMemoryCache` (cachetools TTLCache) for REST responses. Configurable via `GATEWAY_CACHE_*` env vars.
- **Redis cache**: Optional `RedisCache` for distributed caching (`GATEWAY_CACHE_REDIS_URL`).
- **Data sink**: `RedisStreamsSink` publishes EventEnvelopes to Redis Streams topic `heber:events`. Heber consumers read from this stream.
  - Circuit breaker protects against Redis failures
  - Dedup cache prevents duplicate events (24h TTL)
  - Connection pool (default 8), batch chunking (2000/chunk), retry with backoff
  - Failed events buffered in memory (max 10,000) and drained on reconnect
- **Stream-to-sink dispatch**: Streaming events are published asynchronously with semaphore-controlled concurrency (max 32 inflight, 512 pending tasks). Backpressure drops events when queue is full.

### WebSocket Streaming

`StreamMultiplexer` (`gateway/core/stream.py`) maintains single upstream Alpaca WebSocket connections per stream type (stocks, options, crypto, news) and fans out to subscribed downstream clients.
- Supports IEX or SIP feeds (stocks), OPRA or indicative (options)
- Lazy connection: upstream connects on first client subscription
- Uses msgpack for OPRA options stream, JSON for others
- Events are validated, wrapped in envelopes, and dispatched to clients + Redis sink
- Heartbeat interval: 30s, timeout: 10s, max missed: 3

### Middleware Stack

Applied in this order (first added = outermost):
1. **CORS** — open in debug, locked in production
2. **GlobalRateLimitMiddleware** — IP-based global rate limit
3. **RateLimitMiddleware** — per-client rate limit (600 req/min default)
4. **CacheMiddleware** — response caching (300s default TTL, 512KB max body)
5. **EventEnvelopeMiddleware** — wraps REST responses in EventEnvelope format
6. **InputValidationMiddleware** — request size/limit validation
7. **RequestMetricsMiddleware** — Prometheus timing
8. **SecurityHeadersMiddleware** — security headers

### Authentication

API key auth via `config/clients.yaml`. Each client has: id, key, role (trader/admin/super_admin), permissions (providers, feeds, max_symbols, rate_limit). Key management CLI at `gateway/cli.py`. SIGHUP reloads config.

### Background Services

- **UW Poller**: Polls UnusualWhales every 5min for flow, darkpool, market_tide. EOD
  per-ticker snapshots (configurable hour/minute) cover: greek_exposure, iv_rank,
  iv_term_structure, oi_change, historic_option_volume, short_interest,
  short_volume, ftds.
- **Treasury Poller**: Polls Alpha Vantage for treasury yields (2year, 10year default).
- **Option Capture**: Periodic Alpaca option chain snapshots + optional WebSocket streaming for configured symbols.
- **Backfill Engine**: Long-running historical data jobs. Chunks by date, rate-limits per provider, publishes through DataSinkRegistry.

### Graceful Shutdown (8-step)

1. Mark as shutting down (health -> 503)
2. Notify connected WebSocket clients
3. Drain period (configurable, default 30s)
4. Stop option capture + multiplexer (15s timeout)
5. Close client connections (1001 Going Away)
6. Flush stream-to-sink publish tasks (2s timeout)
7. Stop pollers, backfill engine, provider registry
8. Reset shutdown coordinator

## Configuration

Pydantic Settings with `GATEWAY_` env prefix. Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_PORT` | `8080` | Server port |
| `GATEWAY_DEBUG` | `false` | Debug mode (enables /docs, CORS *) |
| `GATEWAY_RATE_LIMIT_DEFAULT` | `600` | Requests per minute |
| `GATEWAY_CACHE_DEFAULT_TTL` | `300` | Cache TTL seconds |
| `GATEWAY_CACHE_MAX_SIZE` | `10000` | Max cache entries |
| `GATEWAY_DATA_SINK_ENABLED` | `false` | Enable Redis Streams sink |
| `GATEWAY_DATA_SINK_REDIS_URL` | `""` | Redis URL for Heber sink |
| `GATEWAY_STREAM_USE_IEX` | `false` | Use IEX instead of SIP |
| `GATEWAY_STREAM_LAZY_CONNECT` | `true` | Connect to upstream on demand |
| `GATEWAY_OPTION_CAPTURE_ENABLED` | `false` | Enable option chain capture |
| `APCA_API_KEY_ID` | `""` | Alpaca API key (no prefix) |
| `APCA_API_SECRET_KEY` | `""` | Alpaca secret key (no prefix) |
| `UNUSUAL_WHALES_API_KEY` | `""` | UnusualWhales API key |
| `FINNHUB_API_KEY` | `""` | Finnhub API key |
| `ALPHAVANTAGE_API_KEY` | `""` | Alpha Vantage API key |

## Test Markers

| Marker | Description |
|--------|-------------|
| `perf` | Benchmark/performance tests, excluded by default |
| `unit` | Fast, isolated tests with no I/O or network (reserved for future use) |
| `integration` | Tests with real DB, file I/O, or component interactions (reserved for future use) |
| `e2e` | Full system flow tests (reserved for future use) |
| `slow` | Tests exceeding 1s (reserved for future use) |

Tests use `pytest-asyncio` with `asyncio_mode = "auto"`. TestClient from FastAPI with fixtures in `tests/conftest.py`. Test API key loaded from `config/clients.yaml` (client id: `test`).

## Logging

Uses `empire_core.logger` via `setup_logging("data-gateway")`. Structured JSON logs via structlog. Log files in `logs/` with daily rotation (14-day retention).

## Local Dependencies

Linked via `[tool.uv.sources]`:
- `empire-core` — shared logging, error handling (`../empire-core`)
- `empire-schemas` — shared data schemas (`../empire-schemas`)
- `unusualwhales-python-client` — vendored UW SDK v5.1 (`../unusualwhales_python_client-5.1`)

The `vendor/unusualwhales_sdk/` directory contains a patched copy of the UW SDK used in Docker builds.

## Commit & Changelog Discipline

- Commit often with small, atomic changes
- Update `CHANGELOG.md` for every behavior change, bug fix, or feature
- Format: `## [Unreleased]` with entries grouped by `Added`, `Changed`, `Fixed`, `Removed`
- Write entries from the user's perspective
