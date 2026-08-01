# Repository AI Instructions

This file is shared by Claude Code and Codex. Follow every instruction here regardless of which agent is active.

## Primary repository guidance

Data-Gateway is the unified REST/WebSocket proxy for the Empire monorepo. It normalizes data from 7 providers (Alpaca, UnusualWhales, Finnhub, Alpha Vantage, yfinance, SEC, News) into canonical schemas and publishes EventEnvelopes to Redis Streams for Heber ingestion. Port 8080. Python 3.12, FastAPI, uv.

Doc precedence: hand-written root docs are canonical; the generated set under `docs/` is a regenerable snapshot. See [docs/README.md](docs/README.md) for the full map.

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
# Code deploys are baked images: `make deploy` builds data-gateway:YYYYMMDD-sha
# and recreates only the gateway container. The working tree is NOT production.
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

Producers must stop **before** the sink is drained/closed; otherwise tail
events from in-flight poller iterations are silently dropped once
`sink_registry.close_all()` disables the registry.

1. Mark as shutting down (health -> 503)
2. Notify connected WebSocket clients
3. Drain period (configurable, default 30s)
4. Stop streaming producers — option capture + multiplexer (15s timeout)
5. Stop polling producers — treasury, quotes, trades, crypto, news, UW pollers + backfill engine + provider registry
6. Close client connections (1001 Going Away)
7. Drain bounded queue + close sink connections (`sink_registry.close_all`)
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

---

## Karpathy Coding Guidelines

_Source: [andrej-karpathy-skills](https://github.com/multica-ai/andrej-karpathy-skills) — behavioral guidelines to reduce common LLM coding mistakes. Bias toward caution over speed; for trivial tasks, use judgment._

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## Data Analysis Review

Any data-analysis conclusion — backtest results, strategy-performance claims, Optuna/WFO output, dataset QA, or other statistical/quantitative findings — must be adversarially reviewed before it reaches the user. Challenge the methodology: overfitting, look-ahead/leakage, cherry-picked windows, confounds, unsupported causal claims. Not a proofread pass.

**Reviewer:** `/codex:adversarial-review` with `gpt-5.6-terra` at high reasoning effort, run synchronously with the claim plus its method/data scope. **Fallback** on rate-limit, timeout, auth error, or empty/errored output: `glm-5.2` via opencode (`opencode run -m zai-coding-plan/glm-5.2`), same instructions. These ids are approved policy — don't substitute them; if one is deprecated or unreachable, stop and ask the user, never swap silently. This reviewer + fallback is the single source of truth for both blocks. **If every reviewer is unavailable, do not present the conclusion — stop and surface it to the user. Never silently skip.**

Report the review's findings verbatim alongside the analysis, with your disposition on each — the user, not you, judges what is "material." Withhold or qualify any conclusion the review invalidates.

## Adversarial Review of Code Changes & Plans

Same reviewer, fallback, and "all reviewers down" rule as Data Analysis Review. Run it synchronously and read the result before continuing (overrides any "spawn then stop" default). Give the reviewer the task/acceptance criteria AND the artifact — a concrete plan or the exact diff; if the tool only sees the diff, paste the requirement in so it reviews intent, not just lines.

**Required** (reviewed once, at the highest-leverage point — the plan for multi-step work, the diff otherwise): changes to logic, control flow, schemas, cross-repo contracts, any edit beyond a truly trivial one, any multi-step plan before executing it, and anything safety-critical — order submission, risk limits, position sizing, paper/live toggles, credentials, kill switches, broker auth/cancel paths (non-exhaustive). Safety-critical always requires it.

**Exempt / don't loop:** comment-, doc-, or format-only edits, renames, single-line non-logic changes. Don't re-review the reviewer's own output — but divergence from a reviewed plan is re-reviewable, and new issues a fix introduces are reviewable. A reviewer you believe is wrong gets escalated to the user, not silently overridden.

**Then act on it:** rank findings by severity and report all of them verbatim with your disposition. Critical/high findings must be fixed or stop the work; never commit a safety-critical change carrying an unresolved finding, and never self-classify a safety-critical finding as immaterial — escalate it. The user judges "material."

## Additional repository guidance

The guidance below was retained from the prior `AGENTS.md`. If it conflicts with the primary guidance above, follow the primary guidance.

This file provides guidance to AI coding agents when working with this repository.

Doc precedence: hand-written root docs are canonical; the generated set under `docs/` is a regenerable snapshot. See [docs/README.md](docs/README.md) for the full map.

## Project Overview

Data Gateway is a unified financial data gateway for the Empire Trading Framework. It provides WebSocket multiplexing and REST proxy caching over multiple financial data providers (Alpaca, Unusual Whales, Finnhub, Alpha Vantage, yfinance, SEC EDGAR, NewsAPI.org, and Massive — the latter loaded but not yet route-mapped) via FastAPI.

## Development Commands

```bash
# Install (uv — local SDK + dev tools)
uv sync --extra local --extra dev     # see warning below
# pip alternative (the `local` extra resolves only via uv — install the sibling packages first):
# pip install ../empire-core ../empire-schemas ../unusualwhales_python_client-5.1 && pip install -e ".[dev]"

# Run locally
uv run uvicorn gateway.main:app --reload --port 8080

# Tests
uv run pytest                         # all tests
uv run pytest tests/test_auth.py      # single file
uv run pytest -k "test_name"          # single test by name
uv run pytest --cov=gateway --cov-report=term-missing  # with coverage

# Linting, formatting, type checking
ruff check .                          # lint
ruff format .                         # format
uv run pyright                        # type check (mypy config exists in pyproject.toml but mypy isn't in the dev extras — run via `uvx mypy .` if needed)
pre-commit run --all-files            # all checks at once

# Key management CLI
uv run python -m gateway.cli generate-key
uv run python -m gateway.cli add-client <id>

# Docker (build from monorepo root — context needs empire-core + vendor SDK)
docker build -f Data-Gateway/Dockerfile -t data-gateway .
docker compose -f Data-Gateway/docker-compose.yml up
# Code deploys are baked images: `make deploy` builds data-gateway:YYYYMMDD-sha
# and recreates only the gateway container. The working tree is NOT production.
```

> **Important:** `uv sync` without `--extra local` uninstalls `unusualwhales-python-client`,
> `empire-core`, and `empire-schemas` (they live in `[project.optional-dependencies].local`).
> Always use `--extra local --extra dev` for local development and CI.

## Architecture

**Layered design:** API routes (`gateway/api/`) -> Core logic (`gateway/core/`) -> Provider implementations (`gateway/providers/`), with shared Pydantic models in `gateway/schemas/`. REST endpoints are mounted under the `/api/v1/<provider>/*` prefix; `/health/*`, `/ws`, `/metrics`, and `/catalog/*` sit at the root, along with legacy aliases `/symbology`, `/corporate-actions`, and `/adjustment-factors`; admin endpoints live at `/api/v1/status` and `/api/v1/admin/*`.

- **Provider Registry** (`gateway/core/registry.py`): Loads providers from `config/providers.yaml`, manages their lifecycle and priority routing. Each provider in `gateway/providers/` extends the `DataProvider` ABC from `gateway/core/provider.py`.
- **Stream Multiplexer** (`gateway/core/stream.py`): Shares one upstream Alpaca WebSocket per stream type across all clients. Handles event deduplication and routing for real-time market data.
- **REST Proxy**: Each provider has a router in `gateway/api/` that proxies REST calls through caching (`gateway/core/cache.py`, `gateway/api/middleware/cache.py:CacheMiddleware`) and rate limiting (`gateway/core/rate_limiter.py`).
- **Schemas** (`gateway/schemas/`): Pydantic models for all normalized data (`NormalizedBar`, `NormalizedQuote`, `NormalizedTrade`, flow, options, etc.). Prices/sizes use `Decimal`; timestamps are timezone-aware.
- **EventEnvelope** (`gateway/core/envelope.py`): Every outbound event is wrapped before publishing. `wrap_event()` is the validated REST/batch path; `fast_wrap_streaming_event()` is the WebSocket fast path. `gateway/core/data_sink.py` publishes envelopes to the Redis Streams topic `heber:events` via a bounded per-sink queue + worker pool.
- **Auth** (`gateway/core/auth.py`): API key auth with per-client permissions defined in `config/clients.yaml`. Key supplied via the `X-Gateway-Key` header.
- **Dependency Injection** (`gateway/api/deps.py`): FastAPI dependencies for registry, cache, auth, connections, sink. Used by all route handlers.
- **App Entry** (`gateway/main.py`): FastAPI app factory, lifespan (startup/shutdown), middleware registration, stream-to-sink dispatch.
- **Config** (`gateway/config.py`): pydantic-settings with the `GATEWAY_` env prefix, reads from `.env`.
- **`vendor/unusualwhales_sdk/`**: Patched copy of the UW SDK used in Docker builds.

## Important Files

- `README.md`: service overview and quickstart.
- `PRD.md`: product and behavior contract for routes and services.
- `docs/system-architecture.md`: deep technical design and data flow.
- `docs/RUNBOOK.md`: on-call and operations procedures.
- `docs/api-reference.md`: endpoint and stream contract reference.
- `config/providers.yaml`: provider registry and capabilities.
- `config/clients.yaml`: API key permissions and limits.
- `gateway/main.py`: app startup, middleware registration, lifespan hooks.
- `gateway/api/middleware/`: caching + envelope middleware package.
- `gateway/core/registry.py`: provider lifecycle management.

## UW Poller

`gateway/core/uw_poller.py` runs a background polling loop that fetches Unusual Whales data and publishes events to a Redis stream (`heber:events`) via `gateway/core/data_sink.py`. Events are wrapped in `EventEnvelope` format (`gateway/core/envelope.py`).

**Four independent pollers** with their own intervals:

- **Flow alerts**: every 5 min, market hours only
- **Darkpool trades**: adaptive interval — 15s during morning rush (9:30–10:30 ET), 30s during market hours, 60s during extended hours; active 4 AM–8 PM ET
- **Market/sector tides**: every 1 hour, market hours only (sector tides run on their own hourly timer, independent of market tide)
- **EOD per-ticker snapshots**: once per trading day after market close (configurable hour/minute, default 16:30 ET), fanned out across the ticker universe × 8 feeds (greek_exposure, iv_rank, iv_term_structure, oi_change, historic_option_volume, short_interest, short_volume, ftds), spawned as a background task

The base loop wakes every 15 seconds and checks which pollers are due. Market-awareness comes from `TradingCalendar` (`gateway/core/calendar.py`). A dedup cache (in-memory with Redis-backed persistence, 2-hour TTL) prevents duplicate events across polls and restarts. The poller is started/stopped in the FastAPI lifespan in `gateway/main.py`.

## Key Patterns

- Python 3.12+, line length 120 (ruff lint + `ruff format`; no black)
- Type checking: mypy + pyright (basic mode), both configured in `pyproject.toml`
- Async throughout (FastAPI + async providers)
- structlog via `empire_core.logger` — import from `gateway/core/logger.py`, never reconfigure structlog directly
- Security: detect-secrets via pre-commit; bandit runs as a CI step (`bandit -c pyproject.toml -r gateway/`)
- Vertical-slice feature changes should include tests first, then implementation.

## Adding a New Provider

1. Implement provider in `gateway/providers/<name>.py` inheriting from `gateway/core/provider.py`
2. Add router in `gateway/api/<name>.py` using deps from `gateway/api/deps.py`
3. Register router in `gateway/main.py`
4. Add config to `config/providers.yaml`

## Adding a New Endpoint

Add/update routes in `gateway/api/<provider>.py`. Use dependency injection from `gateway/api/deps.py`. Cache and rate limit parameters are set per-endpoint.

## Gotchas

- WebSocket clients must authenticate within 10 seconds or get disconnected
- Cache is InMemoryCache by default; HybridCache (L1 memory + L2 Redis) when `GATEWAY_CACHE_REDIS_ENABLED` and `GATEWAY_CACHE_REDIS_URL` are set. In-memory portion lost on restart
- Different providers use different symbol formats — use the symbology API (`/api/v1/symbology/`) for conversion
- bandit skips: B101, B104, B110, B311, B324 (see pyproject.toml for rationale)
- **Error-log severity convention:** upstream provider failures are logged at `WARN` for `4xx` (client-correctable) and `ERROR` (with traceback) for `5xx` and unexpected exceptions. See `gateway/api/deps.py` and `gateway/api/alpaca/common.py` — match this when adding provider error handling so caller errors don't pollute the error stream.
- **`_infer_instrument_type` over-tags options** (`gateway/core/envelope.py`): any payload carrying `strike` or `expiry` is flagged `instrument_type=option`. For per-underlying analytics feeds that include an expiry (e.g. `iv_term_structure`), this produces malformed `option:{symbol}` keys (no OCC suffix) that Heber drops. Pass `instrument_type_override="equity"` and `instrument_key_override=f"equity:{ticker.upper()}"` to `wrap_event()` (see `_poll_eod_iv_term_structure` in `gateway/core/uw_poller.py`).
- **Alpaca trading idempotency:** order writes auto-mint a `c-<client_id>-dg-<uuid>` `client_order_id` (ownership-prefixed) so a `504` (timeout) is safely retryable without double-placing. Writes use a 25s wall-clock timeout vs 15s for reads. Don't weaken this contract (`gateway/api/alpaca/trading.py`).
