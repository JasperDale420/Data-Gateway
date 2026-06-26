# AGENTS.md

This file provides guidance to AI coding agents when working with this repository.

## Project Overview

Data Gateway is a unified financial data gateway for the Empire Trading Framework. It provides WebSocket multiplexing and REST proxy caching over multiple financial data providers (Alpaca, Unusual Whales, Finnhub, Alpha Vantage, yfinance, SEC EDGAR, NewsAPI.org) via FastAPI.

## Development Commands

```bash
# Install (uv — local SDK + dev tools)
uv sync --extra local --extra dev     # see warning below
# pip alternative: pip install -e ".[local,dev]"

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
mypy .                                # type check (mypy + pyright both configured)
pre-commit run --all-files            # all checks at once

# Key management CLI
uv run python -m gateway.cli generate-key
uv run python -m gateway.cli add-client <id>

# Docker (build from monorepo root — context needs empire-core + vendor SDK)
docker build -f Data-Gateway/Dockerfile -t data-gateway .
docker compose -f Data-Gateway/docker-compose.yml up
```

> **Important:** `uv sync` without `--extra local` uninstalls `unusualwhales-python-client`,
> `empire-core`, and `empire-schemas` (they live in `[project.optional-dependencies].local`).
> Always use `--extra local --extra dev` for local development and CI.

## Architecture

**Layered design:** API routes (`gateway/api/`) -> Core logic (`gateway/core/`) -> Provider implementations (`gateway/providers/`), with shared Pydantic models in `gateway/schemas/`. REST endpoints are mounted under the `/api/v1/<provider>/*` prefix; `/health/*`, `/ws`, and `/catalog/*` sit at the root.

- **Provider Registry** (`gateway/core/registry.py`): Loads providers from `config/providers.yaml`, manages their lifecycle and priority routing. Each provider in `gateway/providers/` extends the `DataProvider` ABC from `gateway/core/provider.py`.
- **Stream Multiplexer** (`gateway/core/stream.py`): Shares one upstream Alpaca WebSocket per stream type across all clients. Handles event deduplication and routing for real-time market data.
- **REST Proxy**: Each provider has a router in `gateway/api/` that proxies REST calls through caching (`gateway/core/cache.py`, `gateway/api/middleware.py:CacheMiddleware`) and rate limiting (`gateway/core/rate_limiter.py`).
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
- `docs/ARCHITECTURE.md`: deep technical design and data flow.
- `docs/RUNBOOK.md`: on-call and operations procedures.
- `docs/API_REFERENCE.md`: endpoint and stream contract reference.
- `config/providers.yaml`: provider registry and capabilities.
- `config/clients.yaml`: API key permissions and limits.
- `gateway/main.py`: app startup, middleware registration, lifespan hooks.
- `gateway/api/middleware.py`: caching + envelope middleware chain.
- `gateway/core/registry.py`: provider lifecycle management.

## UW Poller

`gateway/core/uw_poller.py` runs a background polling loop that fetches Unusual Whales data and publishes events to a Redis stream (`heber:events`) via `gateway/core/data_sink.py`. Events are wrapped in `EventEnvelope` format (`gateway/core/envelope.py`).

**Three independent pollers** with their own intervals:

- **Flow alerts**: every 5 min, market hours only
- **Darkpool trades**: every 1 min, extended hours (4 AM–8 PM ET)
- **Market/sector tides**: every 1 hour, market hours only

The base loop wakes every 15 seconds and checks which pollers are due. Market-awareness comes from `TradingCalendar` (`gateway/core/calendar.py`). An in-memory dedup cache (2-hour TTL) prevents duplicate events. The poller is started/stopped in the FastAPI lifespan in `gateway/main.py`.

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
- Cache is HybridCache (in-memory + Redis). In-memory portion lost on restart
- Different providers use different symbol formats — use the symbology API (`/api/v1/symbology/`) for conversion
- bandit skips: B101, B104, B110, B311, B324 (see pyproject.toml for rationale)
- **Error-log severity convention:** upstream provider failures are logged at `WARN` for `4xx` (client-correctable) and `ERROR` (with traceback) for `5xx` and unexpected exceptions. See `gateway/api/deps.py` and `gateway/api/alpaca/common.py` — match this when adding provider error handling so caller errors don't pollute the error stream.
- **`_infer_instrument_type` over-tags options** (`gateway/core/envelope.py`): any payload carrying `strike` or `expiry` is flagged `instrument_type=option`. For per-underlying analytics feeds that include an expiry (e.g. `iv_term_structure`), this produces malformed `option:{symbol}` keys (no OCC suffix) that Heber drops. Pass `instrument_type_override="equity"` and `instrument_key_override=f"equity:{ticker.upper()}"` to `wrap_event()` (see `_poll_eod_iv_term_structure` in `gateway/core/uw_poller.py`).
- **Alpaca trading idempotency:** order writes auto-mint a `dg-<uuid>` `client_order_id` so a `504` (timeout) is safely retryable without double-placing. Writes use a 25s wall-clock timeout vs 15s for reads. Don't weaken this contract (`gateway/api/alpaca/trading.py`).
