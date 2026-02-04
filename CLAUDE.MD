# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data Gateway is a unified financial data gateway for the Empire Trading Framework. It provides WebSocket multiplexing and REST proxy caching over multiple financial data providers (Alpaca, Unusual Whales, Finnhub, Alpha Vantage, yfinance, SEC EDGAR) via FastAPI.

## Commands

```bash
# Install
pip install -e ".[dev]"

# Run locally
uvicorn gateway.main:app --reload --port 8080

# Tests
pytest tests/ -v
pytest tests/test_auth.py -v          # single file
pytest --cov=gateway --cov-report=term-missing  # with coverage

# Linting & formatting
pre-commit run --all-files            # all checks at once
ruff check gateway/ tests/            # lint only
black gateway/ tests/                 # format only
pyright gateway/                      # type check only

# Docker
docker-compose up --build
```

## Architecture

**Three-layer design:** API routes (`gateway/api/`) -> Core logic (`gateway/core/`) -> Provider implementations (`gateway/providers/`).

- **Provider Registry** (`gateway/core/registry.py`): Loads providers from `providers.yaml`, manages their lifecycle. Each provider in `gateway/providers/` implements the base interface from `gateway/core/provider.py`.
- **Stream Multiplexer** (`gateway/core/stream.py`): Shares one upstream WebSocket across all clients. Handles event deduplication and routing for real-time market data.
- **REST Proxy**: Each provider has a router in `gateway/api/` that proxies REST calls through caching (`gateway/core/cache.py`, `gateway/api/middleware.py:CacheMiddleware`) and rate limiting (`gateway/core/rate_limiter.py`).
- **Auth** (`gateway/core/auth.py`): API key auth with per-client permissions defined in `clients.yaml`.
- **Dependency Injection** (`gateway/api/deps.py`): FastAPI dependencies for registry, auth, etc. Used by all route handlers.
- **App Entry** (`gateway/main.py`): FastAPI app initialization and router registration.
- **Config** (`gateway/config.py`): pydantic-settings, reads from `.env`.
- **`unusualwhales_sdk/`**: Git submodule containing a custom SDK, installed separately in Docker.

## UW Poller

`gateway/core/uw_poller.py` runs a background polling loop that fetches Unusual Whales data and publishes events to a Redis stream (`heber:events`) via `gateway/core/data_sink.py`. Events are wrapped in `EventEnvelope` format (`gateway/core/envelope.py`).

**Three independent pollers** with their own intervals:
- **Flow alerts**: every 5 min, market hours only
- **Darkpool trades**: every 1 min, extended hours (4 AM–8 PM ET)
- **Market/sector tides**: every 1 hour, market hours only

The base loop wakes every 60 seconds and checks which pollers are due. Market-awareness comes from `TradingCalendar` (`gateway/core/calendar.py`). An in-memory dedup cache (2-hour TTL) prevents duplicate events. The poller is started/stopped in the FastAPI lifespan in `gateway/main.py`.

## Code Style

- Python 3.12+, line length 100 (black + ruff)
- Type checking: pyright basic mode
- Async throughout (FastAPI + async providers)
- structlog for structured JSON logging
- Security: bandit + detect-secrets via pre-commit

## Adding a New Provider

1. Implement provider in `gateway/providers/<name>.py` inheriting from `gateway/core/provider.py`
2. Add router in `gateway/api/<name>.py` using deps from `gateway/api/deps.py`
3. Register router in `gateway/main.py`
4. Add config to `providers.yaml`

## Adding a New Endpoint

Add/update routes in `gateway/api/<provider>.py`. Use dependency injection from `gateway/api/deps.py`. Cache and rate limit parameters are set per-endpoint.

## Gotchas

- WebSocket clients must authenticate within 10 seconds or get disconnected
- Cache is HybridCache (in-memory + Redis). In-memory portion lost on restart
- Different providers use different symbol formats — use the symbology API (`/api/v1/symbology/`) for conversion
- `unusualwhales_sdk/` is a git submodule — run `git submodule update --init` after clone
- bandit skips: B101, B104, B110, B311, B324 (see pyproject.toml for rationale)
