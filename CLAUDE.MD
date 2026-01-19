# Data Gateway - Claude Context File

## Project Overview

**Data Gateway** is a unified financial data gateway for the Empire Trading Framework. It provides a single WebSocket and REST interface for accessing multiple financial data providers with WebSocket multiplexing, REST proxy caching, client authentication, and provider abstraction.

**Tech Stack:**
- Python 3.12+
- FastAPI (async web framework)
- WebSockets for real-time streaming
- structlog for structured logging
- Redis for caching
- Prometheus for metrics
- Docker & Docker Compose

## Architecture

### Core Components

1. **Provider Registry** (`gateway/core/registry.py`)
   - Manages multiple data provider integrations
   - Dynamic provider loading from configuration
   - Provider lifecycle management (startup/shutdown)

2. **Stream Multiplexer** (`gateway/core/stream.py`)
   - WebSocket connection multiplexing
   - One upstream connection shared across multiple clients
   - Event deduplication and routing
   - Handles real-time market data (bars, quotes, trades)

3. **Authentication** (`gateway/core/auth.py`)
   - API key-based client authentication
   - Permission-based access control
   - Client configuration from `clients.yaml`

4. **Caching** (`gateway/core/cache.py`)
   - Intelligent response caching to reduce API calls
   - TTL-based cache invalidation
   - Middleware-based cache layer

5. **Rate Limiting** (`gateway/core/rate_limiter.py`)
   - Per-client rate limiting
   - Protects upstream providers from abuse

## Project Structure

```
Data-Gateway/
├── gateway/                    # Main application package
│   ├── main.py                # FastAPI app initialization
│   ├── config.py              # Settings and configuration
│   ├── api/                   # API route handlers
│   │   ├── alpaca.py          # Alpaca endpoints
│   │   ├── uw.py              # Unusual Whales endpoints
│   │   ├── finnhub.py         # Finnhub endpoints
│   │   ├── alphavantage.py    # Alpha Vantage endpoints
│   │   ├── yf.py              # yfinance endpoints
│   │   ├── sec.py             # SEC EDGAR endpoints
│   │   ├── news.py            # News endpoints
│   │   ├── websocket.py       # WebSocket handler
│   │   ├── health.py          # Health check endpoints
│   │   ├── metrics.py         # Prometheus metrics
│   │   ├── middleware.py      # Custom middleware
│   │   └── deps.py            # Dependency injection
│   ├── core/                  # Core business logic
│   │   ├── auth.py            # Authentication
│   │   ├── cache.py           # Caching layer
│   │   ├── multiplexer.py     # WebSocket multiplexing
│   │   ├── registry.py        # Provider registry
│   │   ├── stream.py          # Stream management
│   │   ├── rate_limiter.py    # Rate limiting
│   │   ├── symbology.py       # Symbol resolution
│   │   ├── calendar.py        # Trading calendar
│   │   ├── metrics.py         # Metrics collection
│   │   └── provider.py        # Base provider interface
│   └── providers/             # Data provider implementations
│       ├── alpaca.py
│       ├── uw.py
│       ├── finnhub.py
│       ├── alphavantage.py
│       ├── yf.py
│       ├── sec.py
│       └── news.py
├── tests/                     # Test suite
├── unusualwhales_sdk/         # Custom Unusual Whales SDK (submodule)
├── clients.yaml               # Client API keys and permissions
├── providers.yaml             # Provider configuration
├── pyproject.toml             # Project dependencies
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Data Providers

| Provider | Module | Data Types | API Key Required | Status |
|----------|--------|-----------|------------------|--------|
| Alpaca | `gateway/providers/alpaca.py` | Equities, Options, Crypto, Forex | Yes | ✅ Full |
| Unusual Whales | `gateway/providers/uw.py` | Flow, Darkpool, Greeks, Institutions | Yes | ✅ Full (100% coverage) |
| Finnhub | `gateway/providers/finnhub.py` | Fundamentals, Technicals, News, Forex | Yes | ✅ Full |
| Alpha Vantage | `gateway/providers/alphavantage.py` | Time Series, Indicators, Economic | Yes | ✅ Full |
| yfinance | `gateway/providers/yf.py` | Fundamentals, History, Options | No | ✅ Full |
| SEC EDGAR | `gateway/providers/sec.py` | Filings, 13F, Insider Trades | No | ✅ Full |
| News | `gateway/providers/news.py` | News articles (EventRegistry) | Yes | 🚧 Stub |

### Provider API Key Environment Variables

- `APCA_API_KEY_ID` - Alpaca API key
- `APCA_API_SECRET_KEY` - Alpaca secret key
- `UNUSUAL_WHALES_API_KEY` - Unusual Whales API key
- `FINNHUB_API_KEY` - Finnhub API key
- `ALPHAVANTAGE_API_KEY` - Alpha Vantage API key
- `NEWS_API_KEY` - EventRegistry API key (optional)

## API Endpoints

### WebSocket Endpoint
- `ws://localhost:8080/ws` - Real-time streaming connection
  - Authentication: `{"action": "auth", "key": "gw_your_api_key"}`
  - Subscribe: `{"action": "subscribe", "feed": "bars", "symbols": ["AAPL", "TSLA"]}`
  - Unsubscribe: `{"action": "unsubscribe", "feed": "bars", "symbols": ["AAPL"]}`

### REST Endpoints by Provider

| Prefix | Provider | Handler |
|--------|----------|---------|
| `/api/v1/alpaca/*` | Alpaca | `gateway/api/alpaca.py` |
| `/api/v1/uw/*` | Unusual Whales | `gateway/api/uw.py` |
| `/api/v1/finnhub/*` | Finnhub | `gateway/api/finnhub.py` |
| `/api/v1/alphavantage/*` | Alpha Vantage | `gateway/api/alphavantage.py` |
| `/api/v1/yf/*` | yfinance | `gateway/api/yf.py` |
| `/api/v1/sec/*` | SEC EDGAR | `gateway/api/sec.py` |
| `/api/v1/calendar/*` | Trading Calendar | `gateway/api/calendar.py` |
| `/api/v1/symbology/*` | Symbol Resolution | `gateway/api/symbology.py` |
| `/health/*` | Health checks | `gateway/api/health.py` |
| `/metrics` | Prometheus metrics | `gateway/api/metrics.py` |

Full API documentation is available at `/docs` when running in debug mode.

## Configuration

### Environment Variables (`/Users/jacobmcmillan/Empire/Data-Gateway/.env`)

**Core Settings:**
- `GATEWAY_DEBUG` - Enable debug mode (default: `false`)
- `GATEWAY_LOG_LEVEL` - Log level (default: `INFO`)
- `GATEWAY_HOST` - Server host (default: `0.0.0.0`)
- `GATEWAY_PORT` - Server port (default: `8080`)

**Cache Settings:**
- `GATEWAY_CACHE_MAX_SIZE` - Max cache entries (default: `10000`)
- `GATEWAY_CACHE_DEFAULT_TTL` - Default TTL in seconds (default: `300`)

**WebSocket Settings:**
- `GATEWAY_AUTH_TIMEOUT_SECONDS` - Authentication timeout (default: `10`)
- `STREAM_USE_IEX` - Use IEX for streaming (default: `false`)

### Client Configuration (`clients.yaml`)

Defines API keys and permissions for connecting clients:

```yaml
clients:
  - id: my_client
    key: gw_my_api_key
    permissions:
      providers: [alpaca, unusual_whales]
      feeds: [bars, quotes, flow]
      max_symbols: 1000
    enabled: true
```

### Provider Configuration (`providers.yaml`)

Configures available data providers and their settings.

## Development

### Setup

```bash
# Install dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install

# Copy environment file
cp .env.example .env
# Edit .env with your API keys
```

### Running Locally

```bash
# Run with hot-reload
uvicorn gateway.main:app --reload --port 8080

# Or use Docker
docker-compose up --build
```

### Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest --cov=gateway --cov-report=term-missing

# Run specific test file
pytest tests/test_auth.py -v
```

### Code Quality

The project uses:
- **ruff** - Fast Python linter
- **black** - Code formatter (line length: 100)
- **pyright** - Type checker
- **bandit** - Security linter
- **detect-secrets** - Secret detection
- **pre-commit** - Git hooks

```bash
# Run all pre-commit hooks
pre-commit run --all-files

# Format code
black gateway/ tests/

# Lint code
ruff check gateway/ tests/

# Type check
pyright gateway/
```

## Key Files to Know

### Entry Point
- `gateway/main.py:198` - FastAPI app instance and initialization

### Configuration
- `gateway/config.py` - Settings using pydantic-settings
- `.env` - Environment variables (not committed)
- `clients.yaml` - Client authentication
- `providers.yaml` - Provider configuration

### Core Logic
- `gateway/core/stream.py` - WebSocket multiplexing logic
- `gateway/core/registry.py` - Provider registry and lifecycle
- `gateway/core/auth.py` - Authentication and permissions
- `gateway/core/cache.py` - Caching implementation

### Recent Changes (Git Status)
- Modified: `gateway/providers/uw.py` - Unusual Whales provider implementation
- Modified: `unusualwhales_sdk` - Custom SDK submodule
- Untracked: `gateway/api/catalog.py` - New catalog API (not yet committed)

## Common Tasks

### Adding a New Provider

1. Create provider implementation in `gateway/providers/new_provider.py`
2. Inherit from base provider interface in `gateway/core/provider.py`
3. Add router in `gateway/api/new_provider_router.py`
4. Register router in `gateway/main.py`
5. Add configuration to `providers.yaml`
6. Add tests in `tests/providers/test_new_provider.py`

### Adding a New Endpoint

1. Create/update router in `gateway/api/<provider>.py`
2. Use dependency injection from `gateway/api/deps.py` for registry/auth
3. Add caching with `cache_ttl` parameter if needed
4. Add rate limiting with `rate_limit` parameter if needed
5. Add tests in `tests/api/test_<provider>.py`

### Debugging WebSocket Issues

1. Check logs for connection lifecycle events
2. Verify client authentication in `gateway/core/auth.py`
3. Check multiplexer state in `gateway/core/stream.py`
4. Use `/health/stream` endpoint to check stream status
5. Enable debug logging with `GATEWAY_LOG_LEVEL=DEBUG`

### Working with Cache

- Cache middleware in `gateway/api/middleware.py:CacheMiddleware`
- TTL can be set per-endpoint or use default
- Cache key includes request path and query params
- Clear cache by restarting the service or using admin endpoints

## Testing Strategy

- **Unit tests** - Test individual functions and classes
- **Integration tests** - Test provider integrations (may hit real APIs)
- **Smoke tests** - Quick validation of core functionality
- Use pytest fixtures for common test setup
- Mock external API calls when possible

## CI/CD

GitHub Actions runs on PRs and pushes to `main`:
1. Pre-commit hooks (ruff, black, pyright, bandit)
2. pytest with coverage
3. SonarCloud analysis

## Monitoring & Observability

- **Metrics**: Prometheus metrics at `/metrics`
- **Health checks**: `/health/liveness`, `/health/readiness`
- **Structured logging**: JSON logs via structlog
- **Connection tracking**: WebSocket connection manager

## Common Gotchas

1. **WebSocket authentication timeout** - Clients must authenticate within 10 seconds
2. **Provider API keys** - Some providers require keys, check `.env.example`
3. **Cache invalidation** - Cache is in-memory, lost on restart
4. **Rate limits** - Both provider-side and client-side rate limits apply
5. **Symbol formats** - Different providers use different symbol formats (use symbology API)

## Useful Commands

```bash
# View logs
docker-compose logs -f gateway

# Check health
curl http://localhost:8080/health/liveness

# Test WebSocket connection
wscat -c ws://localhost:8080/ws

# View metrics
curl http://localhost:8080/metrics

# Run specific provider tests
pytest tests/providers/test_uw.py -v

# Check git status
git status

# View recent commits
git log --oneline -5
```

## License

MIT License
