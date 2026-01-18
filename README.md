# Data Gateway

Unified financial data gateway for the Empire Trading Framework.

## Overview

Data Gateway provides a single WebSocket and REST interface for accessing multiple financial data providers with:

- **WebSocket multiplexing** — One upstream connection shared across multiple clients
- **REST proxy with caching** — Reduce API calls with intelligent caching
- **Client authentication** — API key based auth with permissions
- **Provider abstraction** — Plug-and-play data source integration

## Providers

| Provider | Data Types | API Key | Status |
|---|---|---|---|
| **Alpaca** | Equities, Options, Crypto, Forex | Required | ✅ Full |
| **Unusual Whales** | Flow, Darkpool, Greeks, Institutions | Required | ✅ Full |
| **Finnhub** | Fundamentals, Technicals, News, Forex | Required | ✅ Full |
| **Alpha Vantage** | Time Series, Indicators, Economic | Required | ✅ Full |
| **yfinance** | Fundamentals, History, Options | None | ✅ Full |
| **SEC EDGAR** | Filings, 13F, Insider Trades | None | ✅ Full |
| **News** | News articles (EventRegistry) | Required | 🚧 Stub |

## Quick Start

### Prerequisites

- Python 3.12+
- Docker & Docker Compose (optional)

### Development Setup

```bash
# Clone and enter directory
cd Data-Gateway

# Copy environment file and configure API keys
cp .env.example .env

# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Start locally
uvicorn gateway.main:app --reload --port 8080
```

### Docker

```bash
docker-compose up --build
curl http://localhost:8080/health
```

## API Endpoints

| Prefix | Provider | Examples |
|---|---|---|
| `/api/v1/alpaca/*` | Alpaca | stocks, options, crypto, forex |
| `/api/v1/uw/*` | Unusual Whales | flow, darkpool, greeks, institutions |
| `/api/v1/finnhub/*` | Finnhub | fundamentals, technicals, forex |
| `/api/v1/alphavantage/*` | Alpha Vantage | time series, indicators |
| `/api/v1/yf/*` | yfinance | ticker info, history, options |
| `/api/v1/sec/*` | SEC EDGAR | filings, 13F, insiders |
| `/api/v1/calendar/*` | Trading Calendar | market hours, trading days |
| `/api/v1/symbology/*` | Symbol Resolution | OCC ↔ human format |
| `/health/*` | Health | liveness, readiness, status |
| `/ws` | WebSocket | real-time streaming |

Full OpenAPI docs available at `http://localhost:8080/docs` when running.

### WebSocket

```
ws://localhost:8080/ws
```

**Authentication:**

```json
{"action": "auth", "key": "gw_your_api_key"}
```

## Configuration

### Environment Variables

#### Core Settings

| Variable | Description | Default |
|---|---|---|
| `GATEWAY_DEBUG` | Enable debug mode | `false` |
| `GATEWAY_LOG_LEVEL` | Log level | `INFO` |
| `GATEWAY_HOST` | Server host | `0.0.0.0` |
| `GATEWAY_PORT` | Server port | `8080` |
| `GATEWAY_AUTH_TIMEOUT_SECONDS` | WebSocket auth timeout | `10` |

#### Cache Settings

| Variable | Description | Default |
|---|---|---|
| `GATEWAY_CACHE_MAX_SIZE` | Max cache entries | `10000` |
| `GATEWAY_CACHE_DEFAULT_TTL` | Default TTL (seconds) | `300` |

#### Provider API Keys

| Variable | Provider | Required |
|---|---|---|
| `APCA_API_KEY_ID` | Alpaca | Yes |
| `APCA_API_SECRET_KEY` | Alpaca | Yes |
| `UNUSUAL_WHALES_API_KEY` | Unusual Whales | Yes |
| `FINNHUB_API_KEY` | Finnhub | Yes |
| `ALPHAVANTAGE_API_KEY` | Alpha Vantage | Yes |
| `NEWS_API_KEY` | EventRegistry | Optional |

> **Note:** SEC EDGAR and yfinance require no API keys.

### Client Keys

Edit `clients.yaml` to manage API keys:

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

## Development

### Code Quality

```bash
pip install -e ".[dev]"
pre-commit install
pre-commit run --all-files
```

**Tools:** ruff, black, pyright, bandit, detect-secrets

### Testing

```bash
pytest tests/ -v
pytest --cov=gateway --cov-report=term-missing
```

### CI Pipeline

GitHub Actions runs on PRs and pushes to `main`:

1. Pre-commit hooks + pytest with coverage
2. SonarCloud analysis

## License

MIT
