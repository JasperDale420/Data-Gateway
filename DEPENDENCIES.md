# Data-Gateway Dependencies

## Exports (What This Repo Provides)

### REST API (port 8080)

- Alpaca stock/options/crypto endpoints
- Alpha Vantage time series and indicators
- Finnhub quotes and company data
- Unusual Whales options flow and darkpool data
- yfinance fundamentals
- SEC EDGAR filings

### WebSocket Streams

- Real-time market data via multiplexer
- Subscription-based with reference counting

### Schemas (consumed by other repos)

- `NormalizedBar` — OHLCV bar data
- `NormalizedQuote` — Bid/ask/last quote
- `NormalizedTrade` — Individual trade ticks
- `NormalizedSectorTide` — Sector rotation data
- `NormalizedInsiderTrade` — Insider transaction data
- `NormalizedInstitutionHolding` — 13F holdings
- `NormalizedPoliticianTrade` — Congress trades
- `NormalizedForexRate` — FX rates
- `NormalizedFundamentals` — Company fundamentals
- `EventEnvelope` — Canonical event wrapper for Redis streams
- `SuccessResponse` / `ErrorResponse` — API response wrappers

### Redis Streams

- Publishes EventEnvelope-wrapped events to Redis Streams topics

## Imports (What This Repo Consumes)

- External APIs: Alpaca, UnusualWhales, Finnhub, Alpha Vantage, yfinance, SEC EDGAR
- Redis (for stream publishing and hybrid caching)

## Consumers (Who Uses Our Exports)

| Consumer | What They Use |
|----------|---------------|
| Heber | EventEnvelope (Redis streams), NormalizedBar/Quote/Trade |
| 3Roses | REST API (bars, quotes, snapshots) |
| Cerberus | REST API (bars, quotes, options) |
| Kairos | REST API (UW options flow, IV data) |
| WhaleHunter | REST API (UW flow data, darkpool) |
| Orion | WebSocket streams, REST API |
| Shared-MCP-Server | REST API (all endpoints) |
| EmpireUI | REST API (status, quotes, snapshots) |
| Atlas | REST API (historical data) |
| TheOracle | REST API (fundamentals, quotes) |

## Change Impact Notes

- **Schema changes** in `gateway/schemas/__init__.py` affect ALL consumers
- **EventEnvelope** changes in `gateway/core/envelope.py` break Heber and Orion ingestion
- **API endpoint path changes** break EmpireUI, Shared-MCP-Server, and all trading systems
- **Redis topic name changes** break Heber watcher and Orion subscribers
- **Provider interface changes** in `gateway/core/provider.py` affect all provider implementations
