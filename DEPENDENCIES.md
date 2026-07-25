# Data-Gateway Dependencies

## Exports (What This Repo Provides)

### REST API (port 8080)

- Alpaca stock/options/crypto endpoints
- Alpha Vantage time series and indicators
- Finnhub quotes and company data
- Unusual Whales options flow and darkpool data
- yfinance fundamentals
- SEC EDGAR filings
- News aggregation (NewsAPI.org) via `/api/news`
- Massive historical bars (provider loaded but not yet in routes)

### WebSocket Streams

- Real-time market data via multiplexer
- Subscription-based with reference counting

### Schemas (shared via `empire-schemas`)

Canonical schema definitions live in the shared `empire-schemas` package (`../empire-schemas`), which other repos import directly. `gateway/schemas/__init__.py` re-exports them and adds gateway-strict subclasses (`gateway/schemas/_strict.py`) with extra validation; the wire `EventEnvelope` actually published to Redis is the gateway-local class in `gateway/core/envelope.py`. See [docs/DATA_CONTRACTS.md](docs/DATA_CONTRACTS.md) for the full field-level contract reference.

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

- Publishes EventEnvelope-wrapped events to the Redis Stream topic `heber:events` (frozen contract with Heber)

## Imports (What This Repo Consumes)

- External APIs: Alpaca, UnusualWhales, Finnhub, Alpha Vantage, yfinance, Massive (loaded, not yet routed), SEC EDGAR, NewsAPI.org
- Redis (for stream publishing and hybrid caching)

## Consumers (Who Uses Our Exports)

| Consumer | What They Use |
|----------|---------------|
| Heber | EventEnvelope (Redis streams), NormalizedBar/Quote/Trade; REST API via `heber-watch` client |
| 3Roses | REST API (bars, quotes, snapshots) |
| Cerberus | REST API (bars, quotes, options) |
| Kairos | REST API (UW options flow, IV data) |
| Orbit | REST API (bars, quotes, options flow, flow alerts) |
| WhaleHunter | REST API (UW flow data, darkpool) — no client key provisioned in `config/clients.yaml` |
| Orion | WebSocket streams, REST API |
| EmpireUI | REST API (status, quotes, snapshots) — no client key provisioned in `config/clients.yaml` |
| Atlas | REST API (historical data) |
| Drogon | REST API (bars, quotes, trades) |

Provisioned REST clients are defined in `config/clients.yaml` (cerberus, 3roses, orion, atlas, orbit, kairos, heber-watch, test, drogon).

## Change Impact Notes

- **Schema changes** in the shared `empire-schemas` package (`../empire-schemas`) affect ALL consumers; `gateway/schemas/_strict.py` only adds gateway-side validation
- **EventEnvelope** changes in `gateway/core/envelope.py` break Heber and Orion ingestion
- **API endpoint path changes** break EmpireUI and all trading systems
- **Redis topic name changes** (`heber:events`) break Heber watcher and Orion subscribers
- **Provider interface changes** in `gateway/core/provider.py` affect all provider implementations
