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

Canonical schema definitions live in the shared `empire-schemas` package (`../empire-schemas`), which other repos import directly. `gateway/schemas/__init__.py` re-exports them and adds gateway-strict subclasses (`gateway/schemas/_strict.py`) with extra validation; the wire `EventEnvelope` actually published to Redis is the gateway-local class in `gateway/core/envelope.py`.

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
- Publishes accepted replay envelopes to `heber:events:backfill`. Replay manifests are non-expiring hashes at `gateway:backfill:manifest:<job_id>` and include `backfill_job_id`, `backfill_chunk_id`, and `backfill_manifest_hash` in envelope lineage.
- Reads Heber protocol-v1 readiness from `gateway:backfill:heber:readiness:v1` and exact post-commit chunk acknowledgements from `gateway:backfill:ack:<job_id>:<chunk_id>`. Heber must write acknowledgements only after durable Bronze and Silver commits, with matching `job_id`, `chunk_id`, `manifest_hash`, `record_count`, `event_ids_sha256`, `records_sha256`, a non-empty `commit_id`, timezone-aware `committed_at`, and `status=committed`.

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
- **Replay manifest/readiness/acknowledgement changes** require coordinated Heber consumer and Kairos intake-gate updates; Redis acceptance or stream XACK alone is never proof of a durable Heber write
- **Provider interface changes** in `gateway/core/provider.py` affect all provider implementations
