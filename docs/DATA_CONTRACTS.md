# Data Contracts

Data-Gateway is a producer in the Empire data pipeline (`docs/README.md` § doc
map, `CLAUDE.md`). This document describes the wire contracts it emits —
the `EventEnvelope` wrapper and the `Normalized*` payload schemas — for
consumers building against them (Heber, Orion, and other downstream
systems). See `DEPENDENCIES.md` for the full consumer list and change-impact
notes; this file covers the schema shapes themselves.

## Overview

Every event Data-Gateway publishes — from REST responses, WebSocket streams,
or background pollers — is wrapped in an `EventEnvelope` before it reaches a
consumer. The envelope is defined once, in `gateway/core/envelope.py`, and
that module is the **only** implementation the live publish path uses.

- **Producer**: Data-Gateway (this repo)
- **Consumers**: Heber (Redis Streams `heber:events` / `heber:events:backfill`
  ingestion), Orion (WebSocket + Redis), and other trading systems that read
  the REST API directly (see `DEPENDENCIES.md`)
- **Format**: JSON (Redis Streams payload) / Pydantic model (REST responses)
- **Frozen contract**: changing the envelope's field set, `compute_event_id`'s
  join order/separator/digest size, or any feed's `FEED_UNIQUE_FIELDS` tuple
  silently changes every `event_id` and breaks Heber's dedup. Treat these as
  frozen; see `DEVELOPER_NOTES.md` § "Frozen Wire Contracts" before touching
  any of them.

## EventEnvelope

Defined in `gateway/core/envelope.py`. All outbound events — REST and
streaming — are wrapped in this model before publishing.

| Field | Type | Description |
|---|---|---|
| `event_id` | `str` | BLAKE2b idempotency hash (32 hex chars) |
| `provider` | `str` | Data provider: `alpaca`, `unusual_whales`, `finnhub`, etc. |
| `feed` | `str` | Feed type: `bars`, `quotes`, `trades`, `flow`, `darkpool`, `news`, etc. |
| `source` | `str` | Delivery method: `websocket` or `rest` |
| `instrument_type` | `str` | Asset class: `equity`, `option`, `crypto`, `forex` |
| `instrument_key` | `str` | Canonical key, e.g. `equity:AAPL`, `option:OCC:AAPL250117C00200000`, `crypto:BTC-USD` |
| `symbol` | `str` | Human-readable symbol |
| `ts_event` | `datetime` | Event time from the provider (timezone-aware) |
| `ts_ingest` | `datetime` | Gateway receive/process time (timezone-aware) |
| `schema_version` | `str` | Envelope schema version, currently `"v1"` (`SCHEMA_VERSION` constant) |
| `lineage` | `dict` | Sequence numbers / stream identifiers |
| `quality_flags` | `list[str]` | e.g. `["validated"]` (REST path) or `["streaming"]` (WS fast path) |
| `payload` | `dict` | The normalized event data (one of the `Normalized*` schemas below, serialized) |

Two code paths build this envelope:

- **`wrap_event()`** — the REST/batch path. Accepts a dict or Pydantic model,
  runs full instrument-key validation, and raises `EnvelopeWrapError` (or the
  original exception, if `GATEWAY_STRICT_ENVELOPES=true`) rather than ever
  emitting a malformed `unknown:*` key — Heber's writer-side validator rejects
  those and the record silently drops on Bronze→Silver.
- **`fast_wrap_streaming_event()`** — the WebSocket fast path. Skips Pydantic
  validation and takes `instrument_type` as a pre-computed argument for speed;
  used on the high-frequency Alpaca stream fanout.

Both paths compute the same kind of content-derived `event_id` — never a
random one — so Heber's three-layer dedup (consumer bloom filter, writer
dict, compactor dedupe) treats retries/reconnects/replays as the same event.

## Instrument Keys

Built by `make_instrument_key()` (`gateway/core/envelope.py`):

| Asset class | Format | Example |
|---|---|---|
| Equity | `equity:{SYMBOL}` | `equity:AAPL` |
| Option | `option:OCC:{OCC_CONTRACT}` | `option:OCC:AAPL250117C00200000` |
| Crypto | `crypto:{BASE}-{QUOTE}` | `crypto:BTC-USD` |
| Forex | `forex:{BASE}-{QUOTE}` | `forex:EUR-USD` |

`_validate_instrument_key()` rejects any key that is empty, starts with
`unknown:`, or — for `instrument_type=option` — doesn't match
`^option:OCC:[A-Z]{1,6}\d{6}[CP]\d{8}$`. A payload that fails this check never
reaches the sink; it raises `EnvelopeWrapError` instead (see `strict_envelopes`
in `gateway/config.py`).

> **Known gotcha**: `_infer_instrument_type()` flags any payload carrying a
> `strike` or `expiry` field as `instrument_type=option`. This is correct for
> options-flow feeds, but wrong for per-underlying analytics that happen to
> include an expiry (e.g. `iv_term_structure`) — it produces a malformed
> `option:{symbol}` key with no OCC suffix, which Heber rejects. Pollers for
> such feeds must pass `instrument_type_override="equity"` and
> `instrument_key_override=f"equity:{ticker.upper()}"` to `wrap_event()`. See
> `_poll_eod_iv_term_structure` in `gateway/core/uw_poller.py` for the
> reference fix, and `CLAUDE.md` / `DEVELOPER_NOTES.md` for the same note.

## Event ID / Idempotency

`compute_event_id()` hashes `provider|feed|instrument_key|ts_event.isoformat()`
plus feed-specific unique fields with BLAKE2b (16-byte digest, 32 hex chars).
The unique fields exist so two genuinely distinct events for the same
instrument at the same timestamp don't collapse to one `event_id` — each
feed's tuple is chosen from a real Heber-dedup incident (documented inline in
`gateway/core/envelope.py`'s `FEED_UNIQUE_FIELDS`). A sample of the mapping:

| Feed | Unique fields (in hash order) |
|---|---|
| `trades` | `trade_id` |
| `bars` | `timeframe`, `timestamp` |
| `quotes` | `bid_price`, `ask_price`, `bid_size`, `ask_size` |
| `flow` / `flow_alerts` | `expiry`, `strike`, `put_call`, `premium`, `volume` |
| `darkpool` | `tracking_id`, `price`, `size`, `notional` |
| `oi_change` | `option_symbol`, `symbol`, `date`, `call_oi_change` |
| `iv_term_structure` | `symbol`, `expiry` |
| `historic_option_volume` | `symbol`, `date`, `expiry` |
| `congress_trades` | `ticker`, `name`, `transaction_date`, `txn_type`, `amounts` |
| `insider_trades` | `ticker`, `owner_name`, `transaction_date`, `id` |
| `income_statement` / `balance_sheet` / `cash_flow` | `ticker`/`symbol`, `fiscal_date_ending`, `report_type` |

The full, current mapping (25+ feeds) lives in `FEED_UNIQUE_FIELDS` in
`gateway/core/envelope.py` — treat that dict, not this table, as the source
of truth; it changes as new feeds are added.

## Redis Streams Topics

| Topic | Purpose | Cap (MAXLEN) | Configurable via |
|---|---|---|---|
| `heber:events` | Live/real-time events (streaming + REST + non-EOD pollers) | `GATEWAY_DATA_SINK_MAX_STREAM_LEN` (default `100000`) | `gateway/core/redis_sink.py` |
| `heber:events:backfill` | Bulk backfill jobs + UW EOD per-ticker snapshots — isolated so a large bulk job can't evict un-consumed live events off the shared stream | `GATEWAY_BACKFILL_STREAM_MAX_LEN` (default `1000000`) | `gateway/core/redis_sink.py`, `gateway/core/backfill.py` (topic name itself overridable via `GATEWAY_BACKFILL_STREAM`) |

## Normalized Payload Schemas

The `payload` field of an `EventEnvelope` is a serialized `Normalized*`
Pydantic model. Canonical schema definitions live in the shared
`empire-schemas` package (`../empire-schemas`, vendored into this repo's CI
at `ci/empire_schemas/` and kept byte-synced — see `DEVELOPER_NOTES.md`).
`gateway/schemas/__init__.py` re-exports them and shadows a subset with
**gateway-strict subclasses** (`gateway/schemas/_strict.py`) that add
Data-Gateway-side invariants not yet enforced upstream:

- Every `datetime` field must be timezone-aware (naive datetimes are rejected).
- Prices and strikes must be `> 0`; volume, open interest, and sizes must be `>= 0`.
- `ResponseMeta.provider` is required (no silent `"alpaca"` default).

`isinstance()` checks against the base `empire-schemas` classes still work
because the strict subclasses inherit from them.

### Market data (`gateway/schemas/market_data.py` → `empire_schemas.market_data`)

| Schema | Key fields |
|---|---|
| `NormalizedBar` | `symbol`, `timestamp`, `open`/`high`/`low`/`close`/`volume` (`Decimal`), `vwap`, `trade_count`, `provider`, `timeframe` |
| `NormalizedQuote` | `symbol`, `timestamp`, `bid_price`/`bid_size`/`ask_price`/`ask_size` (`Decimal`), `bid_exchange`, `ask_exchange`, `conditions`, `tape`, `provider` |
| `NormalizedTrade` | `symbol`, `timestamp`, `price`/`size` (`Decimal`), `trade_id`, `exchange`, `conditions`, `tape`, `taker_side`, `instrument_type`, `provider` |
| `NormalizedForexRate` | `pair`, `timestamp`, `bid`/`ask`/`mid`/`open`/`high`/`low`/`close` (`Decimal`), `provider` |

All prices/sizes use `Decimal`; all timestamps are timezone-aware `datetime`.

### Other categories (see `gateway/schemas/__init__.py` for the authoritative export list)

- **Options**: `NormalizedFlowAlert`, `NormalizedOptionContract`, `NormalizedGreekExposure`, `NormalizedHottestChain`, `NormalizedOptionTrade` (`ci/empire_schemas/empire_schemas/options.py`, `gateway/schemas/options.py`)
- **Alternative data**: `NormalizedDarkpoolTrade`, `NormalizedInsiderTrade`, `NormalizedInstitutionHolding`, `NormalizedPoliticianTrade` (`ci/empire_schemas/empire_schemas/alternative.py`)
- **Fundamentals**: `NormalizedFundamentals`, `NormalizedEarnings`, `NormalizedCorporateAction`, `NormalizedBorrowCost` (`ci/empire_schemas/empire_schemas/fundamentals.py`, `gateway/schemas/fundamentals.py`)
- **Analytics**: `NormalizedNetPremiumTick`, `NormalizedMaxPain`, `NormalizedIVRank`, `NormalizedOIChange`, `NormalizedETFHolding`, `NormalizedETFFlow`, `NormalizedShortData`, `NormalizedFTD`, `NormalizedMarketTide`, `NormalizedSectorTide`, `NormalizedIVTermStructure`, `NormalizedVolatilityStats`, `NormalizedSeasonality`, `NormalizedOrderbook(Level)`, `NormalizedMostActive`, `NormalizedMover` (`ci/empire_schemas/empire_schemas/analytics.py`)
- **News**: `NormalizedNewsArticle`, `NormalizedNewsImage` (`gateway/schemas/news.py`)
- **Trading / account**: `Account`, `Order`, `Position`, `PortfolioHistory`, `Watchlist`, `Clock`, `Calendar`, `Asset`, `Activity` and their `*Response` wrappers (`ci/empire_schemas/empire_schemas/trading.py`) — these are Alpaca trading pass-through models, not events published to Heber
- **WebSocket protocol**: `AuthMessage`, `SubscribeMessage`, `UnsubscribeMessage`, `AuthResult`, `SubscriptionAck` (`gateway/schemas/base.py`)
- **Response envelopes**: `SuccessResponse`, `ErrorResponse`, `ResponseMeta` — the `{"success": ..., "data"/"error": ...}` wrapper used by REST endpoints (distinct from the Redis-stream `EventEnvelope` above)

Example, one specific field-level fix that shipped: `NormalizedOIChange`
gained `option_symbol` (the per-contract OCC key) because
`FEED_UNIQUE_FIELDS["oi_change"]` leads its dedup hash with exactly that
field — omitting it from the schema silently dropped it from the payload and
collapsed every contract sharing `call_oi_change` to one `event_id` at Heber
(see `CHANGELOG.md`, 2026-07 entries). This is the general failure mode to
watch for when adding a field to any `Normalized*` schema that also
participates in `FEED_UNIQUE_FIELDS`.

## Versioning

`EventEnvelope.schema_version` is currently frozen at `"v1"` (`SCHEMA_VERSION`
in `gateway/core/envelope.py`) — there is no v2 in this codebase yet. A
pinned-hash regression test (`tests/test_envelope.py`) catches an accidental
change to the envelope shape or `compute_event_id` on the gateway side.
`empire-schemas`' own envelope module was found dormant/diverged from the
gateway's for months and was re-synced 2026-07-23; per `DEVELOPER_NOTES.md`,
`gateway.core.envelope` is the only implementation the live publish path
uses — keep `empire-schemas` synced to it, never the reverse.

## Validation

- `wrap_event()` raises rather than emitting a malformed or `unknown:*` key.
  With `GATEWAY_STRICT_ENVELOPES=false` (default), the raised
  `EnvelopeWrapError` is caught by callers (REST middleware, pollers), which
  drop the single bad event and keep going. With `GATEWAY_STRICT_ENVELOPES=true`,
  the original exception propagates and REST callers see a `500`.
- Gateway-strict schema subclasses (`gateway/schemas/_strict.py`) reject naive
  datetimes and non-positive prices/strikes at construction time, before an
  event is ever wrapped.

## Consumers & Change Impact

See `DEPENDENCIES.md` for the current consumer list (Heber, 3Roses, Cerberus,
Kairos, Orbit, Orion, EmpireUI, Atlas, Drogon, etc.) and the change-impact
notes for schema/topic/endpoint changes. See `docs/CROSS_REPO_DEPS.md` at the
monorepo root for the ecosystem-wide contract map.
