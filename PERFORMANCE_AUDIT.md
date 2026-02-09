# Performance Audit - Data Gateway

Date: 2026-02-05
Auditor: Codex (GPT-5)
Scope: Repository-wide performance audit focused on low-risk improvements without significant logic changes.

## Goals and Constraints

- Improve throughput, latency, and memory behavior.
- Avoid architectural rewrites and major behavior changes.
- Prioritize changes that are testable and incremental.

## Audit Method

1. Mapped repository structure and module sizes.
2. Reviewed hot paths first: HTTP middleware, WebSocket fanout, provider calls, cache, bulk/replay.
3. Reviewed selected API router groups and largest providers for repeated work and serialization overhead.
4. Produced prioritized backlog with risk/effort guidance.

## Executive Summary

Top performance bottlenecks are concentrated in three areas:

1. Remaining JSON parse/dump overhead in HTTP envelope path (body re-buffering across cache/envelope has been reduced).
2. Remaining stream-path tuning for fanout/sink guardrail parameters after fanout task burst and callback-sink coupling remediations.
3. Memory-heavy job/replay implementations that materialize full datasets in memory.

The fastest, lowest-risk wins are:

- Continue reducing middleware overhead in `EventEnvelopeMiddleware` (body re-buffering reuse between cache/envelope is implemented).
- Tune stream path limits further (sink publish decoupling and fanout batching are implemented).
- Parallelize selected provider fan-out methods and provider health checks.
- Replace remaining high-overhead parsing loops (`iterrows`, repeated full-cache scans, full-sort-then-slice paths).

## Prioritized Findings and Recommendations

### P0 (Do First)

1. Middleware cache/envelope duplicate body buffering (substantially remediated on 2026-02-07).

- Evidence:
  - Cache middleware now stores buffered bytes on request state for HIT/MISS paths: `gateway/api/middleware.py:335`, `gateway/api/middleware.py:368`.
  - Envelope middleware now short-circuits responses already marked wrapped (`X-Gateway-Envelope: true`) before body parse/dump work: `gateway/api/middleware.py:616`.
  - Envelope middleware now reuses `response.body` bytes when present before iterating `body_iterator`: `gateway/api/middleware.py:735`.
  - Wrapped response serialization now uses compact separators to reduce JSON encoding overhead and response size churn: `gateway/api/middleware.py:681`.
- Impact: Duplicate response-body assembly across cache+envelope path is reduced further; cache-hit wrapped responses now avoid parse/dump entirely, and standard JSON responses avoid iterator re-buffering when body bytes are already available.
- Remaining low-risk follow-up:
  - Optional: evaluate faster JSON codec (`orjson`/equivalent) behind compatibility guardrails for additional parse/dump CPU reduction.

1. Stream path backpressure coupling: sink publish blocked client message path (remediated 2026-02-07).

- Evidence:
  - Sink publish now schedules off callback path via `_schedule_stream_sink_publish(...)`: `gateway/main.py`.
  - Bounded scheduling guardrails, runtime limit configuration, and shutdown draining are in place: `gateway/main.py` (`_configure_stream_sink_dispatch_limits`, `_drain_stream_sink_publish_tasks`).
  - Stream-to-sink scheduler telemetry is now emitted for calibration: `gateway/core/metrics.py` (`gateway_stream_sink_dispatch_events_total`, `gateway_stream_sink_pending_tasks`, `gateway_stream_sink_dispatch_limit`) and hooked into scheduler lifecycle in `gateway/main.py`.
  - Admin status now exposes stream scheduler telemetry snapshot via `/api/v1/status` (`stream_sink_dispatch`) for operator visibility during tuning.
  - Stream sink telemetry snapshots now include derived calibration signals (`pending_utilization`, `completion_rate`, `drop_rate`) via `gateway/core/metrics.py`.
- Impact: Stream callback no longer waits on sink publish/dedup I/O, reducing callback latency coupling under sink backpressure.
- Remaining low-risk follow-up:
  - Calibrate `data_sink_stream_publish_max_inflight` and `data_sink_stream_publish_max_pending` using the new dispatch telemetry under production-like fanout load.

1. Stream fanout per-message task burst (remediated 2026-02-07).

- Evidence:
  - Fanout now runs in bounded client batches via `_iter_client_batches(...)` before each `gather(...)`: `gateway/core/stream.py`.
  - In-flight semaphore and batch limits are now runtime-configurable through `stream_fanout_max_inflight` and `stream_fanout_batch_size`.
  - Fanout telemetry is now emitted for calibration: `gateway/core/metrics.py` (`gateway_stream_fanout_events_total`, `gateway_stream_fanout_batch_size`, `gateway_stream_fanout_limit`) and hooked into fanout lifecycle in `gateway/core/stream.py`.
  - Admin status now exposes stream fanout telemetry snapshot via `/api/v1/status` (`stream_fanout`) for operator visibility during tuning.
  - Stream fanout telemetry snapshots now include derived calibration signals (`avg_batch_size`, `batch_fill_ratio`, `error_rate`) via `gateway/core/metrics.py`.
- Impact: Reduces single-message task allocation burst and smooths event-loop pressure at high fanout while preserving delivery semantics. `_iter_client_batches(...)` now yields lazily without precomputing full batch lists, reducing per-message allocation overhead for large subscriber sets. Stream market-data validation now also reuses a cached validator instance (`_get_stream_validator(...)`) and a static message-type map in `gateway/core/stream.py`, removing repeated hot-path resolver/map allocations per validated message. News-symbol fanout lookup now deduplicates repeated symbols before subscription index reads, trimming redundant lookup work on duplicate-symbol payloads.
- Remaining low-risk follow-up:
  - Calibrate `stream_fanout_max_inflight` and `stream_fanout_batch_size` with telemetry under production-like loads.

### P1 (High Value, Low/Medium Risk)

1. Sequential provider health checks on admin/status endpoints (remediated 2026-02-08).

- Evidence:
  - `gateway/core/registry.py` now executes checks through `asyncio.gather(...)` in `health_check_all(...)`.
  - Concurrency + exception behavior is covered in `/Users/jacobmcmillan/Empire/Data-Gateway/tests/test_registry.py`.
- Impact: Admin/status provider health checks are no longer strictly linear in cumulative provider latency.
- Additional low-risk optimization (2026-02-09):
  - `/api/v1/status` now supports `include_provider_health=false` in `gateway/api/admin.py`, allowing low-latency status polling without running upstream provider probes on every request.
  - Regression coverage added in `/Users/jacobmcmillan/Empire/Data-Gateway/tests/test_admin_status.py`.
- Additional combined optimization batch (2026-02-09):
  - `/api/v1/status` provider health checks now use a short-lived in-memory TTL cache (`5s`) for repeated status polling with `include_provider_health=true`.
  - `/api/v1/status` now supports `force_provider_health_refresh=true` to bypass the TTL cache for one request.
  - Extended regression coverage in `/Users/jacobmcmillan/Empire/Data-Gateway/tests/test_admin_status.py` validates cache reuse and force-refresh behavior.
- Additional combined optimization batch (2026-02-09):
  - `/api/v1/status` now returns `provider_health_cache` metadata (`source`, `ttl_seconds`, `age_seconds`) so operators can confirm whether health status came from live probe, cache reuse, or skip path.
  - `/api/v1/status` now supports `provider_health_cache_ttl_seconds` for per-request cache TTL tuning during calibration.
  - Extended regression coverage in `/Users/jacobmcmillan/Empire/Data-Gateway/tests/test_admin_status.py` validates metadata and TTL override behavior.
- Follow-up:
  - Optional: expose per-provider health-check latency histograms in admin views for calibration.

1. Provider multi-quote fan-out serialization (remediated 2026-02-07).

- Evidence:
  - `gateway/providers/alphavantage.py` uses bounded semaphore concurrency for `get_quotes(...)`.
  - `gateway/providers/finnhub.py` now uses bounded semaphore concurrency for `get_quotes(...)` with fail-soft per-symbol behavior.
- Impact: Multi-symbol quote fetch latency is no longer strictly linear in symbol count for these providers while retaining provider-limit safety controls.
- Follow-up:
  - Optional: add provider-specific latency histograms by requested symbol count to tune default concurrency.

1. Bulk job result materialization creates high peak memory (partially remediated 2026-02-07).

- Evidence:
  - `gateway/core/bulk.py` now spills large result sets to temp JSONL storage when in-memory results exceed configured threshold (`bulk_results_max_in_memory`).
  - JSONL downloads were previously materialized as one large string (`lines = [json.dumps(...)]` then join).
- Impact: Large jobs can hit high RSS and GC churn; download-time amplification for JSONL has been reduced.
- Remediation completed:
  - `gateway/core/bulk.py` now provides bounded `iter_results_jsonl_chunks(...)` and no longer builds a full list-of-lines for JSONL formatting.
  - `gateway/api/bulk.py` now serves JSONL downloads via `StreamingResponse` backed by chunk iteration.
  - `gateway/core/bulk.py` now supports transparent result spool-to-disk fallback for oversized jobs and uses spool-aware result iteration for JSONL streaming and result streams.
- Remaining low-risk follow-up:
  - Add optional paged backend storage for multi-process/distributed workers.

1. Replay preload/sort overhead (substantially remediated 2026-02-07).

- Evidence:
  - `gateway/core/replay.py` now accepts async/sync iterable loader outputs and streams replay messages without requiring full list materialization.
  - List-backed loaders now sort only when timestamps are out of order.
  - Large list-backed loader outputs now spool to temp JSONL when above configurable in-memory threshold (`replay_messages_max_in_memory`) and stream from disk with cleanup.
- Impact: Reduces replay startup/memory pressure for streaming loaders and avoids unnecessary sort cost on already ordered list data.
- Remaining low-risk follow-up:
  - Add optional paged/distributed replay backend for very large windows across multi-worker deployments.

1. UW poller per-event sequential dedupe/publish path (remediated 2026-02-07).

- Evidence:
  - `gateway/core/uw_poller.py` now performs batched Redis dedupe reads, bounded-concurrency publish fanout, and batched dedupe-write updates via shared `_publish_envelopes(...)`.
  - Publish concurrency is now runtime-configurable via `GATEWAY_UW_POLLER_PUBLISH_MAX_INFLIGHT`.
- Impact: Reduces per-event round-trip serialization in flow/darkpool/market-tide/sector-tide poll loops and improves poll-cycle throughput under bursty snapshots.

1. yfinance historical conversion uses `iterrows()`.

- Evidence:
  - `gateway/providers/yfinance.py:190`.
- Impact: `iterrows` is slow for large DataFrames.
- Low-risk fix:
  - Replace with `itertuples(index=True)` and direct attribute access.

1. Alpha Vantage provider AV-3 helper/sort rollout is complete; remaining work is optional heavy-series limit tuning.

- Evidence:
  - Shared request/rate-limit helper: `gateway/providers/alphavantage.py:143` (`_fetch_json`) with `17` call sites.
  - Shared sort-head helper: `gateway/providers/alphavantage.py:164` (`_top_time_series_items`) used in indicator/forex/crypto data paths.
  - Targeted benchmark snapshot (local): ordered head extraction `6.02us` helper vs `357.91us` full sort (`59.46x` faster); unordered fallback `1.10x` overhead.
- Impact: Main AV-3 duplication/sort hotspots are remediated; largest remaining Alpha Vantage cost center is full-history parse/sort behavior.
- Low-risk fix:
  - Optionally add `max_points` limits on heavy full-history methods where route contracts allow.

### P2 (Medium Priority)

1. Cache custom TTL expiry pruning cadence (remediated 2026-02-07).

- Evidence:
  - `gateway/core/cache.py` now uses `CUSTOM_PRUNE_SET_INTERVAL` + `_custom_sets_since_prune` to prune custom-TTL expiries periodically instead of scanning on every custom set.
- Impact: Reduces repeated O(n) expiry scan overhead on custom-TTL-heavy write paths while preserving on-read expiry validation behavior.
- Follow-up:
  - Optional: tune prune interval with production write profiles.

1. In-memory cache max-size enforcement loop behavior (remediated 2026-02-07).

- Evidence:
  - `gateway/core/cache.py` now computes overflow once and evicts exact counts from custom/default tiers using bounded loops.
- Impact: Avoids repeated size recomputation/branch churn under burst insert overflow while preserving eviction order semantics (custom LRU-first, then default cache).
- Follow-up:
  - Optional: add dedicated microbenchmark for burst over-capacity insertion.

1. WebSocket message-loop settings lookup overhead (remediated 2026-02-07).

- Evidence:
  - `gateway/api/websocket.py` now resolves `max_message_size` once before the receive loop and accepts endpoint-injected value from `websocket_endpoint(...)`.
- Impact: Removes repeated settings cache lookup and attribute access from each incoming frame branch while preserving message-size enforcement behavior.
- Follow-up:
  - Optional: add websocket microbenchmark for mixed text/binary frame validation overhead.

1. Envelope serialization hot-path overhead (remediated 2026-02-07).

- Evidence:
  - `gateway/core/envelope.py` now builds JSON-ready envelope dicts directly in `wrap_event(...)` rather than constructing/dumping a Pydantic model per event.
- Impact: Removes per-message model construction/serialization cost on stream hot paths while preserving envelope schema, metrics, and fallback behavior.
- Follow-up:
  - Optional: add sampled validation mode toggle for debugging environments.

1. Finnhub bar normalization index-loop overhead (remediated 2026-02-07).

- Evidence:
  - `gateway/providers/finnhub.py` now iterates bar arrays via `zip(..., strict=False)` in `get_bars(...)`.
- Impact: Slightly lower loop overhead, clearer mapping logic, and safer handling of provider payload array-length mismatches.
- Follow-up:
  - Optional: emit a warning metric when payload arrays are truncated by shortest-list zip behavior.

1. Main stream callback sink-registry lookup overhead (remediated 2026-02-07).

- Evidence:
  - `gateway/main.py` now stores a stable sink-registry reference via `_set_stream_sink_registry(...)` during lifespan setup and uses it directly in `_on_stream_data(...)`.
- Impact: Removes per-event dependency/getter lookup from the stream callback hot path while preserving sink enable/disable behavior.
- Follow-up:
  - Optional: add stream callback microbenchmark coverage that isolates registry lookup overhead.

## Recommended Implementation Waves

### Wave 1 (1-2 sessions, lowest risk)

1. Parallelize provider health checks.
2. Hoist WebSocket `max_bytes` lookup out of per-message branches (completed 2026-02-07).
3. Replace `iterrows` in yfinance history conversion.
4. Switch Alpha Vantage CSV parsing to `csv.DictReader`.
5. Replace serial multi-quote provider loops with bounded concurrency (completed 2026-02-07).

### Wave 2

1. Tune decoupled sink publishing limits/telemetry in stream websocket send path (base decoupling is complete).
2. Optimize envelope serialization path (cache/envelope body-reuse cooperation completed).
3. Tune bounded fanout batching parameters (base batching rollout is complete).

### Wave 3

1. Add streaming storage for bulk/replay outputs (bulk JSONL + JSON downloads, replay iterable ingestion + replay/bulk spill guardrails are complete; optional paged backend storage remains).
2. Improve cache pruning strategy for custom TTL workloads (completed 2026-02-07).
3. Add envelope fast-path serialization for websocket traffic (completed 2026-02-07).

## Verification Plan (for each wave)

- Add or update benchmark tests for:
  - GET cached + wrapped response latency (p50/p95).
  - Stream fanout throughput at N clients.
  - Bulk job peak RSS during large result sets.
- Regression tests:
  - Existing middleware behavior (`tests/test_middleware_streaming.py`).
  - Endpoint schema contracts and auth behavior.
- Operational validation:
  - Compare Prometheus request/latency and memory metrics before/after.

## Audit Coverage Tracker

Legend:

- COMPLETE = reviewed directly in this run
- PARTIAL = sampled/high-risk sections reviewed
- PENDING = not yet deeply reviewed in this run

| Area | Files | Status | Notes |
|---|---:|---|---|
| `gateway/main.py` startup/lifespan/stream callback | 1 | COMPLETE | Core startup/shutdown and stream callback audited |
| `gateway/api/middleware.py` | 1 | COMPLETE | Main HTTP hot path audited in detail |
| `gateway/core/stream.py` | 1 | COMPLETE | Fanout and connection lifecycle audited |
| `gateway/core/cache.py` | 1 | COMPLETE | In-memory + redis cache paths audited |
| `gateway/core/registry.py` | 1 | COMPLETE | Provider lifecycle and health checks audited |
| `gateway/core/bulk.py` | 1 | COMPLETE | Job processing and result handling audited |
| `gateway/core/replay.py` | 1 | COMPLETE | Session replay path audited |
| `gateway/core/uw_poller.py` | 1 | COMPLETE | Polling and dedupe/publish loops audited |
| `gateway/core/envelope.py` | 1 | COMPLETE | Envelope serialization path audited |
| `gateway/api/websocket.py` | 1 | COMPLETE | Message loop + subscription path audited |
| Provider `gateway/providers/news.py` | 1 (333 LOC) | COMPLETE | Keyword hoisting, shared fetch/readiness helpers, pagination normalization |
| Provider `gateway/providers/alphavantage.py` | 1 (946 LOC) | COMPLETE | csv.DictReader, bounded quote fan-out, shared fetch helper, sort-head optimization |
| Provider `gateway/providers/alpaca.py` | 1 (2153 LOC) | COMPLETE | Option-chain limit threading, shared client DNE path, conversion-path optimization, shared timestamp-parse helper reuse across news/orderbook/normalization paths |
| Provider `gateway/providers/uw.py` | 1 (4672 LOC) | COMPLETE | Route-helper rollout, route-cache telemetry, `_call_sync` concurrency gating, native pagination |
| Provider `gateway/providers/yfinance.py` | 1 (386 LOC) | COMPLETE | Cache-before-provider, route helper consolidation, health-check offload, `iterrows` remediation |
| Provider `gateway/providers/sec.py` | 1 (434 LOC) | COMPLETE | Cache-before-provider, helper consolidation, filing key normalization |
| Provider `gateway/providers/finnhub.py` | 1 (1280 LOC) | COMPLETE | Cache-before-provider, dedupe, date/key helper consolidation, admin health-check parallelization |
| API routers `gateway/api/alpaca/*` | 14 files (60 endpoints) | COMPLETE | Option-chain snapshot + stock-trades over-fetch reductions, stock snapshot concurrency, shared list-parser rollout, shared execution-helper rollout across all Alpaca route modules (stock/options/metadata/forex/account/corporate/news/screener/crypto/watchlists/trading), safe-GET cache + in-flight dedupe rollout for metadata, news, corporate actions, account configurations, trading low-churn reads (`assets`, `asset`, `calendar`), screener endpoints (`most-actives`, `movers`), and both live/historical forex reads (`/forex/rates`, `/forex/rates/historical`) |
| API routers `gateway/api/finnhub/*` + control-plane routers | 15 files (61 endpoints) | COMPLETE | Cache-before-provider, dedupe, date/key helper consolidation, Finnhub route-level cache hit/miss telemetry (`finnhub_company_news`, `finnhub_market_news`, `finnhub_quote`, `finnhub_bars`, `finnhub_mutual_fund_profile`, `finnhub_mutual_fund_holdings`, `finnhub_mutual_fund_sector`, `finnhub_insider_sentiment`, `finnhub_upgrade_downgrade`, `finnhub_social_sentiment`, `finnhub_support_resistance`, `finnhub_pattern_recognition`, `finnhub_forex_rates`, `finnhub_forex_exchanges`, `finnhub_forex_symbols`, `finnhub_forex_candles`, `finnhub_crypto_exchanges`, `finnhub_crypto_symbols`, `finnhub_crypto_candles`, `finnhub_crypto_profile`, `finnhub_fda_calendar`, `finnhub_congress_trading`, `finnhub_lobbying`, `finnhub_usa_spending`, `finnhub_earnings_calendar`, `finnhub_recommendations`, `finnhub_eps_estimates`, `finnhub_revenue_estimates`, `finnhub_ebit_estimates`, `finnhub_ebitda_estimates`, `finnhub_price_target`, `finnhub_etf_profile`, `finnhub_etf_holdings`, `finnhub_etf_sector`, `finnhub_etf_country`, `finnhub_index_constituents`, `finnhub_index_historical`, `finnhub_company_profile`, `finnhub_financials`, `finnhub_peers`, `finnhub_metrics`, `finnhub_executives`, `finnhub_ownership`, `finnhub_fund_ownership`, `finnhub_insider_transactions`) |
| API routers `gateway/api/uw/*` | 26 (125 endpoints) | COMPLETE | Route-helper rollout, route-cache telemetry, native pagination |
| API routers `gateway/api/alphavantage/*` | 9 (30 endpoints) | COMPLETE | AV-1/AV-2/AV-3 rollouts |
| API router `gateway/api/yf.py` | 1 (16 endpoints) | COMPLETE | Cache-before-provider, route helper consolidation |
| API router `gateway/api/sec.py` | 1 (10 endpoints) | COMPLETE | Cache-before-provider, helper consolidation |
| API routers `gateway/api/{bulk,calendar,corporate,news,quality,replay,symbology,metrics}.py` | 8 files (34 endpoints incl. replay WS) | COMPLETE | Fetcher binding guards, cache-hit-first parsing, bulk streaming downloads, replay control-loop remediation, metrics refresh throttling |
| Core modules `gateway/core/{security,quality,calendar,symbology,validator}.py` | 5 (2395 LOC) | COMPLETE | Validator hot-path optimization, symbology allocation trimming, quality timestamp/sort reductions, middleware import hoist |
| Core infrastructure modules `gateway/core/{adjustments,auth,balancer,circuit_breaker,connections,...}` | 15 (3380 LOC) | COMPLETE | Adjustment lookup optimization, breaker caching, rate-limiter wait tuning, bounded sink dispatch |
| Tests (`tests/`) | 28 files (303 tests, 4491 LOC) | COMPLETE | Fixture scope caching, autouse override narrowing, sleep-free circuit breaker timing |
| Scripts `scripts/{live_provider_smoke.py,generate_provider_contract.py}` | 2 (351 LOC) | COMPLETE | Concurrency, handler pre-indexing |
| Benchmark/profiling readiness | 14 files/areas | COMPLETE | Latest calibration 2026-02-07: gate pass at `1.07s` vs `3.60s` suite budget |

## Next-Run Audit Plan (Targeted)

1. **UW provider**: Telemetry-driven inflight tuning; expand native pagination where post-filter semantics allow.
2. **Alpha Vantage provider**: Optional full-history `max_points` tuning + broader runtime profiling validation.
3. **Alpaca routers**: Route-helper consolidation is complete; continue selective safe-GET cache/dedupe expansion where payload shape and staleness bounds are clear (metadata, news, corporate actions, account configurations, trading low-churn reads, screener endpoints, and both live/historical forex reads are now covered).
4. **Alpaca provider**: Continue shared client-use and logging tuning follow-ups; timestamp conversion-path consolidation is now in place.
5. **Non-provider routers**: Finnhub cached-router telemetry sweep is complete (news, quotes/bars, funds, analysis, forex, crypto, alternative-data, earnings, ETF/index, fundamentals). Follow-up is optional telemetry naming/metric-cardinality review during regular perf monitoring.
6. **Core modules**: Benchmark calibration.
7. **Stream path**: Telemetry-calibrate configured fanout/sink limits (`stream_fanout_max_inflight`, `stream_fanout_batch_size`, `data_sink_stream_publish_max_inflight`, `data_sink_stream_publish_max_pending`) using `gateway_stream_sink_dispatch_events_total`, `gateway_stream_sink_pending_tasks`, `gateway_stream_sink_dispatch_limit`, `gateway_stream_fanout_events_total`, `gateway_stream_fanout_batch_size`, `gateway_stream_fanout_limit`, and `/api/v1/status` snapshots (`stream_sink_dispatch`, `stream_fanout`) including derived metrics (`pending_utilization`, `completion_rate`, `drop_rate`, `avg_batch_size`, `batch_fill_ratio`, `error_rate`).
8. **Perf guardrails**: Continue periodic monitoring/tuning via `scripts/perf_release_readiness.py` and `PERF_RELEASE_READINESS.md`.

## Notes

- This audit intentionally avoids recommending significant logic/behavior rewrites.
- Proposed changes are intended to preserve API contracts and endpoint semantics.
- Prioritization is based on expected latency/throughput gain per engineering effort.
- Deep-dive artifacts were removed during doc audit on 2026-02-07; all findings are summarized inline above.
