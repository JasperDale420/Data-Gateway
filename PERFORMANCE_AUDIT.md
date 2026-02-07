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

1. Middleware cache/envelope duplicate body buffering (partially remediated on 2026-02-07).
- Evidence:
  - Cache middleware now stores buffered bytes on request state for HIT/MISS paths: `gateway/api/middleware.py:335`, `gateway/api/middleware.py:368`.
  - Envelope middleware now reuses pre-buffered bytes before iterating body: `gateway/api/middleware.py:616`, `gateway/api/middleware.py:723`.
  - JSON parse/dump path still exists for wrapping: `gateway/api/middleware.py:626`, `gateway/api/middleware.py:677`.
- Impact: Duplicate response-body assembly across cache+envelope path is reduced, lowering per-request memory churn; remaining cost is JSON parse/dump for envelope construction.
- Remaining low-risk follow-up:
  - Add optional cache-hit short-circuit when payload is already wrapped/known-safe.
  - Evaluate lightweight envelope assembly path to reduce JSON serialization overhead.

2. Stream path backpressure coupling: sink publish blocked client message path (remediated 2026-02-07).
- Evidence:
  - Sink publish now schedules off callback path via `_schedule_stream_sink_publish(...)`: `gateway/main.py`.
  - Bounded scheduling guardrails, runtime limit configuration, and shutdown draining are in place: `gateway/main.py` (`_configure_stream_sink_dispatch_limits`, `_drain_stream_sink_publish_tasks`).
- Impact: Stream callback no longer waits on sink publish/dedup I/O, reducing callback latency coupling under sink backpressure.
- Remaining low-risk follow-up:
  - Calibrate `data_sink_stream_publish_max_inflight` and `data_sink_stream_publish_max_pending` with telemetry under production-like fanout load.

3. Stream fanout per-message task burst (remediated 2026-02-07).
- Evidence:
  - Fanout now runs in bounded client batches via `_iter_client_batches(...)` before each `gather(...)`: `gateway/core/stream.py`.
  - In-flight semaphore and batch limits are now runtime-configurable through `stream_fanout_max_inflight` and `stream_fanout_batch_size`.
- Impact: Reduces single-message task allocation burst and smooths event-loop pressure at high fanout while preserving delivery semantics.
- Remaining low-risk follow-up:
  - Calibrate `stream_fanout_max_inflight` and `stream_fanout_batch_size` with telemetry under production-like loads.

### P1 (High Value, Low/Medium Risk)

4. Sequential provider health checks on admin/status endpoints.
- Evidence:
  - `gateway/core/registry.py:126-134` checks providers sequentially.
  - `gateway/api/admin.py:97`, `gateway/api/admin.py:214` call this for admin endpoints.
- Impact: Slow endpoints proportional to sum of provider latencies.
- Low-risk fix:
  - Use `asyncio.gather(..., return_exceptions=True)` in `health_check_all`.

5. Sequential quote fan-out in providers.
- Evidence:
  - `gateway/providers/finnhub.py:137-147` loops serially over symbols.
  - `gateway/providers/alphavantage.py:165-175` loops serially over symbols.
- Impact: For multi-symbol requests, latency scales linearly.
- Low-risk fix:
  - Use bounded concurrency (`Semaphore`) + `asyncio.gather` to fetch symbols in parallel within provider limits.

6. Bulk job result materialization creates high peak memory.
- Evidence:
  - `gateway/core/bulk.py:504` keeps extending `job.results` in memory.
  - `gateway/core/bulk.py:457-458` builds full JSONL list before join.
- Impact: Large jobs can hit high RSS and GC churn.
- Low-risk fix:
  - Stream results to temp file/redis stream as they complete.
  - Keep summary stats in memory; page result retrieval.

7. Replay preloads and sorts full message list.
- Evidence:
  - `gateway/core/replay.py:331` loads all messages.
  - `gateway/core/replay.py:346` sorts full list before replay.
- Impact: Memory and startup latency increase with history size.
- Low-risk fix:
  - Support iterator-based loaders that yield already-time-ordered messages.
  - Keep current list behavior as fallback for compatibility.

8. UW poller does sequential dedupe-read, publish, dedupe-write per event.
- Evidence:
  - `gateway/core/uw_poller.py:327-339`, `387-398`, `455-466`.
- Impact: High per-event network round trips and limited throughput.
- Low-risk fix:
  - Batch dedupe operations (pipeline) where supported.
  - Publish in bounded batches, not strictly one-by-one.

9. yfinance historical conversion uses `iterrows()`.
- Evidence:
  - `gateway/providers/yfinance.py:190`.
- Impact: `iterrows` is slow for large DataFrames.
- Low-risk fix:
  - Replace with `itertuples(index=True)` and direct attribute access.

10. Alpha Vantage provider AV-3 helper/sort rollout is complete; remaining work is optional heavy-series limit tuning.
- Evidence:
  - Shared request/rate-limit helper: `gateway/providers/alphavantage.py:143` (`_fetch_json`) with `17` call sites.
  - Shared sort-head helper: `gateway/providers/alphavantage.py:164` (`_top_time_series_items`) used in indicator/forex/crypto data paths.
  - Targeted benchmark snapshot (local): ordered head extraction `6.02us` helper vs `357.91us` full sort (`59.46x` faster); unordered fallback `1.10x` overhead.
- Impact: Main AV-3 duplication/sort hotspots are remediated; largest remaining Alpha Vantage cost center is full-history parse/sort behavior.
- Low-risk fix:
  - Optionally add `max_points` limits on heavy full-history methods where route contracts allow.

### P2 (Medium Priority)

11. Cache custom TTL path performs full custom-cache expiry scan on each custom set.
- Evidence:
  - `gateway/core/cache.py:80`, `152-157`.
- Impact: O(n) behavior on custom-TTL heavy workloads.
- Low-risk fix:
  - Prune opportunistically on interval/count threshold, not every set.

12. In-memory cache max-size enforcement may pop entries one-by-one in tight loop.
- Evidence:
  - `gateway/core/cache.py:159-170`.
- Impact: Burst inserts can spend time in repeated loop iteration.
- Low-risk fix:
  - Compute overflow count and pop in bounded for-loop with counter.

13. WebSocket message loop repeatedly calls `get_settings()` per incoming frame.
- Evidence:
  - `gateway/api/websocket.py:213`, `226`.
- Impact: Small but constant overhead on hot message path.
- Low-risk fix:
  - Resolve `max_bytes` once at loop entry.

14. Envelope construction still pays Pydantic serialization overhead on every message.
- Evidence:
  - `gateway/core/envelope.py:328-364` uses `model_construct` then `model_dump`.
- Impact: Extra object construction on high-throughput stream path.
- Low-risk fix:
  - Fast-path dict assembly for known fields; retain model path behind debug/validation flag.

15. Finnhub bar normalization uses index-based loop.
- Evidence:
  - `gateway/providers/finnhub.py:210-223`.
- Impact: Minor overhead and reduced readability.
- Low-risk fix:
  - Use `zip(timestamps, opens, highs, lows, closes, volumes, strict=False)`.

16. Main stream callback performs repeated dependency lookup import on each event.
- Evidence:
  - `gateway/main.py:117-120` imports and resolves sink registry per message.
- Impact: Small overhead per message under high throughput.
- Low-risk fix:
  - Resolve sink registry once in lifespan and capture in callback closure or lightweight getter reference.

## Recommended Implementation Waves

### Wave 1 (1-2 sessions, lowest risk)

1. Parallelize provider health checks.
2. Hoist WebSocket `max_bytes` lookup out of per-message branches.
3. Replace `iterrows` in yfinance history conversion.
4. Switch Alpha Vantage CSV parsing to `csv.DictReader`.
5. Replace serial multi-quote provider loops with bounded concurrency.

### Wave 2

1. Tune decoupled sink publishing limits/telemetry in stream websocket send path (base decoupling is complete).
2. Optimize envelope serialization path (cache/envelope body-reuse cooperation completed).
3. Tune bounded fanout batching parameters (base batching rollout is complete).

### Wave 3

1. Add streaming storage for bulk/replay outputs.
2. Improve cache pruning strategy for custom TTL workloads.
3. Add envelope fast-path serialization for websocket traffic.

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
| Provider `gateway/providers/news.py` | 1 (333 LOC) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_NEWS_PROVIDER_DEEP_DIVE.md` |
| Provider `gateway/providers/alphavantage.py` | 1 (946 LOC) | COMPLETE | Dedicated deep pass completed; AV-3 helper/sort rollout complete (`csv.DictReader`, bounded quote fan-out, shared fetch helper, sort-head optimization); see `PERFORMANCE_AUDIT_ALPHAVANTAGE_PROVIDER_DEEP_DIVE.md` |
| Provider `gateway/providers/alpaca.py` | 1 (2153 LOC) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_ALPACA_PROVIDER_DEEP_DIVE.md` |
| Provider `gateway/providers/uw.py` | 1 (4672 LOC) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` |
| Provider `gateway/providers/yfinance.py` | 1 (386 LOC) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_YF_DEEP_DIVE.md` |
| Provider `gateway/providers/sec.py` | 1 (434 LOC) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_SEC_DEEP_DIVE.md` |
| Provider `gateway/providers/finnhub.py` | 1 (1280 LOC) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_FINNHUB_CONTROL_PLANE_DEEP_DIVE.md` |
| API routers `gateway/api/alpaca/*` | 14 files (60 endpoints) | COMPLETE | Full route-level Alpaca audit complete; see `PERFORMANCE_AUDIT_ALPACA_DEEP_DIVE.md` |
| API routers `gateway/api/finnhub/*` + `gateway/api/admin.py` + `gateway/api/catalog.py` + `gateway/api/health.py` | 15 files (61 endpoints) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_FINNHUB_CONTROL_PLANE_DEEP_DIVE.md` |
| API routers `gateway/api/uw/*` | 26 (125 endpoints) | COMPLETE | Full route-level UW audit complete; see `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` |
| API routers `gateway/api/alphavantage/*` | 9 (30 endpoints) | COMPLETE | Full route-level Alpha Vantage audit complete; see `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md` |
| API router `gateway/api/yf.py` | 1 (16 endpoints) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_YF_DEEP_DIVE.md` |
| API router `gateway/api/sec.py` | 1 (10 endpoints) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_SEC_DEEP_DIVE.md` |
| API routers `gateway/api/{bulk,calendar,corporate,news,quality,replay,symbology,metrics}.py` | 8 files (34 endpoints incl. replay WS) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_NON_PROVIDER_ROUTERS_DEEP_DIVE.md` |
| Core modules `gateway/core/{security,quality,calendar,symbology,validator}.py` | 5 (2395 LOC) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_CORE_MODULES_DEEP_DIVE.md` |
| Core infrastructure modules `gateway/core/{adjustments,auth,balancer,circuit_breaker,connections,corporate_actions,data_sink,dedup,metrics,multiplexer,normalizer,rate_limiter,redis_sink,provider}.py` | 15 (3380 LOC) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_CORE_INFRA_DEEP_DIVE.md` |
| Tests (`tests/`) | 28 files (303 tests, 4491 LOC) | COMPLETE | Dedicated execution-path pass completed; see `PERFORMANCE_AUDIT_TESTS_DEEP_DIVE.md` |
| Scripts `scripts/{live_provider_smoke.py,generate_provider_contract.py}` | 2 (351 LOC) | COMPLETE | Dedicated static/code-path pass completed; see `PERFORMANCE_AUDIT_CORE_MODULES_DEEP_DIVE.md` |
| Benchmark/profiling readiness (`.github/workflows/{ci,release-readiness}.yml`, `pyproject.toml`, targeted perf-sensitive tests/core hot paths) | 14 files/areas | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` |

## Next-Run Audit Plan (Targeted)

1. Continue UW implementation from `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` (Wave 1 route-helper rollout and `_call_sync` concurrency gating/metrics are complete; native pagination support is now implemented for flow/darkpool/institutions with fallback behavior, and next UW focus is telemetry-driven inflight tuning plus expanding native pagination where post-filter semantics allow).
2. Continue Alpha Vantage implementation from `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md` (AV-1/AV-2/AV-3 rollouts complete; next optional focus is full-history limit tuning and broader runtime profiling validation).
3. Continue yfinance Wave 1 optimizations from `PERFORMANCE_AUDIT_YF_DEEP_DIVE.md` (cache-before-provider, route helper consolidation, and health-check offload; provider `iterrows` hot paths remediated).
4. Implement SEC Wave 1 optimizations from `PERFORMANCE_AUDIT_SEC_DEEP_DIVE.md` (cache-before-provider, helper consolidation, filing key normalization).
5. Continue Finnhub/control-plane Wave 1 optimizations from `PERFORMANCE_AUDIT_FINNHUB_CONTROL_PLANE_DEEP_DIVE.md` (cache-before-provider, dedupe, and date/key helper consolidation; admin health-check parallelization completed).
6. Implement Alpaca Wave 1 optimizations from `PERFORMANCE_AUDIT_ALPACA_DEEP_DIVE.md` (route helper consolidation, cache/dedupe for safe GETs, over-fetch reductions).
7. Implement non-provider router Wave 1 optimizations from `PERFORMANCE_AUDIT_NON_PROVIDER_ROUTERS_DEEP_DIVE.md` (bulk streaming downloads, fetcher binding guards, cache-hit-first parsing in news).
8. Implement Alpaca provider Wave 1 optimizations from `PERFORMANCE_AUDIT_ALPACA_PROVIDER_DEEP_DIVE.md` (shared client use for DNE path, conversion-path optimization, limit/logging tuning).
9. Continue Alpha Vantage provider follow-up from `PERFORMANCE_AUDIT_ALPHAVANTAGE_PROVIDER_DEEP_DIVE.md` (optional full-history `max_points` tuning + broader runtime profiling).
10. Implement News provider Wave 1 optimizations from `PERFORMANCE_AUDIT_NEWS_PROVIDER_DEEP_DIVE.md` (keyword hoisting, shared fetch/readiness helpers, pagination normalization with effective page size).
11. Implement core modules Wave 1 optimizations from `PERFORMANCE_AUDIT_CORE_MODULES_DEEP_DIVE.md` (validator hot-path optimization, quality timestamp/sort reductions, symbology allocation trimming, middleware import hoist).
12. Implement core infrastructure Wave 1 optimizations from `PERFORMANCE_AUDIT_CORE_INFRA_DEEP_DIVE.md` (adjustment lookup optimization, breaker caching, rate-limiter wait tuning, and bounded sink dispatch tuning).
13. Implement tests Wave 1 optimizations from `PERFORMANCE_AUDIT_TESTS_DEEP_DIVE.md` (fixture scope caching, autouse override narrowing, sleep-free circuit breaker timing tests).
14. Operate BENCH guardrails from `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` (monitor auto-ratcheted budgets/baselines, tune multipliers/windows, and periodically promote stable active configs via `scripts/perf_release_readiness.py` and `PERF_RELEASE_READINESS.md`).
15. Continue stream-path optimization from this audit: telemetry-calibrate configured fanout/sink limits (`stream_fanout_max_inflight`, `stream_fanout_batch_size`, `data_sink_stream_publish_max_inflight`, `data_sink_stream_publish_max_pending`).

## Notes

- This audit intentionally avoids recommending significant logic/behavior rewrites.
- Proposed changes are intended to preserve API contracts and endpoint semantics.
- Prioritization is based on expected latency/throughput gain per engineering effort.
