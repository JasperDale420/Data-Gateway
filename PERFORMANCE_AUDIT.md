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

1. Double buffering and repeated JSON serialization in HTTP middleware chain.
2. High per-message task/serialization overhead in stream fanout path.
3. Memory-heavy job/replay implementations that materialize full datasets in memory.

The fastest, lowest-risk wins are:

- Reduce duplicated response buffering in `CacheMiddleware` + `EventEnvelopeMiddleware`.
- Decouple sink publishing from client send in stream path.
- Parallelize selected provider fan-out methods and provider health checks.
- Replace high-overhead parsing loops (`iterrows`, ad-hoc CSV split, repeated full-cache scans).

## Prioritized Findings and Recommendations

### P0 (Do First)

1. Middleware double-buffering and re-serialization on GET responses.
- Evidence:
  - `gateway/api/middleware.py:365` buffers response body for cache writes.
  - `gateway/api/middleware.py:610` buffers response body again for envelope wrapping.
  - `gateway/api/middleware.py:624` parses JSON and `gateway/api/middleware.py:675` re-dumps JSON.
- Impact: High CPU + memory amplification on every cachable/wrappable GET; adds latency spikes on larger payloads.
- Low-risk fix:
  - Add a request/state flag so envelope middleware can skip re-buffering when response is a cache hit and already wrapped/known-safe.
  - Short-circuit envelope wrapping for cache hits where `X-Gateway-Envelope` or a deterministic marker is present.
  - Keep behavior identical for uncached responses.

2. Stream path backpressure coupling: sink publish blocks client message path.
- Evidence:
  - `gateway/main.py:122` awaits `sink_registry.publish_all(...)` inside `_on_stream_data`.
- Impact: Slow sink/Redis path directly increases client message latency and drop risk under load.
- Low-risk fix:
  - Move sink publish to fire-and-forget task (bounded queue/semaphore) so websocket send path is not blocked.
  - Preserve at-least-once best effort with structured error logs and metrics.

3. Stream fanout creates per-message task burst across all clients.
- Evidence:
  - `gateway/core/stream.py:991` uses `asyncio.gather(*(_send(...) for client in clients))` for every inbound message.
- Impact: Task allocation pressure and event-loop overhead at high message rates/fanout.
- Low-risk fix:
  - Reuse a bounded worker queue for outbound fanout instead of per-message task fanout.
  - Or chunk client sends into fixed-size batches while keeping semaphore control.

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

10. CSV parsing in Alpha Vantage uses repeated string split.
- Evidence:
  - `gateway/providers/alphavantage.py:1014-1018`, `1038-1042`, `1074-1078`.
- Impact: Parsing overhead and edge-case fragility for quoted commas.
- Low-risk fix:
  - Use `csv.DictReader` on `io.StringIO(response.text)`.

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

1. Decouple sink publishing from stream websocket send path.
2. Reduce middleware duplicate body buffering work (cache/envelope cooperation).
3. Introduce bounded batch fanout instead of full per-message gather bursts.

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
| Providers (`alpaca`, `finnhub`, `alphavantage`, `sec`, `news`, `yfinance`) | 7 | PARTIAL | High-traffic methods audited; not every endpoint method line-by-line |
| Provider `gateway/providers/uw.py` | 1 (4672 LOC) | COMPLETE | Dedicated deep pass completed; see `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` |
| API routers `gateway/api/alpaca/*`, `gateway/api/finnhub/*`, `gateway/api/catalog.py`, `gateway/api/health.py`, `gateway/api/admin.py` | 17 sampled | PARTIAL | Common patterns audited; full route-by-route perf pass still pending |
| API routers `gateway/api/uw/*` | 26 (125 endpoints) | COMPLETE | Full route-level UW audit complete; see `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` |
| API routers `gateway/api/alphavantage/*` | 9 (30 endpoints) | COMPLETE | Full route-level Alpha Vantage audit complete; see `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md` |
| API routers `gateway/api/sec.py`, `gateway/api/yf.py`, others | remaining | PENDING | Needs dedicated endpoint-level perf pass |
| `gateway/core/security.py`, `gateway/core/quality.py`, `gateway/core/calendar.py`, `gateway/core/symbology.py`, `gateway/core/validator.py` | 5 sampled via patterns | PARTIAL | Not deeply profiled for computational hotspots |
| Tests (`tests/`) | sampled | PARTIAL | Perf-oriented tests exist; no full perf harness yet |
| `scripts/` | 2 | PENDING | Runtime scripts not performance-profiled |

## Next-Run Audit Plan (Targeted)

1. Implement UW Wave 1 optimizations from `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` (shared route helper, serializer/accessor dedupe, pagination guardrails).
2. Implement Alpha Vantage Wave 1 optimizations from `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md` (cache-before-provider, helper consolidation, serialization normalization).
3. Full router audit for `gateway/api/yf.py`.
4. Build a lightweight benchmark harness (`pytest -k perf` style) for middleware + stream fanout.
5. Validate memory growth scenarios for bulk/replay with synthetic large datasets.

## Notes

- This audit intentionally avoids recommending significant logic/behavior rewrites.
- Proposed changes are intended to preserve API contracts and endpoint semantics.
- Prioritization is based on expected latency/throughput gain per engineering effort.
