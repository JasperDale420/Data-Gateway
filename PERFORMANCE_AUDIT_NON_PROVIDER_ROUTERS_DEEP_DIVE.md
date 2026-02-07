# Non-Provider Router Performance Audit Deep Dive

Date: 2026-02-06
Auditor: Codex (GPT-5)
Primary scope: `gateway/api/{bulk,calendar,corporate,news,quality,replay,symbology,metrics}.py`
Secondary scope: route-adjacent service paths in `gateway/core/{bulk,calendar,corporate_actions,adjustments,metrics,replay,symbology,quality}.py`

## Objective

Complete the pending deep pass for non-provider-specific API routers, identify low-risk performance improvements without significant logic changes, and explicitly track audited vs future scope.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| Non-provider API routers | 8 files | COMPLETE | Full endpoint-level pass across all routes in scope |
| Route-adjacent core service paths | 8 files | PARTIAL | Targeted verification of route-coupled performance paths only |
| Runtime profiling/benchmarks | N/A | PENDING | Static/code-path audit in this run |

## Inventory and Measured Hotspots

- Router group size: `2271` LOC, `34` endpoints (including replay WebSocket route).
- Endpoint distribution:
  - `bulk`: 7
  - `calendar`: 5
  - `corporate`: 5
  - `news`: 3
  - `quality`: 3
  - `replay`: 6
  - `symbology`: 4
  - `metrics`: 1
- Caching/dedup in this group:
  - Present only in `news` routes (`3` endpoints).
  - No cache/dedup in other `31` endpoints.
- Repeated configuration/fetcher wiring at request-time:
  - `set_*fetcher` calls in routes: `4`
  - sites: `gateway/api/bulk.py:196`, `gateway/api/bulk.py:433`, `gateway/api/calendar.py:344`, `gateway/api/corporate.py:153`
- Thread offload in routes:
  - `asyncio.to_thread(...)`: `2` sites (calendar route path)
- Date parsing hotspots:
  - `fromisoformat` usage: `16` call sites in route layer

## Priority Findings (Low-Risk Changes Only)

### P0-1: Bulk download endpoints materialize full result payloads in memory before response

Evidence:
- Route builds full payload strings:
  - `gateway/api/bulk.py:301` (`manager.get_results_jsonl(job_id)`)
  - `gateway/api/bulk.py:312` (`json.dumps({"data": job.results})`)
- Core bulk manager stores all records in memory and builds JSONL via full list + join:
  - `gateway/core/bulk.py:203` (`results` list on job)
  - `gateway/core/bulk.py:457` (list of serialized lines)
  - `gateway/core/bulk.py:458` (`"\n".join(...)`)

Impact:
- Peak memory grows with result size at generation time and again at download serialization time.
- Large completed jobs can trigger high RSS and GC pressure during download.

Low-risk fix path:
1. Switch `bulk` download route to `StreamingResponse` backed by `get_results_stream(...)`.
2. Keep the existing `jsonl`/`json` output contract unchanged.
3. Preserve current auth/job checks; change only response emission strategy.

### P0-2: Request-time mutation of singleton fetchers in bulk/calendar/corporate routes

Evidence:
- Routes set global singleton fetchers on each request:
  - `gateway/api/bulk.py:196`
  - `gateway/api/bulk.py:433`
  - `gateway/api/calendar.py:344`
  - `gateway/api/corporate.py:153`
- Singleton services expose mutable fetcher setters:
  - `gateway/core/bulk.py:278`
  - `gateway/core/calendar.py:383`
  - `gateway/core/corporate_actions.py:130`

Impact:
- Repeated closure allocation and global setter writes on hot paths.
- Raises maintenance and concurrency risk for behavior drift across requests.

Low-risk fix path:
1. Configure fetchers once during startup/lifespan instead of per-request.
2. Add `has_*_fetcher` guard paths in routers to avoid repeated setter calls.
3. Keep existing provider fallback behavior.

### P1-3: Replay WebSocket control loop uses exception-driven polling every second

Evidence:
- Control loop calls `wait_for(receive_json, timeout=1.0)`:
  - `gateway/api/replay.py:337`
  - `gateway/api/replay.py:339`
- Timeout path raises/catches every second when no control message:
  - `gateway/api/replay.py:356`

Impact:
- Constant exception allocation/handling overhead per active replay session.
- Increased event-loop churn at higher concurrent replay counts.

Low-risk fix path:
1. Move control handling to a background receive task and avoid timeout exceptions as normal flow.
2. Keep control message semantics (`pause`, `resume`, `seek`, `stop`) unchanged.

### P1-4: Calendar route fallbacks swallow provider failures and immediately retry on each request

Evidence:
- Broad fallback exception handling:
  - `gateway/api/calendar.py:154`
  - `gateway/api/calendar.py:245`
- Provider call path uses sync SDK offloaded with `to_thread`:
  - `gateway/api/calendar.py:114`
  - `gateway/api/calendar.py:205`

Impact:
- Repeated upstream failure periods can keep paying provider call latency every request.
- Hidden failure mode reduces observability of degraded performance.

Low-risk fix path:
1. Add short-lived degraded-mode/circuit flag after repeated provider failures.
2. Log fallback reason once per interval.
3. Keep static calendar fallback output unchanged.

### P1-5: News route parses datetime parameters before cache-hit short-circuit

Evidence:
- Datetime parsing happens before cache check:
  - `gateway/api/news.py:54`
  - `gateway/api/news.py:55`
- Cache lookup occurs afterward:
  - `gateway/api/news.py:59`

Impact:
- Cache hits still pay parse overhead and date parsing errors are evaluated on hot path even when payload is cached.

Low-risk fix path:
1. Build normalized key and check cache first.
2. Parse datetime only on miss.
3. Keep API behavior and error contract unchanged.

### P2-6: Metrics endpoint recomputes dynamic memory gauges on every scrape

Evidence:
- Per-request update and metrics generation:
  - `gateway/api/metrics.py:23`
  - `gateway/api/metrics.py:26`
- Memory update logic collects process metrics with multiple probes/import paths:
  - `gateway/core/metrics.py:172`
  - `gateway/core/metrics.py:195`

Impact:
- High scrape frequencies can create avoidable system-call overhead.

Low-risk fix path:
1. Throttle memory metric refresh with a short interval (for example 5-15s).
2. Continue serving `generate_latest()` on every scrape.
3. Keep metric names and labels unchanged.

### P2-7: Unbounded batch symbol resolution can become CPU-heavy for oversized payloads

Evidence:
- Batch request model has no explicit max length:
  - `gateway/api/symbology.py:47`
- Endpoint iterates and resolves each symbol serially:
  - `gateway/api/symbology.py:137`

Impact:
- Very large request bodies can create burst CPU and response latency.

Low-risk fix path:
1. Add max symbol count guard on `BatchResolveRequest`.
2. Keep response schema and per-symbol error behavior unchanged.

### P2-8: List endpoints return full in-memory collections with no pagination controls

Evidence:
- Bulk jobs list returns all jobs for client:
  - `gateway/api/bulk.py:372`
  - `gateway/api/bulk.py:379`
- Replay sessions list returns all sessions:
  - `gateway/api/replay.py:267`
  - `gateway/api/replay.py:270`

Impact:
- Response size and serialization cost grow linearly with retained job/session volume.

Low-risk fix path:
1. Add optional `limit`/`offset` query parameters with current behavior as default when omitted.
2. Preserve existing response payload shape for backward compatibility.

## Implementation Plan to Start Addressing Issues

### Wave NPR-1 (Immediate, lowest risk)

1. Stream bulk download responses (`jsonl` first) instead of building full strings in memory.
2. Guard `set_*fetcher` calls so fetchers are not re-bound on every request.
3. Move news datetime parsing to cache-miss path.

### Wave NPR-2

1. Replace replay exception-driven timeout loop with dedicated control-receive task.
2. Add calendar fallback degradation cache/window on provider failure.
3. Add optional pagination to bulk/replay list endpoints.

### Wave NPR-3

1. Throttle metrics memory-refresh frequency.
2. Add symbology batch size limits and request metrics.
3. Add per-endpoint cache-hit/miss instrumentation for applicable read-heavy routes.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Endpoints | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/api/bulk.py` | 7 | COMPLETE | Streaming downloads, fetcher binding guard, list pagination |
| `gateway/api/calendar.py` | 5 | COMPLETE | Failure fallback throttling, provider call guardrails |
| `gateway/api/corporate.py` | 5 | COMPLETE | Fetcher binding guard and route-level dedupe/caching policy |
| `gateway/api/news.py` | 3 | COMPLETE | Cache-hit-first parsing path, cache key normalization |
| `gateway/api/quality.py` | 3 | COMPLETE | Real analyzer integration path and payload size controls |
| `gateway/api/replay.py` | 6 | COMPLETE | WebSocket control loop optimization, list pagination |
| `gateway/api/symbology.py` | 4 | COMPLETE | Batch request caps and lightweight response caching |
| `gateway/api/metrics.py` | 1 | COMPLETE | Dynamic metric refresh throttling |

## Remaining Audit Scope (Future Runs)

1. Full provider deep passes still pending:
   - `gateway/providers/alpaca.py`
   - `gateway/providers/alphavantage.py`
   - `gateway/providers/news.py`
2. Deeper computational hotspot audit for sampled core modules:
   - `gateway/core/security.py`
   - `gateway/core/quality.py`
   - `gateway/core/calendar.py`
   - `gateway/core/symbology.py`
   - `gateway/core/validator.py`
3. Runtime benchmark harness and load validation for:
   - middleware/cache/envelope paths
   - stream/replay fanout paths
   - bulk memory behavior under large jobs
