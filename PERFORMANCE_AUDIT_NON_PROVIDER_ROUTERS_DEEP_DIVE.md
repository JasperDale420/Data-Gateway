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

### P0-2: Request-time mutation of singleton fetchers in bulk/calendar/corporate routes (remediated 2026-02-07)

Evidence:
- Routes now guard singleton fetcher binding with `has_*_fetcher()` checks:
  - `gateway/api/bulk.py:176`
  - `gateway/api/bulk.py:418`
  - `gateway/api/calendar.py:301`
  - `gateway/api/corporate.py:90`
- Regression coverage validates no-rebind behavior across affected routes:
  - `tests/test_router_fetcher_guards.py:78`
  - `tests/test_router_fetcher_guards.py:103`
  - `tests/test_router_fetcher_guards.py:127`
  - `tests/test_router_fetcher_guards.py:148`

Impact:
- Removes repeated closure allocation and singleton setter writes from request hot paths while preserving existing provider fallback behavior.

### P1-3: Replay WebSocket control loop uses exception-driven polling every second (remediated 2026-02-07)

Evidence:
- Replay control handling now uses dedicated control helpers/task instead of `wait_for(..., timeout=1.0)`:
  - `gateway/api/replay.py:286`
  - `gateway/api/replay.py:319`
- WebSocket endpoint now waits on replay/control task completion rather than timeout-driven exception flow:
  - `gateway/api/replay.py:384`
  - `gateway/api/replay.py:389`
- Regression coverage validates pause/resume/seek/stop and disconnect control behavior:
  - `tests/test_replay.py:419`
  - `tests/test_replay.py:451`
  - `tests/test_replay.py:461`

Impact:
- Removes per-session timeout exception churn from idle replay WebSocket control paths while preserving existing control semantics.

### P1-4: Calendar route fallbacks swallow provider failures and immediately retry on each request (remediated 2026-02-07)

Evidence:
- Calendar provider fallback now uses per-route degraded windows with cooldown:
  - `gateway/api/calendar.py:36`
  - `gateway/api/calendar.py:48`
  - `gateway/api/calendar.py:76`
- Market-hours/trading-days provider paths now gate retries with degraded-state checks:
  - `gateway/api/calendar.py:151`
  - `gateway/api/calendar.py:245`
- Regression coverage validates degraded-window behavior and retry after cooldown expiry:
  - `tests/test_calendar_api.py:35`
  - `tests/test_calendar_api.py:48`
  - `tests/test_calendar_api.py:79`

Impact:
- Reduces repeated upstream retry latency during provider outages while preserving static-calendar fallback responses and adding bounded degradation logging.

### P1-5: News route parses datetime parameters before cache-hit short-circuit (remediated 2026-02-07)

Evidence:
- Cache lookup now runs before date parsing:
  - `gateway/api/news.py:51`
  - `gateway/api/news.py:52`
- Datetime parsing now runs only on cache miss:
  - `gateway/api/news.py:58`
  - `gateway/api/news.py:59`
- Regression coverage validates cache-hit behavior with invalid datetime inputs:
  - `tests/test_news_router.py:49`

Impact:
- Cache-hit requests now skip avoidable datetime parsing work in the hot path, reducing per-hit CPU overhead and avoiding unnecessary parse evaluation.

### P2-6: Metrics endpoint recomputes dynamic memory gauges on every scrape (remediated 2026-02-07)

Evidence:
- Metrics endpoint now calls throttled updater before scrape generation:
  - `gateway/api/metrics.py:23`
- Core metrics now exposes interval-based updater (`10s` default):
  - `gateway/core/metrics.py:9`
  - `gateway/core/metrics.py:248`
- Regression coverage validates throttling and force-refresh behavior:
  - `tests/test_metrics.py:23`
  - `tests/test_metrics.py:38`
  - `tests/test_metrics.py:51`

Impact:
- Reduces repeated system-call/process-probe overhead on high-frequency metrics scrapes while preserving metric names/labels and scrape response behavior.

### P2-7: Unbounded batch symbol resolution can become CPU-heavy for oversized payloads (remediated 2026-02-07)

Evidence:
- `BatchResolveRequest` now enforces symbol-count bounds (`1..500`):
  - `gateway/api/symbology.py:47`
- Batch endpoint now records request-size telemetry via dedicated metric:
  - `gateway/api/symbology.py:139`
  - `gateway/core/metrics.py:192`
- Regression coverage validates max-size guardrails and metric emission:
  - `tests/test_symbology_api.py:8`
  - `tests/test_symbology_api.py:19`

Impact:
- Prevents oversized batch payload bursts from creating unbounded CPU work while preserving existing response schema and per-symbol error behavior.

### P2-8: List endpoints return full in-memory collections with no pagination controls (remediated 2026-02-07)

Evidence:
- Bulk jobs endpoint now supports optional `limit`/`offset` query parameters:
  - `gateway/api/bulk.py:366`
  - `gateway/api/bulk.py:372`
- Replay sessions endpoint now supports optional `limit`/`offset` query parameters:
  - `gateway/api/replay.py:269`
  - `gateway/api/replay.py:275`
- Regression coverage validates pagination behavior for both endpoints:
  - `tests/test_list_pagination.py:50`
  - `tests/test_list_pagination.py:74`

Impact:
- Prevents unbounded response growth on retained job/session-heavy clients while preserving response shape and default behavior when pagination params are omitted.

## Implementation Plan to Start Addressing Issues

### Wave NPR-1 (Immediate, lowest risk)

1. Stream bulk download responses (`jsonl` first) instead of building full strings in memory.
2. Guard `set_*fetcher` calls so fetchers are not re-bound on every request (completed 2026-02-07).
3. Move news datetime parsing to cache-miss path.

### Wave NPR-2

1. Replace replay exception-driven timeout loop with dedicated control-receive task (completed 2026-02-07).
2. Add calendar fallback degradation cache/window on provider failure (completed 2026-02-07).
3. Add optional pagination to bulk/replay list endpoints (completed 2026-02-07).

### Wave NPR-3

1. Throttle metrics memory-refresh frequency (completed 2026-02-07).
2. Add symbology batch size limits and request metrics (completed 2026-02-07).
3. Add per-endpoint cache-hit/miss instrumentation for applicable read-heavy routes.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Endpoints | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/api/bulk.py` | 7 | COMPLETE | Optional startup-only fetcher wiring and list ordering/retention policy |
| `gateway/api/calendar.py` | 5 | COMPLETE | Provider call guardrails and fallback telemetry tuning |
| `gateway/api/corporate.py` | 5 | COMPLETE | Route-level dedupe/caching policy review |
| `gateway/api/news.py` | 3 | COMPLETE | Cache-hit-first parsing path, cache key normalization |
| `gateway/api/quality.py` | 3 | COMPLETE | Real analyzer integration path and payload size controls |
| `gateway/api/replay.py` | 6 | COMPLETE | Replay-task completion telemetry and websocket lifecycle observability |
| `gateway/api/symbology.py` | 4 | COMPLETE | Lightweight response caching and optional batch parallelization profiling |
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
