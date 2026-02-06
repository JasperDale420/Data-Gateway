# Core Infrastructure Performance Audit Deep Dive

Date: 2026-02-06
Auditor: Codex (GPT-5)
Primary scope: `gateway/core/{adjustments,auth,balancer,circuit_breaker,connections,corporate_actions,data_sink,dedup,metrics,multiplexer,normalizer,rate_limiter,redis_sink,provider}.py`
Secondary scope: integration call paths in `gateway/main.py`, `gateway/api/deps.py`, and `gateway/api/corporate.py`

## Objective

Complete a deep performance pass of the remaining core infrastructure modules that were not explicitly tracked in prior audits, with low-risk recommendations that avoid significant logic changes.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| Core infrastructure modules | 15 (3380 LOC) | COMPLETE | Full static/code-path pass with line-level hotspot evidence |
| Integration call-path context | 3 (`gateway/main.py`, `gateway/api/deps.py`, `gateway/api/corporate.py`) | COMPLETE | Reviewed usage concentration and runtime impact |
| Runtime profiling/benchmarks | N/A | PENDING | No production microbench run in this pass |

## Inventory and Measured Hotspots

- Module set size: `3380` LOC.
- Method mix across this module set:
  - `54` async methods
  - `97` sync methods
- High-impact structure observations:
  - Data sink fanout creates per-event background tasks: `gateway/core/data_sink.py:146`
  - Data sink path resolves circuit breaker per sink publish and per health check:
    - `gateway/core/data_sink.py:153`
    - `gateway/core/data_sink.py:186`
  - Adjustment path repeatedly sorts factors in hot loops:
    - `gateway/core/adjustments.py:192`
    - `gateway/core/adjustments.py:251`
  - Request dedupe uses a single global async lock for all keys:
    - `gateway/core/dedup.py:54`
    - `gateway/core/dedup.py:78`
  - Message dedupe hashes with `json.dumps(..., sort_keys=True)` + MD5 per message:
    - `gateway/core/dedup.py:160-161`
  - Per-provider rate limiter uses repeated window loops and blocking sleep backoff:
    - `gateway/core/rate_limiter.py:97-98`
    - `gateway/core/rate_limiter.py:269`
  - Request metrics normalize path on every HTTP request:
    - `gateway/core/metrics.py:210`
    - `gateway/core/metrics.py:252-263`

Runtime integration context:
- Sink publish still awaited in stream callback path:
  - `gateway/main.py:122`
- Provider rate limiter is applied from API dependency layer:
  - `gateway/api/deps.py:271-303`
- Adjustment/corporate services are used by corporate API and bulk adjustments:
  - `gateway/api/corporate.py:306-334`
  - `gateway/core/bulk.py:838-841`

## Priority Findings (Low-Risk Changes Only)

### P0-1: Adjustment factor lookup is repeatedly sorted inside bar loops

Evidence:
- Factors are sorted in `apply_adjustment(...)` each call: `gateway/core/adjustments.py:192`
- Factors are sorted again in `_get_factor_for_date(...)`: `gateway/core/adjustments.py:251`
- `adjust_bars(...)` iterates bars and calls factor lookup per bar: `gateway/core/adjustments.py:215-225`

Impact:
- For large bar arrays, complexity amplifies to repeated `sorted(...)` work per row.
- Bulk adjustment workloads pay avoidable CPU overhead.

Low-risk fix path:
1. Pre-sort factors once in `adjust_bars(...)` and pass to lookup helper.
2. Use binary search (`bisect`) over factor dates for O(log n) lookup.
3. Preserve adjusted price/volume output schema and semantics.

### P0-2: Data sink publish path can create unbounded background task growth

Evidence:
- One task is created per sink per event: `gateway/core/data_sink.py:146`
- Tasks are retained in `_background_tasks` until completion: `gateway/core/data_sink.py:79`, `147-148`

Impact:
- Under sustained ingress or slow sink backends, task count and memory can spike.
- Increases event-loop overhead and latency jitter.

Low-risk fix path:
1. Introduce bounded publish queue + worker pool for sink dispatch.
2. Apply semaphore or max in-flight cap per sink.
3. Keep existing fire-and-forget behavior contract for callers.

Status (2026-02-06):
- Completed:
  - Implemented per-sink bounded in-flight dispatch with drop-on-backpressure:
    - `gateway/core/data_sink.py:70-205`
  - Added publish scheduling/backpressure stats:
    - `gateway/core/data_sink.py:89`
    - `gateway/core/data_sink.py:123-125`
  - Added perf assertions for bounded sink backlog:
    - `tests/perf/test_perf_stream_sink.py:89-113`

### P1-3: Data sink path does repeated breaker resolution and sequential health checks

Evidence:
- Breaker fetched for each publish call: `gateway/core/data_sink.py:153`
- Breaker fetched again during health checks: `gateway/core/data_sink.py:186`
- Sink health checks are looped sequentially: `gateway/core/data_sink.py:184-193`

Impact:
- Extra registry/lock overhead on high message volume.
- Health endpoint latency scales with sum of sink health check latencies.

Low-risk fix path:
1. Cache breaker object per sink in registry.
2. Use `asyncio.gather(..., return_exceptions=True)` for `health_check_all`.
3. Keep error handling and boolean status contract unchanged.

### P1-4: Request deduplicator serializes all keys through one lock

Evidence:
- Shared global lock around pending-map operations: `gateway/core/dedup.py:54`, `78`

Impact:
- Unrelated requests with different keys contend on the same lock.
- Reduces throughput under mixed-key concurrent load.

Low-risk fix path:
1. Use per-key lock striping or lock-free fast path for existing futures.
2. Keep result/exception sharing semantics unchanged.

### P1-5: Rate limiter blocking mode uses generic backoff sleeps instead of window reset hints

Evidence:
- Blocking acquire loop sleeps with exponential backoff: `gateway/core/rate_limiter.py:260-271`
- `retry_after` from `try_acquire()` is available but not used to schedule sleep exactly.

Impact:
- Extra wakeups and longer-than-needed wait behavior under sustained throttling.

Low-risk fix path:
1. Sleep based on computed `retry_after` (bounded by `max_wait`) instead of generic exponential stepping.
2. Preserve existing API and exceptions.

### P2-6: Auth success logs at info level on every authenticated request

Evidence:
- `gateway/core/auth.py:123`

Impact:
- High request rates produce heavy structured logging overhead.

Low-risk fix path:
1. Demote to debug or sampled info logging.
2. Preserve warning/error logs for failed auth paths.

### P2-7: Metrics path normalization repeats heuristic parsing on every request

Evidence:
- Request metrics always call `_normalize_path(...)`: `gateway/core/metrics.py:210`
- Normalization loops path segments + heuristic checks: `gateway/core/metrics.py:252-263`

Impact:
- Small but constant CPU overhead on hot HTTP path.

Low-risk fix path:
1. Add small LRU cache for normalized path strings.
2. Keep cardinality-reduction behavior unchanged.

### P2-8: Legacy/dormant core modules add maintenance overhead with minimal runtime value

Evidence:
- `KeyLoadBalancer` references are confined to module exports and self-definition:
  - `gateway/core/__init__.py:4`, `46-47`
  - `gateway/core/balancer.py:56`, `166`
- Legacy multiplexer module appears unused by runtime stream path (stream now defines its own `SubscriptionManager`):
  - `gateway/core/stream.py:86`, `324`

Impact:
- Extra code surface and testing burden without corresponding runtime gain.

Low-risk fix path:
1. Mark legacy modules explicitly deprecated in tracker/docs and gate loading where possible.
2. Plan removal in a compatibility-safe cleanup wave.

## Implementation Plan to Start Addressing Issues

### Wave CORE-INFRA-1 (Immediate, lowest risk)

1. Optimize adjustment factor lookup (single sort + binary search strategy).
2. Add bounded in-flight sink publishing with per-sink limits.
3. Cache sink circuit breakers and parallelize sink health checks.
4. Use precise `retry_after` waits in rate limiter blocking mode.

Wave status (2026-02-06):
- `2` complete

### Wave CORE-INFRA-2

1. Add lock striping/per-key optimization to request deduplicator.
2. Add LRU memoization for metrics path normalization.
3. Reduce auth success log volume (`info` -> `debug`/sampled).

### Wave CORE-INFRA-3

1. Evaluate legacy module retirement plan (`balancer`, legacy multiplexer paths).
2. Add targeted microbenchmarks for:
   - adjustment application throughput on large bar sets
   - sink publish queue behavior under backpressure
   - rate limiter acquire path under contention.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Audit Status | Future Run Focus |
|---|---|---|
| `gateway/core/adjustments.py` | COMPLETE | single-sort factor lookup + bisect optimization |
| `gateway/core/corporate_actions.py` | COMPLETE | avoid redundant sort/filter work in fallback paths |
| `gateway/core/data_sink.py` | COMPLETE | tune bounded dispatch policy and breaker caching/health parallelism |
| `gateway/core/dedup.py` | COMPLETE | reduce global lock contention and hashing overhead |
| `gateway/core/rate_limiter.py` | COMPLETE | retry_after-aligned blocking waits |
| `gateway/core/auth.py` | COMPLETE | auth success log-volume reduction |
| `gateway/core/metrics.py` | COMPLETE | path normalization memoization |
| `gateway/core/connections.py` | COMPLETE | broadcast fanout tuning for high connection counts |
| `gateway/core/circuit_breaker.py` | COMPLETE | low-priority lock/registry micro-optimizations |
| `gateway/core/redis_sink.py` | COMPLETE | import-path and serialization micro-optimization |
| `gateway/core/balancer.py` | COMPLETE | deprecation/cleanup decision |
| `gateway/core/multiplexer.py` | COMPLETE | deprecation/cleanup decision |
| `gateway/core/normalizer.py` | COMPLETE | assess activation/use or retirement |
| `gateway/core/provider.py` | COMPLETE | no major hotspots (interface-only) |
| `gateway/core/redis_cache.py` | COMPLETE | no major hotspots (compat layer) |

## Remaining Audit Scope (Future Runs)

1. Runtime benchmark/profiling suite execution for middleware, stream/replay fanout, bulk/adjustments, and sink backpressure behavior.
2. Implementation and measurement pass for CORE-INFRA Wave 1/2 recommendations.
3. Optional legacy cleanup audit for dormant compatibility modules once migration constraints are confirmed.
