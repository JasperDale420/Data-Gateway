# Alpha Vantage Performance Audit Deep Dive

Date: 2026-02-05
Auditor: Codex (GPT-5)
Primary scope: `gateway/api/alphavantage/*.py`
Secondary context: `gateway/providers/alphavantage.py`

## Objective

Produce a route-level Alpha Vantage performance audit focused on low-risk improvements that do not significantly change endpoint behavior, and explicitly track what is fully audited now vs deferred to future runs.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| Alpha Vantage API routers | 9 files (`gateway/api/alphavantage/*.py`) | COMPLETE | Full route-level pass across all endpoints |
| Alpha Vantage provider | 1 file (`gateway/providers/alphavantage.py`) | PARTIAL | AV-3 provider rollout complete (`csv.DictReader`, bounded quotes, shared fetch helper, sort-head optimization); heavier time-series limit tuning is future work |
| Runtime benchmark/profiler execution | N/A | PENDING | Static/code-path audit only in this run |

## Inventory and Measured Hotspots

- Alpha Vantage route package size: `1121` LOC.
- Route handlers (`@router.*`): `30`.
- Full route cache/rate-limit/provider pattern counts:
  - `provider = registry.get("alphavantage")`: `20`
  - `cached = await cache.get(key)`: `20`
  - `await require_provider_rate_limit("alphavantage")`: `20`
  - `await cache.set(key, ...)`: `20`
- Cached response metadata duplication:
  - `"cached": True` response blocks: `20`
  - `"cached": False` response blocks: `20`
- Route `model_dump*` usage: `5` (all in `timeseries.py`).
- Technical indicator convenience wrappers: `10` (`indicators.py`).
- Effective route TTL profile (`cache.set` call sites):
  - `60s`: 2
  - `300s`: 4
  - `3600s`: 12
  - `86400s`: 2
- Provider context signals:
  - Provider LOC: `946`.
  - Shared `_fetch_json(...)` call sites: `17`.
  - Remaining direct client GET call sites: `5` (health-check + CSV endpoints).
  - Inline `"Note"` checks in method bodies: `0` (centralized in `_fetch_json`).
  - CSV parsing helper uses `csv.DictReader`: `gateway/providers/alphavantage.py:873`.
  - Multi-symbol quote fetch uses semaphore-bounded fan-out: `gateway/providers/alphavantage.py:216`.
  - Sort-head helper applied to indicator/forex/crypto paths: `gateway/providers/alphavantage.py:164`.

## Remediation Progress (2026-02-07)

- `AV-3.1` complete: provider CSV endpoints parse through shared `csv.DictReader` helper (`gateway/providers/alphavantage.py:873`).
- `AV-3.2` complete: `get_quotes(...)` executes with bounded concurrency via `asyncio.Semaphore` (`gateway/providers/alphavantage.py:216`).
- `AV-3.3` complete: shared `_fetch_json(...)` helper and `_top_time_series_items(...)` sort-head optimization are in place with targeted micro-benchmark validation.
- Targeted benchmark snapshot (local run, `N=5000`, 300 loops):
  - ordered head extraction: helper `6.02us` vs full sort `357.91us` (`59.46x` faster).
  - unordered fallback: helper `1393.26us` vs full sort `1267.10us` (`1.10x` overhead).

## Priority Findings (Low-Risk Changes Only)

### P0-1: Provider lookup runs before cache check in endpoint hot path

Evidence:
- `gateway/api/alphavantage/timeseries.py:31` resolves provider before cache lookup at `gateway/api/alphavantage/timeseries.py:36`.
- `gateway/api/alphavantage/fundamentals.py:30` resolves provider before cache lookup at `gateway/api/alphavantage/fundamentals.py:35`.
- Same pattern across all 20 cache-enabled handlers.

Impact:
- Every cache hit still pays registry lookup overhead.
- Small per-request cost, amplified on high cache-hit traffic.

Low-risk fix path:
1. Build cache key first.
2. Return cache hit immediately.
3. Resolve provider only on cache miss.

### P0-2: Large `full` time-series payloads are cached as full in-memory objects

Evidence:
- `gateway/api/alphavantage/timeseries.py:67` allows `outputsize="full"` for intraday.
- `gateway/api/alphavantage/timeseries.py:107` allows `outputsize="full"` for daily.
- Full payload is serialized and cached in route layer:
  - `gateway/api/alphavantage/timeseries.py:92` to `gateway/api/alphavantage/timeseries.py:94`
  - `gateway/api/alphavantage/timeseries.py:133` to `gateway/api/alphavantage/timeseries.py:135`

Impact:
- High memory pressure for large symbol/timeframe requests.
- Extra serialization latency and larger cache residency footprint.

Low-risk fix path:
1. Keep response shape the same, but skip route cache for `outputsize=full`.
2. Keep caching for compact responses where hit-rate and size profile are favorable.
3. Add metrics for payload size and cache hit/miss by outputsize.

### P1-3: Cache key cardinality is high on free-form endpoints

Evidence:
- Free-form search key:
  - `gateway/api/alphavantage/timeseries.py:233` uses `q.lower()` in cache key.
- Technical indicator key includes many dimensions and path-driven indicator value:
  - `gateway/api/alphavantage/indicators.py:41` to `gateway/api/alphavantage/indicators.py:47`.
- Search result cache TTL is long:
  - `gateway/api/alphavantage/timeseries.py:245` uses `ttl=86400`.

Impact:
- Low cache reuse with potentially high key growth under diverse queries.

Low-risk fix path:
1. Add bounded normalization for search key values (trim length, collapse whitespace).
2. Optionally shorten search TTL or apply LRU budget by prefix.
3. Keep indicator cache but monitor hit-rate by key dimension mix.

### P1-4: Route response-building logic is duplicated across 20 handlers

Evidence:
- Same cache-hit response shape repeated in most routes (`"cached": True` blocks).
- Same provider call + cache set + response meta pattern repeated in all cache-enabled handlers.
- Example blocks:
  - `gateway/api/alphavantage/forex.py:35` to `gateway/api/alphavantage/forex.py:51`
  - `gateway/api/alphavantage/calendars.py:37` to `gateway/api/alphavantage/calendars.py:53`

Impact:
- Higher maintenance cost and drift risk when changing caching/meta behavior.
- Small repeated allocation overhead per request.

Low-risk fix path:
1. Introduce a shared helper in `gateway/api/alphavantage/common.py` for cache-hit/cache-miss response wrapping.
2. Keep endpoint contracts identical.
3. Migrate `timeseries.py` and `fundamentals.py` first (highest endpoint count).

### P1-5: Inconsistent serialization path in `timeseries.py`

Evidence:
- Most time-series endpoints use JSON-safe model dump:
  - `gateway/api/alphavantage/timeseries.py:92`
  - `gateway/api/alphavantage/timeseries.py:133`
  - `gateway/api/alphavantage/timeseries.py:173`
- Monthly endpoint uses `model_dump()` without explicit JSON mode:
  - `gateway/api/alphavantage/timeseries.py:210`.

Impact:
- Inconsistent serialization path can add avoidable conversion work downstream and makes behavior harder to reason about.

Low-risk fix path:
1. Standardize on `model_dump(mode="json")` for route-layer model serialization.
2. Validate response equivalence with snapshot/contract tests.

### P2-6 (Provider-Adjacent): CSV parsing modernization (remediated 2026-02-07)

Evidence:
- Shared helper parsing in calendars/listings:
  - `gateway/providers/alphavantage.py:873`
  - `gateway/providers/alphavantage.py:909`
  - `gateway/providers/alphavantage.py:940`

Impact:
- Extra allocations and fragile parsing for quoted-comma CSV data.

Status:
1. Replaced split-based parsing with `csv.DictReader`.
2. Preserved returned field names and list-of-dict contract.

### P2-7 (Provider-Adjacent): Multi-symbol quote fan-out (remediated 2026-02-07)

Evidence:
- `gateway/providers/alphavantage.py:218` uses bounded `asyncio.Semaphore`.
- `gateway/providers/alphavantage.py:228` uses `asyncio.gather(...)` for controlled fan-out.

Impact:
- Latency scales linearly with symbol count.

Status:
1. Bounded concurrency implemented with config clamp (`1..5`, default `2`).
2. Fail-soft behavior preserved (skip bad symbol, continue list).

## Implementation Plan to Start Addressing Issues

### Wave AV-1 (Immediate, lowest risk)

1. Reorder route logic to check cache before provider lookup in all 20 cache-enabled handlers.
2. Add a common route helper in `gateway/api/alphavantage/common.py` for standardized cached/uncached response wrapping.
3. Standardize `timeseries.py` serialization to `mode="json"` for all model dumps.

Wave status (2026-02-07):
- `AV-1` complete (shared helper added in `common.py` and applied across all Alpha Vantage route files)
- `AV-2` complete (full-output cache bypass, search-key guardrails, endpoint-level cache/payload instrumentation implemented)
- `AV-3` complete (`csv.DictReader`, bounded quote fan-out, shared fetch helper, and sort-head optimization delivered with targeted benchmark evidence)

### Wave AV-2

1. Skip caching for `outputsize=full` time-series endpoints.
2. Add cache cardinality guardrails on free-form search keys.
3. Add endpoint-level cache hit/miss and payload-size instrumentation.

### Wave AV-3

1. Replace provider split-based CSV parsing with `csv.DictReader`.
2. Introduce bounded parallelism for `get_quotes`.
3. Validate end-to-end latency/memory impact with targeted benchmark runs.

## Alpha Vantage File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Endpoints | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/api/alphavantage/timeseries.py` | 6 | COMPLETE | AV-1/AV-2 route changes complete; monitor `full` output miss-path payload and cache impact |
| `gateway/api/alphavantage/fundamentals.py` | 5 | COMPLETE | AV-1 helper migration complete; monitor endpoint-level cache hit-rate |
| `gateway/api/alphavantage/indicators.py` | 11 | COMPLETE | AV-1 helper migration complete; monitor high-cardinality indicator-key hit-rate |
| `gateway/api/alphavantage/calendars.py` | 3 | COMPLETE | AV-1 helper migration complete; validate large-result cache TTL/size policy |
| `gateway/api/alphavantage/crypto.py` | 2 | COMPLETE | AV-1 helper migration complete; validate TTL hit-rate assumptions |
| `gateway/api/alphavantage/forex.py` | 2 | COMPLETE | AV-1 helper migration complete; validate short TTL hit-rate assumptions |
| `gateway/api/alphavantage/economic.py` | 1 | COMPLETE | AV-1 helper migration complete; monitor cache cardinality |
| `gateway/api/alphavantage/common.py` | 0 | COMPLETE | AV shared helper + search-key guardrail + cache/payload instrumentation is in place |
| `gateway/api/alphavantage/__init__.py` | 0 | COMPLETE | No performance hotspots; router composition only |
| `gateway/providers/alphavantage.py` | 34 async methods | PARTIAL | AV-3 rollout complete; remaining provider follow-up is optional heavy time-series parse/sort limit tuning + broader runtime profiling |

## Future Runs (Outside Alpha Vantage)

1. Optional Alpha Vantage provider follow-up: add `max_points` style caps for heavy full-history parse/sort paths where route contracts allow.
2. Continue non-Alpha-Vantage implementation priorities from `PERFORMANCE_AUDIT.md`.
