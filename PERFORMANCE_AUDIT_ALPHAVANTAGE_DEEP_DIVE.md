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
| Alpha Vantage provider | 1 file (`gateway/providers/alphavantage.py`) | PARTIAL | Reviewed route-adjacent latency/parse hotspots; no runtime profiling yet |
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
  - Provider LOC: `1082`.
  - `response = await self._client.get(...)` call sites: `21`.
  - `"Note"` rate-limit checks: `16`.
  - Manual CSV parse blocks (`split("\n")` + `split(",")`): `3`.
  - Multi-symbol quote fetch loop is sequential: `gateway/providers/alphavantage.py:168`.

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

### P2-6 (Provider-Adjacent): CSV parsing is manual and allocates heavily

Evidence:
- Manual CSV parsing with split logic:
  - `gateway/providers/alphavantage.py:1014` to `gateway/providers/alphavantage.py:1018`
  - `gateway/providers/alphavantage.py:1038` to `gateway/providers/alphavantage.py:1042`
  - `gateway/providers/alphavantage.py:1074` to `gateway/providers/alphavantage.py:1078`

Impact:
- Extra allocations and fragile parsing for quoted-comma CSV data.

Low-risk fix path:
1. Replace split-based parsing with `csv.DictReader`.
2. Keep returned field names and list-of-dict contract unchanged.

### P2-7 (Provider-Adjacent): Multi-symbol quote retrieval is sequential

Evidence:
- `gateway/providers/alphavantage.py:165` to `gateway/providers/alphavantage.py:175` loops symbols serially.

Impact:
- Latency scales linearly with symbol count.

Low-risk fix path:
1. Use bounded concurrency with a semaphore.
2. Keep fail-soft behavior (skip bad symbol, continue list).
3. Cap concurrency to respect Alpha Vantage free-tier constraints.

## Implementation Plan to Start Addressing Issues

### Wave AV-1 (Immediate, lowest risk)

1. Reorder route logic to check cache before provider lookup in all 20 cache-enabled handlers.
2. Add a common route helper in `gateway/api/alphavantage/common.py` for standardized cached/uncached response wrapping.
3. Standardize `timeseries.py` serialization to `mode="json"` for all model dumps.

Wave status (2026-02-06):
- `AV-1` in progress (shared helper added in `common.py`; `timeseries.py` and `fundamentals.py` migrated to cache-first helper flow; remaining Alpha Vantage route files pending migration)
- `AV-2` pending
- `AV-3` pending

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
| `gateway/api/alphavantage/timeseries.py` | 6 | COMPLETE | AV-1 helper migration complete; AV-2 full-output cache policy and search key guardrails pending |
| `gateway/api/alphavantage/fundamentals.py` | 5 | COMPLETE | AV-1 helper migration complete; monitor endpoint-level cache hit-rate |
| `gateway/api/alphavantage/indicators.py` | 11 | COMPLETE | Helper migration and high-cardinality key monitoring |
| `gateway/api/alphavantage/calendars.py` | 3 | COMPLETE | Helper migration and large-result cache policy checks |
| `gateway/api/alphavantage/crypto.py` | 2 | COMPLETE | Helper migration |
| `gateway/api/alphavantage/forex.py` | 2 | COMPLETE | Helper migration |
| `gateway/api/alphavantage/economic.py` | 1 | COMPLETE | Helper migration |
| `gateway/api/alphavantage/common.py` | 0 | COMPLETE | AV shared helper is in place; continue rolling migration to remaining route files |
| `gateway/api/alphavantage/__init__.py` | 0 | COMPLETE | No performance hotspots; router composition only |
| `gateway/providers/alphavantage.py` | 34 async methods | PARTIAL | CSV parsing modernization, bounded multi-quote concurrency, benchmark validation |

## Future Runs (Outside Alpha Vantage)

1. Complete AV-1 route-helper migration in `gateway/api/alphavantage/{indicators,calendars,crypto,forex,economic}.py`.
2. Implement AV-2 (`outputsize=full` cache policy, search-key cardinality guardrails, and cache/payload metrics).
3. Implement AV-3 provider optimizations in `gateway/providers/alphavantage.py` (`csv.DictReader`, bounded quote fan-out, benchmark validation).
4. Continue non-Alpha-Vantage implementation priorities from `PERFORMANCE_AUDIT.md`.
