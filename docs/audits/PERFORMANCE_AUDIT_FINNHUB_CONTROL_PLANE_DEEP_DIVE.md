# Finnhub + Control-Plane Performance Audit Deep Dive

Date: 2026-02-05
Auditor: Codex (GPT-5)
Primary scope: `gateway/api/finnhub/*`, `gateway/api/admin.py`, `gateway/api/catalog.py`, `gateway/api/health.py`
Secondary scope: `gateway/providers/finnhub.py`, `gateway/core/registry.py`

## Objective

Complete the next pending router audit block with low-risk performance findings, preserve existing behavior, and track remaining unaudited areas for future runs.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| Finnhub API routers | 12 files | COMPLETE | Full endpoint-level pass across all Finnhub route modules |
| Control-plane routers (`admin`, `catalog`, `health`) | 3 files | COMPLETE | Full endpoint-level pass |
| Finnhub provider | 1 file (`gateway/providers/finnhub.py`) | COMPLETE | Full provider pass for route-adjacent hotspots |
| Runtime profiling/benchmarks | N/A | PENDING | Static/code-path audit in this run |

## Inventory and Measured Hotspots

- Finnhub route package size: `1905` LOC, `45` endpoints.
- Control-plane route size:
  - `gateway/api/admin.py`: `287` LOC, `7` endpoints.
  - `gateway/api/catalog.py`: `761` LOC, `6` endpoints.
  - `gateway/api/health.py`: `102` LOC, `3` endpoints.
- Pending Alpaca route block remains large for future run: `14` files, `60` endpoints, `2032` LOC.

Finnhub router repetition counts:
- `provider = registry.get("finnhub")`: `45`
- `cached = await cache.get(key)`: `45`
- `await require_provider_rate_limit("finnhub")`: `45`
- `await cache.set(key, ...)`: `45`
- `"cached": True` response blocks: `45`
- `"Provider error: {str(e)}"` wrappers: `45`
- In-flight dedupe usage: `0`

Finnhub router TTL mix (`cache.set`):
- `86400`: `16`
- `3600`: `21`
- `300`: `5`
- `1800`: `1`
- `60`: `1`
- `CACHE_TTL` constant usage: `1`

Finnhub provider context:
- File size: `1280` LOC, `49` async methods.
- `response = await self._client.get(...)`: `46`
- `response.raise_for_status()`: `45`
- `return response.json()` direct passthroughs: `26`
- Sequential multi-quote loop: `gateway/providers/finnhub.py:140`
- Index-based bar loop: `gateway/providers/finnhub.py:210`

## Priority Findings (Low-Risk Changes Only)

### P0-1: Provider lookup occurs before cache-hit return on all Finnhub endpoints

Evidence:
- `gateway/api/finnhub/quotes.py:31` resolves provider before cache check at `gateway/api/finnhub/quotes.py:36`.
- Same pattern repeated across all 45 Finnhub endpoints.

Impact:
- Every cache hit still pays registry lookup overhead.

Low-risk fix path:
1. Build key and check cache first.
2. Resolve provider only on cache miss.
3. Keep response contracts unchanged.

### P0-2: No in-flight request deduplication on Finnhub routes

Evidence:
- No dedupe usage in Finnhub route package (`0` matches for dedupe calls).
- Contrast: yfinance router already deduplicates by cache key (`gateway/api/yf.py:41` to `gateway/api/yf.py:43`).

Impact:
- Concurrent cold-key bursts can issue duplicate upstream calls (thundering herd behavior).

Low-risk fix path:
1. Reuse existing `gateway/core/dedup.py` with `key` as dedupe key.
2. Apply first to high-volume routes (`/quote`, `/bars`, `/news`, `/earnings`).

### P1-3: Date-window routes create high-cardinality keys; TTLs are often long

Evidence:
- Key includes start/end in multiple routes:
  - `gateway/api/finnhub/quotes.py:78`
  - `gateway/api/finnhub/news.py:36`
  - `gateway/api/finnhub/alternative.py:70`
  - `gateway/api/finnhub/forex.py:139`
- TTL distribution includes many long-lived entries (`86400` and `3600` dominate).

Impact:
- Low-reuse date combinations can fill in-memory cache with entries unlikely to be reused.

Low-risk fix path:
1. Shorten TTL or bypass cache for custom date-window requests.
2. Keep long TTL for static lists (`exchanges`, profile metadata) where hit-rate is high.
3. Add per-prefix key cardinality/hit-rate metrics.

### P1-4: Repeated datetime parsing in route layer (9 start + 9 end conversions)

Evidence:
- Repeated `datetime.fromisoformat(start/end)` blocks across 9 routes, for example:
  - `gateway/api/finnhub/news.py:47`
  - `gateway/api/finnhub/quotes.py:89`
  - `gateway/api/finnhub/alternative.py:81`

Impact:
- Extra repetitive parsing/branching overhead and duplicated validation paths.

Low-risk fix path:
1. Add a shared parse helper in `gateway/api/finnhub/common.py`.
2. Normalize parsing/validation once and reuse.

### P1-5: Admin status endpoints depend on sequential provider health checks

Evidence:
- `gateway/api/admin.py:97` and `gateway/api/admin.py:214` call `registry.health_check_all()`.
- `gateway/core/registry.py:126` to `gateway/core/registry.py:134` runs health checks sequentially.

Impact:
- Admin status latency grows with sum of provider latencies.

Low-risk fix path:
1. Switch `health_check_all` to bounded concurrent gather.
2. Preserve error handling with `return_exceptions=True` semantics.

### P1-6: Catalog endpoints return very large static objects each request

Evidence:
- Full stream catalog is returned at `gateway/api/catalog.py:577`.
- Full provider catalog is returned at `gateway/api/catalog.py:720`.
- Key lists are rebuilt per request (`list(...keys())`) at `gateway/api/catalog.py:591`, `gateway/api/catalog.py:735`, `gateway/api/catalog.py:755`, `gateway/api/catalog.py:758`.

Impact:
- Repeated JSON serialization of large static payloads increases CPU/bandwidth on catalog-heavy clients.

Low-risk fix path:
1. Cache serialized catalog responses in process (or short TTL cache entry).
2. Add optional gzip/etag behavior for catalog routes.
3. Keep output schema exactly the same.

### P2-7: Admin recent-logs endpoint copies deque to list every request

Evidence:
- `gateway/api/admin.py:134` does `list(_error_buffer)[-limit:]`.

Impact:
- Avoidable allocation overhead under heavy admin polling.

Low-risk fix path:
1. Convert only the required slice from the right side (e.g., `itertools.islice` on reversed deque).
2. Keep endpoint response unchanged.

### P2-8 (Provider-adjacent): Sequential quote fan-out and index-based bar loop

Evidence:
- Sequential multi-quote loop: `gateway/providers/finnhub.py:140` to `gateway/providers/finnhub.py:147`.
- Index-based bar construction: `gateway/providers/finnhub.py:210` to `gateway/providers/finnhub.py:223`.

Impact:
- Multi-symbol quote latency scales linearly.
- Index-heavy bar loop is slightly slower and less robust to uneven arrays.

Low-risk fix path:
1. Use bounded concurrency in `get_quotes`.
2. Replace index loop with `zip(..., strict=False)` in bar normalization.

## Implementation Plan to Start Addressing Issues

### Wave FH-CP-1 (Immediate, lowest risk)

1. Reorder Finnhub routes to check cache before provider lookup.
2. Add shared Finnhub route helper for cache/miss/rate-limit/response.
3. Introduce shared date parser helper for start/end query params.

### Wave FH-CP-2

1. Add in-flight dedupe on Finnhub routes.
2. Apply selective caching policy for high-cardinality date-window keys.
3. Parallelize `registry.health_check_all` for admin endpoints.

Wave status (2026-02-06):
- `1` pending
- `2` pending
- `3` complete (`gateway/core/registry.py::health_check_all` now runs provider checks concurrently)

### Wave FH-CP-3

1. Add lightweight response caching/etag strategy for large catalog endpoints.
2. Optimize admin recent-log slicing path.
3. Apply provider-level quote fan-out and bar-loop micro-optimizations.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Endpoints | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/api/finnhub/__init__.py` | 0 | COMPLETE | No hotspot, router composition only |
| `gateway/api/finnhub/common.py` | 0 | COMPLETE | Add shared helpers (request flow + date parsing) |
| `gateway/api/finnhub/quotes.py` | 2 | COMPLETE | Dedup + cache-before-provider + key-cardinality policy |
| `gateway/api/finnhub/news.py` | 2 | COMPLETE | Dedup + date-window cache policy |
| `gateway/api/finnhub/fundamentals.py` | 8 | COMPLETE | Helper consolidation + TTL tuning |
| `gateway/api/finnhub/earnings.py` | 7 | COMPLETE | Helper consolidation + date parser reuse |
| `gateway/api/finnhub/analysis.py` | 5 | COMPLETE | Helper consolidation |
| `gateway/api/finnhub/alternative.py` | 4 | COMPLETE | Date parser reuse + key policy |
| `gateway/api/finnhub/crypto.py` | 4 | COMPLETE | Date parser reuse + TTL tuning |
| `gateway/api/finnhub/forex.py` | 4 | COMPLETE | Date parser reuse + TTL tuning |
| `gateway/api/finnhub/etf.py` | 6 | COMPLETE | Helper consolidation + static-data cache policy |
| `gateway/api/finnhub/funds.py` | 3 | COMPLETE | Helper consolidation + static-data cache policy |
| `gateway/api/admin.py` | 7 | COMPLETE | Parallel health checks + log slice optimization |
| `gateway/api/catalog.py` | 6 | COMPLETE | Large static response caching/etag |
| `gateway/api/health.py` | 3 | COMPLETE | Ready-check instrumentation only |
| `gateway/providers/finnhub.py` | 49 async methods | COMPLETE | Bounded multi-quote concurrency + bar-loop cleanup |
| `gateway/core/registry.py` | health-check paths | COMPLETE | `health_check_all` concurrency implemented; monitor admin/status latency trends |

## Remaining Audit Scope (Future Runs)

1. Full route-level deep pass for `gateway/api/alpaca/*` (14 files, 60 endpoints).
2. Deep provider audits for remaining partial providers (`gateway/providers/alpaca.py`, `gateway/providers/alphavantage.py`, `gateway/providers/news.py`).
3. Full deep profiling for sampled core modules (`security`, `quality`, `calendar`, `symbology`, `validator`) plus benchmark harness work.
