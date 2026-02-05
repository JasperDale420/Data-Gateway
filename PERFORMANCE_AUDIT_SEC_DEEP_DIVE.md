# SEC Performance Audit Deep Dive

Date: 2026-02-05
Auditor: Codex (GPT-5)
Primary scope: `gateway/api/sec.py`
Secondary scope: `gateway/providers/sec.py`

## Objective

Deliver a deep performance audit for SEC EDGAR endpoints with low-risk improvements that preserve existing behavior, and clearly mark audited areas vs future follow-up.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| SEC API router | 1 (`gateway/api/sec.py`) | COMPLETE | Full endpoint-level pass |
| SEC provider | 1 (`gateway/providers/sec.py`) | COMPLETE | Full provider pass with route-adjacent hotspot review |
| Runtime profiling/benchmarks | N/A | PENDING | Static/code-path audit in this run |

## Inventory and Measured Hotspots

- SEC router size: `347` LOC.
- SEC provider size: `434` LOC.
- SEC route handlers (`@router.get`): `10`.
- Repeated route pattern counts in `gateway/api/sec.py`:
  - `provider = registry.get("sec")`: `10`
  - `cached = await cache.get(cache_key)`: `10`
  - `await require_provider_rate_limit("sec")`: `10`
  - `await cache.set(cache_key, ...)`: `10`
  - `"cached": True` response blocks: `10`
  - `"cached": False` response blocks: `10`
  - `"Provider error: {str(e)}"` handling blocks: `10`
- Route cache key call sites: `10`.
- Route cache TTL distribution:
  - `CACHE_TTL` (`3600s`): `9`
  - `300s`: `1` (search endpoint)
- Provider outbound request call sites: `9` (`self._client.get(...)`).
- Provider repeats same submissions endpoint call in multiple methods:
  - `gateway/providers/sec.py:152`
  - `gateway/providers/sec.py:195`
  - `gateway/providers/sec.py:256`

## Priority Findings (Low-Risk Changes Only)

### P0-1: Provider lookup happens before cache-hit return in all endpoints

Evidence:
- `gateway/api/sec.py:46` resolves provider before cache check at `gateway/api/sec.py:51`.
- Same pattern appears in every route.

Impact:
- Each cache hit still pays registry lookup overhead.
- Small per-request cost amplified under heavy cache-hit traffic.

Low-risk fix path:
1. Build cache key first.
2. Return cache-hit response early.
3. Resolve provider only on cache miss.

### P0-2: Route boilerplate is repeated across all 10 handlers

Evidence:
- Repeated pattern: provider lookup -> cache get -> rate limit -> provider call -> cache set -> response.
- Example blocks:
  - `gateway/api/sec.py:50` to `gateway/api/sec.py:61`
  - `gateway/api/sec.py:108` to `gateway/api/sec.py:124`
  - `gateway/api/sec.py:331` to `gateway/api/sec.py:347`

Impact:
- Increased maintenance cost and drift risk.
- Repeated allocation/closure work per request.

Low-risk fix path:
1. Add shared SEC route helper for cache/miss flow and standardized response meta.
2. Keep response contracts exactly the same.

### P1-3: No in-flight deduplication for SEC cold-key bursts

Evidence:
- `gateway/api/sec.py` has no `get_deduplicator`/`dedupe` usage.
- yfinance routes already use request dedupe (`gateway/api/yf.py:41` to `gateway/api/yf.py:43`).

Impact:
- Concurrent requests for the same uncached SEC key can trigger duplicate upstream calls (thundering herd).

Low-risk fix path:
1. Reuse existing deduplicator (`gateway/core/dedup.py`) with cache key as dedupe key.
2. Apply to highest-traffic SEC routes first (`/filings`, `/facts`, `/search`).

### P1-4: Case normalization drift in filing cache keys can fragment cache hits

Evidence:
- Query-form endpoint key uses raw `form_type`:
  - `gateway/api/sec.py:108`.
- Path-form endpoint key uppercases `form_type`:
  - `gateway/api/sec.py:141`.
- Provider filtering uppercases form type internally:
  - `gateway/providers/sec.py:213`.

Impact:
- Same logical request can produce multiple cache keys when case differs (e.g., `10-k` vs `10-K`).

Low-risk fix path:
1. Normalize `form_type` in cache key generation for all filing routes (e.g., uppercase once).
2. Keep endpoint behavior and response content unchanged.

### P1-5: Raw search query in cache key increases key cardinality

Evidence:
- Search key uses raw `q` string:
  - `gateway/api/sec.py:331`.

Impact:
- Whitespace/case variants of the same query generate separate keys and reduce cache reuse.

Low-risk fix path:
1. Normalize query in key path (trim, lowercase, collapse whitespace).
2. Keep stored response payload unchanged.

### P2-6: Large XBRL payloads cached in count-based in-memory cache

Evidence:
- Large payload route caching:
  - company facts route caches `facts` payload (`gateway/api/sec.py:235`, `gateway/api/sec.py:243`).
  - provider returns full facts dictionary (`gateway/providers/sec.py:306`).
- In-memory cache limits by entry count, not payload bytes:
  - `gateway/core/cache.py:41` and `gateway/core/cache.py:159`.

Impact:
- A few very large SEC XBRL entries can consume disproportionate memory.

Low-risk fix path:
1. Add payload-size guard or endpoint-specific cache policy for very large SEC responses.
2. Preserve response contracts while preventing oversized cache entries.

### P2-7: Repeated submissions fetch logic can be centralized

Evidence:
- Similar submissions fetch in multiple provider methods:
  - `gateway/providers/sec.py:152`
  - `gateway/providers/sec.py:195`
  - `gateway/providers/sec.py:256`

Impact:
- Duplicated request/parsing logic increases maintenance and error-surface area.

Low-risk fix path:
1. Add provider helper like `_get_submissions(cik)` to centralize fetch + parse path.
2. Keep method outputs unchanged.

## Implementation Plan to Start Addressing Issues

### Wave SEC-1 (Immediate, lowest risk)

1. Reorder SEC routes to check cache before provider lookup.
2. Add shared SEC route helper for common cache/miss/response flow.
3. Normalize `form_type` cache key generation in filings routes.

### Wave SEC-2

1. Add in-flight deduplication (same approach as yfinance).
2. Normalize search query key generation.
3. Add SEC endpoint cache-hit/miss metrics and key-cardinality instrumentation.

### Wave SEC-3

1. Add cache payload-size guard for large XBRL responses.
2. Consolidate provider submissions fetch logic in a shared helper.
3. Validate latency/memory impacts via benchmark runs.

## SEC File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Endpoints/Methods | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/api/sec.py` | 10 endpoints | COMPLETE | Route helper migration, dedupe integration, cache key normalization |
| `gateway/providers/sec.py` | 14 async methods | COMPLETE | Submissions helper consolidation, large-payload guard strategy |

## Future Runs (Outside SEC)

1. Implement Wave 1 for UW/Alpha Vantage/yfinance/SEC deep-dive findings.
2. Full route-by-route deep pass for sampled API groups (`gateway/api/alpaca/*`, `gateway/api/finnhub/*`, `gateway/api/admin.py`, `gateway/api/catalog.py`, `gateway/api/health.py`).
3. Deep provider audits for remaining partial providers (`alpaca`, `finnhub`, `alphavantage`, `news`).
4. Build benchmark harness for middleware, fanout, and large-payload endpoint behavior.
