# yfinance Performance Audit Deep Dive

Date: 2026-02-05
Auditor: Codex (GPT-5)
Primary scope: `gateway/api/yf.py`
Secondary scope: `gateway/providers/yfinance.py`

## Objective

Deliver a deep performance audit for the yfinance API surface with low-risk improvements that avoid significant logic changes, and clearly track what is fully audited now vs what remains for future runs.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| yfinance API router | 1 (`gateway/api/yf.py`) | COMPLETE | Full endpoint-level pass across all handlers |
| yfinance provider | 1 (`gateway/providers/yfinance.py`) | COMPLETE | Full provider pass with route-adjacent hotspot analysis |
| Runtime benchmark/profiler execution | N/A | PENDING | Static/code-path audit in this run |

## Inventory and Measured Hotspots

- yfinance API router size: `600` LOC.
- yfinance provider size: `386` LOC.
- yfinance route handlers (`@router.get`): `16`.
- Repeated route pattern counts in `gateway/api/yf.py`:
  - `provider = registry.get("yfinance")`: `16`
  - `cached = await cache.get(cache_key)`: `16`
  - `await require_provider_rate_limit("yfinance")`: `16`
  - `await _dedupe(cache_key, _fetch)`: `16`
  - `await cache.set(cache_key, ...)`: `16`
  - `"cached": True` response blocks: `16`
  - `"cached": False` response blocks: `16`
  - `"Provider error: {str(e)}"` handling blocks: `16`
- Router cache TTL policy:
  - Single TTL for all endpoints: `CACHE_TTL = 300` at `gateway/api/yf.py:20`.
- High-cardinality history key:
  - `gateway/api/yf.py:184` includes `period`, `interval`, `start`, `end`.
- Provider thread offloading:
  - `return await asyncio.to_thread(...)` appears `14` times.
- Provider heavy conversion patterns:
  - `yf.Ticker(symbol.upper())` appears `15` times.
  - `.to_dict(` usages: `14` total (`8` plain + `6 orient="records"`).
  - `.iterrows()` usages: `2` (`gateway/providers/yfinance.py:190`, `gateway/providers/yfinance.py:382`).

## Priority Findings (Low-Risk Changes Only)

### P0-1: Provider lookup executes before cache-hit return in every endpoint

Evidence:
- `gateway/api/yf.py:54` resolves provider before cache check at `gateway/api/yf.py:59`.
- Same pattern repeated in all 16 handlers.

Impact:
- Every cache hit still pays registry lookup overhead.
- Small per-request cost but amplified by high cache-hit traffic.

Low-risk fix path:
1. Build cache key first.
2. Return cache hit immediately.
3. Resolve provider only on cache miss.

### P0-2: Route logic is duplicated 16 times (cache + dedupe + rate-limit + response)

Evidence:
- Pattern appears identically across all handlers:
  - cache get, inner `_fetch`, rate limit, dedupe, cache set, response/meta assembly.
- Example blocks:
  - `gateway/api/yf.py:58` to `gateway/api/yf.py:73`
  - `gateway/api/yf.py:184` to `gateway/api/yf.py:211`
  - `gateway/api/yf.py:553` to `gateway/api/yf.py:569`

Impact:
- Repeated code increases maintenance overhead and drift risk.
- Additional closure allocation and repeated object assembly on each request.

Low-risk fix path:
1. Add a shared yfinance route helper for the standard cache/miss fetch flow.
2. Keep endpoint output contracts unchanged.
3. Migrate in phases: start with the highest-traffic handlers (`history`, `ticker`, `options`).

### P1-3: History endpoint creates high-cardinality cache keys and caches heavy payloads at fixed 300s TTL

Evidence:
- Cache key includes variable date window and interval:
  - `gateway/api/yf.py:184`.
- Only one global TTL for all endpoints:
  - `gateway/api/yf.py:20`.

Impact:
- Large number of low-reuse keys for custom date-range history queries.
- Potential memory pressure from caching large history payloads with low hit probability.

Low-risk fix path:
1. Keep fixed TTL for high-reuse endpoints, but use shorter/conditional TTL for custom date-range history keys.
2. Optionally skip caching for highly unique history requests (custom start/end).
3. Instrument key cardinality + hit-rate by endpoint.

### P1-4: History path does expensive row iteration + model conversion

Evidence:
- Provider iterates DataFrame rows via `iterrows`:
  - `gateway/providers/yfinance.py:190`.
- Route then serializes each model:
  - `gateway/api/yf.py:200`.

Impact:
- CPU overhead from per-row pandas iteration plus model serialization.
- Cost grows with longer periods/finer intervals.

Low-risk fix path:
1. Replace `iterrows()` with `itertuples()` in provider history normalization.
2. Keep response schema unchanged.
3. Validate bar ordering and value parity with existing tests.

### P1-5: Provider health check performs blocking yfinance call directly on event loop

Evidence:
- `gateway/providers/yfinance.py:79` to `gateway/providers/yfinance.py:80` accesses `yf.Ticker(...).info` synchronously in `health_check`.

Impact:
- Health endpoints can block event loop during yfinance network/scrape latency.
- Adds tail-latency risk when health checks run with other async operations.

Low-risk fix path:
1. Move health-check ticker fetch into `asyncio.to_thread`.
2. Preserve current health response semantics.

### P2-6: Repeated `yf.Ticker(...)` construction and full-frame `.to_dict()` conversions

Evidence:
- `yf.Ticker(symbol.upper())` appears in nearly every provider method.
- Full frame conversions:
  - financial/holders/actions/sustainability methods use `.to_dict(...)` across large tables (`gateway/providers/yfinance.py:136`, `gateway/providers/yfinance.py:229`, `gateway/providers/yfinance.py:263`, `gateway/providers/yfinance.py:332`).

Impact:
- Repeated object creation and broad conversions increase CPU/memory usage.

Low-risk fix path:
1. Introduce one provider helper to construct ticker instance and reuse method-local conversion code.
2. For large frames, bound returned rows where endpoint contract permits.
3. Keep output shape stable for all current routes.

## Implementation Plan to Start Addressing Issues

### Wave YF-1 (Immediate, lowest risk)

1. Reorder route flow to check cache before provider lookup in all 16 endpoints.
2. Add shared route helper for cache/dedupe/rate-limit/response pattern.
3. Move `health_check` yfinance call into `asyncio.to_thread`.

### Wave YF-2

1. Replace provider `iterrows()` usage with `itertuples()` in history and major-holders transforms.
2. Introduce endpoint-specific TTL policy (or selective no-cache) for low-reuse custom history windows.
3. Add cache key-cardinality and hit-rate metrics per yfinance endpoint.

Wave status (2026-02-06):
- `1` complete (`get_history` and `get_major_holders` migrated to `itertuples`)
- `2` pending
- `3` pending

### Wave YF-3

1. Consolidate ticker construction and conversion helpers in provider.
2. Review/bound very large table conversions where endpoint semantics allow.
3. Validate p50/p95 latency and memory impact with benchmark runs.

## yfinance File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Endpoints/Methods | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/api/yf.py` | 16 endpoints | COMPLETE | Route helper migration, cache-before-provider reorder, history cache policy tuning |
| `gateway/providers/yfinance.py` | 19 async methods | COMPLETE | `itertuples` migration complete; health-check offload and conversion helper consolidation pending |

## Future Runs (Outside yfinance)

1. Deep route audit for `gateway/api/sec.py`.
2. Implement UW Wave 1 from `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md`.
3. Implement Alpha Vantage Wave 1 from `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md`.
4. Build benchmark harness for middleware + stream fanout + provider-heavy routes.
