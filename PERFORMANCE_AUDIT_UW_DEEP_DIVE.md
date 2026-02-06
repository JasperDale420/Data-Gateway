# Unusual Whales Performance Audit Deep Dive

Date: 2026-02-05
Auditor: Codex (GPT-5)
Scope: `gateway/providers/uw.py` and all `gateway/api/uw/*.py` routes

## Objective

Identify low-risk performance improvements for the UW provider/router surface without significant logic changes, and document what is fully audited now vs what remains for future runs.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| UW provider internals | 1 (`gateway/providers/uw.py`) | COMPLETE | Full file pass with hotspot and repetition analysis |
| UW API routers | 26 (`gateway/api/uw/*.py`) | COMPLETE | Route-level pattern audit across all handlers |
| UW runtime profiling | N/A | PENDING | No benchmark/profiler run yet; static/code-path audit only |

## Inventory and Measured Hotspots

- UW router files: 26, total `4122` LOC.
- UW provider file: `4672` LOC.
- UW API handlers (`@router.*`): `125`.
- Repeated route pattern counts in UW routers:
  - `cached = await cache.get(cache_key)`: `125`
  - `await require_provider_rate_limit("unusual_whales")`: `125`
  - `await cache.set(cache_key, ...)`: `125`
- Cursor pagination endpoints: `7` (`flow.py` + `market.py`).
- `fetch_limit = limit + offset + 1` usage: `7`.
- UW route-side `model_dump(mode="json")` calls: `29`.
- Provider async methods in `gateway/providers/uw.py`: `136`.
- Provider `_call_sync(...)` invocations: `136`.
- Provider `_extract_data(response)` loops/calls: `103`.
- Dynamic lambda getter pattern (`lambda k, d=None...`): `59`.
- UW route cache TTL distribution:
  - `ttl=30`: 6
  - `ttl=60`: 33
  - `ttl=300`: 61
  - `ttl=3600`: 24
  - `ttl=86400`: 1

## Priority Findings (Low-Risk Changes Only)

### P0-1: Offset pagination over-fetches linearly with cursor depth

Evidence:
- `gateway/api/uw/common.py:34` to `gateway/api/uw/common.py:53` uses offset-based slicing after data is fetched.
- `gateway/api/uw/flow.py:39` to `gateway/api/uw/flow.py:41` computes `fetch_limit = limit + offset + 1`.
- `gateway/api/uw/market.py:41` to `gateway/api/uw/market.py:43` does the same over-fetch strategy.

Impact:
- Deep cursor pages can require increasingly large fetches and in-memory slicing, producing avoidable provider/network/CPU load.

Low-risk fix path:
1. Keep current cursor contract, but add a capped offset guard (for example, configurable max cursor depth) to avoid worst-case requests.
2. Where SDK endpoint supports native pagination params, pass offset/page to provider call instead of over-fetching.
3. Keep fallback over-fetch behavior only for endpoints lacking native pagination support.

### P0-2: Route boilerplate repeated in all 125 handlers

Evidence:
- Common pattern repeats in every route:
  - cache lookup (`gateway/api/uw/stock.py:30`)
  - provider lookup (`gateway/api/uw/stock.py:34`)
  - provider rate limit (`gateway/api/uw/stock.py:35`)
  - cache set (`gateway/api/uw/stock.py:44`)
- Same shape appears in `gateway/api/uw/options.py:33` to `gateway/api/uw/options.py:47` and `gateway/api/uw/intelligence.py:32` to `gateway/api/uw/intelligence.py:46`.

Impact:
- Small per-request overhead repeated at scale.
- High maintenance cost and drift risk when behavior changes (cache policy, metadata shape, logging).

Low-risk fix path:
1. Add a UW route helper in `gateway/api/uw/common.py` to centralize cache-get/rate-limit/call/cache-set flow.
2. Migrate highest-traffic routes first (`flow.py`, `market.py`, `stock.py`), then roll out to remaining files.
3. Keep endpoint response JSON shape identical to avoid contract churn.

### P1-3: Every SDK call is thread-offloaded; concurrency may pressure threadpool

Evidence:
- `_call_sync` wraps all SDK calls with `asyncio.to_thread`: `gateway/providers/uw.py:172` to `gateway/providers/uw.py:174`.
- `_call_sync(...)` call sites: `136` in the same file.

Impact:
- Under high request concurrency, threadpool contention/context-switching can add latency spikes.

Low-risk fix path:
1. Add provider-level semaphore around `_call_sync` to bound concurrent SDK calls.
2. Make bound configurable via settings (for example `uw_max_inflight_calls`).
3. Add metric for queue wait time before thread offload.

### P1-4: Normalization loops repeatedly allocate dynamic getter lambdas

Evidence:
- Dynamic getter appears in many loops, for example:
  - `gateway/providers/uw.py:410` to `gateway/providers/uw.py:414`
  - `gateway/providers/uw.py:448` to `gateway/providers/uw.py:452`
  - `gateway/providers/uw.py:501` to `gateway/providers/uw.py:505`
- Count across file: `59` occurrences.

Impact:
- Repeated closure allocations and repeated attribute/dict fallback logic add CPU overhead in large payload normalization.

Low-risk fix path:
1. Replace inline lambda pattern with one shared helper (for example `_field(item, key, default=None)`).
2. Keep normalization outputs unchanged.
3. Apply first in largest/high-traffic normalization methods, then batch-apply across file.

### P1-5: Repeated model serialization work in router layer

Evidence:
- Router-level model dumps in hot endpoints:
  - `gateway/api/uw/flow.py:42`
  - `gateway/api/uw/flow.py:71`
  - `gateway/api/uw/options.py:43`
  - `gateway/api/uw/common.py:104`

Impact:
- Additional CPU allocation/serialization overhead per request.

Low-risk fix path:
1. Centralize serialization via one helper in `gateway/api/uw/common.py`.
2. Avoid re-serializing if provider already returns plain dicts.
3. Keep final response schema unchanged.

### P2-6: Cursor in cache keys increases cardinality and reduces cache reuse

Evidence:
- Cursor-partitioned keys:
  - `gateway/api/uw/flow.py:32`
  - `gateway/api/uw/flow.py:61`
  - `gateway/api/uw/flow.py:87`
  - `gateway/api/uw/flow.py:116`
  - `gateway/api/uw/market.py:34`
  - `gateway/api/uw/market.py:61`
  - `gateway/api/uw/market.py:88`

Impact:
- Deep-page reads generate many low-reuse keys and reduce cache efficiency.

Low-risk fix path:
1. Keep caching for first page(s) where reuse is highest.
2. Optionally skip caching for deep cursor pages above configurable threshold.
3. Track hit-rate by endpoint and cursor-depth before broad rollout.

### P2-7: Post-fetch list slicing after full extraction in provider wrapper methods

Evidence:
- Full data extract then local slice:
  - `gateway/providers/uw.py:3382` to `gateway/providers/uw.py:3385`
  - `gateway/providers/uw.py:3564` to `gateway/providers/uw.py:3567`
  - `gateway/providers/uw.py:3650` to `gateway/providers/uw.py:3653`
  - `gateway/providers/uw.py:3811` to `gateway/providers/uw.py:3814`
  - `gateway/providers/uw.py:3846` to `gateway/providers/uw.py:3849`

Impact:
- Unnecessary in-memory work when SDK can support limiting at source.

Low-risk fix path:
1. Pass native `limit/page` params into SDK calls where available.
2. Keep local slicing only as fallback when endpoint lacks pagination support.

## Implementation Plan to Start Addressing Issues

### Wave UW-1 (Immediate, Lowest Risk)

1. Add instrumentation and guardrails:
- Add metric for `_call_sync` queue wait and execution latency.
- Add configurable max cursor depth for offset-paginated endpoints.
- Add per-endpoint cache hit/miss counter tags for UW routes.

2. Introduce shared route helper:
- Implement helper in `gateway/api/uw/common.py` for cache/rate-limit/provider-call response flow.
- Refactor `flow.py`, `market.py`, `stock.py` first.

3. Introduce shared field accessor helper:
- Replace dynamic lambda getter pattern in top 10 high-use normalization methods.

Wave status (2026-02-06):
- `1` in progress (cursor-depth guardrail + shared UW cached route helper implemented)
- `2` in progress (applied to `flow.py`, `market.py`, `stock.py`, `options.py`, `misc.py`, `alerts.py`, and `insiders.py`; remaining UW routes pending rollout)
- `3` pending

### Wave UW-2

1. Replace over-fetch pagination where SDK supports direct paging.
2. Apply shared route helper to remaining UW route files.
3. Reduce redundant model serialization paths.

### Wave UW-3

1. Add bounded SDK concurrency gate around `_call_sync`.
2. Tune per-endpoint TTL policy based on real hit-rate.
3. Expand to provider-wide normalization cleanup.

## UW File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Endpoints | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/providers/uw.py` | 136 async methods | COMPLETE | Apply shared accessor helper, add bounded `_call_sync` concurrency, profile normalization hotspots |
| `gateway/api/uw/stock.py` | 23 | COMPLETE | Shared route helper migration complete; validate TTL policy per endpoint with hit-rate metrics |
| `gateway/api/uw/contracts.py` | 9 | COMPLETE | Shared helper migration |
| `gateway/api/uw/calendar.py` | 8 | COMPLETE | Shared helper migration |
| `gateway/api/uw/extended.py` | 8 | COMPLETE | Shared helper migration and serialization dedupe |
| `gateway/api/uw/flow_analytics.py` | 6 | COMPLETE | Shared helper migration |
| `gateway/api/uw/institutions.py` | 6 | COMPLETE | Shared helper migration and source-limit checks |
| `gateway/api/uw/misc.py` | 6 | COMPLETE | Shared helper migration complete; review endpoint-specific TTL policy |
| `gateway/api/uw/market_data.py` | 5 | COMPLETE | Shared helper migration |
| `gateway/api/uw/etf_extended.py` | 4 | COMPLETE | Shared helper migration |
| `gateway/api/uw/flow.py` | 4 | COMPLETE | Replace over-fetch pagination first |
| `gateway/api/uw/intelligence.py` | 4 | COMPLETE | Shared helper migration |
| `gateway/api/uw/market.py` | 4 | COMPLETE | Shared helper migration complete; replace over-fetch with native paging when available |
| `gateway/api/uw/options.py` | 4 | COMPLETE | Shared helper migration complete; keep serialization dedupe in shared response path |
| `gateway/api/uw/politicians.py` | 4 | COMPLETE | Shared helper migration |
| `gateway/api/uw/volatility.py` | 4 | COMPLETE | Serialization dedupe |
| `gateway/api/uw/alerts.py` | 3 | COMPLETE | Shared helper migration complete; keep pagination path centralized |
| `gateway/api/uw/earnings.py` | 3 | COMPLETE | Serialization dedupe |
| `gateway/api/uw/etf.py` | 3 | COMPLETE | Serialization dedupe |
| `gateway/api/uw/greeks.py` | 3 | COMPLETE | Serialization dedupe |
| `gateway/api/uw/options_data.py` | 3 | COMPLETE | Shared helper migration |
| `gateway/api/uw/shorts.py` | 3 | COMPLETE | Serialization dedupe |
| `gateway/api/uw/screener.py` | 2 | COMPLETE | Serialization dedupe |
| `gateway/api/uw/seasonality.py` | 2 | COMPLETE | Serialization dedupe |
| `gateway/api/uw/insiders.py` | 4 | COMPLETE | Shared helper migration complete; validate TTLs by endpoint hit-rate |
| `gateway/api/uw/common.py` | 0 | COMPLETE | Add shared UW handler wrapper + serializer helper |
| `gateway/api/uw/__init__.py` | 0 | COMPLETE | No perf hotspots; keep as router composition only |

## Future Runs (Outside UW)

These areas remain open for deep pass in later runs:

1. `gateway/api/alphavantage/*` full route-level performance audit.
2. `gateway/api/yf.py` route-level audit paired with provider-level yfinance optimization validation.
3. Stream throughput benchmark harness for `gateway/core/stream.py` and middleware stack.
4. Memory-profile validation for `gateway/core/bulk.py` and `gateway/core/replay.py`.
