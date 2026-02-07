# Alpaca Router Performance Audit Deep Dive

Date: 2026-02-06
Auditor: Codex (GPT-5)
Primary scope: `gateway/api/alpaca/*`
Secondary scope: route-adjacent call paths in `gateway/providers/alpaca.py`

## Objective

Complete a full route-level Alpaca performance audit with low-risk recommendations that preserve API behavior, and clearly track what remains for future runs.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| Alpaca API routers | 14 files | COMPLETE | Full endpoint-level pass across all Alpaca route modules |
| Alpaca provider (route-adjacent methods only) | 1 file (`gateway/providers/alpaca.py`) | PARTIAL | Verified route-coupled hotspots only; full provider deep pass still pending |
| Runtime profiling/benchmarks | N/A | PENDING | Static/code-path audit in this run |

## Inventory and Measured Hotspots

- Alpaca route package size: `2032` LOC, `60` endpoints.
- Endpoint mix:
  - `48` GET (read-heavy)
  - `12` mutating routes (POST/PUT/PATCH/DELETE)
- Route pattern counts across `gateway/api/alpaca/*.py`:
  - `provider = registry.get("alpaca")`: `60`
  - `require_provider_rate_limit("alpaca"... )`: `60`
  - `except Exception as e` wrappers: `60`
  - `"Provider error: {str(e)}"` wrappers: `60`
  - `asyncio.to_thread(...)` call sites: `27`
  - `model_dump(mode="json")` call sites: `23`
  - comma-list parsing (`split(",")`) call sites: `18`
  - route-level cache/dedupe references: `0`

Route composition by file:
- `gateway/api/alpaca/stock.py`: `9` endpoints
- `gateway/api/alpaca/trading.py`: `17` endpoints
- `gateway/api/alpaca/options.py`: `7` endpoints
- `gateway/api/alpaca/crypto.py`: `7` endpoints
- `gateway/api/alpaca/watchlists.py`: `7` endpoints
- `gateway/api/alpaca/metadata.py`: `4` endpoints
- `gateway/api/alpaca/account.py`: `3` endpoints
- `gateway/api/alpaca/forex.py`: `2` endpoints
- `gateway/api/alpaca/screener.py`: `2` endpoints
- `gateway/api/alpaca/news.py`: `1` endpoint
- `gateway/api/alpaca/corporate.py`: `1` endpoint

Provider context for this route audit:
- `gateway/providers/alpaca.py` size: `2153` LOC.
- Method shape: `36` async methods, `39` sync methods.
- Route-coupled sync methods are wrapped in route-level `asyncio.to_thread(...)` in account/trading/watchlist modules.

## Priority Findings (Low-Risk Changes Only)

### P0-1: Read-heavy Alpaca routes have no route-level cache or in-flight dedupe

Evidence:
- No cache/dedupe references in Alpaca route package (`0` matches for cache/dedup patterns).
- All `48` GET endpoints currently go directly to provider paths on every request.

Impact:
- Repeated client polling causes avoidable upstream calls and normalization work.
- Concurrent identical cold-key requests can fan out duplicate calls.

Low-risk fix path:
1. Add shared cache+dedupe helper in `gateway/api/alpaca/common.py`.
2. Apply first to market-data reads with stable short TTL opportunities:
   - stocks latest quote/bars/trades/snapshot endpoints
   - options chain/snapshots (short TTL)
   - crypto latest/snapshot/orderbook
   - metadata lookups (`/meta/*`)
3. Keep mutating routes uncached.

### P0-2: Request boilerplate is duplicated across all 60 endpoints despite an unused common helper

Evidence:
- Common helper exists: `gateway/api/alpaca/common.py:20`.
- Routes still perform direct provider lookup and 503 handling in each endpoint:
  - `gateway/api/alpaca/stock.py:37`
  - `gateway/api/alpaca/options.py:38`
  - `gateway/api/alpaca/trading.py:28`
  - same pattern repeated package-wide.

Impact:
- Higher maintenance drift risk and repeated per-request branching/construction.
- Makes cross-cutting optimizations (cache policy, dedupe, standard metrics) expensive to apply consistently.

Low-risk fix path:
1. Use dependency helper (`get_alpaca_provider`) for provider resolution.
2. Add shared execution wrapper for:
   - provider-rate-limit check
   - standardized provider-error mapping
   - optional cache/dedupe stage for GET routes
3. Migrate endpoints incrementally file-by-file without changing response schema.

### P1-3: Heavy route-level `asyncio.to_thread(...)` usage on trading/account/watchlist paths

Evidence:
- `asyncio.to_thread(...)` appears `27` times across:
  - `gateway/api/alpaca/trading.py` (17)
  - `gateway/api/alpaca/watchlists.py` (7)
  - `gateway/api/alpaca/account.py` (3)
- Corresponding provider methods are synchronous, for example:
  - `gateway/providers/alpaca.py:1508` (`get_account`)
  - `gateway/providers/alpaca.py:1522` (`create_order`)
  - `gateway/providers/alpaca.py:1973` (`get_watchlists`)

Impact:
- Repeated thread-offload boilerplate and argument packing in route layer.
- Harder to standardize executor limits and instrumentation.

Low-risk fix path:
1. Centralize offload in one route helper (`run_sync_provider_call`) or async wrappers inside provider.
2. Add bounded executor policy/metrics in one place.
3. Keep route signatures and response payloads unchanged.

### P1-4: Over-fetch then trim patterns add avoidable CPU/network work (remediated 2026-02-07)

Evidence:
- Stock trades route now threads `limit` into provider call instead of local route slicing:
  - `gateway/api/alpaca/stock.py`
  - `gateway/providers/alpaca.py`
- Option chain snapshot route now requests bounded chain window from provider (`limit=100`) instead of local truncation:
  - `gateway/api/alpaca/options.py`
  - `gateway/providers/alpaca.py`

Impact:
- Removed major route-side over-fetch/trim patterns for Alpaca stock trades and option-chain snapshots.
- Remaining Alpaca performance work is now centered on helper consolidation, caching, and concurrency tuning.

Low-risk fix path:
1. Thread endpoint limit through to provider methods where supported (completed 2026-02-07 for stock trades and option-chain snapshot).
2. Add optional `limit` parameter to `get_option_chain(...)` used by snapshot route (completed 2026-02-07).
3. Preserve existing defaults to keep behavior stable.

### P1-5: Sequential snapshot composition increases tail latency (remediated 2026-02-07)

Evidence:
- Stock snapshot now fetches quote and latest bar concurrently via `asyncio.gather(...)`:
  - `gateway/api/alpaca/stock.py`

Impact:
- Lowers snapshot tail latency by running independent provider calls in parallel while preserving response schema.

Low-risk fix path:
1. Use `asyncio.gather` for independent quote/bar requests (completed 2026-02-07).
2. Keep same response shape and error handling.

### P2-6: Repeated comma-list parsing/normalization logic is scattered (partially remediated 2026-02-07)

Evidence:
- `split(",")` parsing appears `18` times across route files.
- Patterns vary (`strip().upper()` vs raw `split(",")`), for example:
  - normalized parse: `gateway/api/alpaca/stock.py:204`
  - raw parse: `gateway/api/alpaca/watchlists.py:57`

Impact:
- Small per-request overhead and inconsistent normalization paths.

Low-risk fix path:
1. Add shared parser helper for delimited lists in `gateway/api/alpaca/common.py` (completed 2026-02-07).
2. Parameterize trimming/uppercasing so behavior stays explicit (completed 2026-02-07).
3. Continue migrating remaining lower-traffic routes to the shared helper.

### P2-7: Static-ish metadata/logo endpoints can benefit from cache headers and short TTL route caching

Evidence:
- Metadata routes always fetch from provider:
  - `gateway/api/alpaca/metadata.py:18`
  - `gateway/api/alpaca/metadata.py:47`
- Logo bytes endpoint returns raw image without explicit cache headers:
  - `gateway/api/alpaca/metadata.py:76`

Impact:
- Repeated requests for mostly stable reference data and logos create avoidable provider load.

Low-risk fix path:
1. Add short route cache for metadata lists.
2. Add `Cache-Control`/ETag strategy for logo responses.
3. Preserve response body/content type.

## Implementation Plan to Start Addressing Issues

### Wave ALP-1 (Immediate, lowest risk)

1. Introduce shared Alpaca route helper for provider lookup, rate-limit, and standardized exception handling.
2. Add reusable comma-list parser helper and migrate high-traffic files (`stock.py`, `options.py`, `crypto.py`) (completed 2026-02-07; also applied to `forex.py` and `news.py`).
3. Add route cache + in-flight dedupe for selected GET endpoints (market data + metadata only).

### Wave ALP-2

1. Remove over-fetch patterns by propagating route limits into provider methods (stock-trades and option-chain snapshot paths completed 2026-02-07; continue broader endpoint review).
2. Parallelize independent snapshot sub-calls (`quotes` + `bars`) with `asyncio.gather` (completed 2026-02-07 for stock snapshot route).
3. Centralize sync provider offload logic (`to_thread`) for trading/account/watchlists.

### Wave ALP-3

1. Add lightweight endpoint-level cache hit/miss metrics for Alpaca routes.
2. Add short-lived cache headers for logo/static metadata endpoints.
3. Validate latency and upstream call reductions with targeted benchmark scenarios.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Endpoints | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/api/alpaca/__init__.py` | 0 | COMPLETE | Router composition only |
| `gateway/api/alpaca/common.py` | 0 | COMPLETE | Shared list parser helper is now implemented; remaining focus is shared route execution + cache/dedupe helpers |
| `gateway/api/alpaca/stock.py` | 9 | COMPLETE | Over-fetch removal and snapshot concurrency are complete; remaining focus is cache/dedupe |
| `gateway/api/alpaca/options.py` | 7 | COMPLETE | Chain snapshot over-fetch reduction and list-parser consolidation are complete; helper consolidation remains |
| `gateway/api/alpaca/crypto.py` | 7 | COMPLETE | List-parser consolidation is complete; remaining focus is cache policy |
| `gateway/api/alpaca/forex.py` | 2 | COMPLETE | List-parser consolidation is complete; remaining focus is cache policy |
| `gateway/api/alpaca/news.py` | 1 | COMPLETE | List-parser consolidation is complete; remaining focus is cache policy + helper consolidation |
| `gateway/api/alpaca/screener.py` | 2 | COMPLETE | Cache policy + helper consolidation |
| `gateway/api/alpaca/corporate.py` | 1 | COMPLETE | Helper consolidation |
| `gateway/api/alpaca/metadata.py` | 4 | COMPLETE | Metadata/logo caching strategy |
| `gateway/api/alpaca/trading.py` | 17 | COMPLETE | `to_thread` centralization + repeated flow helper |
| `gateway/api/alpaca/account.py` | 3 | COMPLETE | `to_thread` centralization + helper consolidation |
| `gateway/api/alpaca/watchlists.py` | 7 | COMPLETE | `to_thread` centralization + parser normalization |

## Remaining Audit Scope (Future Runs)

1. Full provider deep pass for `gateway/providers/alpaca.py` (beyond route-adjacent hotspots).
2. Full provider deep passes for remaining partial providers:
   - `gateway/providers/alphavantage.py`
   - `gateway/providers/news.py`
3. Deep route-level passes for remaining non-provider-specific API modules:
   - `gateway/api/bulk.py`
   - `gateway/api/calendar.py`
   - `gateway/api/corporate.py`
   - `gateway/api/news.py`
   - `gateway/api/quality.py`
   - `gateway/api/replay.py`
   - `gateway/api/symbology.py`
   - `gateway/api/metrics.py`
4. Runtime benchmark harness and profiling passes for middleware, stream fanout, and memory-heavy bulk/replay paths.
