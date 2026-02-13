# Alpaca Provider Performance Audit Deep Dive

Date: 2026-02-06
Auditor: Codex (GPT-5)
Primary scope: `gateway/providers/alpaca.py`
Secondary scope: route-adjacent usage in `gateway/api/alpaca/*`

## Objective

Complete a full deep performance pass of the Alpaca provider implementation with low-risk recommendations that avoid significant logic changes, and clearly track what remains for future runs.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| Alpaca provider | 1 (`gateway/providers/alpaca.py`) | COMPLETE | Full provider pass across async market data + sync trading SDK paths |
| Alpaca routes (usage context) | 14 (`gateway/api/alpaca/*`) | COMPLETE | Already fully audited; used here as call-path context only |
| Runtime profiling/benchmarks | N/A | PENDING | Static/code-path audit in this run |

## Inventory and Measured Hotspots

- Provider size: `2153` LOC.
- Method mix:
  - `36` async methods
  - `39` sync methods
- Request/response path metrics:
  - `self._client.get(...)`: `32`
  - `response.raise_for_status()`: `32`
  - `response.json()`: `31`
  - `if not self._client` guards: `33`
  - `if not self._trading_client` guards: `28`
  - `_model_to_dict(...)` call sites: `26`
  - `logger.info(...)` call sites: `44`
  - `logger.error(...)` call sites: `60`
  - in-method schema imports: `7`
  - ad-hoc synchronous `httpx.post(...)` call sites: `1`

Key shape observations:
- Market-data methods primarily normalize into `Normalized*` models via per-record loops.
- Trading/account/watchlist methods return SDK objects converted through `_model_to_dict(...)`.
- Mixed return styles exist (normalized models, plain dict payloads, raw API fragments).

## Priority Findings (Low-Risk Changes Only)

### P0-1: One trading path bypasses shared clients and uses ad-hoc blocking HTTP

Evidence:
- `gateway/providers/alpaca.py:1793` uses direct `httpx.post(...)` for do-not-exercise.
- It creates a fresh connection per call instead of using the existing pooled clients.

Impact:
- No connection reuse on this path, extra handshake/allocator overhead.
- Blocks thread worker for full network duration when invoked through route offload.

Low-risk fix path:
1. Route this call through the existing async client (or a dedicated long-lived sync client).
2. Keep endpoint behavior and returned payload unchanged.
3. Reuse shared headers/timeout/event hooks.

### P0-2: Trading SDK conversion path repeatedly deep-converts large SDK objects

Evidence:
- Recursive conversion helper:
  - `gateway/providers/alpaca.py:1496`
  - recursion branch at `gateway/providers/alpaca.py:1501`
- Used across bulk-return methods:
  - orders `gateway/providers/alpaca.py:1633`
  - positions `gateway/providers/alpaca.py:1699`
  - assets `gateway/providers/alpaca.py:1857`
  - calendar `gateway/providers/alpaca.py:1900`
  - activities/watchlists `gateway/providers/alpaca.py:1964`, `gateway/providers/alpaca.py:1980`

Impact:
- CPU and allocation overhead scales with collection size and nested object depth.

Low-risk fix path:
1. Add list/dict fast-path handling inside `_model_to_dict` (iterative where possible).
2. Prefer SDK-native `model_dump(mode="json")` directly for top-level lists when available.
3. Keep response schema and field names stable.

### P1-3: Over-fetch tendency in option chain provider path

Evidence:
- Option chain request hard-codes `limit=1000`:
  - `gateway/providers/alpaca.py:474`
- Route snapshot callers may only consume first small subset (already observed in route audit).

Impact:
- Extra network payload and normalization cost when consumers need only top-N contracts.

Low-risk fix path:
1. Add optional `limit` parameter to `get_option_chain(...)`.
2. Default to current behavior for compatibility.
3. Use lower limits from snapshot-focused callers.

### P1-4: Repeated in-method imports and repeated request boilerplate

Evidence:
- In-method schema imports at:
  - `gateway/providers/alpaca.py:466`
  - `gateway/providers/alpaca.py:696`
  - `gateway/providers/alpaca.py:1032`
  - `gateway/providers/alpaca.py:1094`
  - `gateway/providers/alpaca.py:1141`
  - `gateway/providers/alpaca.py:1211`
  - `gateway/providers/alpaca.py:1315`
- Common request pattern repeated across most async methods:
  `get -> raise_for_status -> json -> normalize/log`.

Impact:
- Small but steady overhead on hot paths and higher maintenance drift risk.

Low-risk fix path:
1. Hoist stable imports to module level.
2. Add shared helper (for example `_get_json(path, params, log_ctx)`).
3. Preserve endpoint semantics and logging keys.

### P1-5: High-frequency info logging across market-data methods

Evidence:
- `logger.info(...)` occurs `44` times across the provider.
- Includes hot data retrieval paths:
  - bars `gateway/providers/alpaca.py:221`
  - latest bars/trades `gateway/providers/alpaca.py:325`, `gateway/providers/alpaca.py:352`
  - quotes/trades/news/screener variants across file.

Impact:
- Under load, structured info logs add measurable CPU and I/O overhead.

Low-risk fix path:
1. Demote high-volume success logs to debug or sample by interval.
2. Keep error logs intact.
3. Preserve key telemetry via metrics rather than per-call info logs.

### P2-6: Mixed return-shape strategy creates extra downstream serialization branching

Evidence:
- Some methods return normalized objects/lists:
  - `get_bars/get_quotes/get_trades` sections.
- Others return raw dict fragments:
  - `gateway/providers/alpaca.py:415`
  - `gateway/providers/alpaca.py:445`
  - `gateway/providers/alpaca.py:1282`
  - `gateway/providers/alpaca.py:1303`

Impact:
- Route layer needs varied serialization paths, which increases CPU branching and maintenance complexity.

Low-risk fix path:
1. Define consistent provider return conventions per endpoint class (normalized model list vs plain dict).
2. Keep outward API contracts unchanged while reducing conversion ambiguity internally.

### P2-7: Repeated timestamp parsing/string replacement on per-record normalization path

Evidence:
- Normalizers repeatedly call:
  - `gateway/providers/alpaca.py:2108`
  - `gateway/providers/alpaca.py:2123`
  - `gateway/providers/alpaca.py:2139`
- Similar conversion used in news/orderbook paths:
  - `gateway/providers/alpaca.py:1064`
  - `gateway/providers/alpaca.py:1354`

Impact:
- Small per-record overhead amplified on large historical payloads.

Low-risk fix path:
1. Centralize timestamp parse helper for Alpaca ISO strings.
2. Reuse helper in normalizers/news/orderbook mapping.

## Implementation Plan to Start Addressing Issues

### Wave ALP-PROV-1 (Immediate, lowest risk)

1. Replace ad-hoc blocking `httpx.post` in do-not-exercise with shared client path.
2. Add optional `limit` parameter to `get_option_chain(...)` and thread through snapshot callers.
3. Reduce high-volume success logs (`info` -> `debug` or sampled logs).

### Wave ALP-PROV-2

1. Optimize `_model_to_dict(...)` with list/dict fast-path handling.
2. Hoist in-method imports and consolidate repeated async request boilerplate.
3. Add shared Alpaca timestamp parse helper and use it across normalizers.

### Wave ALP-PROV-3

1. Standardize return-shape conventions across provider methods.
2. Add benchmark coverage for:
   - bulk SDK object conversion paths
   - high-cardinality historical normalization
   - option chain payload size impact with varying limits.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Methods | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/providers/alpaca.py` | 75 methods (36 async, 39 sync) | COMPLETE | Conversion-path optimization, request helper consolidation, logging/over-fetch tuning |

## Remaining Audit Scope (Future Runs)

1. Remaining provider deep passes:
   - `gateway/providers/alphavantage.py`
   - `gateway/providers/news.py`
2. Deeper computational hotspot audits for sampled core modules:
   - `gateway/core/security.py`
   - `gateway/core/quality.py`
   - `gateway/core/calendar.py`
   - `gateway/core/symbology.py`
   - `gateway/core/validator.py`
3. Runtime benchmark/profiling suite execution for middleware, stream/replay fanout, and bulk memory behavior.
