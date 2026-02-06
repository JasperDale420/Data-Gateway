# Alpha Vantage Provider Performance Audit Deep Dive

Date: 2026-02-06
Auditor: Codex (GPT-5)
Primary scope: `gateway/providers/alphavantage.py`
Secondary scope: usage context in `gateway/api/alphavantage/*`

## Objective

Complete a full deep performance pass of the Alpha Vantage provider implementation with low-risk recommendations that avoid significant logic changes, and clearly track remaining audit scope.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| Alpha Vantage provider | 1 (`gateway/providers/alphavantage.py`) | COMPLETE | Full provider pass across quote/time-series/fundamentals/indicator/CSV paths |
| Alpha Vantage routes (context) | 9 (`gateway/api/alphavantage/*`) | COMPLETE | Already audited; referenced for integration context only |
| Runtime profiling/benchmarks | N/A | PENDING | Static/code-path audit in this run |

## Inventory and Measured Hotspots

- Provider size: `1082` LOC.
- Method mix:
  - `34` async methods
  - `4` sync methods (constructor/properties)
- Request/response path metrics:
  - `self._client.get(...)`: `21`
  - `response.raise_for_status()`: `20`
  - `response.json()`: `18`
  - `if not self._client` guards: `21`
  - `if not self._api_key` guards: `22`
  - explicit `"Rate limit exceeded"` checks/raises: `17`
  - broad `except Exception as e` blocks: `22`
- CSV endpoints parsed via ad-hoc split logic: `3`
  - `gateway/providers/alphavantage.py:1014`
  - `gateway/providers/alphavantage.py:1038`
  - `gateway/providers/alphavantage.py:1074`
- Sorting hotspots:
  - full-list sort in bar loaders: `gateway/providers/alphavantage.py:244`, `gateway/providers/alphavantage.py:304`, `gateway/providers/alphavantage.py:355`
  - sorted-and-slice patterns: `gateway/providers/alphavantage.py:707`, `gateway/providers/alphavantage.py:865`, `gateway/providers/alphavantage.py:937`

## Priority Findings (Low-Risk Changes Only)

### P0-1: CSV endpoints use manual split parsing (`split(",")`) instead of CSV parser

Evidence:
- Manual parsing in earnings/IPO/listing calendars:
  - `gateway/providers/alphavantage.py:1014-1018`
  - `gateway/providers/alphavantage.py:1038-1042`
  - `gateway/providers/alphavantage.py:1074-1078`

Impact:
- Extra string allocations.
- Fragile on quoted commas and edge cases.

Low-risk fix path:
1. Replace with `csv.DictReader(io.StringIO(response.text))`.
2. Keep output field names and list structure unchanged.

### P0-2: Request boilerplate and rate-limit-note handling are duplicated across most methods

Evidence:
- Repeated pattern across quote, bars, fundamentals, indicators, forex, crypto, economic methods:
  `client.get -> raise_for_status -> json -> if "Note" in data -> map/return`.
- Quantified duplicates:
  - `self._client.get(...)`: `21`
  - `if "Note" in data`: `16`
  - `except Exception as e`: `22`

Impact:
- Repeated branching/allocation overhead.
- Drift risk across methods when adding retries, timeout tuning, or provider error contracts.

Low-risk fix path:
1. Introduce shared helper (for example `_fetch_json(function, **params)`).
2. Centralize:
   - common guard checks (`_client`, `_api_key`)
   - rate-limit `"Note"` handling
   - standardized provider error mapping/log fields.
3. Keep method return schemas unchanged.

### P1-3: Multi-symbol quote path is strictly sequential

Evidence:
- `gateway/providers/alphavantage.py:168` loops symbols sequentially.
- Each iteration awaits `gateway/providers/alphavantage.py:170` (`get_quote`).

Impact:
- Latency scales linearly with symbol count.

Low-risk fix path:
1. Add bounded concurrency mode (small semaphore, configurable for paid tiers).
2. Keep current sequential mode as default for strict free-tier safety.

### P1-4: Full-series parse + full sort for time-series methods

Evidence:
- Intraday/daily/weekly parse all bars then sort:
  - `gateway/providers/alphavantage.py:227-245`
  - `gateway/providers/alphavantage.py:288-305`
  - `gateway/providers/alphavantage.py:339-356`
- Monthly also sorts full list before return:
  - `gateway/providers/alphavantage.py:607`

Impact:
- CPU and memory overhead on large responses (`outputsize=full`, multi-year histories).

Low-risk fix path:
1. Add optional `max_points`/limit parameter and truncate during parse.
2. Preserve existing default behavior when limit is omitted.
3. Avoid extra full sort when provider ordering is already acceptable for caller needs.

### P1-5: Indicator/forex/crypto data paths sort full maps before slicing top 100

Evidence:
- `gateway/providers/alphavantage.py:707`
- `gateway/providers/alphavantage.py:865`
- `gateway/providers/alphavantage.py:937`

Impact:
- Extra sort cost when only a bounded head subset is returned.

Low-risk fix path:
1. Parse only required head items when order is already newest-first.
2. Or sort once in a shared utility and reuse across these paths.

### P2-6: Inconsistent time-series ordering across methods

Evidence:
- Intraday/daily/weekly return most-recent-first (`reverse=True` sorts).
- Monthly returns ascending (`gateway/providers/alphavantage.py:607`).

Impact:
- Downstream callers may need extra normalization/sorting work.

Low-risk fix path:
1. Standardize ordering contract internally (or explicitly annotate per method).
2. Keep external route behavior unchanged; normalize at provider boundary.

## Implementation Plan to Start Addressing Issues

### Wave AV-PROV-1 (Immediate, lowest risk)

1. Replace manual CSV split parsing with `csv.DictReader` on all CSV endpoints.
2. Add shared `_fetch_json` helper with centralized `"Note"` handling.
3. Keep response shapes and route contracts identical.

### Wave AV-PROV-2

1. Add optional bounded-concurrency mode for `get_quotes`.
2. Add optional `max_points` limits for heavy time-series methods.
3. Consolidate and standardize sort/slice behavior.

### Wave AV-PROV-3

1. Add provider-level benchmark tests for:
   - CSV parsing throughput
   - full-history bar parse/sort cost
   - multi-symbol quote latency under sequential vs bounded-concurrency modes.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Methods | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/providers/alphavantage.py` | 38 methods (34 async, 4 sync) | COMPLETE | Helper consolidation, CSV parser migration, bounded-concurrency + sort/limit tuning |

## Remaining Audit Scope (Future Runs)

1. Remaining provider deep pass:
   - `gateway/providers/news.py`
2. Deeper computational hotspot audits for sampled core modules:
   - `gateway/core/security.py`
   - `gateway/core/quality.py`
   - `gateway/core/calendar.py`
   - `gateway/core/symbology.py`
   - `gateway/core/validator.py`
3. Runtime benchmark/profiling suite execution for middleware, stream/replay fanout, and bulk memory behavior.
