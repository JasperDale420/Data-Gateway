# Alpha Vantage Provider Performance Audit Deep Dive

Date: 2026-02-07
Auditor: Codex (GPT-5)
Primary scope: `gateway/providers/alphavantage.py`
Secondary scope: usage context in `gateway/api/alphavantage/*`

## Objective

Complete a full deep performance pass of the Alpha Vantage provider implementation with low-risk recommendations that avoid significant logic changes, and clearly track remaining audit scope.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| Alpha Vantage provider | 1 (`gateway/providers/alphavantage.py`) | COMPLETE | Full provider pass completed; AV-PROV-1 partially remediated (`csv.DictReader` + bounded quote fan-out) |
| Alpha Vantage routes (context) | 9 (`gateway/api/alphavantage/*`) | COMPLETE | Already audited; referenced for integration context only |
| Runtime profiling/benchmarks | N/A | PENDING | Static/code-path audit in this run |

## Inventory and Measured Hotspots

- Provider size: `1096` LOC.
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
- CSV endpoints parsed via shared helper with `csv.DictReader`: `3`
  - `gateway/providers/alphavantage.py:1006`
- Sorting hotspots:
  - full-list sort in bar loaders: `gateway/providers/alphavantage.py:244`, `gateway/providers/alphavantage.py:304`, `gateway/providers/alphavantage.py:355`
  - sorted-and-slice patterns: `gateway/providers/alphavantage.py:714`, `gateway/providers/alphavantage.py:883`, `gateway/providers/alphavantage.py:955`

## Remediation Progress (2026-02-07)

- Complete: CSV calendar/listing parsing migrated to shared `csv.DictReader` helper in `gateway/providers/alphavantage.py:1006`.
- Complete: `get_quotes(...)` now uses bounded semaphore concurrency with configurable `quotes_max_concurrency` in `gateway/providers/alphavantage.py:180` and `config/providers.yaml`.
- Pending: shared JSON fetch helper consolidation, indicator/forex/crypto sort/limit tuning, and provider micro-benchmark validation.

## Priority Findings (Low-Risk Changes Only)

### P0-1: CSV endpoint parsing modernization (remediated 2026-02-07)

Evidence:
- Shared parser helper now used by earnings/IPO/listing endpoints:
  - `gateway/providers/alphavantage.py:1006-1014`
  - `gateway/providers/alphavantage.py:1039`
  - `gateway/providers/alphavantage.py:1056`
  - `gateway/providers/alphavantage.py:1088`

Impact:
- Extra string allocations.
- Fragile on quoted commas and edge cases.

Status:
1. Implemented with `csv.DictReader(io.StringIO(payload))`.
2. Added coverage in `tests/test_alphavantage_provider.py` for quoted-comma payloads.

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

### P1-3: Multi-symbol quote fan-out (remediated with bounded concurrency on 2026-02-07)

Evidence:
- `gateway/providers/alphavantage.py:182` creates `asyncio.Semaphore(self._quotes_max_concurrency)`.
- `gateway/providers/alphavantage.py:192` executes bounded fan-out with `asyncio.gather(...)`.
- `gateway/providers/alphavantage.py:57-66` parses/clamps `quotes_max_concurrency` config.

Impact:
- Latency scales linearly with symbol count.

Status:
1. Bounded concurrency mode implemented with conservative default (`2`) and clamp (`1..5`).
2. Fail-soft semantics preserved (per-symbol warning + continue).

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

1. Complete: replaced manual CSV parsing with `csv.DictReader` on all CSV endpoints.
2. Pending: add shared `_fetch_json` helper with centralized `"Note"` handling.
3. Complete: added bounded `get_quotes` concurrency with config guardrails.

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

1. Runtime benchmark/profiling suite execution for middleware, stream/replay fanout, and bulk memory behavior.
2. Alpha Vantage provider follow-up remediation: shared fetch helper + sort/limit tuning for indicator/forex/crypto paths.
