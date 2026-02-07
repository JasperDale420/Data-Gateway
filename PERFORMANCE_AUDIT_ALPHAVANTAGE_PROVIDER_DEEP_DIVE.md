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
| Alpha Vantage provider | 1 (`gateway/providers/alphavantage.py`) | COMPLETE | Full provider pass completed; AV-PROV-1 remediated (`csv.DictReader`, bounded quote fan-out, shared fetch helper, sort-head optimization) |
| Alpha Vantage routes (context) | 9 (`gateway/api/alphavantage/*`) | COMPLETE | Already audited; referenced for integration context only |
| Runtime profiling/benchmarks | N/A | PENDING | Static/code-path audit in this run |

## Inventory and Measured Hotspots

- Provider size: `946` LOC.
- Method mix:
  - `34` async methods
  - `4` sync methods (constructor/properties)
- Request/response path metrics:
  - `await self._fetch_json(...)`: `17`
  - direct `client.get(...)` call sites: `5` (health-check + CSV endpoints)
  - explicit `"Rate limit exceeded"` checks/raises in method bodies: `0` (centralized in `_fetch_json`)
  - broad `except Exception as e` blocks: `22`
- CSV endpoints parsed via shared helper with `csv.DictReader`: `3`
  - `gateway/providers/alphavantage.py:873`
- Sorting hotspots:
  - full-list sort in bar loaders: `gateway/providers/alphavantage.py:263`, `gateway/providers/alphavantage.py:319`, `gateway/providers/alphavantage.py:369`
  - sorted-and-slice patterns remediated via helper: `gateway/providers/alphavantage.py:164`, `gateway/providers/alphavantage.py:635`, `gateway/providers/alphavantage.py:771`, `gateway/providers/alphavantage.py:821`

## Remediation Progress (2026-02-07)

- Complete: CSV calendar/listing parsing migrated to shared `csv.DictReader` helper in `gateway/providers/alphavantage.py:873`.
- Complete: `get_quotes(...)` uses bounded semaphore concurrency with configurable `quotes_max_concurrency` in `gateway/providers/alphavantage.py:216` and `config/providers.yaml`.
- Complete: shared `_fetch_json(...)` and `_ensure_ready(...)` now handle request boilerplate + rate-limit-note checks across quote/time-series/fundamentals/indicator/forex/crypto/economic methods (`gateway/providers/alphavantage.py:143`).
- Complete: `_top_time_series_items(...)` helper replaces full-sort-then-slice patterns for indicator/forex/crypto paths (`gateway/providers/alphavantage.py:164`).
- Complete: targeted helper micro-benchmark captured:
  - ordered head extraction: `6.02us` helper vs `357.91us` full sort (`59.46x` faster)
  - unordered fallback: `1393.26us` helper vs `1267.10us` full sort (`1.10x` overhead)

## Priority Findings (Low-Risk Changes Only)

### P0-1: CSV endpoint parsing modernization (remediated 2026-02-07)

Evidence:
- Shared parser helper now used by earnings/IPO/listing endpoints:
  - `gateway/providers/alphavantage.py:865-874`
  - `gateway/providers/alphavantage.py:896`
  - `gateway/providers/alphavantage.py:909`
  - `gateway/providers/alphavantage.py:940`

Impact:
- Extra string allocations.
- Fragile on quoted commas and edge cases.

Status:
1. Implemented with `csv.DictReader(io.StringIO(payload))`.
2. Added coverage in `tests/test_alphavantage_provider.py` for quoted-comma payloads.

### P0-2: Request boilerplate and rate-limit-note handling dedupe (remediated 2026-02-07)

Evidence:
- Shared helper now centralizes request flow:
  - `gateway/providers/alphavantage.py:143` (`_fetch_json`)
  - `gateway/providers/alphavantage.py:132` (`_ensure_ready`)
- Quantified rollout:
  - `await self._fetch_json(...)`: `17`
  - inline `if "Note" in data`: `0`

Impact:
- Repeated branching/allocation overhead.
- Drift risk across methods when adding retries, timeout tuning, or provider error contracts.

Status:
1. Shared `_fetch_json(...)` helper implemented and applied.
2. Readiness and rate-limit-note handling centralized.
3. Method return schemas preserved.

### P1-3: Multi-symbol quote fan-out (remediated with bounded concurrency on 2026-02-07)

Evidence:
- `gateway/providers/alphavantage.py:212` creates `asyncio.Semaphore(self._quotes_max_concurrency)`.
- `gateway/providers/alphavantage.py:222` executes bounded fan-out with `asyncio.gather(...)`.
- `gateway/providers/alphavantage.py:57-66` parses/clamps `quotes_max_concurrency` config.

Impact:
- Latency scales linearly with symbol count.

Status:
1. Bounded concurrency mode implemented with conservative default (`2`) and clamp (`1..5`).
2. Fail-soft semantics preserved (per-symbol warning + continue).

### P1-4: Full-series parse + full sort for time-series methods

Evidence:
- Intraday/daily/weekly parse all bars then sort:
  - `gateway/providers/alphavantage.py:229-287`
  - `gateway/providers/alphavantage.py:289-336`
  - `gateway/providers/alphavantage.py:338-376`
- Monthly also sorts full list before return:
  - `gateway/providers/alphavantage.py:556`

Impact:
- CPU and memory overhead on large responses (`outputsize=full`, multi-year histories).

Low-risk fix path:
1. Add optional `max_points`/limit parameter and truncate during parse.
2. Preserve existing default behavior when limit is omitted.
3. Avoid extra full sort when provider ordering is already acceptable for caller needs.

### P1-5: Indicator/forex/crypto sort-head optimization (remediated 2026-02-07)

Evidence:
- Shared helper now used in all three paths:
  - `gateway/providers/alphavantage.py:164`
  - `gateway/providers/alphavantage.py:635`
  - `gateway/providers/alphavantage.py:771`
  - `gateway/providers/alphavantage.py:821`

Impact:
- Extra sort cost when only a bounded head subset is returned.

Status:
1. Added helper to short-circuit to head extraction when provider order is already descending.
2. Falls back to `sorted(..., reverse=True)[:limit]` when unordered.

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
2. Complete: shared `_fetch_json` helper with centralized `"Note"` handling is implemented.
3. Complete: added bounded `get_quotes` concurrency with config guardrails.

### Wave AV-PROV-2

1. Complete: bounded-concurrency mode for `get_quotes`.
2. Pending: optional `max_points` limits for heavy time-series methods.
3. Complete: consolidated sort/slice behavior for indicator/forex/crypto via shared helper.

### Wave AV-PROV-3

1. Add provider-level benchmark tests for:
   - CSV parsing throughput
   - full-history bar parse/sort cost
   - multi-symbol quote latency under sequential vs bounded-concurrency modes.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Methods | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/providers/alphavantage.py` | 38 methods (34 async, 4 sync) | COMPLETE | Optional `max_points` tuning for heavy full-history methods + broader runtime profiling |

## Remaining Audit Scope (Future Runs)

1. Runtime benchmark/profiling suite execution for middleware, stream/replay fanout, and bulk memory behavior.
2. Optional Alpha Vantage provider follow-up: add `max_points`-style limits for heavy full-history parse/sort methods where route contracts allow.
