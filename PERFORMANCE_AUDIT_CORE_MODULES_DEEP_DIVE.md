# Core Modules Performance Audit Deep Dive

Date: 2026-02-06
Auditor: Codex (GPT-5)
Primary scope: `gateway/core/security.py`, `gateway/core/quality.py`, `gateway/core/calendar.py`, `gateway/core/symbology.py`, `gateway/core/validator.py`
Secondary scope: runtime call paths in `gateway/core/stream.py`, `gateway/api/quality.py`, `gateway/api/middleware.py`, and `scripts/*`

## Objective

Complete a deep computational hotspot pass of the previously sampled core utility modules, with low-risk recommendations that avoid significant logic changes and clearly identify what still needs future audit/profiling.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| Core utility modules | 5 (`gateway/core/{security,quality,calendar,symbology,validator}.py`) | COMPLETE | Full static pass with usage-context verification |
| Runtime call-path context | 3 (`gateway/core/stream.py`, `gateway/api/quality.py`, `gateway/api/middleware.py`) | COMPLETE | Reviewed only relevant invocation paths |
| Runtime scripts | 2 (`scripts/live_provider_smoke.py`, `scripts/generate_provider_contract.py`) | COMPLETE | Static/code-path audit complete |
| Runtime profiling/benchmarks | N/A | PENDING | No instrumented benchmark run in this pass |

## Inventory and Measured Hotspots

- Core-module size: `2395` LOC total.
- Method mix across core modules:
  - `1` async method
  - `78` sync methods
- File sizes:
  - `gateway/core/security.py`: `674` LOC
  - `gateway/core/quality.py`: `480` LOC
  - `gateway/core/calendar.py`: `447` LOC
  - `gateway/core/symbology.py`: `431` LOC
  - `gateway/core/validator.py`: `363` LOC
- Scripts size: `351` LOC total across `2` scripts.

Key measured patterns:
- Validator conversion calls on hot path:
  - `_to_decimal(...)` call sites: `7` (`gateway/core/validator.py:108-111`, `207-208`, `267`)
  - future timestamp checks with `datetime.now(UTC)`: `3` (`gateway/core/validator.py:123`, `222`, `280`)
- Quality analyzer repeated timestamp parsing:
  - `_parse_timestamp(...)` call sites in loops: `5` (`gateway/core/quality.py:248`, `258`, `337`, `340`, `370`)
  - full sort in gap detection: `gateway/core/quality.py:331`
- Calendar range scan is day-by-day loop:
  - `while current <= end`: `gateway/core/calendar.py:296`
- Symbology repeated resolve paths:
  - `self.resolve(...)` called by public helpers at `gateway/core/symbology.py:397`
  - bounded resolver memoization + clone-on-read now implemented in `gateway/core/symbology.py:142-220` (remediated 2026-02-07)
  - direct human option format assembly now implemented in `gateway/core/symbology.py:420-434` (remediated 2026-02-07)
- Security validation integration overhead on HTTP path:
  - per-request dynamic import in middleware: `gateway/api/middleware.py:54`

## Priority Findings (Low-Risk Changes Only)

### P0-1: Stream validation path pays high per-message conversion overhead

Evidence:
- Stream handler validates each incoming bar/quote/trade message:
  - `gateway/core/stream.py:893-925`
- Validator converts numeric fields via `Decimal(str(...))` in hot paths:
  - `gateway/core/validator.py:108-111`
  - `gateway/core/validator.py:207-208`
  - `gateway/core/validator.py:267`
- Each validator method performs separate `datetime.now(UTC)` future checks:
  - `gateway/core/validator.py:123`, `222`, `280`

Impact:
- Extra CPU and allocations on high-rate stream traffic.
- Increased event-loop pressure under fanout load.

Low-risk fix path:
1. Add raw-message validator entry points to avoid intermediate dict remapping in `stream.py`.
2. Introduce numeric fast-path (int/float) before Decimal fallback where precision-sensitive arithmetic is not required.
3. Resolve `now_utc` once per validate call and reuse.
4. Keep error codes and rejection behavior unchanged.

### P0-2: `/quality/analyze` endpoint traverses payloads multiple times

Evidence:
- Endpoint computes per-type analysis first:
  - `gateway/api/quality.py:183-189`
- Then performs issue detection pass again:
  - `gateway/api/quality.py:192`
- Analyzer issue helpers loop the same collections:
  - `gateway/core/quality.py:367-409`
  - `gateway/core/quality.py:415-429`

Impact:
- Duplicate CPU work for large ad-hoc analysis payloads.

Low-risk fix path:
1. Add optional "collect issues" mode inside analyze methods.
2. Reuse computed issue data in endpoint response instead of a second full pass.
3. Preserve response schema.

### P1-3: Quality timestamp parsing and sorting work can be reduced

Evidence:
- `analyze_quotes` parses timestamp for the current quote, then reparses for prev assignment:
  - `gateway/core/quality.py:248`, `258`
- Gap detection always sorts:
  - `gateway/core/quality.py:331-334`
- Timeframe map is rebuilt per call:
  - `gateway/core/quality.py:438-452`

Impact:
- Unnecessary allocations and parsing cost in repeated quality checks.

Low-risk fix path:
1. Parse quote timestamp once per loop iteration and reuse.
2. Hoist timeframe map to module/class constant.
3. Add optional "already sorted" fast path (or monotonicity check) before sorting.

### P1-4: Calendar trading-day enumeration scales linearly by calendar day

Evidence:
- Day-by-day loop for range generation:
  - `gateway/core/calendar.py:296-318`
- Hard stop triggered only after >1000 trading days:
  - `gateway/core/calendar.py:321`

Impact:
- Large date ranges can spend significant CPU in date iteration.

Low-risk fix path:
1. Add explicit max date-span guard using `(end - start).days` before iteration.
2. Keep existing semantics but fail fast on unrealistic windows.
3. Optionally pre-allocate expected list capacity via span heuristics.

### P1-5: Symbology helpers re-resolve and reformat repeatedly (remediated 2026-02-07)

Evidence:
- `SymbolResolver` now has bounded memoization with clone-on-read semantics:
  - `gateway/core/symbology.py:142`
  - `gateway/core/symbology.py:155`
  - `gateway/core/symbology.py:214`
- Option provider-format generation now builds human format directly without temporary object allocation:
  - `gateway/core/symbology.py:421`
  - `gateway/core/symbology.py:433`
- Regression coverage for cache bounding + mutation isolation:
  - `tests/test_symbology.py:134`
  - `tests/test_symbology.py:142`

Impact:
- Repeated symbol resolution requests now avoid re-running regex/parse/provider-format assembly work for hot symbols.
- Option human-format rendering no longer allocates an intermediate `ResolvedSymbol`, reducing transient object churn while preserving output contracts.

### P2-6: Security validation path has avoidable per-request overhead (partially remediated 2026-02-07)

Evidence:
- Input validation middleware now hoists validator import to module scope (no per-request dynamic import on `content-length` paths):
  - `gateway/api/middleware.py:22`
  - `gateway/api/middleware.py:54`
- Symbol-array validation is serial per symbol:
  - `gateway/core/security.py:198-201`

Impact:
- Removes avoidable per-request import overhead from HTTP validation path while preserving validation behavior.
- Remaining symbol-array validation path is still serial and may contribute overhead on large websocket symbol batches.

Low-risk fix path:
1. Hoist `get_input_validator` import to module scope in middleware (completed 2026-02-07).
2. Optionally dedupe symbols before validation where endpoint semantics allow.
3. Keep error contracts unchanged.

### P2-7: Runtime scripts are sequential where bounded concurrency is safe

Evidence:
- `live_provider_smoke.py` executes provider checks serially:
  - `scripts/live_provider_smoke.py:101-124`
- Route contract generator scans each file and searches for handlers after each route match:
  - `scripts/generate_provider_contract.py:47-55`

Impact:
- Slower operator feedback for smoke checks and contract generation.

Low-risk fix path:
1. Parallelize provider smoke checks with bounded `asyncio.gather` (small concurrency).
2. In contract generator, pre-index function boundaries once per file.
3. Preserve outputs and exit-code behavior.

## Implementation Plan to Start Addressing Issues

### Wave CORE-1 (Immediate, lowest risk)

1. Add validator hot-path optimizations (single `now_utc`, numeric fast-path, raw-message entry points).
2. Remove duplicate timestamp parsing in quality analyzer and hoist timeframe map constant.
3. Remove temporary `ResolvedSymbol` allocation in symbology human-format path (completed 2026-02-07, along with bounded resolve memoization).
4. Hoist middleware input-validator import (completed 2026-02-07).

### Wave CORE-2

1. Consolidate quality analyze + issue-detection into a single-pass option.
2. Add calendar span guardrails and optional pre-allocation hints for trading-day range generation.
3. Add bounded concurrency for `scripts/live_provider_smoke.py` provider checks.

### Wave CORE-3

1. Add microbenchmarks for:
   - stream validator throughput (bars/quotes/trades)
   - quality analyzer gap/issue detection on 1k/10k records
   - symbology resolution throughput with repeated symbol sets.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Audit Status | Future Run Focus |
|---|---|---|
| `gateway/core/security.py` | COMPLETE | Middleware integration overhead and symbol-validation batching |
| `gateway/core/quality.py` | COMPLETE | Timestamp parse dedupe, single-pass issue detection |
| `gateway/core/calendar.py` | COMPLETE | Large-range guardrails and iteration efficiency |
| `gateway/core/symbology.py` | COMPLETE | Microbenchmark cache-hit effectiveness and tune cache-cap strategy if needed |
| `gateway/core/validator.py` | COMPLETE | Stream hot-path numeric/timestamp optimization |
| `scripts/live_provider_smoke.py` | COMPLETE | Parallel provider checks |
| `scripts/generate_provider_contract.py` | COMPLETE | Handler index precomputation |

## Remaining Audit Scope (Future Runs)

1. Runtime benchmark/profiling suite execution for middleware, stream/replay fanout, bulk memory behavior, and provider/core microbenchmarks.
2. Full performance audit of `tests/` execution paths (fixture setup cost, heavy integration tests, and benchmark gate strategy).
3. Implementation and measurement pass for remaining Wave CORE-1/2 recommendations (validator hot path, quality pass consolidation, calendar/script follow-ups, security symbol-batch follow-up).
