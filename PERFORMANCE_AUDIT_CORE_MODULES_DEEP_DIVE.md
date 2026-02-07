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
  - quote timestamp parse now performed once per quote iteration in `analyze_quotes(...)` (remediated 2026-02-07): `gateway/core/quality.py:248-273`
  - timeframe map now hoisted to module constant `_TIMEFRAME_TO_MINUTES` (remediated 2026-02-07): `gateway/core/quality.py:15-28`
  - gap detection now uses sorted-input fast path and single-pass timestamp parsing (remediated 2026-02-07): `gateway/core/quality.py:335-372`
- Calendar range scan is day-by-day loop:
  - `while current <= end`: `gateway/core/calendar.py:305`
  - upfront max-span guardrail now enforced (remediated 2026-02-07): `gateway/core/calendar.py:299-303`
- Symbology repeated resolve paths:
  - `self.resolve(...)` called by public helpers at `gateway/core/symbology.py:397`
  - bounded resolver memoization + clone-on-read now implemented in `gateway/core/symbology.py:142-220` (remediated 2026-02-07)
  - direct human option format assembly now implemented in `gateway/core/symbology.py:420-434` (remediated 2026-02-07)
- Security validation integration overhead on HTTP path:
  - per-request dynamic import in middleware: `gateway/api/middleware.py:54`

## Priority Findings (Low-Risk Changes Only)

### P0-1: Stream validation path pays high per-message conversion overhead (remediated 2026-02-07)

Evidence:
- Stream handler now validates raw provider payloads directly, removing per-message remapping dict allocations:
  - `gateway/core/stream.py:901-908`
- Validator now accepts canonical and stream-native key shapes in-place (`symbol/S`, `timestamp/t`, price/size aliases):
  - `gateway/core/validator.py:106-116`
  - `gateway/core/validator.py:209-216`
  - `gateway/core/validator.py:270-274`
- Decimal conversion now uses type-aware fast paths (`Decimal`, `int`, `float`, `str`) before generic fallback:
  - `gateway/core/validator.py:320-340`
- Future timestamp checks now compute `now_utc` once per validate call and reuse it in helper comparison:
  - `gateway/core/validator.py:125-131`
  - `gateway/core/validator.py:225-231`
  - `gateway/core/validator.py:283-289`
- Regression coverage added for stream-native validator payloads:
  - `tests/test_validator.py:186`
  - `tests/test_validator.py:242`
  - `tests/test_validator.py:278`

Impact:
- Removes avoidable hot-path allocations and reduces conversion overhead during high-frequency stream validation.
- Preserves validation error codes/contracts while broadening accepted input shapes for internal stream paths.

### P0-2: `/quality/analyze` endpoint traverses payloads multiple times (remediated 2026-02-07)

Evidence:
- `/quality/analyze` now uses shared `issues_out` collectors during per-type analysis and no longer calls `detect_issues(...)` as a second endpoint pass:
  - `gateway/api/quality.py:181`
  - `gateway/api/quality.py:184`
  - `gateway/api/quality.py:187`
  - `gateway/api/quality.py:192`
- Core analyzer now supports optional issue collection in analysis methods:
  - `gateway/core/quality.py:210`
  - `gateway/core/quality.py:229`
  - `gateway/core/quality.py:238`
  - `gateway/core/quality.py:258`
- Regression coverage validates collected issues and route behavior without `detect_issues(...)` dependency:
  - `tests/test_quality.py:157`
  - `tests/test_quality.py:167`
  - `tests/test_quality_router.py:8`

Impact:
- Removes endpoint-level duplicate analysis traversal and preserves response schema/issue codes.
- For quote paths, crossed-quote issue detection now happens inline during analysis rather than in a separate pass.

### P1-3: Quality timestamp parsing and sorting work can be reduced (remediated 2026-02-07)

Evidence:
- `analyze_quotes(...)` now parses quote timestamp once per loop and reuses it for stale checks and prev assignment:
  - `gateway/core/quality.py:248`
  - `gateway/core/quality.py:273`
- Gap detection now only sorts when input timestamps are not already in order and avoids reparsing previous timestamps every loop:
  - `gateway/core/quality.py:345`
  - `gateway/core/quality.py:352`
  - `gateway/core/quality.py:370`
- Timeframe lookup map is now module-scoped and reused:
  - `gateway/core/quality.py:15`
  - `gateway/core/quality.py:451`
- Regression coverage added for stale-quote detection and unsorted bar-gap handling:
  - `tests/test_quality.py:111`
  - `tests/test_quality.py:159`

Impact:
- Reduces repeated timestamp parsing and per-call dict allocation in quality analysis hot paths while preserving output schemas and issue semantics.
- Avoids unnecessary full sort work for already ordered bar payloads that dominate normal ingestion flows.

### P1-4: Calendar trading-day enumeration scales linearly by calendar day (partially remediated 2026-02-07)

Evidence:
- Calendar trading-day path now applies explicit date-span guard before loop:
  - `gateway/core/calendar.py:299`
  - `gateway/core/calendar.py:300`
- Legacy partial-result safety break (`len(trading_days) > 1000`) was removed:
  - `gateway/core/calendar.py` (`get_trading_days` no longer truncates via late break)
- Regression coverage added for oversize-range guard and inverted-range behavior:
  - `tests/test_calendar.py:102`
  - `tests/test_calendar.py:110`

Impact:
- Prevents unbounded large-range iteration and avoids silently truncated partial results for oversized requests.
- Keeps small/normal ranges unchanged; core iteration remains linear within accepted bounds.

Low-risk fix path:
1. Add explicit max date-span guard using `(end - start).days` before iteration (completed 2026-02-07).
2. Keep existing semantics but fail fast on unrealistic windows (completed 2026-02-07).
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

### P2-6: Security validation path has avoidable per-request overhead (remediated 2026-02-07)

Evidence:
- Input validation middleware now hoists validator import to module scope (no per-request dynamic import on `content-length` paths):
  - `gateway/api/middleware.py:22`
  - `gateway/api/middleware.py:54`
- Symbol-array validation now dedupes case-insensitive duplicates before per-symbol validation:
  - `gateway/core/security.py:198`
  - `gateway/core/security.py:201`
- Regression coverage validates duplicate-symbol dedupe behavior:
  - `tests/test_security.py:102`

Impact:
- Removes avoidable per-request import overhead from HTTP validation path while preserving validation behavior.
- Reduces repeated symbol-regex validation work for duplicate-heavy symbol arrays while preserving error contracts and array-limit checks.

Low-risk fix path:
1. Hoist `get_input_validator` import to module scope in middleware (completed 2026-02-07).
2. Optionally dedupe symbols before validation where endpoint semantics allow (completed 2026-02-07).
3. Keep error contracts unchanged.

### P2-7: Runtime scripts are sequential where bounded concurrency is safe (remediated 2026-02-07)

Evidence:
- `live_provider_smoke.py` now executes provider checks with bounded async concurrency:
  - `scripts/live_provider_smoke.py:25`
  - `scripts/live_provider_smoke.py:84`
  - `scripts/live_provider_smoke.py:145`
- Regression coverage added for missing-provider handling, health-check failures, and semaphore enforcement:
  - `tests/test_live_provider_smoke.py:11`
  - `tests/test_live_provider_smoke.py:25`
  - `tests/test_live_provider_smoke.py:44`
- Route contract generator scans each file and searches for handlers after each route match:
  - route-to-handler binding now uses pre-indexed function definition offsets:
    - `scripts/generate_provider_contract.py:36`
    - `scripts/generate_provider_contract.py:45`
    - `scripts/generate_provider_contract.py:71`
  - regression coverage added for handler index/lookup behavior:
    - `tests/test_generate_provider_contract.py:22`
    - `tests/test_generate_provider_contract.py:37`

Impact:
- Provider smoke checks no longer block on full serial provider sequencing, improving operator feedback time for multi-provider checks.
- Contract generator no longer performs repeated full-file handler regex scans for each route match.

Low-risk fix path:
1. Parallelize provider smoke checks with bounded `asyncio.gather` (small concurrency) (completed 2026-02-07).
2. In contract generator, pre-index function boundaries once per file (completed 2026-02-07).
3. Preserve outputs and exit-code behavior.

## Implementation Plan to Start Addressing Issues

### Wave CORE-1 (Immediate, lowest risk)

1. Add validator hot-path optimizations (single `now_utc`, numeric fast-path, raw-message entry points) (completed 2026-02-07).
2. Remove duplicate timestamp parsing in quality analyzer and hoist timeframe map constant (completed 2026-02-07, including sorted-input fast path for gap detection).
3. Remove temporary `ResolvedSymbol` allocation in symbology human-format path (completed 2026-02-07, along with bounded resolve memoization).
4. Hoist middleware input-validator import (completed 2026-02-07).
5. Dedupe symbol arrays before per-symbol validation in security validator (completed 2026-02-07).

### Wave CORE-2

1. Consolidate quality analyze + issue-detection into a single-pass option (completed 2026-02-07).
2. Add calendar span guardrails and optional pre-allocation hints for trading-day range generation (span guardrails completed 2026-02-07).
3. Add bounded concurrency for `scripts/live_provider_smoke.py` provider checks (completed 2026-02-07).

### Wave CORE-3

1. Add microbenchmarks for:
   - stream validator throughput (bars/quotes/trades)
   - quality analyzer gap/issue detection on 1k/10k records
   - symbology resolution throughput with repeated symbol sets.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Audit Status | Future Run Focus |
|---|---|---|
| `gateway/core/security.py` | COMPLETE | Benchmark duplicate-heavy symbol-array validation cost |
| `gateway/core/quality.py` | COMPLETE | Benchmark validation for large payloads and optional deeper bar-path pass fusion |
| `gateway/core/calendar.py` | COMPLETE | Optional pre-allocation hints and benchmark validation |
| `gateway/core/symbology.py` | COMPLETE | Microbenchmark cache-hit effectiveness and tune cache-cap strategy if needed |
| `gateway/core/validator.py` | COMPLETE | Stream-validation microbenchmarking and optional additional alias-path coverage |
| `scripts/live_provider_smoke.py` | COMPLETE | Optional concurrency tuning and end-to-end latency benchmark |
| `scripts/generate_provider_contract.py` | COMPLETE | Optional end-to-end file-scan benchmark on large route modules |

## Remaining Audit Scope (Future Runs)

1. Runtime benchmark/profiling suite execution for middleware, stream/replay fanout, bulk memory behavior, and provider/core microbenchmarks.
2. Full performance audit of `tests/` execution paths (fixture setup cost, heavy integration tests, and benchmark gate strategy).
3. Implementation and measurement pass for remaining Wave CORE-1/2 recommendations (calendar/script/core benchmark validation).
