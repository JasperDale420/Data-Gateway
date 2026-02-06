# Benchmarking and Profiling Readiness Performance Audit Deep Dive

Date: 2026-02-06
Auditor: Codex (GPT-5)
Primary scope: Benchmark/profiling readiness across CI, test harness configuration, and hot-path measurement coverage.
Secondary scope: Performance-sensitive runtime paths in middleware, stream fanout, sink publishing, replay, dedup, metrics, and adjustments.

## Objective

Complete a deep audit of repository performance-measurement readiness so future optimization work can be validated with repeatable baselines, without significant logic changes.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| CI performance-gate coverage | 2 workflows | COMPLETE | No benchmark/perf regression gate currently present |
| Benchmark tooling and pytest config | 1 (`pyproject.toml`) | COMPLETE | No `perf` marker or benchmark plugin configuration |
| Perf-sensitive test paths | 3 (`tests/test_middleware_streaming.py`, `tests/test_optimization.py`, `tests/test_replay.py`) | COMPLETE | Measured for baseline readiness; failures block clean perf baselines |
| Hot-path source anchor validation | 8 core/runtime files | COMPLETE | Anchored middleware/stream/sink/dedup/metrics/rate-limit hot paths |
| Runtime microbench sample | 1 ad-hoc run | COMPLETE | Fresh local microbench figures captured |
| Dedicated benchmark harness + CI budget enforcement | N/A | FUTURE | Implementation phase pending |

## Evidence Snapshot

### CI and Tooling Gaps

- CI runs tests and coverage but no dedicated perf gate:
  - `.github/workflows/ci.yml:44-47`
- Release-readiness workflow has targeted tests but no perf budget checks:
  - `.github/workflows/release-readiness.yml:33-40`
- `pyproject.toml` pytest config does not define a perf marker suite or benchmark config:
  - `pyproject.toml:121-126`

### Baseline Readiness Blockers from Targeted Test Runs

Commands run:
- `pytest -q tests/test_middleware_streaming.py tests/test_optimization.py --durations=10`
- `pytest -q tests/test_replay.py --durations=10`

Results:
- `tests/test_middleware_streaming.py` + `tests/test_optimization.py`: `2 failed`, `4 passed`
  - Header expectation drift around `X-Gateway-Cache`:
    - `tests/test_middleware_streaming.py:31-32`
    - `tests/test_optimization.py:78`, `tests/test_optimization.py:85`
- `tests/test_replay.py`: `8 failed`, `6 passed`
  - Signature drift (missing `client_id` in test calls):
    - test calls: `tests/test_replay.py:82`, `tests/test_replay.py:186`
    - runtime signatures: `gateway/core/replay.py:121`, `gateway/core/replay.py:243`

### Measured Local Microbench Baseline (2026-02-06)

Command run:
- `python - <<'PY' ...` ad-hoc microbench script over core hot paths

Measured outputs:
- `wrap_event_10k_items_x100`: `0.0014s` (`71,422.2 ops/s`)
- `metrics_normalize_path_200k`: `0.2121s` (`942,865.9 ops/s`)
- `message_dedup_is_duplicate_100k_unique`: `0.2854s` (`350,328.3 ops/s`)
- `adjust_bars_50k_rows`: `0.0847s` (`590,274.6 rows/s`)
- `broadcast_1000_clients`: `0.0127s` (`79,039.1 sends/s`)
- `sink_publish_all_2000_msgs_delay_0.001s`: `0.0172s` (`115,995.5 msg/s`), `peak_bg_tasks=2000`
- `request_dedup_same_key_400`: `0.0012s`
- `request_dedup_unique_keys_400`: `0.0011s`

Interpretation:
- Raw function throughput is generally healthy in this synthetic baseline.
- `peak_bg_tasks=2000` confirms task-growth risk in sink publish path under burst traffic.

## Priority Findings (Low-Risk Changes Only)

### P0-1: No dedicated benchmark harness or perf marker exists

Evidence:
- `pyproject.toml:121-126`
- `.github/workflows/ci.yml:44-47`

Impact:
- Optimization work cannot be regression-tested with stable perf baselines.
- Runtime drift can ship unnoticed as long as functional tests pass.

Low-risk fix path:
1. Add a `perf` marker and a dedicated benchmark test module set.
2. Keep benchmarks side-effect free and deterministic (local fixtures, no network).
3. Keep regular CI behavior intact; run perf gate in separate opt-in job.

### P0-2: Benchmark baseline quality is blocked by known failing tests

Evidence:
- `tests/test_middleware_streaming.py:31-32`
- `tests/test_optimization.py:78`, `tests/test_optimization.py:85`
- `tests/test_replay.py:82`, `tests/test_replay.py:186`

Impact:
- Current perf runs inherit noise from test contract drift.
- Trend comparisons become low-confidence.

Low-risk fix path:
1. Repair header expectation drift for cache middleware tests.
2. Update replay tests to current `client_id` signatures.
3. Freeze green baseline before introducing perf budgets.

### P1-3: CI workflows have no latency/throughput budget enforcement

Evidence:
- `.github/workflows/ci.yml:44-47`
- `.github/workflows/release-readiness.yml:33-40`

Impact:
- Performance regressions cannot fail CI.

Low-risk fix path:
1. Add a dedicated perf workflow or optional CI job for `pytest -m perf`.
2. Export benchmark JSON artifact and compare against fixed thresholds.
3. Start with coarse guardrails (p95 latency and throughput floors), then tighten.

### P1-4: Sink publish path can build large fire-and-forget task sets

Evidence:
- Source path creates one task per sink publish:
  - `gateway/core/data_sink.py:145-148`
- Stream callback currently calls sink publish on each stream event:
  - `gateway/main.py:116-123`
- Microbench observed `peak_bg_tasks=2000` during burst.

Impact:
- Burst traffic can increase memory and event-loop scheduling overhead.

Low-risk fix path:
1. Add bounded sink dispatch workers with max in-flight limit.
2. Preserve existing non-blocking caller contract.
3. Add perf test asserting in-flight bound under burst.

### P1-5: Middleware and fanout hotspots still lack benchmark guardrails

Evidence:
- Cache and envelope response buffering/serialization:
  - `gateway/api/middleware.py:365-368`
  - `gateway/api/middleware.py:610-615`
  - `gateway/api/middleware.py:624`
  - `gateway/api/middleware.py:675`
- Stream fanout gather pattern:
  - `gateway/core/stream.py:991`

Impact:
- Main latency-sensitive paths are not protected by reproducible perf tests.

Low-risk fix path:
1. Add perf cases for cached GET + envelope path and fanout throughput at fixed client counts.
2. Track both latency and peak memory for these tests.

## Implementation Plan to Begin Addressing Issues

### Wave BENCH-1 (Baseline Stabilization)

1. Fix benchmark-blocking test drift (`middleware` headers, `replay` signatures).
2. Add pytest `perf` marker and split perf tests from functional suites.
3. Establish a deterministic local perf dataset/fixture layer.

### Wave BENCH-2 (Harness and Coverage)

1. Add benchmark tests for:
   - middleware cache+envelope path
   - stream fanout throughput
   - sink publish in-flight task growth
   - replay/session scheduling overhead
2. Add lightweight memory assertions (peak RSS bounds) for bulk/replay scenarios.
3. Store benchmark outputs as JSON artifacts for trend comparisons.

### Wave BENCH-3 (CI Guardrails)

1. Add a dedicated CI perf job (`pytest -m perf`) with explicit thresholds.
2. Fail only on material regression thresholds initially.
3. Publish benchmark and durations artifact for visibility.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File/Area | Audit Status | Future Run Focus |
|---|---|---|
| `.github/workflows/ci.yml` | COMPLETE | add perf job + benchmark artifacts |
| `.github/workflows/release-readiness.yml` | COMPLETE | decide if perf gate belongs here or separate workflow |
| `pyproject.toml` | COMPLETE | add pytest `perf` marker and benchmark config |
| `tests/test_middleware_streaming.py` | COMPLETE | align cache-header expectations for green perf baseline |
| `tests/test_optimization.py` | COMPLETE | isolate perf assertions into dedicated `perf` suite |
| `tests/test_replay.py` | COMPLETE | update calls for `client_id` signature |
| `gateway/api/middleware.py` | COMPLETE | benchmark duplicate buffering/serialization path |
| `gateway/core/stream.py` | COMPLETE | benchmark fanout strategy under high client counts |
| `gateway/core/data_sink.py` | COMPLETE | enforce bounded in-flight publish tasks |
| `gateway/main.py` | COMPLETE | assess sink-call placement in stream callback with perf tests |
| `gateway/core/dedup.py` | COMPLETE | benchmark lock contention and hash cost |
| `gateway/core/adjustments.py` | COMPLETE | benchmark factor lookup/search optimization candidates |
| `gateway/core/metrics.py` | COMPLETE | benchmark path-normalization memoization value |
| `gateway/core/rate_limiter.py` | COMPLETE | benchmark retry/wait strategy under throttle contention |

## Remaining Audit Scope (Future Runs)

1. Implement BENCH-1 baseline stabilization and lock clean green baseline.
2. Implement BENCH-2 benchmark harness and capture first artifacted baseline.
3. Implement BENCH-3 CI guardrails and tune thresholds after 3-5 baseline runs.
