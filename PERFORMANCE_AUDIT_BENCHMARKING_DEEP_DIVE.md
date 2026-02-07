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
| CI performance-gate coverage | 3 workflows | COMPLETE | Dedicated perf gate workflow added with threshold/artifact output |
| Benchmark tooling and pytest config | 3 (`pyproject.toml`, `config/perf_budgets.json`, `config/perf_baseline.json`) | COMPLETE | `perf` marker split plus versioned budgets and baseline timings |
| Perf-sensitive test paths | 3 (`tests/test_middleware_streaming.py`, `tests/test_optimization.py`, `tests/test_replay.py`) | COMPLETE | Measured for baseline readiness; failures block clean perf baselines |
| Hot-path source anchor validation | 8 core/runtime files | COMPLETE | Anchored middleware/stream/sink/dedup/metrics/rate-limit hot paths |
| Runtime microbench sample | 1 ad-hoc run | COMPLETE | Fresh local microbench figures captured |
| Dedicated benchmark harness (pytest marker + baseline suite) | 2 files (`pyproject.toml`, `tests/perf/test_perf_baseline.py`) | COMPLETE | `perf` marker added and baseline perf suite introduced |
| Wave 2 stream/sink perf coverage | 1 file (`tests/perf/test_perf_stream_sink.py`) | COMPLETE | Added fanout semaphore-bound plus single/multi-sink bounded backpressure tests |
| Wave 2 replay/bulk memory perf coverage | 1 file (`tests/perf/test_perf_replay_bulk_memory.py`) | COMPLETE | Added replay loop memory profile and bulk stream-vs-JSONL peak-allocation comparison |
| Runtime sink in-flight hardening | 1 file (`gateway/core/data_sink.py`) | COMPLETE | Per-sink in-flight cap with backpressure drops and publish stats added |
| CI budget enforcement | 1 workflow + 1 script | COMPLETE | Thresholded perf gate + artifact publishing implemented |
| Baseline history automation | 1 workflow + 3 scripts + 3 unit tests | COMPLETE | Added rolling history, baseline refresh, automatic budget ratcheting, active-config promotion utility, and release-readiness diff/apply helper |

## Latest Calibration Snapshot (2026-02-07)

- Ran perf gate with tracked config:
  - `python scripts/perf_gate.py --budgets-file config/perf_budgets.json --baseline-file config/perf_baseline.json`
  - Result: `9 passed`, suite runtime `1.07s`
- Refreshed active perf history/baselines from current summary:
  - `python scripts/perf_baseline_manager.py ... --summary-file perf-summary.json ...`
  - Result: `ratchet_applied=True` with `history=3`, `pass_samples=3`
- Promoted active perf config to tracked config:
  - `python scripts/perf_release_readiness.py --apply`
  - Updated `config/perf_budgets.json`:
    - `suite_max_seconds`: `6.0 -> 3.6`
    - tightened sink/fanout test budgets (`0.2/0.08/0.45/0.2` -> `0.066/0.066/0.144/0.189`)
  - Updated `config/perf_baseline.json`:
    - `suite_baseline_seconds`: `0.934 -> 1.19`
    - refreshed per-test timings and added `test_stream_fanout_batching_bounds_task_burst_with_high_semaphore`

## Evidence Snapshot

### CI and Tooling Gaps

- CI runs tests and coverage but no dedicated perf gate:
  - `.github/workflows/ci.yml:44-47`
- Release-readiness workflow has targeted tests but no perf budget checks:
  - `.github/workflows/release-readiness.yml:33-40`
- `pyproject.toml` defines dedicated perf marker split:
  - `pyproject.toml:125-130`
- Dedicated CI perf guardrail workflow now enforces runtime threshold and uploads artifacts:
  - `.github/workflows/perf-guardrail.yml`
  - `scripts/perf_gate.py`
  - `config/perf_budgets.json`
  - `config/perf_baseline.json`

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

### Wave 2 Perf Harness Validation (2026-02-06)

Command run:
- `pytest -q tests/perf -m perf --durations=15`

Result:
- `7 passed in 0.95s`

Added coverage:
- `tests/perf/test_perf_stream_sink.py`:
  - validates stream fanout max in-flight concurrency respects semaphore limits
  - validates sink publish in-flight backlog remains bounded for single and multi-sink blocked I/O
- `tests/perf/test_perf_replay_bulk_memory.py`:
  - profiles replay loop peak allocations on large in-memory message batches
  - compares bulk result streaming allocation profile against JSONL materialization overhead

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

Status (2026-02-06):
- Completed:
  - Added pytest marker + default exclusion from non-perf suites:
    - `pyproject.toml:125-130`
  - Added dedicated perf baseline tests:
    - `tests/perf/test_perf_baseline.py:1-43`

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

Status (2026-02-06):
- Completed:
  - Updated middleware/cache tests to use public health routes:
    - `tests/test_middleware_streaming.py:20-27`
    - `tests/test_optimization.py:24-47`
  - Updated replay tests to current signatures with `client_id`:
    - `tests/test_replay.py:82`
    - `tests/test_replay.py:186`
  - Validation run:
    - `pytest -q tests/test_middleware_streaming.py tests/test_optimization.py tests/test_replay.py --durations=10`
    - Result: `19 passed in 0.18s`

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

Status (2026-02-06):
- Completed:
  - Added dedicated perf gate workflow:
    - `.github/workflows/perf-guardrail.yml`
  - Added reusable perf gate runner with suite + per-test threshold checks and artifact output:
    - `scripts/perf_gate.py`
  - Added versioned perf budgets:
    - `config/perf_budgets.json`
  - Added baseline timings for trend-delta checks:
    - `config/perf_baseline.json`
  - Added trend-delta regression guardrails for suite and per-test timings.
  - Gate validates `pytest -m perf`, enforces runtime threshold, and publishes:
    - `perf-junit.xml`
    - `perf-output.txt`
    - `perf-summary.json`

### P1-4: Sink publish path can build large fire-and-forget task sets

Evidence:
- Source path now applies per-sink in-flight admission before task creation:
  - `gateway/core/data_sink.py:162-179`
  - `gateway/core/data_sink.py:181-191`
- Stream callback currently calls sink publish on each stream event:
  - `gateway/main.py:116-123`
- Perf test validates bounded background task growth under blocked sink I/O:
  - `tests/perf/test_perf_stream_sink.py`

Impact:
- Burst traffic is now bounded by in-flight caps; overflow events are dropped with stats/logging.

Low-risk fix path:
1. Add bounded sink dispatch workers with max in-flight limit.
2. Preserve existing non-blocking caller contract.
3. Add perf test asserting in-flight bound under burst.

Status (2026-02-06):
- Completed:
  - Added runtime per-sink bounded in-flight dispatch with backpressure drops:
    - `gateway/core/data_sink.py:70-205`
  - Added publish scheduling/backpressure counters:
    - `gateway/core/data_sink.py:89`
    - `gateway/core/data_sink.py:123-125`
  - Added boundedness assertion in perf suite:
    - `tests/perf/test_perf_stream_sink.py`

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

Status (2026-02-06):
- Completed:
  - Existing middleware baseline tests:
    - `tests/perf/test_perf_baseline.py:15-43`
  - Added stream fanout semaphore-bound perf test:
    - `tests/perf/test_perf_stream_sink.py`
  - Added replay/session scheduling + bulk/replay memory-bound perf coverage:
    - `tests/perf/test_perf_replay_bulk_memory.py:25-119`

## Implementation Plan to Begin Addressing Issues

### Wave BENCH-1 (Baseline Stabilization)

1. Fix benchmark-blocking test drift (`middleware` headers, `replay` signatures).
2. Add pytest `perf` marker and split perf tests from functional suites.
3. Establish a deterministic local perf dataset/fixture layer.

Wave status (2026-02-06):
- `1` complete
- `2` complete
- `3` complete (initial deterministic perf baselines in `tests/perf/test_perf_baseline.py`)

### Wave BENCH-2 (Harness and Coverage)

1. Add benchmark tests for:
   - middleware cache+envelope path
   - stream fanout throughput
   - sink publish in-flight task growth
   - replay/session scheduling overhead
2. Add lightweight memory assertions (peak RSS bounds) for bulk/replay scenarios.
3. Store benchmark outputs as JSON artifacts for trend comparisons.

Wave status (2026-02-06):
- `1` complete (middleware + stream/sink + replay/session coverage in place)
- `2` complete (bulk/replay memory assertions added)
- `3` complete (sink bounded in-flight hardening implemented, including slower-backend sink profile coverage)

### Wave BENCH-3 (CI Guardrails)

1. Add a dedicated CI perf job (`pytest -m perf`) with explicit thresholds.
2. Fail only on material regression thresholds initially.
3. Publish benchmark and durations artifact for visibility.

Wave status (2026-02-06):
- `1` complete
- `2` complete (suite + per-test budgets via `config/perf_budgets.json`)
- `3` complete (Junit, raw output, and JSON summary artifacts published)
- `4` complete (history-window automation added for baseline refresh/rotation and budget ratcheting)

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File/Area | Audit Status | Future Run Focus |
|---|---|---|
| `.github/workflows/ci.yml` | COMPLETE | add perf job + benchmark artifacts |
| `.github/workflows/release-readiness.yml` | COMPLETE | decide if perf gate belongs here or separate workflow |
| `.github/workflows/perf-guardrail.yml` | COMPLETE | tune branch policy, thresholds, and artifact retention |
| `scripts/perf_gate.py` | COMPLETE | evolve trend policy and baseline-update automation |
| `scripts/perf_baseline_manager.py` | COMPLETE | tune ratchet multipliers/windows and promotion policy for active configs |
| `scripts/perf_promote_active_configs.py` | COMPLETE | use on cadence to promote stable active configs into versioned config files |
| `scripts/perf_release_readiness.py` | COMPLETE | run dry-run+apply promotion with explicit diffs and markdown report |
| `config/perf_budgets.json` | COMPLETE | ratchet suite/per-test budgets from CI baseline history |
| `config/perf_baseline.json` | COMPLETE | refresh baseline timing anchors from CI trend windows |
| `pyproject.toml` | COMPLETE | maintain pytest `perf` marker split |
| `PERF_RELEASE_READINESS.md` | COMPLETE | standardize operator checklist for perf config promotion |
| `tests/perf/test_perf_baseline.py` | COMPLETE | expand middleware/replay/bulk perf assertions |
| `tests/perf/test_perf_stream_sink.py` | COMPLETE | tune boundedness thresholds for multi-sink/slow-backend scenarios |
| `tests/perf/test_perf_replay_bulk_memory.py` | COMPLETE | tune thresholds and add additional replay/bulk scenarios as needed |
| `tests/test_perf_baseline_manager.py` | COMPLETE | preserve ratchet/rotation invariants as manager evolves |
| `tests/test_perf_promote_active_configs.py` | COMPLETE | preserve promotion safety (write + dry-run) behavior |
| `tests/test_perf_release_readiness.py` | COMPLETE | preserve dry-run reporting and apply-mode promotion behavior |
| `tests/test_middleware_streaming.py` | COMPLETE | align cache-header expectations for green perf baseline |
| `tests/test_optimization.py` | COMPLETE | isolate perf assertions into dedicated `perf` suite |
| `tests/test_replay.py` | COMPLETE | update calls for `client_id` signature |
| `gateway/api/middleware.py` | COMPLETE | benchmark duplicate buffering/serialization path |
| `gateway/core/stream.py` | COMPLETE | benchmark fanout strategy under high client counts |
| `gateway/core/data_sink.py` | COMPLETE | tune cap/drop policy and optionally add queue-worker mode |
| `gateway/main.py` | COMPLETE | assess sink-call placement in stream callback with perf tests |
| `gateway/core/dedup.py` | COMPLETE | benchmark lock contention and hash cost |
| `gateway/core/adjustments.py` | COMPLETE | benchmark factor lookup/search optimization candidates |
| `gateway/core/metrics.py` | COMPLETE | benchmark path-normalization memoization value |
| `gateway/core/rate_limiter.py` | COMPLETE | benchmark retry/wait strategy under throttle contention |

## Remaining Audit Scope (Future Runs)

1. Monitor CI artifact trends and adjust ratchet multipliers/windows if false positives or slack appear.
2. Use `scripts/perf_release_readiness.py` (or `scripts/perf_promote_active_configs.py`) to promote `.perf/perf_*.active.json` snapshots into versioned config files on a cadence (for example weekly) when stable.
3. Add provider-specific perf micro-slices only if BENCH artifacts show unexplained trend regressions.
