# Tests Performance Audit Deep Dive

Date: 2026-02-06
Auditor: Codex (GPT-5)
Primary scope: `tests/` execution paths and fixture architecture
Secondary scope: supporting runtime behavior in `tests/conftest.py`

## Objective

Complete a full test-suite performance audit focused on execution-time overhead and low-risk improvements that speed local/CI runs without changing test intent.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| Test suite execution paths | 28 (`tests/**/*.py`) | COMPLETE | Full static pass + collected runtime timing evidence |
| Fixture architecture | 1 (`tests/conftest.py`) | COMPLETE | Deep pass of fixture scopes and setup costs |
| Runtime timing sample | 1 run (`pytest -q --durations=25`) | COMPLETE | Measured baseline with slowest-test breakdown |
| Benchmark harness coverage | N/A | PENDING | No dedicated perf-marker benchmark gate yet |

## Inventory and Measured Hotspots

- Test inventory:
  - `28` Python test files
  - `303` collected tests
  - `4491` LOC across `tests/*.py`
- Runtime baseline (`pytest -q --durations=25` on 2026-02-06):
  - total runtime: `2.26s`
  - pass/fail: `288 passed`, `15 failed`
- Top measured slow tests:
  - `tests/test_circuit_breaker.py::TestCircuitBreakerStates::test_circuit_transitions_to_half_open` (`0.15s`)
  - `tests/test_circuit_breaker.py::TestCircuitBreakerStates::test_half_open_closes_after_successes` (`0.15s`)
  - `tests/test_circuit_breaker.py::TestCircuitBreakerStates::test_half_open_reopens_on_failure` (`0.15s`)

Measured structure signals:
- Autouse dependency override fixture runs for every test:
  - `tests/conftest.py:126-149`
- Repeated fixture file I/O in function-scoped fixtures:
  - `tests/conftest.py:165`, `175`, `185`, `195`, `205`, `215`, `225`, `235`, `246`, `256`
- Explicit sleep-based timing tests:
  - `tests/test_circuit_breaker.py:103`, `126`, `152`
- Per-test ad-hoc TestClient creation in several files:
  - `tests/test_middleware_streaming.py:24`, `45`, `64`, `87`
  - `tests/test_error_contract.py:38`, `51`, `64`
  - `tests/test_optimization.py:73`
  - extra raw app clients in integration tests: `tests/test_api_integration.py:276`, `286`

## Priority Findings (Low-Risk Changes Only)

### P0-1: Autouse dependency override fixture adds global per-test setup/teardown overhead

Evidence:
- Function-scoped autouse fixture clears caches and rewires dependencies for every test:
  - `tests/conftest.py:126-149`

Impact:
- Constant setup overhead across all `303` tests.
- Scales linearly with test count, even for pure unit tests that do not use FastAPI dependency overrides.

Low-risk fix path:
1. Split fixtures into:
   - lightweight session/module-scoped baseline overrides,
   - targeted function-scoped overrides only for tests that require mutable overrides.
2. Keep behavior-equivalent dependency wiring for integration tests.

### P0-2: Fixture data loading performs repeated disk I/O per test invocation

Evidence:
- Multiple fixtures open JSON/TXT files each call:
  - `tests/conftest.py:165`, `175`, `185`, `195`, `205`, `215`, `225`, `235`, `246`, `256`

Impact:
- Avoidable filesystem overhead and repeated JSON parse cost.

Low-risk fix path:
1. Convert static fixture-data loaders to `scope="session"`.
2. Cache parsed fixture payloads in memory and return copies when mutation risk exists.
3. Keep fixture API unchanged for test callers.

### P1-3: Sleep-based circuit-breaker tests dominate suite wall time

Evidence:
- Explicit `await asyncio.sleep(0.15)` calls:
  - `tests/test_circuit_breaker.py:103`, `126`, `152`
- These are top 3 slowest tests in measured run.

Impact:
- Fixed latency floor of ~`0.45s` for the suite from three tests alone.

Low-risk fix path:
1. Replace real sleeps with time-control strategy (time mocking or test clock injection).
2. Keep state-transition assertions identical.

### P1-4: Repeated TestClient construction adds avoidable app startup cost

Evidence:
- Multiple tests create ad-hoc `TestClient(...)` instead of sharing fixtures:
  - `tests/test_middleware_streaming.py:24`, `45`, `64`, `87`
  - `tests/test_error_contract.py:38`, `51`, `64`
  - `tests/test_optimization.py:73`
  - `tests/test_api_integration.py:276`, `286`

Impact:
- Repeated app startup and middleware stack initialization overhead.

Low-risk fix path:
1. Use shared client fixtures where isolation constraints allow.
2. Keep dedicated raw-client fixture only for auth-bypass cases.

### P1-5: Endpoint validation tests include debugging-print and broad per-endpoint loops

Evidence:
- Full route dump prints in normal test execution:
  - `tests/test_endpoint_validation.py:62-68`
- Bulk endpoint parametrization executes multiple HTTP requests:
  - `tests/test_endpoint_validation.py:334-355`

Impact:
- Extra log noise and minor runtime overhead in CI.

Low-risk fix path:
1. Gate route-debug printing behind env flag.
2. Keep breadth coverage but move high-volume endpoint loops behind marker if needed (`@pytest.mark.smoke` vs full).

### P2-6: Current suite has 15 known failures, reducing feedback efficiency

Evidence:
- Latest measured run summary:
  - `15 failed, 288 passed in 2.26s`
- Failures clustered in:
  - `tests/test_corporate.py` (fetcher config assumptions)
  - `tests/test_replay.py` (signature drift)
  - `tests/test_middleware_streaming.py`, `tests/test_optimization.py` (header expectations)
  - `tests/test_endpoint_validation.py` (shape assertion mismatch)

Impact:
- Slows iteration due repeated red runs and obscures pure performance regressions.

Low-risk fix path:
1. Stabilize failing tests first.
2. Then establish a clean baseline runtime/perf budget to detect regressions.

## Implementation Plan to Start Addressing Issues

### Wave TESTS-1 (Immediate, lowest risk)

1. Convert static fixture file loaders to session scope with cached parsed payloads.
2. Reduce global autouse fixture cost by scoping dependency overrides more narrowly.
3. Replace circuit-breaker sleep waits with deterministic time controls.

### Wave TESTS-2

1. Consolidate TestClient creation into shared fixtures.
2. Gate debug printing in endpoint validation tests.
3. Separate fast smoke path and full integration path via pytest markers.

### Wave TESTS-3

1. Resolve current failing tests to restore a clean baseline.
2. Add a dedicated performance gate target (for example `pytest -m perf --durations=20`).
3. Track suite runtime trends in CI artifacts.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File/Area | Audit Status | Future Run Focus |
|---|---|---|
| `tests/conftest.py` | COMPLETE | Fixture scope optimization and I/O caching |
| `tests/test_circuit_breaker.py` | COMPLETE | Remove sleep waits via deterministic time control |
| `tests/test_endpoint_validation.py` | COMPLETE | Reduce non-essential request/log overhead |
| `tests/test_api_integration.py` | COMPLETE | Consolidate repeated TestClient startup paths |
| Remaining `tests/**/*.py` execution paths | COMPLETE | Marker strategy + runtime budget split |

## Remaining Audit Scope (Future Runs)

1. Runtime benchmark/profiling suite for production paths (`middleware`, `stream`, `replay`, `bulk`, providers/core).
2. Implementation and measurement pass for Wave TESTS-1/2 recommendations.
3. CI performance budget enforcement after test-suite failures are stabilized.
