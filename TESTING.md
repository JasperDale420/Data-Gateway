# Testing Guide

This repository uses pytest for unit and integration tests, with TDD as the default workflow.

## Test Workflow

1. Write a failing test that reproduces the bug or defines the new behavior.
2. Implement the smallest code change to make the test pass.
3. Refactor while keeping tests green.
4. Update logs and documentation if behavior changed.

## Running Tests

First install dependencies with `uv sync --extra local --extra dev` (or `make setup`) — plain `uv sync` removes the local-only packages (empire-core, empire-schemas, unusualwhales-python-client). Run the commands below via `uv run` (for example, `uv run pytest`) or from an activated `.venv`.

```bash
# Default suite (excludes perf-marked tests)
pytest

# Quiet mode (useful in CI)
pytest -q

# Single file
pytest tests/test_auth.py -v

# Single test
pytest tests/test_auth.py::test_authenticate_valid_key -v

# Coverage summary (fails the run if total coverage drops below 58% —
# `fail_under` in pyproject.toml [tool.coverage.report]; ratchet up as coverage improves)
pytest --cov=gateway --cov-report=term-missing
```

## Test Layout

- `tests/test_*.py`: unit and integration tests.
- `tests/perf/`: performance tests (excluded by default marker config).
- `tests/integration/`: real-Redis integration tests (marker `integration`); skip automatically when no Redis is reachable at `GATEWAY_TEST_REDIS_URL` (default `redis://localhost:6379/15`). Run explicitly with `pytest -m integration tests/integration`.
- `tests/smoke/`: smoke-level checks for key flows.
- `tests/fixtures/`: reusable test payloads and fixture data.

## Quality Gate

Run this before commit when code behavior changes:

```bash
pytest -q && ruff check . && ruff format --check .
```

Optionally add `uv run mypy gateway/` for type checking (mypy is installed via the dev extras). CI gates on the mypy dirty-file allowlist (`ci/mypy_dirty_allowlist.txt`): a file not on the list gaining mypy errors fails the build.

If a Docker-related change is made, also rebuild and verify:

```bash
docker-compose build gateway
docker-compose up -d gateway
curl http://localhost:8080/health/ready
```

## Test Design Rules

- Keep tests deterministic: no real network calls in unit tests.
- Prefer behavior-driven names (for example, `test_risk_check_blocks_order_when_limit_exceeded`).
- Cover edge cases: empty payloads, malformed data, rate limits, and permission failures.
- Keep regression tests for every bug fix.
