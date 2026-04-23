.PHONY: setup test lint format typecheck run clean

## Local development setup — installs runtime + local SDK + dev tools.
## Bare `uv sync` will NOT work because empire-core, empire-schemas, and
## unusualwhales-python-client are in optional [local] extras.
setup:
	uv sync --extra local --extra dev

## Run unit tests (fast, no network).
test: setup
	uv run pytest -m unit --tb=short -q

## Run all tests (includes integration / perf).
test-all: setup
	uv run pytest --tb=short -q

## Lint with ruff.
lint:
	ruff check .

## Auto-format with ruff.
format:
	ruff format .

## Type-check with mypy.
typecheck:
	mypy .

## Start the gateway server (debug mode).
run: setup
	uv run uvicorn gateway.main:app --host 0.0.0.0 --port 8080 --reload

## Remove cached / compiled artifacts.
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache
