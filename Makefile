.PHONY: setup test test-all lint format typecheck arch run deploy clean install-hooks

## Local development setup — installs runtime + local SDK + dev tools.
## Bare `uv sync` will NOT work because empire-core, empire-schemas, and
## unusualwhales-python-client are in optional [local] extras.
setup:
	uv sync --extra local --extra dev

## Run the test suite (everything except perf benchmarks and the real-Redis
## integration tests). The `unit` marker was only ever applied to one file, so
## the old `-m unit` target ran ~4 tests — this runs the real suite instead.
test: setup
	uv run pytest -m "not perf and not integration" --tb=short -q

## Run all tests (includes integration / perf).
test-all: setup
	uv run pytest --tb=short -q

## Install the local git hooks (commit-stage lint/secrets + the pre-push gate),
## both via the pre-commit framework. REQUIRED: this repo is private on the free
## GitHub plan, so branch protection / required status checks are unavailable —
## the pre-push gate is the only mechanical check before code reaches the remote.
install-hooks:
	pre-commit install
	pre-commit install --hook-type pre-push
	@echo "commit + pre-push hooks installed"

## Lint with ruff.
lint:
	ruff check .

## Auto-format with ruff.
format:
	ruff format .

## Type-check with mypy.
typecheck:
	mypy .

## Architecture contracts (import-linter, config in .importlinter).
arch:
	uv run lint-imports

## Start the gateway server (debug mode).
run: setup
	uv run uvicorn gateway.main:app --host 0.0.0.0 --port 8080 --reload

## Deploy current source to the running gateway container. Source is
## bind-mounted, so a restart reloads committed code — committed fixes don't
## take effect until you run this. Use `make deploy BUILD=1` for dep/Dockerfile
## changes (rebuilds the image).
## Build + deploy the current source as a baked image (RESTART=1 for a
## config-only reload without rebuild). Never touches redis.
deploy:
	./scripts/deploy.sh $(if $(RESTART),--restart,)

## Remove cached / compiled artifacts.
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache
