# Contributing to Data Gateway

Thank you for contributing to Data Gateway! This document provides guidelines for development.

## Quick Start

```bash
# Clone and setup
cd Data-Gateway
uv sync --extra local --extra dev   # bare `uv sync` (or pip) omits the local SDK packages and the gateway will not start
make install-hooks                  # commit-stage hooks + the pre-push gate (.githooks/pre-push: ruff + fast contract tests on `git push`)

# Copy and configure environment
cp .env.example .env
# Edit .env with your API keys

# Run tests
uv run pytest tests/ -v

# Start locally
uv run uvicorn gateway.main:app --reload --port 8080
```

## Development Environment

### Prerequisites

- Python 3.12+
- Docker & Docker Compose (for containerized development)
- API keys for providers you want to test (Alpaca, Unusual Whales, etc.)

### IDE Setup

The project includes VS Code configuration in `.vscode/`. Recommended extensions:

- Python
- Pylance
- Ruff

## Code Style

| Tool | Purpose |
|------|---------|
| **ruff** | Linting and formatting (120 char line length; `ruff format .` / `make format`) |
| **mypy** | Type checking (`make typecheck` or `mypy .`) |
| **bandit** | Security linting |
| **detect-secrets** | Prevent accidental secret commits |

Run the commit-stage checks (ruff lint + format, detect-secrets, hygiene hooks):

```bash
pre-commit run --all-files
```

Type checking and security linting are not part of pre-commit — run them separately (both also run in CI):

```bash
make typecheck                        # mypy
bandit -c pyproject.toml -r gateway/  # bandit
```

## Project Structure

```
gateway/
├── api/          # FastAPI routers (packages for alpaca/, uw/, finnhub/, alphavantage/; single modules for the rest) + middleware/
├── core/         # Business logic (auth, cache, stream, etc.)
├── providers/    # Data provider implementations
└── schemas/      # Pydantic response models
```

## Testing

### Running Tests

```bash
# All tests
pytest tests/ -v

# With coverage
pytest --cov=gateway --cov-report=term-missing

# Specific file
pytest tests/test_auth.py -v

# WebSocket tests
pytest tests/test_websocket.py -v
```

### Test Requirements

- **Unit tests**: keep overall coverage above the CI floor (`fail_under = 58` in pyproject.toml, ratcheted up as coverage improves); aim to fully cover new code
- **Integration tests**: Mock upstream providers
- **WebSocket tests**: Use `TestClient.websocket_connect()`

## Adding Features

### Adding a New Provider

1. Create `gateway/providers/{name}.py` implementing `DataProvider`
2. Add entry to `config/providers.yaml`
3. Add router in `gateway/api/{name}.py`
4. Register router in `gateway/main.py`
5. Write tests in `tests/test_{name}_provider.py`
6. Update documentation

### Adding a New Endpoint

1. Check PRD.md for specification
2. Add to appropriate router in `gateway/api/`
3. Use dependency injection via `gateway/api/deps.py`
4. Add `response_model=SuccessResponse` for type safety
5. Write tests covering success + error cases
6. Update CHANGELOG.md

## Pull Request Workflow

1. Create feature branch from `master`

   ```bash
   git checkout -b feat/my-feature
   ```

2. Make changes with tests

3. Run quality checks

   ```bash
   pre-commit run --all-files
   pytest tests/ -v
   ```

4. Update documentation:
   - Add entry to CHANGELOG.md under `## [Unreleased]`
   - Update docs/api-reference.md if adding endpoints
   - Update README.md if changing user-facing features

5. Open PR with description

## Commit Messages

Use conventional commits:

| Prefix | Description |
|--------|-------------|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `docs:` | Documentation only |
| `refactor:` | Code restructuring |
| `test:` | Test changes |
| `chore:` | Build, deps, etc. |

Examples:

```
feat: add /catalog/streams API discovery endpoint
fix: handle MessagePack decoding for OPRA options stream
docs: update README with WebSocket feed examples
```

## Documentation

- **README.md**: User-facing overview and quickstart
- **docs/api-reference.md**: Complete endpoint reference
- **CHANGELOG.md**: Version history (keep updated!)
- **PRD.md**: Product specification (source of truth)

## Getting Help

- Check existing issues for similar problems
- Review PRD.md for specification questions
- Check AGENTS.md for codebase context
