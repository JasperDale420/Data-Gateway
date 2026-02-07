# Contributing to Data Gateway

Thank you for contributing to Data Gateway! This document provides guidelines for development.

## Quick Start

```bash
# Clone and setup
cd Data-Gateway
pip install -e ".[dev]"
pre-commit install

# Copy and configure environment
cp .env.example .env
# Edit .env with your API keys

# Run tests
pytest tests/ -v

# Start locally
uvicorn gateway.main:app --reload --port 8080
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
| **ruff** | Linting and formatting |
| **black** | Code formatting (100 char line length) |
| **pyright** | Type checking |
| **bandit** | Security linting |
| **detect-secrets** | Prevent accidental secret commits |

Run all checks:

```bash
pre-commit run --all-files
```

## Project Structure

```
gateway/
├── api/          # FastAPI route handlers (one file per provider)
├── core/         # Business logic (auth, cache, stream, etc.)
├── providers/    # Data provider implementations
└── schemas.py    # Pydantic response models
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

- **Unit tests**: 80%+ coverage on new code
- **Integration tests**: Mock upstream providers
- **WebSocket tests**: Use `TestClient.websocket_connect()`

## Adding Features

### Adding a New Provider

1. Create `gateway/providers/{name}.py` implementing `DataProvider`
2. Add entry to `config/providers.yaml`
3. Add router in `gateway/api/{name}.py`
4. Register router in `gateway/main.py`
5. Write tests in `tests/providers/test_{name}.py`
6. Update documentation

### Adding a New Endpoint

1. Check PRD.md for specification
2. Add to appropriate router in `gateway/api/`
3. Use dependency injection via `gateway/api/deps.py`
4. Add `response_model=SuccessResponse` for type safety
5. Write tests covering success + error cases
6. Update CHANGELOG.md

## Pull Request Workflow

1. Create feature branch from `main`

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
   - Update API_REFERENCE.md if adding endpoints
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
- **API_REFERENCE.md**: Complete endpoint reference
- **CHANGELOG.md**: Version history (keep updated!)
- **PRD.md**: Product specification (source of truth)

## Getting Help

- Check existing issues for similar problems
- Review PRD.md for specification questions
- Check CLAUDE.MD for codebase context
