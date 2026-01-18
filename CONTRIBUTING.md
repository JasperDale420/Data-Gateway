# Contributing to Data Gateway

## Development Setup

```bash
pip install -e ".[dev]"
pre-commit install
```

## Code Style

- **Formatter:** black, ruff
- **Linter:** ruff, pyright
- **Security:** bandit, detect-secrets

Run all checks:

```bash
pre-commit run --all-files
```

## Pull Request Workflow

1. Create feature branch from `main`
2. Make changes with tests
3. Run `pytest tests/ -v`
4. Update `CHANGELOG.md` under `## [Unreleased]`
5. Open PR with description

## Testing Requirements

- Unit tests for new functionality
- 80%+ coverage on new code
- Integration tests mock upstream providers

```bash
pytest tests/ -v
pytest --cov=gateway --cov-report=term-missing
```

## Adding a Provider

1. Create `gateway/providers/{name}.py` implementing `DataProvider`
2. Add entry to `providers.yaml`
3. Add router in `gateway/api/{name}.py`
4. Write tests in `tests/providers/test_{name}.py`

## Commit Messages

Use conventional commits:

- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation
- `refactor:` code refactoring
- `test:` test changes
