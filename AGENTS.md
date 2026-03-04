# AGENTS.md

Project-specific AI agent instructions for Data Gateway.

## Project Overview

Data Gateway is a FastAPI service that gives one unified API and WebSocket layer for market data providers (Alpaca, Unusual Whales, Finnhub, Alpha Vantage, Yahoo Finance, SEC, News).

## Architecture

- Use vertical slices by feature area in `gateway/api/`, `gateway/core/`, and `gateway/providers/`.
- Keep provider integrations behind `DataProvider` interfaces.
- Keep response contracts in `gateway/schemas.py` and `gateway/api/schemas.py`.
- Preserve safe defaults for auth, rate limits, and trading roles.

## Development Commands

```bash
pip install -e ".[dev]"
pytest tests/ -v
ruff check .
mypy .
uvicorn gateway.main:app --reload --port 8080
docker-compose up --build
```

## Key Patterns

- Fail fast on startup/config/auth errors.
- Continue batch/background processing with structured error logs per failed item.
- Use structured logging (`structlog`) with useful context and `exc_info=True` on exceptions.
- Prefer small, test-first changes and update docs/changelog in the same change.

## Important Files

- `README.md` - quick start and feature overview
- `PRD.md` - product and API requirements
- `docs/ARCHITECTURE.md` - system design and data flow
- `docs/RUNBOOK.md` - operations and incident procedures
- `docs/API_REFERENCE.md` - API reference entry point

## Testing

- Write a failing test before production changes.
- Add regression tests for every bug fix.
- Run `pytest -q && ruff check . && mypy .` before finalizing.

## Common Pitfalls

- Bypassing normalized schemas and returning provider-specific payloads directly.
- Forgetting to enforce client permissions for new REST/WS surfaces.
- Letting docs drift from implemented routes. Regenerate contract with:
  `python scripts/generate_provider_contract.py`.
