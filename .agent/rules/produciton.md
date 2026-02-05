---
trigger: always_on
---

# Data Gateway Development Rules

## Core References
- **PRD.md** — The single source of truth for all specifications
- **CHANGELOG.md** — Update after every commit
- **README.md** — Keep setup/run/test instructions current

## Architecture Principles

### Provider Plugin System
- All data sources implement the `DataProvider` abstract base class
- Providers are registered via `config/providers.yaml`, not hardcoded
- Use normalized schemas (`NormalizedBar`, `NormalizedQuote`, etc.)
- Provider additions require: implementation + config + tests

### WebSocket First
- Real-time data flows through WebSocket multiplexer
- REST is for historical/bulk data only
- Maintain subscription reference counting for upstream efficiency

### Error Handling
- Fail loudly on startup issues (missing config, bad credentials)
- Use error codes from PRD (GW-Exxxx format)
- Log structured JSON with correlation IDs

## Implementation Standards

### Adding a New Provider
1. Create `gateway/providers/{name}.py` implementing `DataProvider`
2. Add entry to `config/providers.yaml`
3. Write tests: `tests/providers/test_{name}.py`
4. Verify: initialization, health check, data normalization

### Adding a New Endpoint
1. Check PRD for endpoint specification
2. Add to appropriate router in `gateway/api/`
3. Use dependency injection via `gateway/api/deps.py`
4. Write tests covering success + error cases
5. Update OpenAPI schema if needed

### Testing Requirements
- Unit tests: 80%+ coverage on new code
- Integration tests: mock upstream providers
- Run `pytest tests/ -v` before every commit
- WebSocket tests use `TestClient` with `websocket_connect()`

## Code Patterns

### Config via Environment
```python
from gateway.config import get_settings
settings = get_settings()  # Cached singleton
```

### Dependency Injection
```python
from gateway.api.deps import get_cache, get_authenticator

@router.get("/endpoint")
async def handler(cache: InMemoryCache = Depends(get_cache)):
    ...
```

### Structured Logging
```python
import structlog
logger = structlog.get_logger()
logger.info("event_name", key="value", error=str(e))
```

### Cache Usage
```python
cache = get_cache()
if cached := cache.get(cache_key):
    return cached
result = await fetch_data()
cache.set(cache_key, result, ttl=300)
```

## Build & Deploy

### Local Development
```bash
pip install -e ".[dev]"
uvicorn gateway.main:app --reload --port 8080
```

### Docker
```bash
docker-compose up --build
```

### Verification Checklist
- [ ] `pytest tests/ -v` passes
- [ ] `ruff check gateway/` no errors
- [ ] Health endpoints return 200
- [ ] WebSocket auth handshake works
- [ ] CHANGELOG.md updated

## PRD Section Reference

| Topic | PRD Section |
|-------|-------------|
| API endpoints | API Specification |
| Error codes | Error Handling |
| WebSocket protocol | WebSocket API |
| Provider interface | Provider Extensibility Architecture |
| Data schemas | Data Architecture |
| Rate limiting | Rate Limiting |
| Security | Security Architecture |
| Testing | Quality Assurance & Testing |
