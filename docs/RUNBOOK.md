# Runbook

## Purpose

This runbook covers startup, health checks, troubleshooting, and recovery procedures for Data Gateway.

## Startup

### Local

```bash
cp .env.example .env
pip install -e ".[dev]"
uvicorn gateway.main:app --host 0.0.0.0 --port 8080 --reload
```

### Docker

```bash
docker-compose up --build -d
docker-compose ps
```

## Health Checks

- Liveness: `GET /health/live`
- Readiness: `GET /health/ready`
- Service status: `GET /health/status`

Example:

```bash
curl http://localhost:8080/health/live
```

## Common Incidents

### 1. Auth failures (`401`)

- Confirm `X-Gateway-Key` is present.
- Verify key exists in `config/clients.yaml`.
- Verify provider/feed permissions for that client.

### 2. Upstream provider errors (`5xx`/timeouts)

- Check provider credentials in `.env`.
- Verify network access and provider service status.
- Review structured logs for provider-specific context and tracebacks.

### 3. WebSocket disconnect churn

- Validate auth message is sent immediately after connect.
- Confirm symbol and feed permissions.
- Check multiplexer logs for reconnect attempts and terminal errors.

### 4. Cache/data sink Redis issues

- Verify Redis URL settings (`GATEWAY_CACHE_REDIS_URL`, `GATEWAY_DATA_SINK_REDIS_URL`).
- In Docker, ensure host is `redis` not `localhost`.
- Confirm Redis service health in `docker-compose ps`.

## Recovery Steps

1. Capture failing endpoint/feed and client id.
2. Check logs with request id / event context.
3. Validate config and credentials.
4. Restart service (or stack) after config correction.
5. Re-run health checks and one representative API call.

## Operational Commands

```bash
# test suite
pytest -q

# lint + type checks
ruff check .
mypy .

# regenerate provider contract docs
python scripts/generate_provider_contract.py

# verify contract in CI mode
python scripts/generate_provider_contract.py --check
```

## Escalation Data to Collect

- Exact timestamp (UTC)
- Endpoint or feed affected
- Client id / role
- Provider(s) involved
- Error code and message
- Relevant log lines with traceback
