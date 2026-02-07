# Data Gateway — Operations Runbook

Day-to-day operational procedures for running, monitoring, and troubleshooting the Data Gateway.

---

## 1  Startup

### 1.1  Local (bare-metal)

```bash
# Install dependencies (first time / after lock file changes)
uv sync            # or: pip install -e ".[dev]"

# Copy and configure environment
cp .env.example .env
# Edit .env — fill in provider API keys (see § 2)

# Run
uvicorn gateway.main:app --reload --port 8080
```

### 1.2  Docker Compose

```bash
docker-compose up --build          # foreground
docker-compose up --build -d       # background
docker-compose logs -f gateway     # tail logs
```

Services started:

| Service   | Container             | Port  | Purpose                       |
|-----------|-----------------------|-------|-------------------------------|
| `gateway` | `data-gateway`        | 8080  | FastAPI application           |
| `redis`   | `data-gateway-redis`  | 6379  | Cache + Heber data-sink store |

### 1.3  Verify startup

```bash
curl http://localhost:8080/health          # → {"status":"ok"}
curl http://localhost:8080/health/ready    # → {"status":"ready", "checks":{…}}
```

If `/health/ready` returns `"not_ready"`, inspect the `checks` object to identify the failing component (cache, sinks, etc.).

---

## 2  Configuration Reference

All settings use the `GATEWAY_` prefix (except provider API keys).
Full defaults live in `gateway/config.py`; see `.env.example` for a commented template.

### 2.1  Provider API Keys

| Variable                  | Provider        | Required |
|---------------------------|-----------------|----------|
| `APCA_API_KEY_ID`         | Alpaca          | Yes      |
| `APCA_API_SECRET_KEY`     | Alpaca          | Yes      |
| `UNUSUAL_WHALES_API_KEY`  | Unusual Whales  | Yes      |
| `FINNHUB_API_KEY`         | Finnhub         | Yes      |
| `ALPHAVANTAGE_API_KEY`    | Alpha Vantage   | Yes      |
| `NEWS_API_KEY`            | NewsAPI.org     | Optional |
| `SEC_USER_AGENT`          | SEC EDGAR       | Optional |

*yfinance and SEC EDGAR work without keys.*

### 2.2  Key Tuning Parameters

| Variable                                   | Default   | Purpose                              |
|--------------------------------------------|-----------|--------------------------------------|
| `GATEWAY_LOG_LEVEL`                        | `INFO`    | Root log level                       |
| `GATEWAY_CACHE_DEFAULT_TTL`               | `300`     | Default cache TTL (seconds)          |
| `GATEWAY_CACHE_MAX_SIZE`                  | `10000`   | Max in-memory cache entries          |
| `GATEWAY_RATE_LIMIT_DEFAULT`              | `600`     | Default rate limit (req/min)         |
| `GATEWAY_WS_MAX_CLIENTS`                  | `1000`    | Max concurrent WebSocket clients     |
| `GATEWAY_WS_IDLE_TIMEOUT`                 | `300`     | WS idle disconnect (seconds)         |
| `GATEWAY_STREAM_USE_IEX`                  | `false`   | Use IEX feed instead of SIP          |
| `GATEWAY_MEMORY_TARGET_MB`                | `512`     | Memory target before GC pressure     |
| `GATEWAY_MEMORY_HARD_LIMIT_MB`            | `1024`    | Hard memory ceiling                  |
| `GATEWAY_SHUTDOWN_DRAIN_SECONDS`          | `30`      | Graceful shutdown drain period       |
| `GATEWAY_DATA_SINK_ENABLED`              | `false`   | Enable Redis Streams data sink       |
| `GATEWAY_DATA_SINK_REDIS_URL`            | —         | Redis URL for data sink              |

### 2.3  Config Files

| File                      | Purpose                                       |
|---------------------------|-----------------------------------------------|
| `config/providers.yaml`   | Provider registry: module, class, capabilities |
| `config/clients.yaml`     | Client API keys, permissions, rate limits     |

---

## 3  Health & Monitoring

### 3.1  Health Endpoints

| Endpoint           | Auth | Purpose                                        |
|--------------------|------|------------------------------------------------|
| `GET /health`      | No   | **Liveness** — returns `{"status":"ok"}`       |
| `GET /health/ready`| No   | **Readiness** — checks cache, connections, sinks |
| `GET /health/status`| No  | **Detailed** — version, component stats        |

### 3.2  Prometheus Metrics

```bash
curl -H "X-API-Key: <key>" http://localhost:8080/metrics
```

Returns Prometheus text format. Scrape with Prometheus, Grafana Agent, or Datadog.

### 3.3  Admin Endpoints (require API key)

| Endpoint                             | Method | Purpose                            |
|--------------------------------------|--------|------------------------------------|
| `/api/v1/status`                     | GET    | System status + connected clients  |
| `/api/v1/logs`                       | GET    | Recent error logs (in-memory)      |
| `/api/v1/errors/summary`            | GET    | Error code counts (last hour)      |
| `/api/v1/rate-limits`               | GET    | Provider rate-limit status         |
| `/api/v1/providers`                  | GET    | List all providers + health        |
| `/api/v1/providers/{name}/enable`   | POST   | Enable a provider at runtime       |
| `/api/v1/providers/{name}/disable`  | POST   | Disable a provider at runtime      |

---

## 4  Common Operations

### 4.1  Restart the gateway

```bash
# Docker
docker-compose restart gateway

# Local — SIGHUP triggers config reload without full restart
kill -HUP $(pgrep -f "uvicorn gateway.main:app")
```

### 4.2  Rotate provider API keys

1. Update the key in `.env` (or secrets manager).
2. Restart the gateway (see § 4.1).
3. Verify with: `curl http://localhost:8080/health/ready`

### 4.3  Add a new client

Edit `config/clients.yaml`:

```yaml
- id: my-new-client
  key: gw_my_new_client_key_xxxxx
  permissions:
    providers: ["alpaca", "uw"]
    feeds: ["bars", "quotes", "trades", "flow"]
    max_symbols: 500
    rate_limit: 300
  enabled: true
```

Restart the gateway for changes to take effect.

### 4.4  Enable / disable a provider at runtime

```bash
# Disable finnhub without restart
curl -X POST -H "X-API-Key: <key>" \
  http://localhost:8080/api/v1/providers/finnhub/disable

# Re-enable
curl -X POST -H "X-API-Key: <key>" \
  http://localhost:8080/api/v1/providers/finnhub/enable
```

### 4.5  Enable the Heber data sink

Set in `.env`:

```env
GATEWAY_DATA_SINK_ENABLED=true
GATEWAY_DATA_SINK_REDIS_URL=redis://localhost:6379/1
```

Restart. Verify sink health:

```bash
curl http://localhost:8080/health/ready
# "sinks": "ok" should appear in checks
```

---

## 5  WebSocket Operations

### 5.1  Connect and subscribe

```python
import websockets, json

async with websockets.connect("ws://localhost:8080/ws") as ws:
    # Authenticate (must happen within 10s)
    await ws.send(json.dumps({
        "action": "auth",
        "key": "gw_your_api_key"
    }))
    resp = await ws.recv()

    # Subscribe
    await ws.send(json.dumps({
        "action": "subscribe",
        "bars": ["AAPL", "MSFT"],
        "quotes": ["AAPL"]
    }))
```

### 5.2  Key limits

| Parameter                     | Default | Notes                             |
|-------------------------------|---------|-----------------------------------|
| Auth timeout                  | 10s     | Client must auth within this window |
| Idle timeout                  | 5 min   | Disconnects idle clients          |
| Max connection duration       | 24h     | Hard session limit                |
| Heartbeat interval            | 30s     | Server → client ping              |
| Max message size              | 64 KB   | Per WebSocket frame               |

### 5.3  Troubleshooting WebSocket

| Symptom                       | Likely Cause                           | Fix                               |
|-------------------------------|----------------------------------------|------------------------------------|
| `auth_timeout`                | Client didn't send auth within 10s     | Send auth message immediately      |
| `connection_limit_exceeded`   | Upstream provider connection cap        | Close stale sessions; check `GATEWAY_WS_MAX_CLIENTS` |
| No data after subscribe       | Symbol not streaming on provider       | Verify symbol format (see API_REFERENCE.md) |

---

## 6  Troubleshooting

### 6.1  Gateway won't start

| Error                                  | Cause                                     | Resolution                                    |
|----------------------------------------|-------------------------------------------|-----------------------------------------------|
| `ValidationError` on startup           | Missing or invalid env var                | Check `.env` against `.env.example`           |
| `Provider initialization failed`       | Bad API key or provider unreachable       | Verify keys; check network; check `config/providers.yaml` |
| `Address already in use`               | Port 8080 occupied                        | `lsof -i :8080` and kill the process          |
| Redis connection refused (Docker)      | Redis not started yet                     | `docker-compose up redis` first; check `depends_on` |

### 6.2  HTTP errors

| Code | Meaning                    | Action                                                  |
|------|----------------------------|---------------------------------------------------------|
| 401  | Missing / invalid API key  | Check `X-API-Key` header against `config/clients.yaml`  |
| 403  | Insufficient permissions   | Client lacks access to requested provider/feed          |
| 429  | Rate limited               | Check `GET /api/v1/rate-limits`; increase client limit  |
| 502  | Upstream provider error    | Check provider health via `GET /api/v1/providers`       |
| 503  | Circuit breaker open       | Provider is failing; wait for half-open retry window    |

### 6.3  High memory

1. Check current memory: `GET /metrics` → `process_resident_memory_bytes`
2. If above `GATEWAY_MEMORY_TARGET_MB`, the GC pressure loop should kick in.
3. If approaching `GATEWAY_MEMORY_HARD_LIMIT_MB`, consider:
   - Reducing `GATEWAY_CACHE_MAX_SIZE`
   - Lowering `GATEWAY_WS_MAX_CLIENTS`
   - Restarting the service

### 6.4  Provider rate limits

```bash
# Check which provider is throttled
curl -H "X-API-Key: <key>" \
  "http://localhost:8080/api/v1/rate-limits?provider=alpaca"
```

If a provider is consistently rate-limited, consider reducing the client-level `rate_limit` in `config/clients.yaml` or upgrading the provider plan.

### 6.5  Redis connectivity (data sink)

```bash
# From inside the gateway container
redis-cli -u $GATEWAY_DATA_SINK_REDIS_URL ping   # → PONG

# Check stream length
redis-cli -u $GATEWAY_DATA_SINK_REDIS_URL XLEN heber:events
```

---

## 7  Maintenance Scripts

| Script                                 | Purpose                                      | Usage                                        |
|----------------------------------------|----------------------------------------------|----------------------------------------------|
| `scripts/live_provider_smoke.py`       | Smoke-test all providers (health + sample)   | `python scripts/live_provider_smoke.py`      |
| `scripts/generate_provider_contract.py`| Regenerate endpoint contract from code       | `python scripts/generate_provider_contract.py` |
| `scripts/perf_gate.py`                | Run performance gate (budget enforcement)    | `python scripts/perf_gate.py`                |
| `scripts/perf_baseline_manager.py`    | Manage performance baselines                 | `python scripts/perf_baseline_manager.py`    |
| `scripts/perf_release_readiness.py`   | Promote perf configs for release             | `python scripts/perf_release_readiness.py`   |
| `scripts/perf_promote_active_configs.py` | Promote active performance configurations | `python scripts/perf_promote_active_configs.py` |

---

## 8  Graceful Shutdown

On `SIGTERM` or `SIGINT`:

1. Gateway stops accepting new connections.
2. In-flight requests drain for up to `GATEWAY_SHUTDOWN_DRAIN_SECONDS` (default 30s).
3. WebSocket connections receive a close frame.
4. Stream sink publish tasks drain (2s timeout).
5. Provider connections close.
6. Process exits.

On `SIGHUP`:

- Triggers config reload (re-reads providers, refreshes settings) without full restart.

---

## 9  Docker Rebuild Checklist

When code changes affect the container:

```bash
# 1. Rebuild
docker-compose build gateway

# 2. Restart
docker-compose up -d gateway

# 3. Verify
curl http://localhost:8080/health/ready

# 4. Check logs
docker-compose logs -f gateway --tail=50
```

---

## 10  Quick Reference

```bash
# Health check
curl localhost:8080/health

# Readiness
curl localhost:8080/health/ready

# Provider status
curl -H "X-API-Key: $GW_KEY" localhost:8080/api/v1/providers

# Rate limit status
curl -H "X-API-Key: $GW_KEY" localhost:8080/api/v1/rate-limits

# Recent errors
curl -H "X-API-Key: $GW_KEY" localhost:8080/api/v1/logs

# Prometheus metrics
curl -H "X-API-Key: $GW_KEY" localhost:8080/metrics

# Smoke test all providers
python scripts/live_provider_smoke.py

# Run test suite
pytest tests/ -v
```
