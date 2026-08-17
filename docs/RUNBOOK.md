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
| `EMPIRE_LOG_LEVEL`                         | `INFO`    | Root log level (`GATEWAY_LOG_LEVEL` is a dead knob — defined in Settings but never read by the runtime) |
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
| `GATEWAY_DATA_SINK_MAX_STREAM_LEN`       | `100000`  | `heber:events` stream MAXLEN (trim cap) |
| `GATEWAY_DATA_SINK_REDIS_POOL_SIZE`      | `32`      | Redis connection pool size           |
| `GATEWAY_DATA_SINK_QUEUE_SIZE`           | `16384`   | Per-sink bounded dispatch queue size |
| `GATEWAY_DATA_SINK_WORKER_COUNT`         | `16`      | Worker tasks draining each sink queue |
| `GATEWAY_DATA_SINK_PRODUCER_BLOCK_TIMEOUT_SECONDS` | `0.1` | Producer block-on-full timeout before drop |
| `GATEWAY_UW_EOD_ENABLED`                | `false`   | Enable daily EOD ticker polling      |
| `GATEWAY_UW_EOD_HOUR`                   | `16`      | EOD poll hour (ET, 0-23)            |
| `GATEWAY_UW_EOD_MINUTE`                 | `30`      | EOD poll minute (ET, 0-59)          |
| `GATEWAY_UW_CORE_TICKERS`               | —         | Comma-separated core ticker override |
| `GATEWAY_UW_DYNAMIC_TICKER_COUNT`       | `20`      | Dynamic tickers from UW screener     |
| `GATEWAY_UW_EOD_CONCURRENCY`            | `5`       | Max concurrent ticker polls          |
| `GATEWAY_UW_POLLER_PUBLISH_MAX_INFLIGHT`| `16`      | Max in-flight poller publishes       |

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
curl -H "X-Gateway-Key: <key>" http://localhost:8080/metrics
```

Returns Prometheus text format. Scrape with Prometheus, Grafana Agent, or Datadog.

Alerting rules live in `config/prometheus_alerts.yml`. The on-call-relevant alerts:

| Alert                       | Severity | Fires when                                           | Meaning |
|-----------------------------|----------|------------------------------------------------------|---------|
| `GatewayDown`               | critical | `up{job="data-gateway"} == 0` for 1m                 | Gateway unreachable |
| `AllUpstreamsDisconnected`  | critical | `sum(gateway_provider_healthy) == 0` for 30s         | No healthy provider; cannot serve data |
| `SinkProducerTimeoutDrops`  | critical | `gateway_sink_producer_timeout_drops_total` non-zero (1m) | Dispatch queue full AND workers stalled — **events permanently lost to Heber** (see § 6.6) |
| `SinkBufferEvictionsActive` | critical | `gateway_sink_buffer_evictions_total` rate > 0 (2m)  | Failed-event buffer overflowing — **silent data loss** (see § 6.7) |
| `SinkBufferNearCapacity`    | warning  | `gateway_sink_buffer_size > 9000` (1m)               | Retry buffer > 90% full; eviction imminent (see § 6.7) |
| `HighErrorRate`             | warning  | 5xx error ratio > 5% for 5m                          | Elevated server errors |
| `ProviderUnhealthy`         | warning  | `gateway_provider_healthy == 0` for 2m               | One provider down |
| `MemoryPressure`            | warning  | `gateway_memory_pressure > 80` for 5m                | Memory above target (see § 6.3) |

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
curl -X POST -H "X-Gateway-Key: <key>" \
  http://localhost:8080/api/v1/providers/finnhub/disable

# Re-enable
curl -X POST -H "X-Gateway-Key: <key>" \
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

For a JetStream canary, leave Redis available for the small backfill-proof
control plane and set matching credentials in Gateway, the broker, and Heber:

```env
GATEWAY_DURABLE_OUTBOX_ENABLED=true
GATEWAY_JETSTREAM_ENABLED=true
GATEWAY_JETSTREAM_LANES=backfill
GATEWAY_JETSTREAM_USERNAME=gateway
GATEWAY_JETSTREAM_PASSWORD=<secret>
NATS_USER=gateway
NATS_PASSWORD=<secret>
```

Start the broker with `docker compose --profile jetstream up -d jetstream`,
then recreate Gateway. In Heber, switch only the backfill consumer:

```bash
HEBER_LIVE_INGEST_TRANSPORT=redis \
HEBER_BACKFILL_INGEST_TRANSPORT=jetstream \
docker compose up -d heber-consumer heber-backfill-consumer
```

This keeps `heber:events` live traffic on Redis while only
`heber:events:backfill` enters the durable outbox. Promote live traffic later
by setting `GATEWAY_JETSTREAM_LANES=both` and
`HEBER_LIVE_INGEST_TRANSPORT=jetstream`.

Gateway admits a replay only when Heber's readiness hash identifies the exact
selected backfill binding: transport, `backfill` lane, stream, and durable
consumer. Missing fields or a Redis/JetStream, stream, or consumer mismatch
fail closed before provider fetch or publication.

Roll back by restoring `HEBER_BACKFILL_INGEST_TRANSPORT=redis`, setting
`GATEWAY_DURABLE_OUTBOX_ENABLED=false` and `GATEWAY_JETSTREAM_ENABLED=false`,
then recreating the two services. Leave the outbox volume intact for
investigation; never delete pending rows.

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
| 401  | Missing / invalid API key  | Check `X-Gateway-Key` header against `config/clients.yaml`  |
| 403  | Insufficient permissions   | Client lacks access to requested provider/feed          |
| 429  | Rate limited               | Check `GET /api/v1/rate-limits`; increase client limit  |
| 502  | Upstream provider error    | Check provider health via `GET /api/v1/providers`       |
| 503  | Circuit breaker open / shutting down | Provider failing (wait for half-open window, § 6.8) or gateway draining |

> Provider HTTP errors are severity-split in the logs: 4xx are client-caused and
> log at WARNING (not a gateway fault), only 5xx log at ERROR. See § 6.9 before
> escalating a flood of `provider_request_failed` / `alpaca_bars_error`.

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
curl -H "X-Gateway-Key: <key>" \
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

### 6.6  Sink producer-timeout drops (`SinkProducerTimeoutDrops` — CRITICAL)

Streaming events are dispatched to each sink through a **bounded `asyncio.Queue`
drained by a worker pool** (`gateway/core/data_sink.py`). The producer is the
upstream Alpaca stream callback; it `put`s onto the queue and blocks for at most
`GATEWAY_DATA_SINK_PRODUCER_BLOCK_TIMEOUT_SECONDS` (default 0.1s) when the queue
is full. This is the **only drop path** in the dispatch layer — and **every drop
is a permanently lost event** (it never reaches Heber, with no buffer or retry).

A drop increments `gateway_sink_producer_timeout_drops_total{sink}` and logs at
`CRITICAL` (`data_sink_producer_timeout_drop`). A non-zero rate means the queue
is full **and** workers cannot drain it within 100ms — a true sink stall or hard
overload, not steady-state backpressure (the queue absorbs ~45s of a 350 ev/s
opening-bell burst at the default size).

Triage:

1. Confirm the rate: `curl -s localhost:8080/metrics | grep producer_timeout_drops`.
2. The workers are blocked on `sink.publish` → downstream Redis is the usual
   cause. Check Redis latency and the sink circuit breaker (§ 6.8) and Redis
   connectivity (§ 6.5).
3. If Redis is healthy but throughput is genuinely higher than provisioned,
   raise `GATEWAY_DATA_SINK_WORKER_COUNT` and/or `GATEWAY_DATA_SINK_QUEUE_SIZE`
   and restart.

### 6.7  Failed-event buffer eviction / silent data loss (`SinkBufferEvictionsActive`)

When a publish exhausts its retries (or the sink circuit is OPEN), the event is
held in the Redis sink's **failed-event buffer** — a `deque` capped at
`GATEWAY_DATA_SINK_FAILED_BUFFER_CAPACITY` entries (default 50,000; was a fixed
10,000 at the time of the 2026-05-05 incident below) (`gateway/core/redis_sink.py`).
The buffer **drains automatically on the
next successful reconnect**. While it is filling it emits the
`gateway_sink_buffer_size{sink}` gauge; once full, each new event **evicts the
oldest** (permanently lost) and increments
`gateway_sink_buffer_evictions_total{sink}`.

`SinkBufferNearCapacity` (warning, >9000) is the early signal; act before
`SinkBufferEvictionsActive` (critical) starts paging.

**This failure mode caused a 32-hour silent data outage on 2026-05-05.**
`docker-compose.yml` had raised `GATEWAY_DATA_SINK_MAX_STREAM_LEN` to 500,000
while Redis was still capped at `--maxmemory 1gb`. The `heber:events` stream
crossed ~200K entries, Redis hit memory pressure, `XADD` started timing out past
the 5s operation timeout, the `data_sink:redis_streams` circuit opened, and every
publish was buffered locally. The 10,000-entry buffer overflowed within minutes
and evicted for 32 hours before anyone noticed (Kairos's Scout fetched 0 flow
alerts the next trading day).

Recovery:

1. Check the stream size against the Redis memory cap:
   ```bash
   redis-cli -u $GATEWAY_DATA_SINK_REDIS_URL XLEN heber:events
   redis-cli -u $GATEWAY_DATA_SINK_REDIS_URL CONFIG GET maxmemory
   redis-cli -u $GATEWAY_DATA_SINK_REDIS_URL INFO memory | grep used_memory_human
   ```
   At ~3 KB/entry, ensure `MAXLEN × 3 KB` plus the dedup cache fits under
   `maxmemory`. Keep `GATEWAY_DATA_SINK_MAX_STREAM_LEN` consistent with the
   Redis memory budget — never raise the stream cap without raising `maxmemory`.
2. If Redis is memory-pressured, raise the cap live and persist it in compose:
   ```bash
   redis-cli -u $GATEWAY_DATA_SINK_REDIS_URL CONFIG SET maxmemory 2gb
   ```
3. Once `XADD` latency normalizes the `data_sink:redis_streams` circuit closes
   automatically and the buffer drains on reconnect. Confirm recovery with the
   buffer gauge falling to 0 and stream growth resuming.

> Note: the failed-event buffer is **in-memory only**. Events still buffered when
> the gateway shuts down are logged (`redis_sink_close_buffer_nonempty`) but
> **not persisted to disk** — they are lost.

### 6.8  Sink circuit breaker behaviour

The data-sink circuit (`gateway/core/circuit_breaker.py`,
`data_sink:redis_streams`) opens after **20 consecutive failures** and probes
recovery after **15s** (HALF_OPEN → CLOSED on 2 successes). Each counted failure
has already survived 3 in-sink retries, so reaching 20 means Redis is genuinely
down.

Opening the **data-sink** circuit is **controlled degradation**, not an incident:
it logs at WARNING with code `GW-W1013` (`circuit_opened`), events route to the
failed-event buffer (§ 6.7), and it self-heals when Redis recovers. **Provider**
circuits (`alpaca_rest`, `uw_rest`, etc.) instead log at ERROR (`GW-E1011`) and
are genuine upstream failures. Inspect/reset via `GET /api/v1/status` and the
circuit-breaker registry.

### 6.9  Error-log triage — provider HTTP errors are severity-split

Provider/API HTTP errors are split by status code, so **log severity tells you
whether it is your problem** (`gateway/api/alpaca/common.py`,
`gateway/providers/alpaca/market.py`):

- **4xx → WARNING, NOT a gateway fault.** These are client-caused — e.g. a client
  requesting an index symbol like `SPX` from `/v2/stocks/bars` returns 400. A
  flood of `provider_request_failed` / `alpaca_bars_error` at **WARNING** with a
  4xx `status_code` is a **misconfigured client**, not an incident. Do not chase
  these; identify the client (the log carries its ID/symbol context) and fix the
  request.
- **5xx → ERROR.** Only these are genuine upstream provider failures worth
  paging on. `HighErrorRate` keys off the 5xx ratio, not 4xx.

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

On `SIGTERM` or `SIGINT`, the lifespan handler (`gateway/main.py`) runs the
8-step sequence tracked by `ShutdownCoordinator` (`gateway/core/shutdown.py`):

1. **Mark shutting down** — `/health` and `/health/ready` return 503 `shutting_down`.
2. **Notify connected clients** — broadcast a shutdown message over WebSocket.
3. **Drain period** — continue delivering queued messages for up to
   `GATEWAY_SHUTDOWN_DRAIN_SECONDS` (default 30s).
4. **Stop option capture + multiplexer** — unsubscribe from upstream Alpaca.
5. **Close client connections** — WebSocket close frame `1001` Going Away.
6. **Close sink connections** — `sink_registry.close_all()` drains the per-sink
   worker queues, then makes a final attempt to flush the failed-event buffer to
   Redis (events still buffered after this are logged and lost — § 6.7).
7. **Shutdown remaining services** — pollers (UW, treasury, quotes, trades,
   crypto, news), backfill engine, provider registry.
8. **Reset the shutdown coordinator.**

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
curl -H "X-Gateway-Key: $GW_KEY" localhost:8080/api/v1/providers

# Rate limit status
curl -H "X-Gateway-Key: $GW_KEY" localhost:8080/api/v1/rate-limits

# Recent errors
curl -H "X-Gateway-Key: $GW_KEY" localhost:8080/api/v1/logs

# Prometheus metrics
curl -H "X-Gateway-Key: $GW_KEY" localhost:8080/metrics

# Smoke test all providers
python scripts/live_provider_smoke.py

# Run test suite
pytest tests/ -v
```
