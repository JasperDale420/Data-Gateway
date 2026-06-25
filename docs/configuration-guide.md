# Data-Gateway — Configuration Guide

Every environment variable, every YAML knob. For ops procedures see [RUNBOOK.md](RUNBOOK.md).

---

## 1. Where Configuration Lives

| Source | Purpose |
|--------|---------|
| `gateway/config.py` | Pydantic `Settings(BaseSettings)`. Single source of truth for defaults and types. |
| `.env.example` | Commented template — copy to `.env` for local dev. |
| `.env` | Local secrets and overrides. **Gitignored.** |
| `config/providers.yaml` | Provider load order, classes, capabilities. |
| `config/clients.yaml` | Per-client API keys, roles, permissions. SIGHUP-reloadable. |
| `config/perf_baseline.json` / `config/perf_budgets.json` | Perf-gate thresholds. |
| `config/prometheus_alerts.yml` | Prometheus alerting rules. |

All `GATEWAY_*` env vars are loaded by Pydantic Settings with the `GATEWAY_` prefix. Provider API keys use their provider's native env-var name (no `GATEWAY_` prefix) so SDKs pick them up unchanged.

---

## 2. Core Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_HOST` | `0.0.0.0` | Bind host |
| `GATEWAY_PORT` | `8080` | Bind port |
| `GATEWAY_DEBUG` | `false` | Enables `/docs`, OpenAPI, permissive CORS. **Never** true in prod. |
| `GATEWAY_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `GATEWAY_AUTH_TIMEOUT_SECONDS` | `10` | WebSocket auth-message timeout |
| `GATEWAY_ALLOW_STUB_DATA` | `false` | If false, stub-only endpoints return `501 Not Implemented` |
| `GATEWAY_SHUTDOWN_DRAIN_SECONDS` | `30` | Graceful shutdown drain window |
| `GATEWAY_MEMORY_TARGET_MB` | `512` | Memory target before GC pressure |
| `GATEWAY_MEMORY_HARD_LIMIT_MB` | `1024` | Hard ceiling |

### Empire-wide logging (consumed by `empire_core.logger`)

| Variable | Default | Description |
|----------|---------|-------------|
| `EMPIRE_LOG_LEVEL` | inherits `GATEWAY_LOG_LEVEL` | Overrides global log level |
| `EMPIRE_LOG_FORMAT` | `json` | `json` (prod) or `human` / `dev` (console) |
| `EMPIRE_LOG_DIR` | `./logs` | Directory for rotating daily log files |

---

## 3. Cache

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_CACHE_MAX_SIZE` | `10000` | L1 in-memory entry cap |
| `GATEWAY_CACHE_DEFAULT_TTL` | `300` | Default TTL seconds |
| `GATEWAY_CACHE_REDIS_URL` | — | If set, enables Redis L2 |
| `GATEWAY_CACHE_MAX_BODY_BYTES` | `524288` | 512 KB — skip caching bodies larger than this |

Cache keys include the `X-Gateway-Key` client id (per-client scoping prevents cross-client data leakage). Per-endpoint TTL overrides happen in router code.

---

## 4. Rate Limiting & Auth

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_RATE_LIMIT_DEFAULT` | `600` | Per-client req/min (overridable per-client in `clients.yaml`) |
| `GATEWAY_WS_MAX_CLIENTS` | `1000` | Max concurrent WebSocket clients |
| `GATEWAY_WS_IDLE_TIMEOUT` | `300` | WS idle disconnect seconds |
| `GATEWAY_WS_MAX_DURATION` | `86400` | Max WebSocket connection duration (seconds) |
| `GATEWAY_BEHIND_TRUSTED_PROXY` | `false` | Trust `X-Forwarded-For` header |
| `GATEWAY_TRUSTED_PROXY_CIDRS` | — | Comma-separated CIDRs of trusted proxies |
| `GATEWAY_GC_THRESHOLD_PCT` | `80` | Trigger Python GC at this % memory usage |

> **Note:** There is no `GATEWAY_RATE_LIMIT_GLOBAL` env var. `GlobalRateLimitMiddleware` enforces a hard cap of 10,000 req/min globally and 1,000 req/min per IP by default; these limits are set in code.

---

## 5. Data Sink (Heber egress)

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_DATA_SINK_ENABLED` | `false` | Master switch for Redis Streams sink |
| `GATEWAY_DATA_SINK_REDIS_URL` | — | Redis URL for the sink (often the same Redis as the cache) |
| `GATEWAY_DATA_SINK_REDIS_POOL_SIZE` | `32` | Redis connection pool size; must be >= `GATEWAY_DATA_SINK_WORKER_COUNT` |
| `GATEWAY_DATA_SINK_MAX_STREAM_LEN` | `100000` | `heber:events` MAXLEN trim cap |
| `GATEWAY_DATA_SINK_QUEUE_SIZE` | `16384` | Bounded per-sink dispatch queue |
| `GATEWAY_DATA_SINK_WORKER_COUNT` | `16` | Worker tasks draining each queue |
| `GATEWAY_DATA_SINK_PRODUCER_BLOCK_TIMEOUT_SECONDS` | `0.1` | Max producer block on a full queue before drop |
| `GATEWAY_DATA_SINK_OPERATION_TIMEOUT_SECONDS` | `5.0` | Redis operation timeout |
| `GATEWAY_REST_SINK_EXCLUDED_FEEDS` | `greek_exposure,iv_rank,iv_term_structure,short_data,ftd,flow_alerts,darkpool` | Feeds NOT published to the live Heber stream via REST (prevents duplicates with pollers) |
| `GATEWAY_REST_SINK_LOW_PRIORITY_MAX_QUEUE_UTILIZATION` | `0.70` | Shed low-priority REST publishes above this queue utilization |
| `GATEWAY_STRICT_ENVELOPES` | `false` | Raise on envelope-wrap failure (default: log and continue) |

See [system-architecture.md §6](system-architecture.md#6-data-sink-heber-integration) for the failure model. Drops surface as `gateway_sink_producer_timeout_drops_total{sink}` (alert: `SinkProducerTimeoutDrops`). Buffer evictions surface as `gateway_sink_buffer_evictions_total{sink}` (alert: `SinkBufferEvictionsActive`).

---

## 6. Streaming

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_STREAM_USE_IEX` | `false` | Use IEX feed instead of SIP for stocks |
| `GATEWAY_STREAM_LAZY_CONNECT` | `true` | Open upstream WS only on first client subscription |
| `GATEWAY_STREAM_RECONNECT_BASE_DELAY` | provider default | Exponential-backoff base delay |
| `GATEWAY_STREAM_OPTIONS_FEED` | `opra` | Options feed: `opra` or `indicative` |
| `GATEWAY_STREAM_EAGER_CONNECT_TYPES` | `stocks` | Stream types to connect on startup (comma-separated: `stocks`, `stocks_iex`, `options`, `crypto`, `news`) |
| `GATEWAY_STREAM_RECONNECT_MAX_RETRIES` | `10` | Max upstream WebSocket reconnect attempts |
| `GATEWAY_STREAM_RECONNECT_MAX_DELAY` | `16.0` | Max exponential backoff seconds |
| `GATEWAY_STREAM_FANOUT_MAX_INFLIGHT` | `100` | Max concurrent downstream fanout operations |
| `GATEWAY_STREAM_FANOUT_BATCH_SIZE` | `32` | Fanout batch size |

---

## 6b. Alpaca Trading Call Timeouts

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_ALPACA_TRADING_CALL_TIMEOUT_SECONDS` | `15.0` | READ timeout for trading calls (`get_account`, `get_orders`, etc.) |
| `GATEWAY_ALPACA_TRADING_WRITE_CALL_TIMEOUT_SECONDS` | `25.0` | WRITE timeout (`create_order`, `cancel_order`, etc.) — bumped for opening-bell latency spikes |
| `GATEWAY_ALPACA_TRADING_HTTP_TIMEOUT_SECONDS` | `30.0` | HTTP-level safety-net timeout |
| `GATEWAY_ALPACA_TRADING_MAX_INFLIGHT` | `24` | Max concurrent Alpaca trading calls; 503 when exceeded |
| `GATEWAY_ALPACA_MAX_CONCURRENT_REQUESTS` | `25` | Max concurrent Alpaca market-data requests |
| `GATEWAY_ALPACA_RATE_LIMIT_PER_MINUTE` | `10000` | Alpaca per-minute rate limit |
| `GATEWAY_ALPACA_RATE_LIMIT_PER_SECOND` | `75` | Alpaca per-second rate limit |

---

## 7. UW Poller — Real-Time & EOD

The UW poller starts automatically when `GATEWAY_DATA_SINK_ENABLED=true` and `UNUSUAL_WHALES_API_KEY` is present. There is no separate enable flag for real-time polling.

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_UW_EOD_ENABLED` | `false` | Enable daily EOD per-ticker polling |
| `GATEWAY_UW_EOD_HOUR` | `16` | EOD hour (ET, 0-23) |
| `GATEWAY_UW_EOD_MINUTE` | `30` | EOD minute (ET) |
| `GATEWAY_UW_CORE_TICKERS` | (built-in ~30) | Comma-separated override of core ticker set |
| `GATEWAY_UW_DYNAMIC_TICKER_COUNT` | `20` | Daily dynamic tickers from UW screener |
| `GATEWAY_UW_EOD_CONCURRENCY` | `5` | Max concurrent ticker polls |
| `GATEWAY_UW_POLLER_PUBLISH_MAX_INFLIGHT` | `16` | Max in-flight poller publishes |
| `GATEWAY_UW_EOD_STATE_PATH` | `/app/logs/state/uw_eod_state.json` | Persistent state file (prevents duplicate EOD runs across restarts) |
| `GATEWAY_UW_EOD_CLAIM_STALE_AFTER_SECONDS` | `7200` | Allow retry when a state claim is older than this |

See [system-architecture.md §8](system-architecture.md#8-background-pollers) for the feed cadence table.

---

## 8. Option Capture

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_OPTION_CAPTURE_ENABLED` | `false` | Enable Alpaca option chain snapshot capture |
| `GATEWAY_OPTION_CAPTURE_SYMBOLS` | — | Comma-separated underlyings |
| `GATEWAY_OPTION_CAPTURE_INTERVAL_SECONDS` | varies | Snapshot interval |
| `GATEWAY_OPTION_CAPTURE_WS_ENABLED` | `true` | Enable WebSocket streaming for captured option symbols |
| `GATEWAY_OPTION_CAPTURE_MARKET_HOURS_ONLY` | `true` | Only snapshot during market hours |
| `GATEWAY_OPTION_CAPTURE_SNAPSHOT_TIMEOUT_SECONDS` | `90.0` | Per-snapshot fetch timeout |
| `GATEWAY_OPTION_CAPTURE_SYMBOL_TIMEOUT_OVERRIDES` | — | Per-symbol timeout overrides (e.g. `SPY:45,QQQ:45`) |
| `GATEWAY_OPTION_CAPTURE_PUBLISH_PER_CONTRACT_TRADES` | `true` | Emit `option_trades` feed for each captured contract |

---

## 9. Treasury Poller

The treasury poller starts automatically when `GATEWAY_DATA_SINK_ENABLED=true` and `ALPHAVANTAGE_API_KEY` is configured. There is no separate enable flag.

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_TREASURY_POLLER_MATURITIES` | `2year,10year` | Comma-separated maturities to poll |

---

## 10. Quotes, Trades, Crypto & News Pollers

All pollers require `GATEWAY_DATA_SINK_ENABLED=true` and the relevant provider API key to be set.

**Quotes Poller:**

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_QUOTES_POLLER_ENABLED` | `true` | Enable REST-based quote polling for default symbol set |
| `GATEWAY_QUOTES_POLLER_INTERVAL_SECONDS` | `30` | Polling interval (minimum 5) |
| `GATEWAY_QUOTES_POLLER_SYMBOLS` | — | Override default symbol list (comma-separated; empty = built-in set) |

**Trades Poller:**

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_TRADES_POLLER_ENABLED` | `true` | Enable trade polling |
| `GATEWAY_TRADES_POLLER_INTERVAL_SECONDS` | `30` | Polling interval |
| `GATEWAY_TRADES_POLLER_SYMBOLS` | — | Override symbol list |

**Crypto Poller:**

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_CRYPTO_POLLER_ENABLED` | `true` | Enable crypto bar polling |
| `GATEWAY_CRYPTO_POLLER_INTERVAL_SECONDS` | `60` | Polling interval |
| `GATEWAY_CRYPTO_POLLER_PAIRS` | — | Override pairs (e.g. `BTC/USD,ETH/USD`) |

**News Poller:**

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_NEWS_POLLER_ENABLED` | `true` | Enable news polling |
| `GATEWAY_NEWS_POLLER_INTERVAL_SECONDS` | `120` | Polling interval |
| `GATEWAY_NEWS_POLLER_FETCH_LIMIT` | `50` | Max articles per fetch (1–50) |
| `GATEWAY_NEWS_POLLER_SYMBOLS` | — | Override symbols (empty = market-wide) |

**Flow Fanout:**

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_WS_FLOW_FANOUT_ENABLED` | `true` | Push UW flow events to subscribed WebSocket clients in real time |

---

## 11. Provider API Keys

Provider SDKs read these directly — **no** `GATEWAY_` prefix.

| Variable | Provider | Required |
|----------|----------|----------|
| `APCA_API_KEY_ID` | Alpaca | Yes |
| `APCA_API_SECRET_KEY` | Alpaca | Yes |
| `APCA_API_BASE_URL` | Alpaca | Optional (paper vs live) |
| `UNUSUAL_WHALES_API_KEY` | Unusual Whales | Yes |
| `FINNHUB_API_KEY` | Finnhub | Yes |
| `ALPHAVANTAGE_API_KEY` | Alpha Vantage | Yes |
| `NEWS_API_KEY` | NewsAPI.org | Optional (otherwise news routes return empty) |
| `SEC_USER_AGENT` | SEC EDGAR | Recommended (SEC requires identifying user-agent) |

yfinance requires no key.

---

## 12. `config/providers.yaml`

Defines which providers are loaded, in what order, with what capabilities.

```yaml
providers:
  - name: alpaca
    module: gateway.providers.alpaca
    class: AlpacaProvider
    priority: 100
    enabled: true
    capabilities:
      - bars
      - quotes
      - trades
      - options
      - crypto
      - trading
  - name: unusual_whales
    module: gateway.providers.uw
    class: UWProvider
    priority: 90
    enabled: true
    capabilities:
      - flow
      - darkpool
      - greeks
      - institutional
  # ... finnhub, alphavantage, yfinance, sec, news
```

- `priority` orders providers for capabilities they share (higher = preferred).
- `enabled: false` skips loading.
- Reload requires a process restart (no SIGHUP).

---

## 13. `config/clients.yaml`

Per-client authentication. SIGHUP reloads in place.

```yaml
clients:
  - id: cerberus
    key: gw_cerberus_<hex>
    role: trader
    enabled: true
    permissions:
      providers: [alpaca, unusual_whales, finnhub]
      feeds: [stock_bars, stock_quotes, flow_alerts, darkpool]
      max_symbols: 5000
      ws_subscriptions_max: 5000
      rate_limit: 1200      # overrides GATEWAY_RATE_LIMIT_DEFAULT
  - id: 3roses
    key: gw_3roses_<hex>
    role: trader
    enabled: true
    permissions:
      providers: [alpaca, finnhub]
      feeds: [stock_bars, stock_quotes, stock_trades, news]
      max_symbols: 1000
  - id: test
    key: gw_test_<hex>
    role: admin
    enabled: true
    permissions:
      providers: ['*']
      feeds: ['*']
      max_symbols: 100
```

### Key management CLI
```bash
uv run python -m gateway.cli generate-key                   # mint a new gw_<hex> key
uv run python -m gateway.cli add-client <id> --role trader  # add a client entry
uv run python -m gateway.cli list-clients                   # list configured clients
uv run python -m gateway.cli rotate-key <id>                # rotate a client's key
uv run python -m gateway.cli revoke-client <id>             # disable a client
```

After editing `clients.yaml` directly, `kill -HUP <gateway-pid>` reloads it without restart. The change is also recorded in the audit log (`key_created`, `key_rotated`, `key_revoked`, `config_changed`).

---

## 14. `config/perf_baseline.json` / `perf_budgets.json`

Consumed by `scripts/perf_gate.py` in CI (`.github/workflows/perf-guardrail.yml`).

- **Baseline:** historical measurement, used to compute regressions.
- **Budgets:** absolute limits per operation (e.g. envelope-wrap latency, stream-fanout time).

Manage with:
```bash
python scripts/perf_baseline_manager.py [--update] [--show]
python scripts/perf_promote_active_configs.py
python scripts/merge_static_budgets.py
```

See `docs/audits/PERF_RELEASE_READINESS.md` for the promotion process.

---

## 15. `config/prometheus_alerts.yml`

Prometheus alert rules. Key alerts:

- `SinkProducerTimeoutDrops` — any non-zero `gateway_sink_producer_timeout_drops_total` rate
- `SinkBufferEvictionsActive` — any non-zero `gateway_sink_buffer_evictions_total` rate
- Provider circuit-open alerts
- High WebSocket disconnect rate
- Cache hit-rate drop

---

## 16. Logging Outputs

| File | Levels | Notes |
|------|--------|-------|
| `logs/data-gateway_{YYYY-MM-DD}.log` | DEBUG+ | All events. Daily rotation. |
| `logs/data-gateway_errors_{YYYY-MM-DD}.log` | WARNING+ | Errors-only digest. |
| `logs/audit_{YYYY-MM-DD}.log` (if configured) | All | Audit events (`AuditLogger`). See [AUDIT_LOGGING.md](AUDIT_LOGGING.md). |

Retention: 14 days by default. Override with `EMPIRE_LOG_DIR`.

---

## 17. Docker / Compose

`docker-compose.yml` ships gateway + redis:

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| `gateway` | `data-gateway` | 8080 | FastAPI app |
| `redis` | `data-gateway-redis` | 6379 | Cache + sink + dedup + audit ring |

Environment variables are passed through from the host `.env`. See [deployment-guide.md](deployment-guide.md) for the build context (must be the monorepo root, not the repo root).

---

## 18. Quick Profiles

| Profile | Key vars |
|---------|----------|
| **Local dev** | `GATEWAY_DEBUG=true`, `GATEWAY_ALLOW_STUB_DATA=true`, `GATEWAY_DATA_SINK_ENABLED=false` |
| **Staging** | `GATEWAY_DEBUG=false`, `GATEWAY_DATA_SINK_ENABLED=true`, real provider keys, paper-trading Alpaca base URL |
| **Production** | All of staging + `GATEWAY_LOG_LEVEL=INFO`, `EMPIRE_LOG_FORMAT=json`, live Alpaca base URL, `GATEWAY_UW_EOD_ENABLED=true`, `GATEWAY_QUOTES_POLLER_ENABLED=true`, `GATEWAY_WS_FLOW_FANOUT_ENABLED=true` |

---

## 19. Related Documents

- [project-overview-pdr.md](project-overview-pdr.md)
- [system-architecture.md](system-architecture.md)
- [RUNBOOK.md](RUNBOOK.md) — operational tuning
- [deployment-guide.md](deployment-guide.md) — Docker, CI
- [AUDIT_LOGGING.md](AUDIT_LOGGING.md) — audit events emitted on config changes
- `../.env.example` — commented env template
- `gateway/config.py` — defaults and types
