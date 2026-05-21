---
commit_hash: 869790a85d14
analyzed_at: 2026-05-01T00:52:51Z
scope: gateway/main.py, gateway/core/{envelope,redis_sink,dedup,auth,registry,data_sink,uw_poller,security}.py, gateway/api/{websocket,middleware}.py
files_analyzed: 10
---

## Functions / Methods (high-risk subset)

| File | Function | Lines | Notes |
|------|----------|-------|-------|
| gateway/main.py | `lifespan` | 271-625 | App startup/shutdown — creates registry, multiplexer, sink, 6 pollers, option capture; 8-step shutdown |
| gateway/main.py | `_on_stream_data` | 114-156 | WS event callback — sends to client + schedules sink publish (orjson pre-serialize) |
| gateway/main.py | `_schedule_stream_sink_publish` | 226-247 | Backpressure: drops event silently when `len(_publish_tasks) >= max_pending_tasks` |
| gateway/main.py | `_check_port_available` | 23-41 | Pre-bind socket check; race window before uvicorn bind |
| gateway/main.py | `create_app.handle_sighup` | 384-393 | Reloads settings + auth only, NOT providers.yaml |
| gateway/main.py | `create_app` CORS block | 660-666 | Debug mode: `allow_origins=["*"]` + `allow_credentials=True` (CORS spec violation) |
| gateway/core/envelope.py | `_infer_instrument_type` | 266-279 | Any payload with `strike`/`expiry` → `option`. **Known footgun**: per-underlying analytics need override |
| gateway/core/envelope.py | `wrap_event` (fallback) | 403-427 | On any exception returns minimal envelope with `quality_flags=["error"]` — silent partial failure |
| gateway/core/envelope.py | `fast_wrap_streaming_event` | 430-523 | Uses `os.urandom(16).hex()` for event_id — **non-content-derived; defeats dedup** |
| gateway/core/envelope.py | `compute_event_id` | 124-164 | BLAKE2b 16-byte digest; collision space 2^64 — small for high-volume streams |
| gateway/core/dedup.py | `RequestDeduplicator.dedupe` | 41-84 | Stripe-locked future cache; `hash(key)` randomized per-process |
| gateway/core/auth.py | `ClientAuthenticator._load_clients` | 49-93 | YAML load; no key uniqueness validation; later keys overwrite earlier |
| gateway/core/auth.py | `ClientAuthenticator.authenticate` | 95-152 | Logs `api_key[:10]` on failure → partial credential leak |
| gateway/core/redis_sink.py | `RedisStreamsSink.publish` | 377-485 | 3 retries, exponential backoff, buffers exhausted events to in-memory deque (max 10K) |
| gateway/core/redis_sink.py | `_drain_buffer` | 294-367 | Pipeline drain on reconnect; failed items re-buffered; not persisted to disk |
| gateway/core/redis_sink.py | `_create_client` | 185-196 | Pool size silently capped at 64 (`min(64, int(pool_size))`) |
| gateway/core/registry.py | `ProviderRegistry._load_provider` | 57-93 | Catches ImportError + AttributeError → silent provider drop on misconfig |
| gateway/core/registry.py | `load_from_config` | 22-55 | Catches all exceptions per provider — gateway boots with subset of providers |
| gateway/core/data_sink.py | `DataSinkRegistry.register` | 94-98 | Per-sink semaphore = 512 default for max-in-flight |
| gateway/core/uw_poller.py | `_poll_eod_iv_term_structure` | 840-880 | **Reference impl** of `instrument_type_override="equity"` — other EOD pollers may be missing this |
| gateway/api/websocket.py | `_wait_for_auth` | 200-288 | 10s timeout default; close code 4001 |
| gateway/api/websocket.py | `_message_loop` | 291-356 | Validates message size AFTER receive (full bytes already in memory) |
| gateway/api/websocket.py | `_has_provider_permission` | 661-670 | Empty `providers` list → grants ALL providers (permissive default) |
| gateway/api/websocket.py | `_has_feed_permission` | 688-692 | Empty `feeds` list → grants ALL feeds (permissive default) |
| gateway/api/websocket.py | subscribe partial-fail | 444-548 | Per-feed rollback on cross-feed error; no rollback for per-symbol partial-success within feed |
| gateway/api/middleware.py | `CacheMiddleware._get_cache` | 332-354 | Imports `gateway.main.app` lazily inside request path → circular |
| gateway/api/middleware.py | `RateLimitMiddleware._get_client_info` | 229-243 | `f"key:{api_key[:16]}"` — 16-char prefix collision risk |
| gateway/api/middleware.py | `GlobalRateLimitMiddleware._get_client_ip` | 1265-1273 | Trusts FIRST X-Forwarded-For when proxy enabled — spoofable through untrusted proxy chain |
| gateway/api/middleware.py | `EventEnvelopeMiddleware._wrap_and_send` | 837-958 | Buffers full body to wrap; `max_body_bytes` default 524288; on error returns original (silent) |

## Routes / Endpoints (subset)

| Method | Path | Auth | Notes |
|--------|------|------|-------|
| WS | /ws | API key in first message, 10s timeout | Heartbeat 4s, max 4 missed → 4002 |
| GET | /api/v1/{provider}/* | X-Gateway-Key required | EventEnvelope-wrapped if JSON ≤ 512KB |
| GET | /health, /health/ready | Public | Returns 503 during shutdown |
| GET | /catalog/* | Authed | Catalog of providers/streams/feeds |

## Models / Schemas

| Name | File | Key fields |
|------|------|------------|
| EventEnvelope | envelope.py:26 | event_id (BLAKE2b 16B), provider, feed, source, instrument_type, instrument_key, symbol, ts_event, ts_ingest, payload |
| Client | auth.py:24 | id, permissions, role, enabled |
| ClientPermissions | auth.py:13 | providers, feeds, max_symbols, rate_limit, ws_subscriptions_max |
| RateLimitBucket | middleware.py:94 | sliding-window deque of timestamps, 60s window |
| CacheEntry | middleware.py:255 | base64 content, media_type, headers, created_at, ttl |
