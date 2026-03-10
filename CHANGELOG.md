# Changelog

All notable changes to this project will be documented in this file.

### Changed

- **Option chain snapshot payload contract** (`gateway/core/option_capture.py`, `gateway/schemas/__init__.py`): Added optional `underlying_price` to normalized option contracts and publish it at the top level of `option_chain_snapshot` envelopes so downstream storage and replay do not need to infer spot from per-contract prices.
- **Option capture quality telemetry** (`gateway/core/option_capture.py`, `gateway/core/metrics.py`, `gateway/api/admin.py`): Added per-symbol snapshot quality stats for contract count, Greeks coverage, IV coverage, non-zero open-interest coverage, bid/ask coverage, snapshot age, and websocket add/remove counts. These now show up in the option capture runtime snapshot for admin status and in Prometheus metrics.
- **OPRA-first option streaming** (`gateway/config.py`, `gateway/core/stream.py`, `gateway/main.py`, `docker-compose.yml`): Added `stream_options_feed` / `GATEWAY_STREAM_OPTIONS_FEED` with `opra` as the default, and pass the configured options feed into the Alpaca multiplexer at startup.
- **Budgeted option websocket universe** (`gateway/config.py`, `gateway/core/option_capture.py`): Added `option_capture_ws_contract_limit_per_symbol` with a default budget of 40 contracts per underlying. Full chain snapshots still land in Heber, while websocket `quotes`/`trades` subscriptions are capped to the nearest-expiry, near-ATM, tighter-spread, more-liquid contracts per symbol.

### Fixed

- **Alpaca option snapshot normalization** (`gateway/providers/alpaca.py`, `tests/test_alpaca_provider.py`): Fixed `volume` being populated from `open_interest`, added fallback parsing for snapshot `volume`, `openInterest`, and `underlyingPrice`, and stopped dropping zero-valued Greeks or IV when Alpaca returns `0.0`.
- **Invalid option websocket bars subscriptions** (`gateway/core/option_capture.py`, `gateway/core/stream.py`): The option capture service no longer subscribes to option `bars`, and the upstream options connection now strips any accidental option `bars` subscriptions before sending to Alpaca. Alpaca option websockets support `quotes` and `trades`, not `bars`.
- **Stream Timestamp serialization crash** (`gateway/core/stream.py`): All WebSocket streaming messages (800K+/day) failed with `Type is not JSON serializable: Timestamp` because `orjson.dumps` could not serialize `msgpack.Timestamp` objects from the OPRA options stream. Added `_orjson_default` fallback handler that converts `pandas.Timestamp` (`.isoformat()`) and `msgpack.Timestamp` (`.to_datetime().isoformat()`) to ISO 8601 strings.

- **UW sector tide never polled** (`gateway/core/uw_poller.py`): Sector tide shared `_should_poll_tide()` and `_last_tide_poll` with market tide, so after market tide polled and set the timer, sector tide's check always returned `False`. Added independent `_last_sector_tide_poll` field and `_should_poll_sector_tide()` method so both feeds poll on their own hourly cadence.
- **Alpaca 400 Bad Request on naive datetime params** (`gateway/api/alpaca/stock.py`): FastAPI parses query datetime params without timezone when the client omits it (e.g., `?start=2026-01-14T00:00:00`). Added UTC normalization in `get_stock_bars`, `get_stock_trades`, and `get_historical_quotes` before passing to the provider.

### Fixed

- **Pre-commit** (`detect-secrets`): ignore `logs/` directories during secret scans to prevent generated log artifacts from causing false positives.

- **Provider Error Masking** (`gateway/api/finnhub/*.py`, `gateway/api/yf.py`, `gateway/api/sec.py`): Refactored Finnhub, Yahoo Finance, and SEC routers to utilize centralized execution wrappers (`execute_finnhub_cached`, `execute_yf_cached`, `execute_sec_cached`). This ensures that `httpx.HTTPStatusError` exceptions correctly bubble up to the client instead of being masked as generic 502 Bad Gateway errors.
- **Redis Sink Connection Pool Exhaustion** (`gateway/core/redis_sink.py`): Replaced `aioredis.ConnectionPool` with `aioredis.BlockingConnectionPool` and configured a timeout (`socket_timeout`). This prevents immediate `ConnectionError: Too many connections` exceptions when burst traffic (e.g., 256 concurrent tasks) momentarily exceeds the `max_connections` limit.

- **Alpaca crypto symbol validation and error-log storm prevention** (`backfill.py`, `crypto.py`, `test_backfill.py`, `test_alpaca_crypto_router.py`):
  - Backfill jobs for Alpaca `crypto_bars`/`crypto_trades` now validate symbol format at submit time and reject stock tickers with a clear `400`-style error message before any upstream call.
  - Alpaca crypto REST routes now validate pair format (`BASE/QUOTE`, e.g., `BTC/USD`) before provider execution, returning HTTP 400 for invalid input.
  - Added regression tests to prevent reintroducing invalid-symbol upstream request loops.

- **HTTP client event_hooks conflict** (`http_client.py`): `create_async_http_client` and `create_http_client` now merge caller-provided `event_hooks` (e.g., metrics hooks from providers) with the default logging hooks via a new `_merge_event_hooks()` helper. Previously, providers passing `event_hooks` through `**kwargs` caused `TypeError: httpx.AsyncClient() got multiple values for keyword argument 'event_hooks'`, crashing all 5 providers (alpaca, finnhub, alphavantage, news, sec) on startup.
- **Redis sink connection leak race condition** (`redis_sink.py`): `_reset_connection()` now acquires `_connect_lock` and re-checks `failed_client` identity inside the lock. Previously, concurrent publish chunks could each trigger independent resets without coordination, orphaning old connection pools and leaking connections (observed 266 open connections vs. the expected pool max of 8).
- **Redis sink pool disconnect on reset** (`redis_sink.py`): `_close_stale_client()` now calls `connection_pool.disconnect()` after `client.close()` to force-release all idle TCP sockets from leaked pools.

### Fixed

- **TOCTOU race in DataSinkRegistry dedup** (`data_sink.py`, `cache.py`): Replaced `get()`→`set()` dedup pattern with atomic `set_nx()` (Redis `SET NX EX`). Eliminates race window where two coroutines could both publish the same event.
- **Mutable set view in SubscriptionManager** (`stream.py`): `get_clients_for_symbol_view()` now returns `frozenset` instead of raw mutable `set` reference, preventing accidental mutation of the subscription index during async fanout dispatch.

### Audit

- **Dead code audit — flagged 3 orphaned API modules** (`replay.py`, `quality.py`, `symbology.py`, `__init__.py`): Cross-repo Sourcegraph audit found zero external consumers across all Empire repos for `/api/v1/replay/*`, `/quality/*`, and `/api/v1/symbology/*` endpoints. Added `TODO(audit-2026-02)` comments to module docstrings and import lines. Cerberus and Heber have their own local replay/quality implementations.

### Fixed

- **Architecture violation `core → api` import** (`core/uw_poller.py`, `api/deps.py`): Extracted singleton state management (provider registry, stream multiplexer, data sink registry) from `api/deps.py` into new `core/globals.py`. `uw_poller` and `main.py` now import from `core.globals`; `deps.py` re-exports for backward compatibility. All 4 `import-linter` contracts now pass.

### Fixed

- **Redis sink concurrent chunk race condition** (`redis_sink.py`):
  - `_reset_connection()` now accepts a `failed_client` parameter and only resets when the failed client is still the active one, preventing cascading resets from concurrent chunks
  - `_publish_chunk()`, `publish()`, and `health_check()` capture a local client reference before use, preventing `'NoneType' object has no attribute 'pipeline'` crashes when another chunk triggers a reset mid-flight

- **Docker health check deadlock** (`docker-compose.yml`): Changed health check endpoint from `/ping` (returns 401, marking container unhealthy) to `/health` (public endpoint, returns 200)
- **Redis sink connection leak and reconnect storm** (`redis_sink.py`):
  - `_reset_connection()` is now `async` and `await`s closing the old Redis client pool before discarding it, preventing fire-and-forget connection leaks
  - Added exponential reconnect backoff (0.5s initial, capped at 5s) to prevent tight reconnect-fail loops that saturate Redis with "Too many connections" errors
  - `_ensure_connected()` respects the backoff cooldown before attempting reconnection
  - Added `asyncio.Lock` to `_ensure_connected` to prevent race conditions where multiple connection pools were created during concurrent reconnects
- **UW darkpool SDK settlement enum mismatch** (`vendor/unusualwhales_sdk`): Added `CASH = "cash"` to `SingleTradeSettlement` enum — the UW API returns `"cash"` for some darkpool trades but the SDK only defined `"cash_settlement"`, causing recurring `uw_darkpool_recent_sdk_failed` warnings and SDK-to-raw-HTTP fallbacks

### Removed

- **Orphan `sanity.py`** (`gateway/sanity.py`): Deleted standalone FastAPI app with `/ping` endpoint that was never integrated into the main application — its existence caused confusion and contributed to the health check misconfiguration

### Changed

- **Backfill symbol concurrency** (`backfill.py`): Increased `DEFAULT_SYMBOL_CONCURRENCY` from 5 → 10, allowing more parallel symbol fetching within a single backfill job

## [0.5.83] - 2026-02-19

### Added

- **Bulk cancel endpoint**: Add `POST /cancel-all` functionality to `gateway/api/backfill.py` which was missing from API routing.
- **Flush endpoint**: Add `DELETE /` functionality to `gateway/api/backfill.py` which was missing from API routing.

### Fixed

- **Redis sink concurrency test CPU bottleneck**: Patcher limits `BATCH_CHUNK_SIZE` to 10 in `test_publish_batch_concurrent_chunks` to prevent massive list comprehension payloads from blocking the asyncio runloop and skewing concurrency timing assertions.
- **Auth test debug calls expectation mismatch**: Updated `test_authenticate_valid_key_logs_debug_not_info` to assert 2 expected logger.debug() calls instead of 1.
- **Redis sink connection leak and reconnect storm** (`redis_sink.py`):
  - `_reset_connection()` is now `async` and `await`s closing the old Redis client pool before discarding it, preventing fire-and-forget connection leaks
  - Added exponential reconnect backoff (0.5s initial, capped at 5s) to prevent tight reconnect-fail loops that saturate Redis with "Too many connections" errors
  - `_ensure_connected()` respects the backoff cooldown before attempting reconnection
  - Added `asyncio.Lock` to `_ensure_connected` to prevent race conditions where multiple connection pools were created during concurrent reconnects
- **UW darkpool SDK settlement enum mismatch** (`vendor/unusualwhales_sdk`): Added `CASH = "cash"` to `SingleTradeSettlement` enum — the UW API returns `"cash"` for some darkpool trades but the SDK only defined `"cash_settlement"`, causing recurring `uw_darkpool_recent_sdk_failed` warnings and SDK-to-raw-HTTP fallbacks

### Removed

- **Orphan `sanity.py`** (`gateway/sanity.py`): Deleted standalone FastAPI app with `/ping` endpoint that was never integrated into the main application — its existence caused confusion and contributed to the health check misconfiguration

### Changed

- **Backfill symbol concurrency** (`backfill.py`): Increased `DEFAULT_SYMBOL_CONCURRENCY` from 5 → 10, allowing more parallel symbol fetching within a single backfill job

## [0.5.82] - 2026-02-18

### Changed

- **Redis sink connection pool** (`redis_sink.py`): Replaced single Redis connection with a connection pool (default 8 connections), enabling concurrent pipeline execution from multiple backfill coroutines
- **Redis sink operation timeout** (`redis_sink.py`): Increased default from 1s → 5s to eliminate frequent `redis_sink_connection_reset` under load
- **Redis sink binary mode** (`redis_sink.py`): Removed `decode_responses=True` — payloads are binary orjson, skipping unnecessary UTF-8 encode/decode roundtrip
- **Redis sink concurrent batch chunks** (`redis_sink.py`): Split batches into 2K chunks (down from 5K) and process up to 4 concurrently via `asyncio.gather()`, ~4x throughput improvement
- **Redis sink chunk retry** (`redis_sink.py`): Failed chunks are retried once with a fresh connection before aborting (previously immediate abort)
- **Feed-weighted backfill concurrency** (`backfill.py`): Replaced flat 3-slot per-provider semaphore with separate lightweight (bars/quotes/news → 5 slots) and heavyweight (trades → 2 slots) semaphores per provider, preventing heavyweight jobs from starving lightweight ones

### Added

- **Configurable backfill concurrency** (`config.py`): `GATEWAY_BACKFILL_LIGHTWEIGHT_CONCURRENCY` (default 5) and `GATEWAY_BACKFILL_HEAVYWEIGHT_CONCURRENCY` (default 2) environment variables
- **Configurable Redis sink pool** (`config.py`): `GATEWAY_DATA_SINK_OPERATION_TIMEOUT_SECONDS` (default 5.0) and `GATEWAY_DATA_SINK_REDIS_POOL_SIZE` (default 8) environment variables
- **Feed weight classification tests** (`test_backfill.py`): Tests for heavyweight/lightweight feed classification and concurrency isolation between weight classes
- **Redis sink bandwidth tests** (`test_redis_sink.py`): Tests for connection pool params, concurrent chunk execution, retry on failure, and binary payload encoding

## [0.5.81] - 2026-02-18

### Changed

- **Backfill per-provider concurrency**: Replaced per-provider mutex lock with semaphore (3 concurrent jobs per provider) in `backfill.py`, eliminating head-of-line blocking where one slow job starved all others
- **Concurrent symbol processing**: Symbols within a single backfill job are now fetched concurrently (bounded by semaphore, default 5) instead of sequentially via `asyncio.gather` in `backfill.py`
- **Alpaca inter-chunk delay**: Reduced from 200ms to 50ms in `backfill.py` to improve throughput while staying within rate limits
- **Alpaca rate limit capability**: Fixed stale `rate_limit_requests_per_minute` from 200 to 10000 in `alpaca.py` to match Algo Trader Plus plan

### Added

- **Bulk cancel endpoint**: `POST /api/v1/backfill/cancel-all` cancels all running and queued backfill jobs (`api/backfill.py`)
- **Flush endpoint**: `DELETE /api/v1/backfill` cancels all jobs and purges job history (`api/backfill.py`)
- **Stale job auto-expiry**: Completed/failed/cancelled jobs older than 1 hour are automatically pruned on next submit (`backfill.py`)
- **Backfill architecture tests**: Added tests for `cancel_all`, `flush`, stale expiry, concurrent symbol processing, and new API endpoints (`test_backfill.py`)

## [0.5.80] - 2026-02-17

### Fixed

- **Redis sink batch timeouts** (`redis_sink.py`): Large backfills (874K+ messages) sent entire batch in a single pipeline with 2s timeout, causing perpetual `redis_sink_connection_reset`. Now chunks into 5,000-message pipelines with dynamic timeout scaling
- **Empty error strings** (`redis_sink.py`): `asyncio.TimeoutError` has empty `str()` — error logs now fall back to exception type name

## [0.5.79] - 2026-02-17

### Fixed

- **Greek exposure schema alignment**: Updated `NormalizedGreekExposure` to use per-call/per-put split fields (`call_gamma`, `put_gamma`, `call_delta`, `put_delta`, `call_vanna`, `put_vanna`, `call_charm`, `put_charm`) matching the UW API response. Added `dte` field for expiry-level data. Updated all 3 provider methods (`get_greek_exposure`, `get_greek_exposure_by_strike`, `get_greek_exposure_by_expiry`) and `FEED_UNIQUE_FIELDS` in `envelope.py`.

## [0.5.78] - 2026-02-17

### Fixed

- **Blocking health_check** (`yfinance.py`): `health_check` called `yf.Ticker().info` synchronously on the event loop; wrapped in `asyncio.to_thread` to prevent blocking the async server

### Removed

- **Dead constant** (`yfinance.py`): Remove unused `YFINANCE_CACHE_TTL` — caching is handled at the API route layer (`gateway/api/yf.py`)

### Changed

- **Docstring correction** (`yfinance.py`): Clarify that response caching is handled at the route layer, not the provider

## [0.5.77] - 2026-02-15

### Changed

- **pyproject.toml cleanup**: Remove stale mypy overrides for deleted `gateway.core.redis_cache` and non-existent `gateway.api._legacy`
- **pyproject.toml cleanup**: Remove unused `numpy.typing.mypy_plugin` (no numpy imports in gateway)
- **pyproject.toml cleanup**: Remove unused `black>=24.0` dev dependency (ruff-format handles formatting)

## [0.5.76] - 2026-02-15

### Fixed

- **Heartbeat counter bug** (`websocket.py`): `missed_heartbeats` now resets to 0 on successful send; previously intermittent failures accumulated until false disconnect

### Changed

- **Stream dispatch serialization** (`main.py`): Switch `json.dumps` → `orjson.dumps` in Heber sink dispatch hot path for consistency with fanout
- **Upstream encode** (`stream.py`): Switch `json.dumps` → `orjson.dumps` in `_encode_message` for consistency
- **Redis cache imports** (`cache.py`): Move 4 inline `import json` statements to module level
- **WebSocket message loop** (`websocket.py`): Remove redundant `get_settings()` calls per received message — use pre-resolved `max_bytes`

## [0.5.75] - 2026-02-15

### Removed

- **Dead code: `redis_cache.py`**: Deleted deprecated compatibility shim with zero imports (`core/redis_cache.py`)
- **Dead code: `multiplexer.py`**: Deleted legacy `SubscriptionManager` superseded by `StreamMultiplexer` (`core/multiplexer.py`)
- **Dead code: `MessageDeduplicator`**: Removed unused class, singleton, and imports from `core/dedup.py`

### Fixed

- **TYPE_CHECKING import**: Corrected `StreamMultiplexer` import path in `deps.py` from `multiplexer` → `stream`

## [0.5.74] - 2026-02-15

### Fixed

- **CLI key rotation**: `cmd_rotate_key` now appends old key hash to `old_key_hashes` before overwriting (`cli.py`)
- **Stream lazy connect**: Replace busy-poll loop with `asyncio.Event` in `_ensure_connected` (`stream.py`)
- **Hot-path import**: Move `datetime` import from per-message scope to module level (`stream.py`)

### Performance

- **Stream fanout serialization**: Switch `json.dumps` → `orjson.dumps` for ~5-10x faster envelope serialization (`stream.py`)
- **Uptime loop interval**: Reduce polling from 1s → 5s since Prometheus scrapes at 15-30s intervals (`main.py`)
- **Middleware cache type**: Cache `_cache_type` label to avoid repeated import + isinstance checks per request (`middleware.py`)

## [0.5.73] - 2026-02-15

### Fixed

- **broadcast_shutdown**: Send shutdown messages via `send_json` per connection instead of delegating to `broadcast` which pre-serialized to bytes (`connections.py`)
- **validate_symbols_array**: Route through `validate_symbol` instead of `_matches_any_symbol_pattern` for consistent validation (`security.py`)
- **UW options alias**: Added `/options/{symbol}/iv-rank` alias route (`uw/options.py`)
- **AlphaVantage max_points**: Pass `max_points` through to provider calls and cache keys in `indicators.py`, `forex.py`, and `crypto.py`
- **Calendar degradation**: Added degradation tracking in `get_market_hours` endpoint (`calendar.py`)
- **Fetcher guards**: Added `has_fetcher()` / `has_bars_fetcher()` guards in `bulk.py`, `calendar.py`, and `corporate.py` to prevent double-binding
- **Pagination logic**: Fixed undefined `total`, `has_more`, `next_offset` in `bulk.py:list_jobs` and `replay.py:list_sessions`
- **Envelope seq field**: Added `seq` as top-level field in `fast_wrap_streaming_event` output (`envelope.py`)
- **News symbol ordering**: Use `dict.fromkeys()` for order-preserving deduplication in news message routing (`stream.py`)
- **Replay state filter**: Guard `state` filter with `isinstance(state, str)` to handle direct function calls (`replay.py`)
- **Missing script functions**: Added `_run_provider_smoke_check` to `live_provider_smoke.py` and `_index_function_defs`, `_resolve_handler_name`, `ROUTE_PATTERN` to `generate_provider_contract.py`
- **Rate limiter imports**: Added missing `get_endpoint_rate_limiter` and `EndpointRateLimitExceeded` imports in `bulk.py` and `replay.py`

### Added

- **BulkJobManager.list_jobs_page**: Paginated job listing with `client_id` and `status` filtering (`core/bulk.py`)
- **ReplaySessionManager.list_sessions_page**: Paginated session listing with `client_id` and `state` filtering (`core/replay.py`)
- **trading-bot gateway client**: Created `trading-bot/src/core/gateway_client.py` with `DataGatewayClient` using `X-Gateway-Key` header authentication

## [0.5.72] - 2026-02-14

### Performance

- **orjson serialization**: Replaced `json.dumps` with `orjson.dumps` in `ConnectionManager.broadcast` and `RedisStreamsSink.publish`/`publish_batch`. `orjson.dumps` returns `bytes` directly, eliminating UTF-8 encode/decode overhead for WebSocket and Redis I/O.
- **uvloop event loop**: Explicitly install `uvloop.EventLoopPolicy` in `gateway/main.py` for 2-4x faster asyncio event loop execution. Added `orjson` and `uvloop` as explicit dependencies in `pyproject.toml`.

### Fixed

- **Pre-serialization test assertion**: Updated `test_main_stream_sink.py` to expect pre-serialized JSON string from `_on_stream_data` (aligned with Item #5 pre-serialization optimization).

## [0.5.71] - 2026-02-13

### Performance

- **Raw ASGI middleware**: Converted all 7 `BaseHTTPMiddleware` classes (`RequestMetricsMiddleware`, `InputValidationMiddleware`, `RateLimitMiddleware`, `SecurityHeadersMiddleware`, `GlobalRateLimitMiddleware`, `CacheMiddleware`, `EventEnvelopeMiddleware`) to raw ASGI `__call__` pattern. Eliminates per-request `Request`/`Response` object creation overhead from Starlette's middleware adapter. `CacheMiddleware` and `EventEnvelopeMiddleware` use response body buffering via `send` interceptors for caching and envelope wrapping.
- **Redis pipeline batching**: Added `RedisStreamsSink.publish_batch()` using Redis pipelines to batch multiple `XADD` commands in a single network round trip. Added `DataSinkRegistry.publish_all_batch()` to orchestrate batch publishing with circuit breaker support. Backfill engine `_publish_items` now uses batch publishing for all items in a chunk.

### Fixed

- **CacheMiddleware BYPASS header**: Non-cacheable 200 responses (streaming, missing content-length) now correctly return `X-Gateway-Cache: BYPASS` instead of `MISS`.
- **Stock router test**: Fixed `test_get_stock_trades_threads_limit_to_provider` which was monkeypatching `stock.execute_alpaca_provider_call` — a function that no longer exists in `stock.py`. Test now bypasses the rate limiter and uses the actual provider call path.

## [0.5.70] - 2026-02-13

### Performance

- **Hoisted per-message imports**: Moved `json` and `msgpack` imports from `_decode_message`/`_encode_message` to module level in `gateway/core/stream.py`, eliminating `sys.modules` lookup overhead on every WebSocket message.
- **Cached validator in hot path**: Replaced `get_validator()` call per message with `_get_stream_validator()` (already cached on the multiplexer) in `stream.py:_handle_message`.
- **Eliminated per-message dict creation**: Replaced inline `data_type_map` dict with existing `MESSAGE_TYPE_TO_DATA_TYPE` module constant; added `_VALIDATABLE_FEEDS` frozenset for the feed validation check in `stream.py`.
- **Hoisted metrics imports**: Moved `record_envelope_created` and `record_sink_publish` imports from deferred try/except blocks to module-level in `gateway/core/envelope.py` and `gateway/core/redis_sink.py`.
- **Pre-serialized broadcast JSON**: `ConnectionManager.broadcast` in `gateway/core/connections.py` now serializes the message dict once with `json.dumps` and sends via `send_text`, avoiding redundant `send_json` serialization per client.
- **SHA256 usedforsecurity flag**: Added `usedforsecurity=False` to `hashlib.sha256` in `envelope.py:compute_event_id` to skip FIPS compliance checks.
- **Frozenset for public path check**: `CacheMiddleware._is_public_path` in `gateway/api/middleware.py` now uses class-level `frozenset` and tuple `startswith` for O(1) lookups.

## [0.5.69] - 2026-02-13

### Fixed

- **UW poller out-of-order timestamp log flood**: Downgraded per-item `uw_flow_out_of_order_ts`, `uw_darkpool_out_of_order_ts`, `uw_market_tide_out_of_order_ts`, and `uw_sector_tide_out_of_order_ts` logs from `warning` to `debug` in `gateway/core/uw_poller.py`. The UW API returns data sorted newest-first, so consecutive pairs naturally trigger this check — producing ~5,200 warnings per 9 minutes. The aggregate out-of-order count remains logged at info-level in each poll summary line.
- **Data sink backpressure silently dropping events**: Replaced instant-drop backpressure in `gateway/core/data_sink.py` `_try_acquire_sink_slot` with a 2-second queuing timeout using `asyncio.wait_for`. During burst publishing (e.g., 200+ darkpool records in a batch poll), events now wait briefly for a publish slot instead of being silently dropped. Removed unused `_slot_lock` mutex.

## [0.5.68] - 2026-02-13

### Added

- **Sliding window rate limiting**: Replaced fixed-window counter in `RateLimitBucket` (`middleware.py`) with sliding window deque to prevent burst starvation where the entire allowance was consumed in <1 second.
- **Standard `Retry-After` header**: All 429 responses from `RateLimitMiddleware` and `GlobalRateLimitMiddleware` now include the `Retry-After` header for proper HTTP backoff.
- **Configurable Alpaca rate limits**: Added `GATEWAY_ALPACA_RATE_LIMIT_PER_MINUTE` and `GATEWAY_ALPACA_RATE_LIMIT_PER_SECOND` env vars in `config.py` to override Alpaca provider limits without code changes.
- **Sliding window rate limit tests**: New `tests/test_sliding_window_rate_limit.py` with 10 tests covering burst behavior, window expiry, and Retry-After header presence.

### Changed

- **3roses client rate limit**: Increased from 300 → 6,000 req/min in `clients.yaml` to accommodate ~5,000 symbol pre-market scanner burst.
- **Alpaca provider limits**: Updated from free-tier defaults (200/min, 10/sec) to paid-tier (10,000/min, 75/sec) in `rate_limiter.py`.
- **Provider rate limits aligned with official docs**: Removed arbitrary NewsAPI 10/min (their docs only enforce 100/day). Updated SEC EDGAR from conservative 8/sec + 300/min to official 10/sec + 600/min. Updated module docstring to document each provider's plan tier.
- **Rate limiter queues by default**: Changed `require_provider_rate_limit` default from `block=False` (immediate 429) to `block=True` (wait up to 30s for a slot). All provider calls now queue instead of rejecting.

## [0.5.67] - 2026-02-13

### Added

- **Snapshot envelope wrapping regression tests**: Added `test_wrap_event_symbol_keyed_dict_does_not_crash` and `TestIsSymbolKeyedDict` tests in `tests/test_envelope.py` to lock behavior for symbol-keyed dict payloads and middleware detection heuristic.
- Added `snapshots` and `snapshot` entries to `FEED_MAPPING` in `middleware.py`.

### Fixed

- **Envelope middleware crash on snapshots endpoint (`'dict' object has no attribute 'upper'`)**: Updated `EventEnvelopeMiddleware` in `gateway/api/middleware.py` to detect symbol-keyed dict payloads (where keys are ticker symbols like `"AAPL"`, `"S"`) and flatten them into per-symbol items before wrapping, instead of passing the entire keyed dict to `wrap_event` which misinterpreted tickers like `"S"` as metadata field lookups returning nested dicts.
- **Defensive symbol type guard in `wrap_event`**: Added type check in `gateway/core/envelope.py` to ensure the extracted symbol is always a string, preventing `AttributeError` if a non-string value is returned by the `or` extraction chain.

## [0.5.66] - 2026-02-13

### Added

- Added UW provider regression tests in `tests/test_uw_provider.py` for:
  - IV-rank latest lookup without forced date filtering.
  - IV-rank retry behavior when a date-filtered request returns HTTP 422.
  - Darkpool recent raw-HTTP fallback when SDK payload parsing fails.

### Fixed

- **UW IV-rank EOD poll error flood (HTTP 422)**: Updated `gateway/providers/uw.py` so `get_iv_rank(...)` no longer forces `date=today` when no date is provided, and retries once without date when date-filtered requests are rejected with `422`.
- **UW darkpool recent parser fragility on upstream gateway failures**: Updated `gateway/providers/uw.py` so `get_darkpool_recent(...)` falls back to raw HTTP (`/api/darkpool/recent`) when SDK parsing fails and logs upstream `5xx` as warning context.

## [0.5.65] - 2026-02-13

### Added

- Added `TESTING.md` with Data Gateway test commands, test layout, and TDD expectations.
- Added `DEVELOPER_NOTES.md` with operational gotchas, debugging tips, and high-risk edit areas.

### Changed

- Standardized AI agent guidance file to `AGENTS.md` and aligned content to current repository docs layout.
- Moved operational docs into standard locations:
  - `runbook.md` -> `docs/RUNBOOK.md`
  - `API_REFERENCE.md` -> `docs/API_REFERENCE.md`
- Relocated root audit/report artifacts to `docs/audits/` and normalized checklist naming to `AUDIT_CHECKLIST.md`.
- Updated cross-document links in `README.md`, `CONTRIBUTING.md`, and `docs/API_REFERENCE.md` to match standardized paths.

### Fixed

- Corrected runbook authentication header examples from `X-API-Key` to `X-Gateway-Key` to match actual gateway auth behavior.

## [0.5.64] - 2026-02-12

### Added

- **Alpaca/UW regression coverage**:
  - Added Alpaca option quote normalization tests for scalar/string/null `conditions`.
  - Added UW IV-rank endpoint contract tests for passthrough and fallback payload shapes.
  - Added router contract coverage to lock `block=True` limiter behavior on high-volume Alpaca options routes.

### Fixed

- **Alpaca option quote schema mismatch (502s)**: Updated Alpaca quote normalization to coerce `conditions` into a list shape accepted by `OptionQuote`.
- **UW iv-rank route failures (404/parse errors)**: Updated UW provider `get_iv_rank(...)` to parse iv-rank from raw HTTP payload with robust field fallback mapping.
- **Burst rate-limit amplification on hot options routes**: Updated Alpaca options routers to call provider limiter with `block=True` for chain/quote endpoints so requests queue instead of failing immediately under load.

## [0.5.63] - 2026-02-12

### Changed

- **Adaptive darkpool polling**: Replaced static 60s darkpool poll interval with time-of-day adaptive intervals — 15s during morning rush (9:30-10:30 ET), 30s during normal market hours, 60s during extended hours. Reduces missed trades during peak volume periods. Base loop tick reduced from 60s to 15s to support the faster cadence.

## [0.5.62] - 2026-02-12

### Added

- **REST sink explode regression coverage**: Added middleware tests in `tests/test_middleware_streaming.py` for Alpaca REST dict payloads with nested `bars[]` and `trades[]`, including empty-list skip behavior.

### Fixed

- **Malformed sink payload shape for Alpaca REST bars/trades**: Updated `gateway/api/middleware.py` so sink publishing now explodes nested REST payloads (`data.bars[]` / `data.trades[]`) into one EventEnvelope per item, preserving top-level context like `symbol` and `timeframe`.
- **Empty aggregate sink noise**: Sink publishing now skips empty list payloads for eligible routes instead of emitting empty aggregate envelopes that normalize to null-heavy Silver rows.
- **Bulk Alpaca stocks route feed resolution**: Updated canonical route mapping so `/api/v1/alpaca/stocks/bars` and `/api/v1/alpaca/stocks/trades` resolve to sink feeds `bars` / `trades` instead of fallback `stocks`.
- **Middleware sink eligibility typing cleanup**: Refined Alpaca bars/trades sink eligibility checks to use typed intermediate list variables, eliminating `len(Any | None)` static type errors without changing runtime behavior.

## [0.5.61] - 2026-02-12

### Fixed

- **Flow alerts dedup collision**: Added `flow_alerts` entry to `FEED_UNIQUE_FIELDS` in `envelope.py` — poller wraps flow events with feed `flow_alerts` but only `flow` was mapped, causing zero unique fields in event_id and collisions between different alerts on the same ticker at the same second.
- **Darkpool dedup collision**: Replaced options-specific fields (expiry/strike/put_call) in `FEED_UNIQUE_FIELDS["darkpool"]` with actual darkpool trade fields (`tracking_id`, `price`, `size`, `notional`) — the old fields don't exist on darkpool payloads, so all trades on the same ticker at the same second produced identical event_ids.

## [0.5.60] - 2026-02-12

### Added

- **Alpha Vantage rate-limit mapping regression coverage**: Added tests in `tests/test_alphavantage_common.py` to ensure provider runtime rate-limit errors map to HTTP `429` while unrelated runtime errors still bubble for normal error handling.

### Fixed

- **Alpha Vantage free-tier throttle status mapping**: Updated `gateway/api/alphavantage/common.py` to translate provider-side `"Rate limit exceeded"` runtime failures into explicit HTTP `429` responses (`Provider rate limit exceeded: alphavantage`) instead of generic `502`.
- **Alpha Vantage throttle log severity**: Updated `gateway/providers/alphavantage.py` daily/weekly/monthly handlers to log provider rate-limit failures at warning level (`*_rate_limited`) instead of error level, reducing false-positive error noise in operational logs.

## [0.5.59] - 2026-02-12

### Added

- **Redis sink regression coverage**: Added `tests/test_redis_sink.py` to lock reconnect/reset behavior after Redis LOADING failures and enforce bounded publish/health-check timeout behavior.
- **Readiness degradation/warm-up coverage**: Expanded `tests/test_health.py` with sink-degraded readiness behavior and Redis LOADING cache warm-up classification checks.
- **Alpha Vantage premium fallback coverage**: Expanded `tests/test_alphavantage_provider.py` with daily/weekly/monthly adjusted-endpoint fallback tests when premium endpoints are unavailable.

### Fixed

- **Alpha Vantage adjusted time-series premium fallback**: Updated `gateway/providers/alphavantage.py` so adjusted daily/weekly/monthly requests automatically retry their non-adjusted endpoints when Alpha Vantage returns premium-only errors.
- **Monthly fallback parsing correctness**: Updated monthly time-series parsing to safely map close/volume fields when fallback responses come from non-adjusted payload shapes.
- **Redis sink recovery under LOADING/timeouts**: Updated `gateway/core/redis_sink.py` to enforce bounded Redis operation timeouts and reset connection state on publish/health-check failures so reconnect can happen on the next attempt.
- **Readiness behavior during transient sink/cache startup states**: Updated `gateway/api/health.py` so sink failures report `degraded` (without flipping global readiness), and Redis dataset-loading cache failures are reported as `warming_up`.

### Changed

- **Redis Docker health-check startup tolerance**: Updated `docker-compose.yml` Redis health check with a command that requires `PONG`, increased retries, and a `300s` start period to reduce false unhealthy states during large AOF/RDB load windows.

## [0.5.58] - 2026-02-12

### Fixed

- **Mypy plugin import failure**: Updated `pyproject.toml` mypy plugin path from `numpy.typing.mypy` to `numpy.typing.mypy_plugin`, fixing type-check startup failure (`No module named 'numpy.typing.mypy'`).

## [0.5.57] - 2026-02-12

### Added

- **Alpha Vantage provider error payload regression coverage**: Added tests in `tests/test_alphavantage_provider.py` to ensure `_fetch_json(...)` fails fast when Alpha Vantage returns `Information` / `Error Message` payloads (including mixed rate-limit + premium wording).
- **Timeseries HTTP status propagation regression coverage**: Added `tests/test_alphavantage_timeseries.py::test_timeseries_routes_preserve_http_exception_status_codes` to lock `429` passthrough behavior for intraday/daily/weekly/monthly/search endpoints.

### Fixed

- **Silent empty-success Alpha Vantage responses**: Updated `gateway/providers/alphavantage.py` `_fetch_json(...)` to detect and raise on provider-side error payloads (`Note`, `Information`, `Error Message`) instead of returning empty success data.
- **Alpha Vantage rate-limit status remapping bug**: Updated `gateway/api/alphavantage/timeseries.py` handlers to re-raise `HTTPException` so provider limiter `429` responses are preserved and no longer converted into `502`.

## [0.5.56] - 2026-02-12

### Fixed

- **Alpha Vantage route max-points forwarding**: Updated `gateway/api/alphavantage/timeseries.py` to pass `max_points` through to provider calls for intraday/daily/weekly/monthly endpoints.
- **Alpha Vantage cache-key correctness for max-points requests**: Included `max_points` in Alpha Vantage time-series cache keys so capped responses do not share cache entries with uncapped/full responses.
- **Timeseries route regression tests restored**: Fixed `tests/test_alphavantage_timeseries.py` expectations by restoring intended route behavior (`max_points` forwarded + cache key suffix includes point cap).

## [0.5.55] - 2026-02-12

### Added

- **Alpha Vantage regression guard for daily max-points path**: Added `tests/test_alphavantage_provider.py::test_get_daily_respects_max_points_window` to reproduce and prevent the missing helper crash in time-series iteration.

### Fixed

- **Alpha Vantage daily/intraday/weekly time-series crash**: Restored `AlphaVantageProvider._iter_time_series_items(...)` in `gateway/providers/alphavantage.py` so max-points-limited time-series endpoints no longer raise `AttributeError: _iter_time_series_items` and return `502`.

## [0.5.54] - 2026-02-12

### Fixed

- **Cerberus provider access alignment**: Expanded `config/clients.yaml` `cerberus.permissions.providers` to include `finnhub`, `alphavantage`, and `sec` in addition to existing providers, eliminating gateway `403 Provider access denied` responses for those requested endpoints.

## [0.5.53] - 2026-02-12

### Fixed

- **Docker yfinance cache directory permissions**: Updated `Dockerfile` to create a real `/home/gateway` home directory for the non-root `gateway` user, pre-create `/home/gateway/.cache/py-yfinance`, and assign ownership to `gateway:gateway` so yfinance cache initialization no longer fails with permission-denied warnings.

## [0.5.52] - 2026-02-12

### Added

- **Middleware regression guard for UW flow list envelopes**: Added `tests/test_middleware_streaming.py::test_envelope_middleware_wraps_uw_flow_list_with_sink_enabled` to lock behavior for list payload envelope wrapping when sink publishing is enabled.

### Fixed

- **REST envelope sink batch publish crash**: Restored `EventEnvelopeMiddleware._publish_sink_batch(...)` in `gateway/api/middleware.py` so list payload sink publishes no longer throw `'EventEnvelopeMiddleware' object has no attribute '_publish_sink_batch'` and leak un-awaited coroutine warnings.

## [0.5.51] - 2026-02-12

### Changed

- **Removed embedded trading-bot scaffold**: Deleted legacy `trading-bot/` scripts, docs, and source modules from this repository to keep Data-Gateway scope focused on gateway services and providers.

## [0.5.50] - 2026-02-12

### Fixed

- **Finnhub bars compatibility regression**: `gateway/providers/finnhub.py` now supports both legacy `get_bars(symbol, resolution=...)` calls and batch `get_bars(symbols, timeframe, start, end)` calls without breaking tests or runtime callers.
- **Provider quote batch metric imports**: Added missing `record_provider_quote_batch_size` imports in `gateway/providers/finnhub.py` and `gateway/providers/alpaca.py` to prevent quote-path NameErrors.

## [0.5.49] - 2026-02-12

### Added

- **Trading-bot gateway client regression tests**: Added `tests/test_trading_bot_gateway_client.py` to lock auth/header behavior with TDD coverage for:
  - required `X-Gateway-Key` header usage,
  - API key resolution from explicit arg / env / `config/clients.yaml`,
  - fail-fast error when no key is available.

### Changed

- **Trading-bot gateway auth alignment**: Updated `trading-bot/src/core/gateway_client.py` to use `X-Gateway-Key` (matching gateway middleware), remove hardcoded default key fallback, and fail fast when no valid key source exists.
- **Trading-bot connectivity script auth header**: Updated `trading-bot/test_connectivity.py` to use `X-Gateway-Key` for authenticated endpoint checks.

### Fixed

- **Readiness probe runtime error**: Fixed missing `inspect` import in `gateway/api/health.py` so async cache-delete readiness checks execute correctly (`/health/ready` no longer flips to `not_ready` from `NameError`).

## [0.5.48] - 2026-02-12

### Changed

- **Finnhub bars API compatibility restore**: Updated `gateway/providers/finnhub.py` `get_bars(...)` to support both legacy single-symbol calls (`symbol`, `resolution`) and current batch signature (`symbols`, `timeframe`, `start`, `end`) while preserving normalized timeframe output.
- **Finnhub quote-batch telemetry hook**: Added `record_provider_quote_batch_size` wiring in `gateway/providers/finnhub.py` to keep provider quote batch metrics emitting consistently.
- **Alpaca quote-batch telemetry hook**: Added `record_provider_quote_batch_size` import in `gateway/providers/alpaca.py` to resolve quote-path metric calls.
- **Alpaca timeframe normalization**: Expanded `_convert_timeframe(...)` mapping in `gateway/providers/alpaca.py` to normalize shorthand inputs (`1m`, `5m`, `1h`, `1d`, etc.) into Alpaca-compatible timeframe strings.

## [0.5.47] - 2026-02-12

### Changed

- **Startup crash recovery for gateway boot path**: Restored stream-to-sink dispatch helpers in `gateway/main.py` (`_configure_stream_sink_dispatch_limits`, `_set_stream_sink_registry`, `_schedule_stream_sink_publish`, `_drain_stream_sink_publish_tasks`) and reconnected metric hooks so app startup no longer fails with missing-name errors.
- **Metrics regression repair after merge drift**: Restored missing telemetry primitives in `gateway/core/metrics.py`, including `ROUTE_CACHE_EVENTS`, provider health check timing/snapshots, stream sink/fanout scheduler event helpers, and derived snapshot calculations used by admin status surfaces.
- **Stream fanout telemetry wiring**: Updated `gateway/core/stream.py` imports to include `record_stream_fanout_dispatch_event` and `record_stream_fanout_batch_size` so runtime fanout metrics calls resolve correctly.
- **Provider quote batch metric wiring**: Added `record_provider_quote_batch_size` imports in `gateway/providers/alpaca.py` and `gateway/providers/finnhub.py` to prevent quote-path NameErrors.
- **Metrics endpoint import fix**: Added `update_memory_metrics_if_due` import in `gateway/api/metrics.py` so `/metrics` scrape path executes without unresolved-name failures.
- **Registry capability compatibility hardening**: Updated `gateway/core/registry.py` capability checks to support both legacy list-style capabilities and `ProviderCapabilities` objects, preventing provider ordering regressions.

## [0.5.46] - 2026-02-07

### Added

- **Alpha Vantage provider helper/sort regression coverage**: Expanded `tests/test_alphavantage_provider.py` with shared `_fetch_json(...)` API-key injection and rate-limit-note behavior checks, plus `_top_time_series_items(...)` fast-path and fallback ordering assertions.

### Changed

- **Alpha Vantage provider shared fetch helper rollout**: Added `_ensure_ready(...)` and `_fetch_json(...)` in `gateway/providers/alphavantage.py` and migrated quote/time-series/fundamentals/indicator/forex/crypto/economic methods to use shared request + rate-limit-note handling.
- **Alpha Vantage provider sort-head optimization**: Added `_top_time_series_items(...)` in `gateway/providers/alphavantage.py` and replaced full `sorted(..., reverse=True)[:100]` paths in technical-indicator/forex-daily/crypto-daily payload assembly.
- **Alpha Vantage provider micro-benchmark evidence**: Captured targeted helper benchmark results for ordered vs unordered head extraction and recorded outcomes in Alpha Vantage audit docs to validate the fast-path behavior.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md`, `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md`, and `PERFORMANCE_AUDIT_ALPHAVANTAGE_PROVIDER_DEEP_DIVE.md` to mark AV-3 provider helper/sort remediation complete and narrow remaining Alpha Vantage scope to broader runtime profiling and optional heavy time-series limit tuning.

## [0.5.45] - 2026-02-07

### Added

- **Alpha Vantage provider AV-3 coverage tests**: Added `tests/test_alphavantage_provider.py` for CSV quoted-comma parsing behavior, `quotes_max_concurrency` config parsing/fallback, and bounded `get_quotes(...)` fan-out assertions.

### Changed

- **Alpha Vantage provider bounded quote fan-out**: Updated `gateway/providers/alphavantage.py` `get_quotes(...)` to use semaphore-bounded concurrency (configurable via `alphavantage.config.quotes_max_concurrency`) while preserving fail-soft per-symbol behavior.
- **Alpha Vantage provider CSV parsing hardening**: Replaced split-based CSV parsing in earnings/IPO/listing endpoints with shared `csv.DictReader` parsing helper in `gateway/providers/alphavantage.py` to reduce allocation overhead and handle quoted-comma payloads safely.
- **Alpha Vantage provider config tuning knob**: Updated `config/providers.yaml` to include `alphavantage.config.quotes_max_concurrency: 2` for explicit default bounded parallelism.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md`, `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md`, and `PERFORMANCE_AUDIT_ALPHAVANTAGE_PROVIDER_DEEP_DIVE.md` to mark AV-3 provider rollout progress and remaining benchmark/helper-consolidation follow-ups.

## [0.5.44] - 2026-02-06

### Added

- **Alpha Vantage AV-2 cache/payload metrics**: Added `gateway_alphavantage_route_cache_total` and `gateway_alphavantage_payload_bytes` metrics in `gateway/core/metrics.py` for endpoint-level cache hit/miss and miss-path payload-size visibility.
- **Alpha Vantage AV-2 helper coverage tests**: Expanded `tests/test_alphavantage_common.py` with search-query normalization, cache-disabled behavior, and cache/payload metric emission assertions.

### Changed

- **Alpha Vantage full-output cache policy**: Updated `gateway/api/alphavantage/timeseries.py` so `outputsize=full` on intraday/daily bypasses route caching while preserving response contracts.
- **Alpha Vantage search-key cardinality guardrail**: Added normalized/truncated search cache key handling via `normalize_search_query(...)` in `gateway/api/alphavantage/common.py`, applied in `gateway/api/alphavantage/timeseries.py`.
- **Alpha Vantage helper instrumentation rollout**: Updated `gateway/api/alphavantage/common.py` and all AV route modules to emit endpoint/cache-mode cache events and payload-size observations through shared helper flow.
- **Alpha Vantage audit tracking updates**: Updated `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark AV-2 implemented and narrow remaining Alpha Vantage scope to AV-3 provider internals.

## [0.5.43] - 2026-02-06

### Changed

- **Alpha Vantage AV-1 helper rollout completion**: Refactored `gateway/api/alphavantage/{indicators,calendars,crypto,forex,economic}.py` to use shared `execute_av_cached` cache-first flow, removing remaining per-route provider lookup/rate-limit/cache boilerplate.
- **Alpha Vantage cache-first consistency**: All Alpha Vantage route modules now short-circuit cache hits before provider lookup, preserving response contracts while reducing miss-path drift.
- **Alpha Vantage audit tracking updates**: Updated `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark AV-1 helper migration complete and shift next work to AV-2 cache policy/cardinality instrumentation.

## [0.5.42] - 2026-02-06

### Added

- **Alpha Vantage shared helper tests**: Added `tests/test_alphavantage_common.py` covering cache-hit short-circuit behavior, cache-miss fetch/store flow, and provider-unavailable guardrails for the new shared Alpha Vantage route helper.

### Changed

- **Alpha Vantage shared cached-route helper**: Added `execute_av_cached`, `get_alphavantage_provider`, and `make_response` in `gateway/api/alphavantage/common.py` to centralize cache-first + provider/rate-limit response flow.
- **Alpha Vantage AV-1 route migration (phase 1)**: Refactored `gateway/api/alphavantage/timeseries.py` and `gateway/api/alphavantage/fundamentals.py` to use shared helper flow so cache hits return before provider lookup, reducing repeated boilerplate and miss-path drift.
- **Alpha Vantage serialization normalization**: Standardized monthly time-series serialization in `gateway/api/alphavantage/timeseries.py` to `model_dump(mode="json")` for consistency with other time-series endpoints.
- **Alpha Vantage audit tracking updates**: Updated `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark AV-1 helper rollout in progress with `timeseries.py` and `fundamentals.py` migrated.

## [0.5.41] - 2026-02-06

### Added

- **UW native-offset pagination coverage tests**: Expanded `tests/test_uw_common.py` and `tests/test_uw_provider.py` with cursor page-limit/prefetched-window pagination assertions and provider offset fallback behavior checks.

### Changed

- **UW flow/market native pagination rollout**: Updated `gateway/api/uw/flow.py` and `gateway/api/uw/market.py` (institutions route) to use cursor offset + `limit+1` page fetches with `paginate_offset_response`, reducing offset-depth over-fetch where provider pagination is available.
- **UW shared pagination helpers**: Added `cursor_page_limit` and `paginate_offset_response` in `gateway/api/uw/common.py` for native-offset route pagination response construction.
- **UW provider optional offset/page support with safe fallback**: Updated `gateway/providers/uw.py` flow/darkpool/institutions methods to accept `offset`, attempt native `offset/page` SDK params, and fall back to compatible over-fetch+local-slice behavior when unsupported.
- **UW audit tracking updates**: Updated `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to reflect native pagination rollout in UW flow/darkpool/institutions paths and narrow remaining UW pagination scope.

## [0.5.40] - 2026-02-06

### Added

- **UW provider concurrency guardrail tests**: Added `tests/test_uw_provider.py` to validate `max_inflight_calls` config parsing, bounded `_call_sync` semaphore behavior, and sync-call wait/exec metric hook emission.

### Changed

- **UW provider bounded sync-call concurrency**: Updated `gateway/providers/uw.py` to enforce a semaphore around `_call_sync`, configurable via `unusual_whales.config.max_inflight_calls` (default `32`), reducing thread-offload contention risk under high request concurrency.
- **UW provider sync-call observability**: Added provider sync-call wait/exec/inflight metrics and helper functions in `gateway/core/metrics.py`, and instrumented UW `_call_sync` to emit queue wait and execution durations.
- **UW provider config tuning knob**: Updated `config/providers.yaml` to include explicit `unusual_whales.config.max_inflight_calls`.
- **UW audit tracking updates**: Updated `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark bounded `_call_sync` concurrency/metrics remediation complete and narrow remaining UW scope.

## [0.5.39] - 2026-02-06

### Changed

- **Cache auth fail-closed hardening**: Updated `gateway/api/middleware.py` so non-public `GET` requests now return `401` when `X-Gateway-Key` is missing/invalid before any cache lookup, eliminating unauthenticated cache-leak exposure from accidentally unprotected endpoints.

### Added

- **Auth boundary regression tests**:
  - Added `test_cache_middleware_requires_auth_for_non_public_get` in `tests/test_optimization.py` to verify missing/invalid key rejection and authenticated cache behavior.
  - Added `test_missing_auth_returns_401_for_account` in `tests/smoke/test_smoke.py` to assert unauthenticated access to `/api/v1/alpaca/account` is blocked.

## [0.5.38] - 2026-02-06

### Changed

- **UW Wave 1 route-helper rollout completed**: Refactored `gateway/api/uw/{etf,earnings,seasonality,screener}.py` to use shared `execute_uw_cached` + `make_response`/`paginate_response` flow, removing remaining direct cache/rate-limit/provider-call boilerplate in UW route files.
- **UW audit status tracking updates**: Updated `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark shared-helper rollout complete across all UW route modules and shift remaining UW scope to provider-side concurrency/native-pagination profiling work.

## [0.5.37] - 2026-02-06

### Changed

- **UW helper rollout expansion across analytics/intelligence modules**: Refactored `gateway/api/uw/{institutions,flow_analytics,market_data,intelligence,politicians,volatility,etf_extended,shorts,options_data,greeks}.py` to use shared `execute_uw_cached` + `make_response`/`paginate_response` flow, removing repeated cache/rate-limit/provider-call boilerplate while preserving endpoint contracts and 404 semantics.
- **UW rollout status tracking updates**: Updated `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark twenty UW router modules on shared helper flow and isolate final remaining router files for future migration (`etf.py`, `earnings.py`, `seasonality.py`, `screener.py`).

## [0.5.36] - 2026-02-06

### Changed

- **UW helper rollout to high-volume route modules**: Refactored `gateway/api/uw/contracts.py`, `gateway/api/uw/calendar.py`, and `gateway/api/uw/extended.py` to use shared `execute_uw_cached` + `make_response` flow, removing repeated cache/rate-limit/provider-call boilerplate while preserving endpoint contracts.
- **UW wave tracking updates**: Updated `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to reflect shared-helper rollout across ten UW route modules.

## [0.5.35] - 2026-02-06

### Changed

- **UW router helper rollout expansion**: Refactored `gateway/api/uw/options.py`, `gateway/api/uw/misc.py`, `gateway/api/uw/alerts.py`, and `gateway/api/uw/insiders.py` to use `execute_uw_cached` + shared response builders, removing duplicated cache/rate-limit/provider-call boilerplate while preserving endpoint contracts.
- **UW wave tracking updates**: Updated `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to reflect shared-helper rollout progress across seven UW route modules.

## [0.5.34] - 2026-02-06

### Changed

- **UW stock router dedupe rollout**: Refactored `gateway/api/uw/stock.py` to use shared `execute_uw_cached` flow for cache/rate-limit/provider-call/cache-set behavior across all stock endpoints while preserving endpoint contracts and metadata.
- **UW shared response metadata support**: Updated `gateway/api/uw/common.py::make_response` to accept optional `extra_meta`, enabling consistent metadata assembly in shared-response routes without response-shape changes.
- **UW audit progress tracking**: Updated `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to reflect shared-helper rollout through `flow.py`, `market.py`, and `stock.py`.

### Added

- **UW common response test coverage**: Expanded `tests/test_uw_common.py` with `make_response` extra-metadata serialization assertions.

## [0.5.33] - 2026-02-06

### Added

- **yfinance provider conversion tests**: Added `tests/test_yfinance_provider.py` for history-bar and major-holders DataFrame conversion helpers.

### Changed

- **yfinance row-iteration optimization**: Updated `gateway/providers/yfinance.py` to replace `iterrows()` with `itertuples()` in history and major-holders conversion paths, reducing per-row allocation overhead while preserving response shape.
- **yfinance audit progress tracking**: Updated `PERFORMANCE_AUDIT_YF_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark provider `iterrows` hotspots remediated and narrow remaining yfinance Wave 1 scope.

## [0.5.32] - 2026-02-06

### Added

- **Provider registry concurrency tests**: Added `tests/test_registry.py` to validate concurrent provider health checks and exception capture behavior.

### Changed

- **Parallel provider health checks**: Updated `gateway/core/registry.py` so `health_check_all()` runs provider checks concurrently via `asyncio.gather`, reducing admin/control-plane status latency under multiple providers.
- **Audit progress tracking**: Updated `PERFORMANCE_AUDIT_FINNHUB_CONTROL_PLANE_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark admin health-check parallelization complete and narrow remaining Finnhub/control-plane scope.

## [0.5.31] - 2026-02-06

### Added

- **UW route helper and cursor guardrails**: Added shared UW router utilities in `gateway/api/uw/common.py`:
  - `execute_uw_cached` for unified cache/get-rate-limit/provider-call/cache-set flow
  - `cursor_fetch_limit` and bounded `decode_cursor(..., max_offset=...)` with clamped offset guardrail
  - centralized serializer helpers used by list/pagination response builders.
- **UW common helper tests**: Added `tests/test_uw_common.py` covering cursor clamping, pagination serialization, and cache/rate-limit helper behavior.

### Changed

- **UW flow route dedupe**: Refactored `gateway/api/uw/flow.py` to use shared cached route helper and centralized pagination serialization without changing endpoint contracts.
- **UW market route dedupe**: Refactored `gateway/api/uw/market.py` to use shared cached route helper and list/pagination response builders, reducing repeated boilerplate.
- **Audit progress tracking**: Updated `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark UW Wave 1 router remediation as in progress and narrow remaining UW scope.

## [0.5.30] - 2026-02-06

### Added

- **Perf release-readiness command**: Added `scripts/perf_release_readiness.py` to compare active perf configs against tracked config files, print unified diffs, optionally apply promotion, and emit a markdown report.
- **Release-readiness command tests**: Added `tests/test_perf_release_readiness.py` covering dry-run diff/report behavior and apply-mode promotion.
- **Perf promotion runbook**: Added `PERF_RELEASE_READINESS.md` with a concise operator checklist for dry-run, apply, and validation steps.

### Changed

- **README perf operations docs**: Updated `README.md` with local perf gate command usage and link to the perf release-readiness runbook.
- **Benchmark audit tracking updates**: Updated `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to include the new release-readiness helper and runbook in BENCH operations.

## [0.5.29] - 2026-02-06

### Added

- **Perf active-config promotion utility**: Added `scripts/perf_promote_active_configs.py` to safely promote `.perf/perf_budgets.active.json` and `.perf/perf_baseline.active.json` into versioned `config/` files.
- **Promotion utility tests**: Added `tests/test_perf_promote_active_configs.py` covering write-mode promotion and dry-run no-write behavior.

### Changed

- **Benchmark audit tracking updates**: Updated `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to include the promotion utility in BENCH operations.

## [0.5.28] - 2026-02-06

### Added

- **Automated perf baseline history manager**: Added `scripts/perf_baseline_manager.py` to append/rotate run history, refresh median-based suite/test baselines, and ratchet budgets from history windows.
- **Baseline manager unit tests**: Added `tests/test_perf_baseline_manager.py` to verify history rotation, baseline refresh, ratchet application, and min-sample guard behavior.
- **Slow-backend sink perf profile**: Added `test_sink_publish_backpressure_with_slow_backend_profile` in `tests/perf/test_perf_stream_sink.py` to validate bounded scheduling/backpressure under delayed sink publish latency.

### Changed

- **Perf CI guardrail automation**: Updated `.github/workflows/perf-guardrail.yml` to restore/save rolling `.perf` cache state, use active perf baseline/budget files when available, and publish history automation artifacts.
- **Perf budgets ratchet config**: Updated `config/perf_budgets.json` with `ratchet` settings (history/baseline windows, min samples, multipliers, and floors) for automated threshold tightening.
- **Perf baseline coverage**: Updated `config/perf_baseline.json` with baseline timing entry for the new slow-backend sink perf test.
- **Benchmark audit tracking updates**: Updated `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark BENCH ratcheting, slower-backend sink coverage, and baseline rotation automation as implemented.

## [0.5.27] - 2026-02-06

### Added

- **Versioned perf budgets**: Added `config/perf_budgets.json` with suite-level and per-test runtime budgets for the `tests/perf` gate.
- **Versioned perf baseline timings**: Added `config/perf_baseline.json` for suite/test timing anchors used by trend-delta regression checks.
- **Expanded multi-sink perf coverage**: Added `test_sink_publish_backpressure_multi_sink_bounds` in `tests/perf/test_perf_stream_sink.py` to validate bounded in-flight behavior with two blocked sinks.

### Changed

- **Perf gate budget+trend enforcement upgrade**: Updated `scripts/perf_gate.py` to read `config/perf_budgets.json` and `config/perf_baseline.json`, enforce per-test budgets and trend-delta regressions from JUnit timings, and include `test_times_seconds` in `perf-summary.json`.
- **CI perf workflow budget wiring**: Updated `.github/workflows/perf-guardrail.yml` to run the gate with `--budgets-file config/perf_budgets.json` instead of a hardcoded runtime threshold.
- **Benchmark tracking updates**: Updated `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to reflect per-test budget + trend guardrails and expanded sink coverage, with remaining scope focused on threshold ratcheting and baseline refresh automation.

## [0.5.26] - 2026-02-06

### Added

- **Dedicated perf CI guardrail workflow**: Added `.github/workflows/perf-guardrail.yml` to run `pytest -m perf` on push/PR (`main`/`master`), enforce a coarse runtime threshold, and always upload perf artifacts.
- **Reusable perf gate runner**: Added `scripts/perf_gate.py` to execute perf tests, enforce a configurable max runtime budget, and emit:
  - `perf-junit.xml`
  - `perf-output.txt`
  - `perf-summary.json`

### Changed

- **Benchmark audit Wave 3 tracking**: Updated `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` to mark CI guardrails as COMPLETE and shift remaining scope to threshold tuning, multi-sink perf expansion, and per-test budget evolution.
- **Top-level benchmark next-run refinement**: Updated `PERFORMANCE_AUDIT.md` item 14 from Wave 3 implementation to BENCH guardrail tuning/evolution work.

## [0.5.25] - 2026-02-06

### Added

- **Runtime sink bounded-dispatch hardening**: Updated `gateway/core/data_sink.py` to enforce per-sink in-flight publish caps with backpressure drop handling and publish stats (`scheduled`, `dropped_backpressure`) to prevent unbounded task growth during sink slowdowns.

### Changed

- **Wave 2 sink perf assertions upgraded**: Updated `tests/perf/test_perf_stream_sink.py` from backlog observation to explicit boundedness assertions, validating in-flight task caps under blocked sink I/O.
- **Benchmark/core-infra audit progress tracking**: Updated `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md`, `PERFORMANCE_AUDIT_CORE_INFRA_DEEP_DIVE.md`, and `PERFORMANCE_AUDIT.md` to mark sink in-flight hardening complete and narrow remaining scope to BENCH Wave 3 CI guardrails + threshold tuning.

## [0.5.24] - 2026-02-06

### Added

- **Wave 2 replay/bulk memory perf coverage**: Added `tests/perf/test_perf_replay_bulk_memory.py` with dedicated `pytest -m perf` tests for:
  - replay large-batch loop memory profile and throughput envelope
  - bulk result streaming vs JSONL materialization peak-allocation comparison

### Changed

- **Benchmark deep-dive progress tracking**: Updated `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` to mark replay/bulk Wave 2 perf coverage as COMPLETE, record updated perf suite validation (`6 passed`), and narrow remaining benchmark scope to sink in-flight hardening plus BENCH Wave 3 CI guardrails.
- **Top-level next-run benchmark scope refinement**: Updated `PERFORMANCE_AUDIT.md` item 14 to reflect that Wave 2 perf coverage is now in place and remaining BENCH work is runtime sink bounded-dispatch hardening + CI perf thresholds/artifacts/trend tracking.

## [0.5.23] - 2026-02-06

### Added

- **Wave 2 stream/sink perf coverage**: Added `tests/perf/test_perf_stream_sink.py` with dedicated `pytest -m perf` tests for:
  - stream fanout in-flight semaphore bound validation
  - sink publish backpressure/task-growth profiling under blocked sink I/O

### Changed

- **Benchmark deep-dive progress tracking**: Updated `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` to mark Wave 2 stream/sink perf coverage as COMPLETE, include validated perf run results (`4 passed`), and narrow remaining benchmark scope to replay/bulk memory coverage, runtime sink in-flight bounding, and CI perf guardrails.
- **Top-level next-run benchmark scope refinement**: Updated `PERFORMANCE_AUDIT.md` item 14 to reflect that sink/fanout perf coverage is now in place and remaining BENCH work is replay/bulk memory paths plus CI threshold/artifact enforcement.

## [0.5.22] - 2026-02-06

### Added

- **Initial BENCH-1 perf harness**: Added `tests/perf/test_perf_baseline.py` with dedicated `pytest -m perf` baseline tests for envelope serialization and metrics path normalization hot paths.

### Changed

- **Pytest perf marker split**: Updated `pyproject.toml` to register a `perf` marker and exclude perf tests from default runs (`-m 'not perf'`), enabling explicit benchmark execution without slowing functional CI suites.
- **Benchmark baseline stabilization**: Updated failing perf-sensitive tests to match current contracts:
  - cache-header tests now target public health routes in `tests/test_middleware_streaming.py` and `tests/test_optimization.py`
  - replay tests now pass required `client_id` to `ReplaySession` and `ReplaySessionManager.create_session` in `tests/test_replay.py`
- **Benchmark audit progress tracking**: Updated `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` and `PERFORMANCE_AUDIT.md` to mark BENCH-1 baseline stabilization as complete and shift future scope to BENCH Wave 2/3 (coverage expansion + CI perf guardrails).

## [0.5.21] - 2026-02-06

### Added

- **Benchmark/profiling readiness deep-dive performance audit**: Added `PERFORMANCE_AUDIT_BENCHMARKING_DEEP_DIVE.md` covering CI perf-gating gaps, pytest benchmark-readiness, targeted failing perf-sensitive test slices, and fresh microbench baselines across middleware/stream/sink/replay-adjacent core paths.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to add explicit COMPLETE coverage for benchmark/profiling readiness and shifted next-run scope to BENCH Wave 1 implementation tasks.

## [0.5.20] - 2026-02-06

### Added

- **Core infrastructure deep-dive performance audit**: Added `PERFORMANCE_AUDIT_CORE_INFRA_DEEP_DIVE.md` covering the remaining core infrastructure set (`gateway/core/{adjustments,auth,balancer,circuit_breaker,connections,corporate_actions,data_sink,dedup,metrics,multiplexer,normalizer,rate_limiter,redis_sink,provider}.py`, 3380 LOC) with prioritized low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to add explicit COMPLETE coverage for remaining core infrastructure modules and expanded next-run priorities with implementation-focused CORE-INFRA Wave 1 work.

## [0.5.19] - 2026-02-06

### Added

- **Tests deep-dive performance audit**: Added `PERFORMANCE_AUDIT_TESTS_DEEP_DIVE.md` covering the full `tests/` suite (28 files, 303 tests, 4491 LOC) with measured runtime hotspots from `pytest -q --durations=25` and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `tests/` as COMPLETE and shift remaining next-run scope to implementation waves plus benchmark harness creation.

## [0.5.18] - 2026-02-06

### Added

- **Core modules deep-dive performance audit**: Added `PERFORMANCE_AUDIT_CORE_MODULES_DEEP_DIVE.md` covering `gateway/core/security.py`, `gateway/core/quality.py`, `gateway/core/calendar.py`, `gateway/core/symbology.py`, `gateway/core/validator.py`, plus `scripts/live_provider_smoke.py` and `scripts/generate_provider_contract.py`, with quantified hotspots and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark the sampled core module set and runtime scripts as COMPLETE, and moved next-run priorities to implementation waves, full `tests/` execution-path audit, and benchmark harnessing.

## [0.5.17] - 2026-02-06

### Added

- **News provider deep-dive performance audit**: Added `PERFORMANCE_AUDIT_NEWS_PROVIDER_DEEP_DIVE.md` covering `gateway/providers/news.py` (333 LOC, full provider pass) with quantified hotspots and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/providers/news.py` as COMPLETE and replaced the remaining provider deep-pass item with implementation-focused News provider Wave 1 follow-up.

## [0.5.16] - 2026-02-06

### Added

- **Alpha Vantage provider deep-dive performance audit**: Added `PERFORMANCE_AUDIT_ALPHAVANTAGE_PROVIDER_DEEP_DIVE.md` covering `gateway/providers/alphavantage.py` (1082 LOC, full provider pass) with quantified hotspots and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/providers/alphavantage.py` as COMPLETE and narrowed remaining provider deep-audit scope to `gateway/providers/news.py`.

## [0.5.15] - 2026-02-06

### Added

- **Alpaca provider deep-dive performance audit**: Added `PERFORMANCE_AUDIT_ALPACA_PROVIDER_DEEP_DIVE.md` covering `gateway/providers/alpaca.py` (2153 LOC, full provider pass) with quantified hotspots and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/providers/alpaca.py` as COMPLETE and narrowed remaining provider deep-audit scope to `gateway/providers/alphavantage.py` and `gateway/providers/news.py`.

## [0.5.14] - 2026-02-06

### Added

- **Non-provider router deep-dive performance audit**: Added `PERFORMANCE_AUDIT_NON_PROVIDER_ROUTERS_DEEP_DIVE.md` covering `gateway/api/bulk.py`, `gateway/api/calendar.py`, `gateway/api/corporate.py`, `gateway/api/news.py`, `gateway/api/quality.py`, `gateway/api/replay.py`, `gateway/api/symbology.py`, and `gateway/api/metrics.py` (34 endpoints) with quantified hotspots and low-risk optimization waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark the non-provider router group as COMPLETE and narrowed future scope toward implementation waves, remaining provider deep passes, and benchmark/profiling validation.

## [0.5.13] - 2026-02-06

### Added

- **Alpaca deep-dive performance audit**: Added `PERFORMANCE_AUDIT_ALPACA_DEEP_DIVE.md` covering all `gateway/api/alpaca/*` modules (14 files, 60 endpoints) with quantified hotspots, low-risk optimization recommendations, implementation waves, and audited-vs-future tracking.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/api/alpaca/*` as COMPLETE and explicitly list the remaining non-provider API modules and partial providers requiring future deep audits.

## [0.5.12] - 2026-02-05

### Added

- **Finnhub + control-plane deep-dive performance audit**: Added `PERFORMANCE_AUDIT_FINNHUB_CONTROL_PLANE_DEEP_DIVE.md` covering `gateway/api/finnhub/*`, `gateway/api/admin.py`, `gateway/api/catalog.py`, `gateway/api/health.py`, and `gateway/providers/finnhub.py` with quantified hotspots and low-risk implementation waves.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark Finnhub routers/provider and admin/catalog/health routers as COMPLETE, and narrowed pending route-level audit scope to `gateway/api/alpaca/*`.

## [0.5.11] - 2026-02-05

### Added

- **SEC deep-dive performance audit**: Added `PERFORMANCE_AUDIT_SEC_DEEP_DIVE.md` with endpoint/provider hotspot metrics, prioritized low-risk findings, implementation waves, and audited-vs-future tracking.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/api/sec.py` and `gateway/providers/sec.py` as COMPLETE and revised next-run priorities toward implementation waves and remaining sampled router groups.

## [0.5.10] - 2026-02-05

### Added

- **yfinance deep-dive performance audit**: Added `PERFORMANCE_AUDIT_YF_DEEP_DIVE.md` with endpoint/provider hotspot metrics, prioritized low-risk findings, implementation waves, and audited-vs-future tracking.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/api/yf.py` and `gateway/providers/yfinance.py` as COMPLETE and revised next-run priorities to include `gateway/api/sec.py` deep audit.

## [0.5.9] - 2026-02-05

### Added

- **Alpha Vantage deep-dive performance audit**: Added `PERFORMANCE_AUDIT_ALPHAVANTAGE_DEEP_DIVE.md` with route-level hotspot metrics, prioritized low-risk findings, implementation waves, and audited-vs-future file tracking.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/api/alphavantage/*` as COMPLETE and revised next-run priorities to implementation follow-up plus `gateway/api/yf.py` deep audit.

## [0.5.8] - 2026-02-05

### Added

- **UW deep-dive performance audit**: Added `PERFORMANCE_AUDIT_UW_DEEP_DIVE.md` with prioritized low-risk findings, evidence anchors, route/provider hotspot metrics, phased implementation plan, and file-level audit coverage.

### Changed

- **Top-level performance coverage status update**: Updated `PERFORMANCE_AUDIT.md` to mark `gateway/providers/uw.py` and `gateway/api/uw/*` as COMPLETE after dedicated deep pass, and replaced next-run UW audit tasks with implementation-focused follow-ups.

## [0.5.7] - 2026-02-05

### Added

- **Repository-wide performance audit and execution backlog**: Added `PERFORMANCE_AUDIT.md` with prioritized low-risk optimization findings, implementation waves, verification plan, and a coverage tracker showing audited modules vs future-run audit targets.

## [0.5.6] - 2026-02-05

### Added

- **Provider alignment audit report**: Added `PROVIDER_ALIGNMENT_AUDIT.md` with route inventory, doc-drift analysis, and error-contract findings for UW/Finnhub/Alpha Vantage/SEC/yfinance.
- **Generated provider contract artifact**: Added `scripts/generate_provider_contract.py` and generated `PROVIDER_ENDPOINT_CONTRACT.md` from live FastAPI routes.

### Fixed

- **Middleware streaming safety (TD-031)**:
  - `EventEnvelopeMiddleware` now skips envelope wrapping for unknown-length, streamed, and oversized responses.
  - `CacheMiddleware` now skips caching for streamed event payloads (`text/event-stream`, `application/x-ndjson`) to avoid body buffering.
  - `main.py` now passes `cache_max_body_bytes` into `EventEnvelopeMiddleware` so both cache and envelope logic use the same body-size guard.
- **Middleware regression coverage**:
  - Added tests validating bypass behavior for streaming/large payloads and preserving envelope wrapping for small JSON payloads.
- **Calendar trading-day loop syntax**: Fixed indentation in `TradingCalendar.get_trading_days()` that prevented module import and blocked test execution.
- **Retired duplicate Alpaca stream handlers (TD-014 follow-up)**: Removed unused legacy modules `gateway/providers/alpaca_stream.py`, `gateway/providers/alpaca_options_stream.py`, `gateway/providers/alpaca_crypto_stream.py`, and `gateway/providers/alpaca_news_stream.py` to keep `gateway/core/stream.py` as the single streaming implementation.
- **Standardized HTTP error contract (TD-033)**: Added global HTTPException normalization so API errors consistently return `success=false` with stable `error.code`/`error.message`.
- **Provider docs and PRD contract alignment (TD-032/TD-034)**: Updated API docs to reference generated route contract and added PRD reference to generated endpoint contract.
- **Contract drift guard in CI**: Added CI step to enforce `python scripts/generate_provider_contract.py --check`.
- **Integration auth fixture drift**: Centralized test API-key fixtures in `tests/conftest.py` and removed hardcoded keys from auth/integration/smoke tests to prevent `401` regressions when client keys change.
- **WebSocket disconnect busy-loop**: Stopped `_message_loop` from spinning on post-disconnect `RuntimeError` by treating disconnect-runtime errors as terminal and exiting cleanly.
- **Pytest asyncio loop-scope pinning**: Set `asyncio_default_fixture_loop_scope = "function"` to remove deprecation warnings and lock predictable async fixture behavior across pytest-asyncio upgrades.
- **Clarification for bundled commit scope**: Added `COMMIT_6077c9f_BREAKDOWN.md` to document and categorize the full set of files that landed in `6077c9f` without rewriting commit history.
- **Pre-commit reliability restoration**: Fixed current `ruff`/`mypy` blockers in bulk/calendar/corporate/replay modules and allowlisted known high-entropy OpenAPI schema field names so `pre-commit run --all-files` passes cleanly again.
- **Release-readiness CI workflow**: Added `.github/workflows/release-readiness.yml` to run `pre-commit` plus the targeted auth/integration/smoke/websocket pytest suite on push/PR to `master`.
- **Live provider smoke tooling**: Added `scripts/live_provider_smoke.py`, `LIVE_PROVIDER_SMOKE_CHECKLIST.md`, and generated `LIVE_PROVIDER_SMOKE_REPORT.md` for repeatable runtime checks against Alpaca/Finnhub/AlphaVantage/UW/SEC.
- **Typed provider registry access (step-down of `Any`)**: Introduced provider Protocol types and replaced `Any` casts in bulk/calendar/corporate API paths for stronger mypy guarantees on registry-loaded providers.
- **Audit release-readiness closure**: Added a release-readiness section to `AUDIT_TECHNICAL_DEBT.md` with static debt completion status, regression test status, live provider smoke outcomes, and release-gate recommendation.

## [0.5.5] - 2026-02-04

### Fixed

- **Mypy error in AlpacaProvider**: Fixed `exercise_options_position` return type handling - SDK method returns `None`, code now correctly ignores void return instead of calling `_model_to_dict` on it
- **Import sorting in uw_poller.py**: Fixed ruff I001 import block formatting
- **Type parameter style in yf.py**: Migrated from `TypeVar("T")` to PEP 695 type parameter syntax (`async def _dedupe[T](...)`)

### Added

- **Redis sink debug logging**: Successful Redis publishes now log at debug level with `redis_sink_published` event containing topic, message_id, and event_id for full traceability
- **Prometheus metrics for data pipeline**:
  - `gateway_envelopes_created_total{provider, feed}` - tracks EventEnvelope creation rate by provider and feed
  - `gateway_sink_publish_total{sink, topic, status}` - tracks data sink publish operations with success/error status
- **Publish deduplication gate**: Added Redis-based deduplication in `DataSinkRegistry.publish_all()` to prevent duplicate events in Heber Bronze layer
  - Checks `dedup:publish:{event_id}` before publishing; skips if already sent
  - 24h TTL on dedup keys; fail-open on cache errors
  - ~1-2ms latency per event for data integrity
- **Expanded FEED_MAPPING**: Added 21 new feed type mappings for UW endpoints
  - Market sentiment: `tide`, `market_tide`, `sector_tide`
  - Alternative data: `etf`, `holdings`, `flows`, `shorts`, `short_interest`, `ftd`, `screener`
  - Political/institutional: `insiders`, `institutions`, `politicians`
  - Analytics: `volatility`, `iv_rank`, `seasonality`, `max_pain`
- **Extended event ID unique fields**: Added feed-specific unique field extraction for etf, shorts, screener, market_tide, insiders, institutions, politicians, analytics feeds
- **Normalized schemas for alternative data**: Added three new schemas for UW alternative data feeds:
  - `NormalizedInsiderTrade` - SEC Form 4 insider trading data
  - `NormalizedInstitutionHolding` - 13F institutional holdings
  - `NormalizedPoliticianTrade` - Congressional trade disclosures
- **Normalized schemas for forex and fundamentals**:
  - `NormalizedForexRate` - Currency pair bid/ask/OHLC data
  - `NormalizedFundamentals` - Company financial metrics (PE, market cap, margins, etc.)

### Refactored

- **envelope.py `_extract_unique_fields`**: Converted from if/elif chain to mapping-based lookup with `FEED_UNIQUE_FIELDS` dict to reduce cognitive complexity

---

## [0.5.4] - 2026-01-29

### Fixed

- **WebSocket Connection Cleanup**: Aggressive connection cleanup on shutdown to prevent "connection limit exceeded" errors on restart
  - `UpstreamConnection.stop()`: Now sends explicit close frame with timeout, forces socket abort if stuck
  - `StreamMultiplexer.stop()`: Concurrent connection closure with 10s timeout for all streams
  - `lifespan`: Multiplexer shutdown now happens FIRST (before drain period) to release Alpaca connection slots immediately
  - Added detailed shutdown logging for debugging connection issues
- **Redis Docker Networking**: Fixed Redis connection errors in Docker by overriding `GATEWAY_CACHE_REDIS_URL`, `GATEWAY_DATA_SINK_REDIS_URL`, and `REDIS_URL` in `docker-compose.yml` to use container hostname (`redis://redis:6379/0`) instead of localhost

---

## [0.5.3] - 2026-01-21

### Added

- **Heber Data Sink Integration**: All Gateway data now publishes to Redis Streams for Heber lakehouse ingestion
  - Added `GATEWAY_DATA_SINK_ENABLED`, `GATEWAY_DATA_SINK_REDIS_URL`, `GATEWAY_DATA_SINK_MAX_STREAM_LEN` config
  - Enabled Redis service in `docker-compose.yml` with health checks
  - WebSocket stream data (bars, quotes, trades, news) publishes to `gateway.stream.*` topics
  - REST API responses publish to `gateway.rest.*` topics via `EventEnvelopeMiddleware`

---

## [0.5.2] - 2026-01-20

### Fixed

- **UW SDK StockEarningsTime enum**: Added missing `POSTMARKET` value to handle `"postmarket"` responses from Unusual Whales API that previously caused `ValueError`
- **UW Provider `_extract_data`**: Fixed `KeyError: 0` when handling single-object responses (e.g., `TickerInfo`, `MarketTide`) by checking `isinstance(data, list)` before iterating
- **SuccessResponse schema**: Fixed `ResponseValidationError` by changing `data` field from `dict` to `dict | list | None` to support paginated list responses and null responses
- **Catalog endpoints**: Removed `response_model=SuccessResponse` from catalog discovery endpoints (`/catalog/*`) which return custom discovery structures
- **IV Rank error message**: Enhanced 404 response to include context about possible causes (market hours, data availability, subscription tier)

### Added

- **Endpoint validation test suite**: Added `test_endpoint_validation.py` with 34 tests covering all API routes to catch schema mismatches early

### Changed

- **UW Provider logging**: Added debug logging to `_get_data_safe()` for empty response handling diagnostics

---

## [0.5.1] - 2026-01-19

### Added

- **API Catalog & Discovery**: Runtime API discovery via `/catalog/` endpoints
  - `GET /catalog/` — API summary and discovery entry point
  - `GET /catalog/streams` — WebSocket stream metadata (stocks, options, crypto, news)
  - `GET /catalog/streams/{id}` — Individual stream details with channels and examples
  - `GET /catalog/feeds` — Gateway feed name mappings (18 feed types)
  - `GET /catalog/providers` — REST API provider catalog (7 providers)
  - `GET /catalog/providers/{id}` — Individual provider endpoint listings
- **Extended WebSocket Feeds**: Added support for additional Alpaca channels
  - Stock: `dailyBars`, `updatedBars`, `lulds`, `statuses`, `imbalances`
  - Crypto: `dailyBars`, `updatedBars`, `orderbooks`
- **Documentation**: Created `API_REFERENCE.md` with comprehensive endpoint reference
- **Security Middleware**: `SecurityHeadersMiddleware` for security headers
- **Global Rate Limiting**: `GlobalRateLimitMiddleware` per PRD 7.5.1-2

### Changed

- **README.md**: Added API Discovery and WebSocket Streaming sections
- **Graceful Shutdown**: Extended shutdown with drain period per PRD 6.5/11.3.4
- **SIGHUP Handler**: Hot config reload support per PRD 6.5.4

---

## [0.5.0] - 2026-01-18

### Added

- **Alpaca Trading API via SDK**: Migrated from httpx to `alpaca-py` SDK
  - `GET /api/v1/alpaca/account` — Account information
  - `POST /api/v1/alpaca/orders` — Create orders (market, limit, stop, stop_limit)
  - `GET /api/v1/alpaca/orders` — List orders with filters
  - `GET /api/v1/alpaca/orders/{id}` — Get specific order
  - `GET /api/v1/alpaca/orders:by_client_order_id` — Get by client ID
  - `PATCH /api/v1/alpaca/orders/{id}` — Replace/modify order (NEW)
  - `DELETE /api/v1/alpaca/orders/{id}` — Cancel order
  - `DELETE /api/v1/alpaca/orders` — Cancel all orders
  - `GET /api/v1/alpaca/positions` — All open positions
  - `GET /api/v1/alpaca/positions/{symbol}` — Position for symbol
  - `DELETE /api/v1/alpaca/positions/{symbol}` — Close position
  - `DELETE /api/v1/alpaca/positions` — Close all positions
  - `GET /api/v1/alpaca/portfolio/history` — Portfolio history
  - `GET /api/v1/alpaca/assets` — Available assets
  - `GET /api/v1/alpaca/assets/{symbol}` — Asset info
  - `GET /api/v1/alpaca/clock` — Market clock
  - `GET /api/v1/alpaca/calendar` — Trading calendar
  - `GET /api/v1/alpaca/account/configurations` — Account config (NEW)
  - `PATCH /api/v1/alpaca/account/configurations` — Update config (NEW)
  - `GET /api/v1/alpaca/account/activities` — Account activities (NEW)
  - `GET /api/v1/alpaca/watchlists` — List watchlists (NEW)
  - `POST /api/v1/alpaca/watchlists` — Create watchlist (NEW)
  - `GET /api/v1/alpaca/watchlists/{id}` — Get watchlist (NEW)
  - `PUT /api/v1/alpaca/watchlists/{id}` — Update watchlist (NEW)
  - `DELETE /api/v1/alpaca/watchlists/{id}` — Delete watchlist (NEW)
  - `POST /api/v1/alpaca/watchlists/{id}/assets` — Add asset (NEW)
  - `DELETE /api/v1/alpaca/watchlists/{id}/assets/{symbol}` — Remove asset (NEW)
- **Market Data API Expansion**:
  - `GET /api/v1/alpaca/stocks/bars/latest` — Latest bars (NEW)
  - `GET /api/v1/alpaca/stocks/trades/latest` — Latest trades (NEW)
  - `GET /api/v1/alpaca/stocks/quotes` — Historical quotes (NEW)
  - `GET /api/v1/alpaca/stocks/snapshots` — Snapshots (NEW)
  - `GET /api/v1/alpaca/stocks/auctions` — Auctions (NEW)
  - `GET /api/v1/alpaca/options/trades` — Options trades (NEW)
  - `GET /api/v1/alpaca/options/trades/latest` — Latest trades (NEW)
  - `GET /api/v1/alpaca/options/snapshots/{underlying}` — Snapshots (NEW)
  - `GET /api/v1/alpaca/crypto/bars/latest` — Latest bars (NEW)
  - `GET /api/v1/alpaca/crypto/trades/latest` — Latest trades (NEW)
  - `GET /api/v1/alpaca/logos/{symbol}` — Company logo (NEW)
  - `GET /api/v1/alpaca/fixed-income/prices` — Fixed income prices (NEW)
- **Pydantic Response Models**: Added 60+ typed response schemas for OpenAPI documentation
  - Stock, Options, Crypto, Forex, News, Screener response types
  - Trading API response types (Account, Order, Position, etc.)
- **Unusual Whales API Full Coverage**: 106 endpoints (100% SDK parity)
  - Phase 1: News headlines, Politician people/trades/portfolios/holders
  - Phase 2: Economic/FDA/Market calendars, Market imbalances/options volume/insider trades/sector stats, Market tide by ETF
  - Phase 3: Institution list/activity/holdings/sectors/ownership/filings, Insider transactions/sector flow/ticker flow/insiders
  - Phase 4: Stock info/candles/state, OI per strike/expiry, Greeks/Greek exposure by strike-expiry, ATM options, Flow per strike intraday, Risk reversal skew, Spot exposures, Options volume, Greek flow by expiry, Sector tickers, Stock insider trades
  - Phase 5: ETF info/inflow-outflow/ticker-exposure/country-weights, Screener analysts, Alerts all/configuration
- **Paper/Live trading support**: Uses `APCA_API_BASE_URL` env var
- **New dependency**: `alpaca-py>=0.28`

---

## [0.4.0] - 2026-01-16

### Added

- **WebSocket Multiplexer**: Full upstream connection management for Alpaca streams
  - `StreamMultiplexer`: Manages all upstream WebSocket connections, routes messages to clients
  - `UpstreamConnection`: Single WebSocket connection with auth, subscribe, heartbeat, reconnection
  - `SubscriptionManager`: Tracks client subscriptions, computes aggregate upstream subscriptions
  - `AlpacaStreamType`: Enum for stocks (SIP/IEX), options, crypto, news streams
- **Dynamic subscribe/unsubscribe**: Clients can add/remove symbols in real-time
- **Subscription aggregation**: Multiple clients share single upstream connection per stream type
- **Reconnection with backoff**: Exponential backoff 1s→16s with ±20% jitter per PRD
- **Stream configuration**: `GATEWAY_STREAM_USE_IEX`, `GATEWAY_STREAM_RECONNECT_*` settings
- **Multiplexer dependency**: `get_multiplexer()`/`set_multiplexer()` for DI

### Changed

- `websocket.py`: Subscribe/unsubscribe handlers now wire to `StreamMultiplexer`
- `main.py`: Initializes `StreamMultiplexer` on startup if Alpaca credentials are set

---

## [0.3.0] - 2026-01-14

### Added

- **UnusualWhalesProvider**: Flow, darkpool, market tide, institutions, congress, insiders
- **UW API** (PRD-aligned `/api/v1/uw/*`):
  - `/uw/flow/all`, `/uw/flow/{symbol}`
  - `/uw/darkpool/all`, `/uw/darkpool/{symbol}`
  - `/uw/institutions/{symbol}`, `/uw/congress/{symbol}`, `/uw/insiders/{symbol}`
  - `/uw/market/tide`
- **Cursor pagination**: `next_cursor`, `has_more`, `total_count` per PRD
- **News API stub**: `/api/v1/news/*` returns 501 (EventRegistry pending)
- **Provider stubs**: AlphaVantageProvider, FinnhubProvider
- **Schemas**: `NormalizedFlowAlert`, `NormalizedDarkpoolTrade`, `NormalizedMarketTide`
- **Phase 1 completion**:
  - Per-client rate limits from `permissions.rate_limit`
  - WebSocket subscription limit via `permissions.ws_subscriptions_max`
  - `MessageRingBuffer` for WebSocket message history per symbol
  - `RequestDeduplicator` to coalesce identical in-flight requests
- **Phase 2 options endpoints**:
  - `GET /api/v1/alpaca/options/chain/{underlying}` - full option chain with greeks
  - `GET /api/v1/alpaca/options/chain/{underlying}/snapshot` - chain snapshot
  - `GET /api/v1/alpaca/options/{contract}/bars` - historical option bars
  - `GET /api/v1/alpaca/options/{contract}/quotes` - latest option quotes
- **NormalizedOptionContract** schema with greeks support
- **WebSocket heartbeat monitoring**: 30s timeout with auto-reconnect
- **YFinanceProvider**: Fundamentals, financials, history, options, recommendations
- **yfinance API** (10 endpoints at `/api/v1/yf/*`):
  - `/yf/ticker/{symbol}` - full ticker info
  - `/yf/ticker/{symbol}/info` - company info
  - `/yf/ticker/{symbol}/financials` - income, balance, cash flow
  - `/yf/ticker/{symbol}/earnings` - quarterly/annual earnings
  - `/yf/ticker/{symbol}/history` - historical OHLCV
  - `/yf/ticker/{symbol}/options` - option expirations
  - `/yf/ticker/{symbol}/options/{exp}` - option chain
  - `/yf/ticker/{symbol}/recommendations` - analyst recs
  - `/yf/ticker/{symbol}/holders` - institutional/insider
  - `/yf/ticker/{symbol}/calendar` - earnings/dividend calendar
- **SECProvider**: Filings, 13F, insider trades via data.sec.gov (free API)
- **SEC API** (7 endpoints at `/api/v1/sec/*`):
  - `/sec/company/{cik}` - company info by CIK
  - `/sec/company/ticker/{ticker}` - CIK lookup by ticker
  - `/sec/filings/{cik}` - all filings
  - `/sec/filings/{cik}/{form_type}` - filings by type
  - `/sec/13f/{cik}` - 13F institutional holdings
  - `/sec/insiders/{cik}` - insider trades (Form 3/4/5)
  - `/sec/facts/{cik}` - XBRL company facts
- **Phase 2 completion - Crypto REST** (4 endpoints at `/api/v1/alpaca/crypto/*`):
  - `GET /alpaca/crypto/{pair}/bars` - historical crypto bars
  - `GET /alpaca/crypto/{pair}/trades` - historical crypto trades
  - `GET /alpaca/crypto/{pair}/quotes` - latest crypto quote
  - `GET /alpaca/crypto/{pair}/snapshot` - current snapshot
- **Phase 2 completion - Forex REST** (2 endpoints at `/api/v1/alpaca/forex/*`):
  - `GET /alpaca/forex/rates` - latest FX rates
  - `GET /alpaca/forex/rates/historical` - historical FX rates
- **Phase 2 WebSocket stream handlers**:
  - `AlpacaOptionsStreamHandler` - options WS with heartbeat monitoring
  - `AlpacaCryptoStreamHandler` - crypto WS with heartbeat monitoring
  - `AlpacaNewsStreamHandler` - news WS with heartbeat monitoring

## 2026-03-10

- feat: add a minute-based Alpaca option capture service for `SPY`, `QQQ`, and `IWM`
- feat: publish authoritative `alpaca/option_chain_snapshot` envelopes to the Heber sink while keeping option tape subscriptions in sync through the existing multiplexer
- feat: add `option_capture_*` settings and a full-snapshot Alpaca provider helper for normalized option contracts
- ops: enable the option capture service in the default Docker Compose gateway stack
- test: cover market-hours gating, per-symbol snapshot publishing, partial-failure handling, websocket subscription reconciliation, and full-snapshot normalization

## [0.2.0] - 2026-01-14

### Added

- **Provider framework**: `DataProvider` base class with capabilities and lifecycle hooks
- **Provider registry**: Dynamic provider loading from `config/providers.yaml`
- **AlpacaProvider**: Full REST API support for bars, quotes, trades
- **AlpacaStreamHandler**: WebSocket streaming with reconnection logic
- **REST API**: Alpaca endpoints at `/api/v1/alpaca/stocks/*` (PRD-aligned)
- **SubscriptionManager**: Reference counting with 30s grace period
- **KeyLoadBalancer**: Round-robin key selection with health tracking
- **RateLimitMiddleware**: `X-RateLimit-*` headers per PRD spec
- **CacheMiddleware**: `X-Gateway-Cache` headers with HIT/MISS tracking
- **REST authentication**: `X-Gateway-Key` header requirement
- **WebSocket heartbeat**: 30s interval, disconnect after 3 missed (PRD-aligned)
- **Message format**: `provider`, `feed`, `error_code` fields (PRD-aligned)
- **Admin endpoints**: `/api/v1/status`, `/admin/logs/recent`, `/admin/errors/summary`
- **CLI tool**: `python -m gateway.cli` for key management (generate, rotate, list)
- **Key hashing**: SHA-256 hashed keys for production (`key_hash` field)
- **Schema fields**: Added `timeframe` to Bar, `trade_id` to Trade
- **Test suite**: 34 tests covering all core components

## [0.1.0] - 2026-01-14

### Added

- **Project scaffolding**: FastAPI application with uvicorn
- **Docker support**: Multi-stage Dockerfile with non-root user
- **Configuration**: pydantic-settings with environment variable loading
- **Client authentication**: YAML-based client keys with permissions
- **In-memory cache**: TTLCache with hit/miss statistics
- **Connection manager**: WebSocket connection tracking
- **Health endpoints**: `/health`, `/health/ready`, `/health/status`
- **WebSocket endpoint**: `/ws` with auth handshake and timeout
- **Structured logging**: structlog with JSON output
- **Test suite**: pytest fixtures and unit tests for core components

## 2026-02-21

- chore: workspace sync checkpoint and gitignore audit (2026-02-21)
