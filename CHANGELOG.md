# Changelog

All notable changes to this project will be documented in this file.

## [0.5.73] - 2026-02-07

### Added

- **List pagination regression tests**: Added `tests/test_list_pagination.py` to validate optional `limit`/`offset` behavior for bulk job listing and replay session listing endpoints.

### Changed

- **Bulk list pagination controls**: Updated `gateway/api/bulk.py` `list_jobs(...)` to support optional `limit` and `offset` query parameters after status filtering.
- **Replay list pagination controls**: Updated `gateway/api/replay.py` `list_sessions(...)` to support optional `limit` and `offset` query parameters.
- **Non-provider audit tracking updates**: Updated `PERFORMANCE_AUDIT_NON_PROVIDER_ROUTERS_DEEP_DIVE.md` to mark list-endpoint pagination remediation complete.
- **Top-level audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` non-provider follow-up notes to reflect list-pagination completion.

## [0.5.72] - 2026-02-07

### Added

- **Metrics refresh throttling tests**: Expanded `tests/test_metrics.py` with coverage for interval-throttled memory metric refresh, force-refresh bypass, and metrics endpoint integration with the throttled updater.

### Changed

- **Throttled memory metric updater**: Added `update_memory_metrics_if_due(...)` in `gateway/core/metrics.py` with a default 10-second refresh interval to avoid recomputing process memory probes on every scrape.
- **Metrics route refresh optimization**: Updated `gateway/api/metrics.py` to call `update_memory_metrics_if_due()` instead of unconditional `update_memory_metrics()` before `generate_latest()`.
- **Non-provider audit tracking updates**: Updated `PERFORMANCE_AUDIT_NON_PROVIDER_ROUTERS_DEEP_DIVE.md` to mark metrics refresh throttling remediated.
- **Top-level audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` non-provider follow-up notes to reflect metrics refresh throttling completion.

## [0.5.71] - 2026-02-07

### Added

- **Replay WebSocket control-loop tests**: Expanded `tests/test_replay.py` with coverage for replay WebSocket control action handling (`pause`, `resume`, `seek`, `stop`) and disconnect-driven stop behavior.

### Changed

- **Replay WebSocket control-task flow**: Updated `gateway/api/replay.py` to replace timeout-based `wait_for(receive_json, timeout=1.0)` polling with a dedicated control-receive task plus task-completion coordination.
- **Replay control helper extraction**: Added `_apply_replay_ws_action(...)` and `_receive_replay_control_messages(...)` in `gateway/api/replay.py` to centralize control semantics and reduce per-iteration overhead.
- **Non-provider audit tracking updates**: Updated `PERFORMANCE_AUDIT_NON_PROVIDER_ROUTERS_DEEP_DIVE.md` to mark replay control-loop optimization remediated.
- **Top-level audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` non-provider follow-up notes to reflect replay control-loop completion.

## [0.5.70] - 2026-02-07

### Added

- **Fetcher guard regression tests**: Added `tests/test_router_fetcher_guards.py` to verify bulk bars/options, calendar earnings, and corporate fetcher bindings do not rebind when fetchers are already configured.

### Changed

- **Bulk fetcher rebinding guards**: Updated `gateway/api/bulk.py` to call `set_bars_fetcher(...)` / `set_options_fetcher(...)` only when manager fetchers are missing.
- **Calendar fetcher rebinding guard**: Updated `gateway/api/calendar.py` earnings route to set provider fetcher only when no fetcher is configured.
- **Corporate fetcher rebinding guard**: Updated `gateway/api/corporate.py` fetcher configuration helper to avoid resetting existing fetchers each request.
- **Non-provider audit tracking updates**: Updated `PERFORMANCE_AUDIT_NON_PROVIDER_ROUTERS_DEEP_DIVE.md` to mark request-time singleton fetcher mutation remediation complete.
- **Top-level audit plan updates**: Updated `PERFORMANCE_AUDIT.md` non-provider follow-up notes to reflect completed fetcher-guard work.

## [0.5.69] - 2026-02-07

### Added

- **News router cache-first tests**: Added `tests/test_news_router.py` to validate cache-hit short-circuit behavior (including invalid date inputs) and cache-miss provider fetch/date parsing flow.

### Changed

- **News route cache-first parse ordering**: Updated `gateway/api/news.py` `get_articles(...)` to run cache lookup before symbol/date parsing so cache hits skip unnecessary parsing work.
- **Non-provider audit tracking updates**: Updated `PERFORMANCE_AUDIT_NON_PROVIDER_ROUTERS_DEEP_DIVE.md` to mark news cache-hit-first parsing remediation complete.
- **Top-level audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` next-run plan notes to reflect news cache-hit-first parsing completion.

## [0.5.68] - 2026-02-07

### Added

- **Envelope middleware fast-path tests**: Expanded `tests/test_middleware_streaming.py` with coverage for pre-wrapped response short-circuiting and direct `response.body` reuse fallback behavior.

### Changed

- **Envelope cache-hit short-circuit**: Updated `gateway/api/middleware.py` `EventEnvelopeMiddleware` to skip body parse/dump work when upstream responses already carry `X-Gateway-Envelope: true`.
- **Envelope body-byte reuse optimization**: Updated `gateway/api/middleware.py` `_get_response_body(...)` to use existing `response.body` bytes when available before consuming `body_iterator`.
- **Envelope compact JSON serialization**: Updated wrapped-response encoding to use compact separators (`(",", ":")`) for lower serialization overhead and payload size.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark middleware cache/envelope buffering work substantially remediated and narrow remaining scope.

## [0.5.67] - 2026-02-07

### Added

- **Replay spool cleanup tests**: Expanded `tests/test_replay.py` with coverage for large list-message spooling and temp-file cleanup behavior.

### Changed

- **Replay large-list spool guardrail**: Updated `gateway/core/replay.py` list-loader iteration to spool oversized message lists to temp JSONL and stream replay messages back from disk.
- **Replay spool settings surface**: Added `replay_messages_max_in_memory` and `replay_messages_spool_to_disk` in `gateway/config.py`.
- **Replay file cleanup hardening**: Temp replay spool files are now removed after streaming iteration completion/failure.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark replay spill-to-disk follow-up substantially remediated.

## [0.5.66] - 2026-02-07

### Added

- **Bulk streaming JSON test coverage**: Expanded `tests/test_bulk_manager.py` with `iter_results_json_chunks(...)` output-equivalence coverage.

### Changed

- **Bulk JSON download streaming**: Updated `gateway/api/bulk.py` JSON download path to return `StreamingResponse` backed by chunked JSON iteration instead of materializing full payload strings.
- **Bulk manager chunked JSON iterator**: Added `iter_results_json_chunks(...)` in `gateway/core/bulk.py` for bounded `{\"data\":[...]}` payload generation across in-memory/spooled result sources.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark streaming JSON-array download follow-up complete.

## [0.5.65] - 2026-02-07

### Added

- **Rate limiter blocking-mode tests**: Added `tests/test_rate_limiter.py` covering retry-after-guided blocking waits and timeout throttling behavior.

### Changed

- **Retry-after aligned blocking waits**: Updated `gateway/core/rate_limiter.py` blocking `acquire(...)` flow to sleep using limiter-provided `retry_after` hints, bounded by remaining `max_wait`, instead of generic exponential backoff.
- **Core-infra audit tracking updates**: Updated `PERFORMANCE_AUDIT_CORE_INFRA_DEEP_DIVE.md` to mark rate limiter blocking-wait remediation complete.

## [0.5.64] - 2026-02-07

### Added

- **UW poller settings coverage test**: Expanded `tests/test_uw_poller.py` to verify poller publish concurrency reads from runtime settings.

### Changed

- **Configurable UW poller publish concurrency**: Added `uw_poller_publish_max_inflight` in `gateway/config.py` and wired `gateway/core/uw_poller.py` to use it for bounded publish fanout limits.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to record UW poller publish concurrency as runtime configurable.

## [0.5.63] - 2026-02-07

### Added

- **Request deduplication tests**: Added `tests/test_request_dedup.py` covering same-key request coalescing, independent different-key fetch execution, and stable lock-striping behavior.

### Changed

- **Request deduplicator lock striping**: Updated `gateway/core/dedup.py` `RequestDeduplicator` to use key-based lock striping instead of a single global lock, reducing unrelated-key contention under concurrent load.
- **Request dedup pending cleanup safety**: Pending request cleanup now removes only the matching in-flight future for a key (`is future`) to avoid edge-case key reuse races.
- **Core-infra audit tracking updates**: Updated `PERFORMANCE_AUDIT_CORE_INFRA_DEEP_DIVE.md` to mark request deduplicator lock-striping remediation complete.

## [0.5.62] - 2026-02-07

### Added

- **Metrics helper tests**: Added `tests/test_metrics.py` for path normalization placeholder behavior and bounded cache-size enforcement.

### Changed

- **Bounded metrics path-normalization cache**: Updated `gateway/core/metrics.py` `_normalize_path(...)` to use a bounded in-process cache for repeated paths, reducing repeated string parsing on hot request paths.
- **Metrics cache guardrail**: Added cache cap/reset behavior to prevent unbounded normalization cache growth.
- **Core-infra audit tracking updates**: Updated `PERFORMANCE_AUDIT_CORE_INFRA_DEEP_DIVE.md` to mark metrics path-normalization memoization remediation complete.

## [0.5.61] - 2026-02-07

### Added

- **Auth log-level regression test**: Expanded `tests/test_auth.py` with coverage that successful authentication uses debug-level logging and avoids info-level hot-path log emission.

### Changed

- **Auth success log-volume reduction**: Updated `gateway/core/auth.py` to emit `auth_success` at debug level instead of info while preserving failure-path warning logs.
- **Core-infra audit tracking updates**: Updated `PERFORMANCE_AUDIT_CORE_INFRA_DEEP_DIVE.md` to mark auth success log-volume remediation complete.

## [0.5.60] - 2026-02-07

### Added

- **UW poller publish-path tests**: Added `tests/test_uw_poller.py` covering dedupe behavior (in-memory + Redis-hit) and bounded publish concurrency enforcement.

### Changed

- **UW poller batched dedupe reads/writes**: Updated `gateway/core/uw_poller.py` to batch Redis dedupe lookups and dedupe key writes through shared poller publish flow.
- **UW poller bounded publish fanout**: Updated `gateway/core/uw_poller.py` to publish envelopes with semaphore-bounded concurrency (`_publish_max_inflight`) instead of strict per-event sequential publishes.
- **UW poller loop consolidation**: Refactored flow/darkpool/market-tide/sector-tide publish paths to use shared `_publish_envelopes(...)` logic while retaining out-of-order telemetry and duplicate counters.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark UW poller sequential dedupe/publish remediation complete.

## [0.5.59] - 2026-02-07

### Added

- **Bulk spool coverage tests**: Expanded `tests/test_bulk_manager.py` with spool-to-disk result coverage, spool-aware JSONL iteration checks, and expired-job spool file cleanup assertions.

### Changed

- **Bulk memory guardrail via spool fallback**: Updated `gateway/core/bulk.py` to spill oversized job results to temp JSONL storage once in-memory retention crosses `bulk_results_max_in_memory`.
- **Spool-aware bulk result iteration**: Updated bulk result stream/JSONL helpers in `gateway/core/bulk.py` to iterate transparently across in-memory and spooled records.
- **Bulk JSON download compatibility for spooled jobs**: Updated `gateway/api/bulk.py` JSON download path to use manager-level spool-aware result retrieval.
- **Bulk settings surface**: Added `bulk_results_max_in_memory` and `bulk_results_spool_to_disk` settings in `gateway/config.py`.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark bulk in-memory retention follow-up substantially remediated with spill guardrails.

## [0.5.58] - 2026-02-07

### Added

- **Finnhub quote fan-out tests**: Expanded `tests/test_finnhub_provider.py` with bounded-concurrency and fail-soft batch behavior coverage for `get_quotes(...)`.

### Changed

- **Finnhub bounded quote fan-out**: Updated `gateway/providers/finnhub.py` `get_quotes(...)` to fetch symbols with semaphore-bounded concurrency (`_quotes_max_concurrency`) instead of strict sequential awaits.
- **Finnhub quote concurrency config support**: Added optional `quotes_max_concurrency` provider config parsing in `initialize(...)` with safe integer fallback handling.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark provider multi-quote fan-out serialization remediation complete.

## [0.5.57] - 2026-02-07

### Added

- **Stream sink registry wiring tests**: Updated `tests/test_main_stream_sink.py` to cover stable sink-registry reference usage in `_on_stream_data(...)` with reset-state fixture support.

### Changed

- **Stream callback sink-registry fast path**: Updated `gateway/main.py` to store sink registry once during lifespan (`_set_stream_sink_registry(...)`) and use the stable reference in `_on_stream_data(...)`, removing per-event dependency getter lookups.
- **Lifespan sink registry lifecycle handling**: Sink registry reference is now explicitly reset on startup/shutdown to avoid stale references across app lifecycle transitions.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark stream callback registry lookup remediation complete.

## [0.5.56] - 2026-02-07

### Added

- **Finnhub provider bar-mapping tests**: Added `tests/test_finnhub_provider.py` with coverage for zipped bar-row mapping and mismatched source-array handling in `get_bars(...)`.

### Changed

- **Finnhub bar normalization loop cleanup**: Updated `gateway/providers/finnhub.py` `get_bars(...)` to use `zip(timestamps, opens, highs, lows, closes, volumes, strict=False)` instead of index-based list access.
- **Finnhub mismatch resilience**: Bar normalization now naturally truncates to the shortest complete row set, avoiding index errors when upstream arrays differ in length.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark Finnhub bar loop remediation complete.

## [0.5.55] - 2026-02-07

### Added

- **Envelope fast-path regression tests**: Expanded `tests/test_envelope.py` with coverage for JSON-ready timestamp/lineage fields and Pydantic input payload compatibility on the optimized `wrap_event(...)` path.

### Changed

- **Envelope fast-path serialization**: Updated `gateway/core/envelope.py` `wrap_event(...)` to assemble JSON-ready envelope dicts directly instead of constructing/dumping a Pydantic model per event.
- **Envelope hot-path parity preservation**: Preserved schema fields (`schema_version`, `lineage`, `quality_flags`, payload pass-through), metrics emission, and fallback envelope behavior while reducing stream-path serialization overhead.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark envelope fast-path serialization remediation complete.

## [0.5.54] - 2026-02-07

### Added

- **Replay loader compatibility tests**: Expanded `tests/test_replay.py` with coverage for out-of-order list sorting and async-iterable loader replay behavior.

### Changed

- **Replay iterable ingestion support**: Updated `gateway/core/replay.py` so replay loaders can return async/sync iterables in addition to lists, enabling streamed replay ingestion without mandatory list materialization.
- **Replay sort-on-demand optimization**: Updated `gateway/core/replay.py` list handling to sort replay messages only when timestamps are out of order, skipping unnecessary full-list sorts for already ordered data.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark replay preload/sort item partially remediated and clarify remaining large-window storage follow-up.

## [0.5.53] - 2026-02-07

### Added

- **Cache optimization regression tests**: Expanded `tests/test_cache.py` with coverage for deferred custom-TTL prune cadence and exact overflow-eviction counting behavior in max-size enforcement.

### Changed

- **Custom-TTL prune cadence optimization**: Updated `gateway/core/cache.py` to prune expired custom-TTL entries on configurable set-interval cadence (`CUSTOM_PRUNE_SET_INTERVAL`) instead of scanning custom cache entries on every custom set.
- **Overflow-count max-size enforcement**: Updated `gateway/core/cache.py` max-size enforcement to compute overflow once and evict bounded counts from custom/default tiers, reducing repeated loop/size checks under burst inserts.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark cache prune and max-size enforcement optimizations completed.

## [0.5.52] - 2026-02-07

### Added

- **WebSocket hot-path settings regression test**: Added `tests/test_websocket.py` coverage to ensure `_message_loop(...)` uses injected `max_message_size` without fallback `get_settings()` lookups when the size is provided.

### Changed

- **WebSocket max-size lookup hoist**: Updated `gateway/api/websocket.py` so `websocket_endpoint(...)` passes `settings.ws_max_message_size` into `_message_loop(...)`, and `_message_loop(...)` resolves max-bytes once before the receive loop instead of re-reading settings per frame branch.
- **Stream callback dependency lookup trim**: Updated `gateway/main.py` to import/use `get_sink_registry` at module scope, removing per-event local import overhead in `_on_stream_data(...)`.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark websocket max-size lookup remediation complete and stream callback dependency lookup import overhead partially remediated.

## [0.5.51] - 2026-02-07

### Added

- **Bulk JSONL chunking tests**: Added `tests/test_bulk_manager.py` to validate bounded JSONL chunk iteration behavior and output equivalence with legacy JSONL formatting.

### Changed

- **Bulk download JSONL streaming**: Updated `gateway/api/bulk.py` to serve JSONL job downloads via `StreamingResponse` backed by chunked iteration instead of materializing one large response string first.
- **Bulk manager bounded JSONL iteration**: Added `iter_results_jsonl_chunks(...)` in `gateway/core/bulk.py` and refactored `get_results_jsonl(...)` to reuse chunk assembly, removing the large intermediate list-of-lines allocation.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark bulk JSONL download materialization remediation complete and narrow remaining bulk memory scope to in-memory job result retention strategy.

## [0.5.50] - 2026-02-07

### Added

- **Stream fanout/sink tuning coverage tests**: Added and expanded tests in `tests/test_main_stream_sink.py` and `tests/test_multiplexer.py` for stream sink dispatch limit clamping and `StreamMultiplexer` fanout batch/inflight config behavior.

### Changed

- **Configurable stream fanout tuning**: Updated `gateway/core/stream.py` `StreamMultiplexer` to accept `fanout_max_inflight` and `fanout_batch_size` with safe clamping and default constants.
- **Configurable stream sink dispatch tuning**: Updated `gateway/main.py` stream sink scheduling to use runtime-configured limits (`_configure_stream_sink_dispatch_limits`) instead of fixed constants.
- **Settings surface for stream tuning**: Added `stream_fanout_max_inflight`, `stream_fanout_batch_size`, `data_sink_stream_publish_max_inflight`, and `data_sink_stream_publish_max_pending` to `gateway/config.py`.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to reflect stream tuning knobs rollout and narrow remaining stream work to telemetry-driven parameter calibration.

## [0.5.49] - 2026-02-07

### Added

- **Stream fanout batching perf coverage**: Expanded `tests/perf/test_perf_stream_sink.py` with a high-semaphore fanout test that verifies `StreamMultiplexer` client callback concurrency remains bounded by batch size.

### Changed

- **Stream fanout task-burst reduction**: Updated `gateway/core/stream.py` to fan out clients in bounded batches (`_iter_client_batches(...)`) instead of creating one awaitable per client in a single `gather(...)` call.
- **Fanout defaults centralized**: Added `DEFAULT_FANOUT_MAX_INFLIGHT` and `DEFAULT_FANOUT_BATCH_SIZE` constants in `gateway/core/stream.py` and wired `StreamMultiplexer` to use them for bounded fanout behavior.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark stream fanout task-burst remediation complete and shift remaining stream work to tuning batch/inflight values with telemetry.

## [0.5.48] - 2026-02-07

### Added

- **Stream sink dispatch coverage tests**: Added `tests/test_main_stream_sink.py` covering non-blocking `_on_stream_data(...)` sink scheduling behavior and pending-task backpressure drop behavior.

### Changed

- **Stream callback sink decoupling**: Updated `gateway/main.py` so `_on_stream_data(...)` schedules sink publishes off-path instead of awaiting `publish_all(...)`, reducing client-stream callback latency coupling to sink/dedup I/O.
- **Bounded sink publish scheduling guardrail**: Added bounded stream sink scheduling controls in `gateway/main.py` (`STREAM_SINK_MAX_INFLIGHT_PUBLISH`, `STREAM_SINK_MAX_PENDING_TASKS`) with warning logs on pending-queue drops.
- **Stream sink shutdown drain**: Added `_drain_stream_sink_publish_tasks(...)` in `gateway/main.py` and invoked it during lifespan shutdown to flush/cancel outstanding stream sink publish tasks before sink shutdown.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark stream callback/sink decoupling remediation complete and narrow remaining stream performance scope to fanout batching/task burst reduction.

## [0.5.47] - 2026-02-07

### Added

- **Middleware body-reuse coverage tests**: Expanded `tests/test_middleware_streaming.py` with cache+envelope MISS/HIT integration coverage and direct `_get_response_body(...)` state-reuse/fallback tests.

### Changed

- **Cache/Envelope duplicate-buffering reduction**: Updated `gateway/api/middleware.py` so `CacheMiddleware` stores pre-buffered response bytes on request state and `EventEnvelopeMiddleware` reuses them via `_get_response_body(...)` before falling back to body iteration.
- **Envelope middleware body-read helper**: Added `EventEnvelopeMiddleware._get_response_body(...)` to centralize response-body extraction and preserve existing response contracts while reducing repeated body assembly on cache-mediated paths.
- **Audit tracking updates**: Updated `PERFORMANCE_AUDIT.md` to mark middleware cache/envelope body-reuse remediation complete and narrow remaining middleware performance scope.

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
