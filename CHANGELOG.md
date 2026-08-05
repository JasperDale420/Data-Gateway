# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- **The OPRA option-quote WebSocket leg is off by default** (`docker-compose.yml`): `option_capture` subscribed `ws_contract_limit_per_symbol` (40) × SPY/QQQ/IWM = 120 contracts, ranked nearest-expiry at-the-money — i.e. the highest-tick-rate contracts in all of OPRA. Measured at ~475 events/second, that was **~8.69M quote + ~4.4k option-trade events/day: 99.2% of everything reaching the Redis sink**, 229GB accumulated in `silver/feed=quotes/instrument_type=option`, and the bulk of the 11.5M `cache:dedup:publish:*` keys whose memory pressure caused the 2026-08-04 eviction outage. Nothing reads that Silver partition — the live consumers (Kairos's exit monitor, options-bot) take option quotes over the WebSocket and REST respectively, never from storage. Kairos is unaffected by this change: it subscribes its own position contracts and the multiplexer refcounts subscriptions per client, so dropping `__option_capture__`'s subscriptions cannot tear down a contract another client holds. The 300-second REST chain-snapshot leg (the actual Heber-replay feature, 234 events/day) is unchanged. Re-enable with `GATEWAY_OPTION_CAPTURE_WS_ENABLED=true`.

### Fixed

- A tripped data-sink circuit breaker can no longer stay open forever. Producers pre-check the breaker before enqueueing, but the open-to-half-open probe only ever ran inside `breaker.call()` — with nothing enqueued, no probe could run, so one Redis bounce killed all publishing until a manual restart (2026-08-05: 3+ hours of lost flow during market hours). Once the recovery window elapses, events are admitted again so the worker can probe; batch publish APIs now also run through the breaker, an all-failed batch counts as a sink failure and re-buffers the admitted batch, and volatile messages can never bypass single-probe admission while a probe is in flight. Durable outbox admission (flow writer/watch) intentionally remains outside the volatile breaker's scope.
- **Redis sink headroom raised 4GB → 6GB after a live ingestion outage** (`docker-compose.yml`): on 2026-08-04 the sink Redis sat at 3.47G of its 4G cap for roughly 50 minutes during market hours. With `volatile-lru` the two event streams carry no TTL and cannot be evicted, so once the TTL'd keys were exhausted `XADD` stalled, the `data_sink:redis_streams` circuit breaker opened, and the gateway's 50,000-entry failover buffer overflowed — **2,762,092 events evicted**, 99.2% of them `websocket:quotes`. Redis also restarted twice under AOF-fsync stalls. The streams accounted for only 1.51G; the other ~2G was **11.5 million `cache:dedup:publish:*` keys, one per published event**, a population that scales with event volume rather than with the stream caps and was never budgeted for. Stream caps are unchanged (500K live / 2M backfill) — this is headroom only. Note the circuit breaker did **not** self-heal once Redis recovered: it kept re-opening on a 60s probe with no error logged (it buffers rather than attempting while open), and required a gateway restart to reset. That stale-breaker behaviour is a separate bug and is tracked in `docs/FOLLOW_UPS.md`.

### Added

- **Durable transport startup and lane isolation**: durable-selected JetStream streams are now connected, authenticated, provisioned, and contract-verified before Gateway starts accepting events. Existing broker stream contracts are inspected rather than rewritten. Flow alerts from both pollers and REST atomically admit their writer and watch copies to SQLite; live, backfill, and watch drain independently, so a stalled historical or watch lane cannot block authoritative live delivery. Health and Prometheus now distinguish durable admission from later broker-delivery degradation, while ambiguous PubAcks retain their SQLite rows for retry.
- **Durable transport backlog visibility and throughput**: `/health/status` now reports pending durable events, oldest-event age, retained publish failures, broker connection state, and selected live/backfill lane health. A broker outage is explicitly `degraded_durable_buffering`: events remain safely admitted while delivery waits to resume. Prometheus alerts on one- and five-minute oldest-event age plus a sustained broker disconnect. Concurrent single-event producers now use a bounded fully synchronous SQLite group commit (at most 256 events or 5 ms), preserving per-event admission acknowledgement while sustaining the observed local 1 KiB admission benchmark above 7,500 events/second.
- **Lane-specific durable transport health**: live, backfill, and flow-watch queues now expose independent admission, delivery, pending-count, oldest-age, and publish-failure state. Prometheus backlog and delivery metrics and alerts carry the lane label, so a stalled watch copy is visible without marking its live writer unhealthy (and vice versa); the shared disk-capacity budget remains aggregate.
- **Bounded durable admission and delivery**: the asynchronous SQLite group-commit queue and every SQLite admission transaction now have explicit event-count and byte limits, and a terminal SQLite failure rejects every queued producer instead of leaving later calls waiting. Direct oversized batch calls fail before any write; the routed production path deterministically splits valid backfill and flow batches, keeping each flow writer/watch pair in one transaction. Broker publication uses at most 32 concurrent PubAck requests; an unacknowledged row remains durable, immediately degrades only its own lane, and cannot be masked by a successful concurrent acknowledgement.
- **Durable Heber event transport** (`gateway/core/{durable_outbox,durable_outbox_sink,jetstream}.py`, `gateway/main.py`, `docker-compose.yml`): the opt-in path commits each selected lane to a FULL-synchronous SQLite outbox before producer success, drains it in order only after a matching JetStream PubAck, and never evicts at capacity. Backfill is the safe first canary while live traffic remains on Redis; each event is routed to exactly one transport, and live can be promoted separately. File-backed live and backfill work queues are isolated, bounded, authenticated, and host-persistent. Outbox pressure now sheds low-priority work at 70%, alerts at 60%/90%, and fails closed at the configured disk budget. Backfill envelopes carry exact event and canonical-payload proofs; Heber must atomically write the matching small Redis control-plane acknowledgement after its filesystem commit before Gateway can mark a job verified.
- **Durable, automatically verified market-data replay manifests** (`gateway/core/backfill.py`, `gateway/core/backfill_manifest.py`): backfills now persist non-expiring Redis manifests and per-chunk scope, coverage, record-count, stable-identity, payload-digest, and publication evidence. Gateway publication is distinct from downstream ingestion: a job becomes `verified` only when every chunk has an exact, timezone-aware Heber post-commit acknowledgement; missing, malformed, partial, unhealthy, rate-limited, out-of-range, or changed-identity evidence fails closed as `partial`/`failed`/`unrecoverable` and is automatically reconciled after late acknowledgements or restart. No provider feed is publish-enabled until it can prove complete pagination/coverage; current UW `flow_alerts`, IV term structure, short interest/volume, FTD, congressional, insider, and other snapshot-only/unproven sources are explicitly blocked rather than replaying present-day snapshots as history. A one-symbol/one-completed-market-day canary constraint, structured boundary logs, and a deduplicated `KairosReplayIntakeBlocked` Prometheus alert expose the gate without any manual verification step.
- **Unbounded backfill-history deletion is disabled**: `DELETE /api/v1/backfill` now preserves all durable manifests and acknowledgements and reports `purged=0`; retention requires a future bounded policy that cannot remove active or non-verified evidence.
- **WS clients can subscribe with the canonical feed names `bars`/`quotes`/`trades`** — the wire envelope labels are now accepted on the WebSocket subscribe surface as aliases of `stock_bars`/`stock_quotes`/`stock_trades`, which stay fully supported. Either spelling works for subscribe, unsubscribe, and client permission lists (a client authorized for `stock_bars` may subscribe via `bars` and vice versa); acks and subscription tracking always report the canonical `stock_*` name. `/catalog/feeds` advertises both spellings (`alias_of` marks the aliases), and unknown feed names are still rejected. Wire envelopes published to Redis are unchanged.

### Fixed

- **JetStream settings tests no longer read the developer's `.env`** (`tests/test_jetstream.py`): `gateway.main` calls `load_dotenv()` at import time and `tests/conftest.py` imports it, so every `GATEWAY_*` value from a local `.env` was already in the environment before the tests ran — `_env_file=None` only suppresses pydantic-settings' own dotenv read, not the environment. The "disabled by default" test was therefore asserting local configuration instead of the declared defaults, and its Redis-control-plane-URL case only reached that error because a local `GATEWAY_DATA_SINK_ENABLED=true` satisfied the data-sink precondition; on a clean machine or in CI it failed on "JetStream and durable outbox require the data sink to be enabled". The module now strips `GATEWAY_*` from the environment for each test, and the Redis-URL case enables the data sink explicitly so it exercises the validator rule it names. No production behavior change.

- **Graceful shutdown can now actually finish — deploys and restarts no longer discard queued events** (`docker-compose.yml`): the documented 8-step shutdown sleeps for `GATEWAY_SHUTDOWN_DRAIN_SECONDS` (default 30) at step 3, *before* step 4 stops the producers and step 7 drains and closes the sink. The gateway service set no `stop_grace_period`, so Docker's 10s default SIGTERM→SIGKILL window expired inside that sleep and steps 4–8 never ran: every `make deploy`, `docker compose restart`, and `docker restart` hard-killed the process mid-drain and dropped whatever was still queued for `heber:events`. The service now declares `stop_grace_period: 60s`, which covers the drain plus producer shutdown and sink drain.
- **CI's vendored schema stub now carries the IV-rank provenance fields** (`ci/empire_schemas/empire_schemas/analytics.py`): CI installs `ci/empire_schemas/`, not the monorepo's `../empire-schemas`, so the stub is a second copy of the contract that has to move in lockstep. `NormalizedIVRank` gained `date` and `updated_at` in the shared package when IV-rank responses started retaining Unusual Whales' official source date, but the stub was never updated — `test_uw_iv_rank` and `test_get_iv_rank_parses_raw_http_payload_when_sdk_shape_is_incompatible` therefore passed locally and would have failed in CI. The stub now mirrors the shared model exactly (`date: date | None`, `updated_at: AwareDatetime | None`).
- **The `mypy` gate passes again on the UW market provider** (`gateway/providers/uw/market.py`): the historic-price loop narrowed its optional price cell with `price in (None, "")`, which mypy does not treat as a type guard, so `float(price)` was called on `Any | None` and the whole-repo and `gateway/`-only gates both reported one error. The check is now `price is None or price == ""` — identical filtering, but mypy narrows it. No behavior change.
- **`mypy .` (the documented `make typecheck` gate) now actually passes** (`pyproject.toml`): the vendored SDK copies under `vendor/` and `ci/` contain a duplicate `unusualwhales` package that aborted every whole-repo mypy run with a duplicate-module error, so the Makefile `typecheck` target and the CLAUDE.md `mypy .` command had never worked. Those directories are now excluded, along with `tests/` and `scripts/` (289 pre-existing type errors in 46 files that the enforced gate — CI and pre-commit both run `mypy gateway/` — never checked). `mypy .` and `mypy gateway/` now check the same 192 gateway source files and both exit clean.
- **Durable JetStream drain removes avoidable per-event overhead without weakening proof**: each bounded 256-row drain group keeps at most 32 PubAcks in flight through NATS' shared asynchronous reply subscription, then deletes only matching-stream PubAck rows and records rejected or ambiguous publishes in one atomic SQLite transaction. A broker or cleanup failure preserves every row for its stable-message-ID retry.
- **Durable stream admission now fails closed**: any SQLite outbox admission error poisons the sink until restart, closes every Alpaca upstream before client fan-out, and makes `/health/ready` return 503. JetStream/outbox settings now require the data sink, Compose forwards all cutover credentials and outbox settings, and provider backfill chunks above 5,000 records are rejected before publication.
- **IV-rank responses retain Unusual Whales' official source date and update timestamp** (`gateway/providers/uw/_base.py`): both IV-rank endpoint aliases now return the provider's daily provenance instead of exposing only the normalized value, so consumers can validate the age of the source observation independently from Gateway receipt time.
- **The two exception-handling semgrep rules are now blocking in CI — 76 grandfathered findings paid down to 0** (`gateway/`, `.github/workflows/ci.yml`): `empire-no-bare-exception` (69) and `empire-no-return-none-for-failure` (7) had been excluded from the blocking scan since being fixed in the vendored rules (their upstream patterns never parsed, so they had never run anywhere). The real swallows are fixed: the four silent circuit-breaker pre-checks in the data-sink publish path no longer hide every exception behind `pass` (narrowed to the only expected lookup failure, with a debug log and unchanged fail-open semantics); secret-file read errors, invalid pagination cursors, WebSocket oversize/broadcast/replay send failures, Redis cache/sink cleanup errors, and the holiday-name/health-check fallbacks all log before continuing. Legitimately broad catches — route-boundary 502 mapping (Alpha Vantage ×20, Alpaca common), reload rollbacks, worker/batch/supervisor loops, health degradation, and documented Optional parse helpers — carry `nosemgrep` annotations with one-line justifications. The `--exclude-rule` flags are gone from CI and the semgrep pin moves 1.157.0 → 1.170.0 (which validates the fixed vendored patterns).
- **Alpaca market-data 4xx errors no longer flood the ERROR log** (`gateway/providers/alpaca/market.py`): `get_trades`, `get_latest_bars`, `get_latest_trades`, `get_historical_quotes`, `get_snapshots`, and `get_auctions` now log client-caused 4xx at WARNING and only genuine 5xx upstream failures at ERROR, matching the existing `get_bars`/`get_quotes` convention. Regression tests assert the split for both the paginated and direct request paths.
- **mypy now checks the provider mixin layer for real — 863 grandfathered errors reduced to 0** (`gateway/providers/uw/`, `gateway/providers/alpaca/`, `pyproject.toml`, `ci/mypy_dirty_allowlist.txt`): the UW and Alpaca feature mixins had no base class, so the type checker could not see the shared helpers the composed providers supply at runtime — 831 of the 863 errors traced to that one gap. Each mixin now inherits a type-checking-only alias of its base mixin (plain `object` at runtime, so provider composition and MRO are unchanged), the router/provider `disable_error_code` override groups are gone from `[tool.mypy]`, all 32 residual errors are fixed, and the CI dirty-file allowlist shrinks 37 files → 0. The pass also surfaced (and documents in `docs/FOLLOW_UPS.md`, each with a targeted ignore) 13 latent runtime bugs: ten UW provider methods calling SDK operations the vendored v5.1 SDK does not ship, `get_portfolio_history`/`set_account_configurations` passing kwargs alpaca-py rejects, and an admin provider-reload endpoint that always 404s.
- **Loop-variable/None-safety cleanups flagged by the wider mypy net** (`gateway/core/*_poller.py`, `gateway/api/middleware/`, `gateway/api/health.py`, `gateway/main.py`): the four Alpaca pollers no longer reuse one variable name for two types in their publish loops; `_poll_flow_alerts`/`_poll_darkpool`/`_poll_market_tide` gained the same not-initialized guard the other UW poll methods already had; and cache/envelope middleware handle the not-yet-started-response case explicitly.

### Changed

- **Weekly doc-drift fixes**: `CLAUDE.md`/`AGENTS.md` no longer claim mypy is missing from the `dev` extras and tell readers to run `uvx mypy .` — mypy has been in `dev` since the typecheck gate was fixed, and `mypy .` is the documented, CI-enforced command. `docs/RUNBOOK.md`'s env-var table no longer lists the dead `GATEWAY_LOG_LEVEL` knob as the effective log-level control; it now points at `EMPIRE_LOG_LEVEL`, matching `docs/configuration-guide.md`.
- **Stale mypy overrides for deleted modules removed** (`pyproject.toml`): `gateway.core.labels`, `gateway.core.ring_buffer`, and `gateway.providers.alpaca_stream` no longer exist in the codebase; their entries in the `[tool.mypy]` override lists were dead config and are gone.
- **Coverage floors ratcheted to the new measured reality** (`pyproject.toml`, `.github/workflows/ci.yml`): global `fail_under` 58 → 60 (measured 63.28% on 2026-07-23), Alpaca trading provider floor 95 → 97 (measured 99%); router floor stays 88 (measured 90). `docs/FOLLOW_UPS.md` rewritten as a living ledger — the 2026-07-22/23 debt-paydown pass closed mypy, semgrep, severity, envelope-sync, retention, and deploy-model items; open items now: 13 latent runtime bugs, the stream supervisor's post-deploy connection-limit retry policy, and the legacy mypy override groups.

- **The CI `uv.lock` guard no longer blocks tool-config-only pyproject edits** (`.github/workflows/ci.yml`): changing `[tool.mypy]`/`[tool.ruff]`/etc. never affects dependency resolution, so the PR gate now only fires when the pyproject diff touches dependency-shaped lines (`[project]`/`[build-system]`/`[tool.uv]` sections, dependency arrays, `requires`, or version pins) without a matching `uv.lock` change.
- **Pre-commit runs mypy again** (`.pre-commit-config.yaml`): with the allowlist at zero, commits now run the same `mypy gateway/` invocation as CI (project venv, not the isolated mirrors-mypy environment that would diverge from the CI gate).

### Fixed
- **empire-schemas' envelope module reconciled with the canonical gateway implementation** (`ci/empire_schemas`, empire-schemas repo): the shared package's dormant `compute_event_id`/`FEED_UNIQUE_FIELDS`/`make_instrument_key` had diverged (float-based Decimal stringification, missing dedup keys, naive crypto splitting) — it is now byte-parity-synced (verified against the pinned contract hash). The dead `gateway.schemas` envelope re-exports, which silently handed callers the divergent copy, are removed; `gateway.core.envelope` is the single implementation.

- **`iv_term_structure` expiries no longer risk collapsing to one row** (`gateway/core/envelope.py`): the feed emits one row per expiry of the same underlying, but it had no `FEED_UNIQUE_FIELDS` entry, so every expiry's `event_id` was derived from `provider|feed|instrument_key|ts_event` alone. With no per-row discriminator, two expiries wrapped at the same instant (the payload carries no date/timestamp, so `ts_event` is `now()`) hashed to the identical `event_id` and all but one was dropped at Heber's Bronze dedup. `expiry` is now part of the `event_id` (mirroring `oi_change`/`historic_option_volume`), so every expiry stays distinct. Regression test in `tests/test_envelope_heber_contract.py`.

- **Per-ticker darkpool fetches no longer silently return 0 rows** (`gateway/providers/uw/flow.py`): the SDK's typed `DarkpoolTradeResponse` carries rows in `.data` with an empty `additional_properties`, but `get_darkpool_ticker` only read the latter — every REST `/uw/darkpool/{symbol}` call and every date-targeted darkpool backfill quietly returned nothing. Now parsed via the shared `_extract_data` (both shapes).

### Added

- **`--days` on the UW backfill driver** (`scripts/uw_backfill_driver.py`): tier3 recovery can target explicit ISO dates (e.g. re-fetching days lost to stream eviction) instead of being limited to the universe-asof-derived window.

- **The 16:30 ET EOD run no longer publishes into the live event stream** (`gateway/core/uw_poller.py`): all EOD snapshot polls (greek_exposure, iv_rank, iv_term_structure, oi_change, historic_option_volume, short_interest, short_volume, ftds, congress_trades, insider_trades) now publish to the dedicated backfill stream (`heber:events:backfill`, 1M cap, isolated consumer) instead of `heber:events` (500K cap, market-hours firehose). On 2026-07-20 and again on 2026-07-21 the ~300K-event EOD burst was MAXLEN-evicted unread while the live consumer lagged, permanently losing oi_change, iv_rank, iv_term_structure and historic_option_volume for the day and truncating greek_exposure. EOD data is bulk re-runnable by date — exactly what the backfill stream exists for. Tests in `tests/test_uw_eod_stream_routing.py`.

### Added

- UW `flow_alerts` WebSocket subscriptions can now resume from a Redis stream cursor. The Gateway durably records every published flow envelope before live fan-out, marks each live message with its transport cursor, replays only through a captured high-water mark after reconnect, and explicitly refuses resumability when retained history or replay storage cannot prove a complete handoff.

- Synced `CLAUDE.md` and `AGENTS.md` into one shared instruction set.

### Added

- **Alpaca trading provider test coverage 47% → 99%** (`tests/test_alpaca_trading_provider_methods.py`): 109 new SDK-boundary-mocked tests covering order submission variants (bracket, stop, stop-limit, notional, extended hours, position intent), replace/cancel incl. the benign-cancel-race classifier, positions (incl. the full-close DELETE regression guard), portfolio history, assets, clock/calendar, account configurations, and all watchlist operations — success and error branches. The CI money-path floor for this file rises 45 → 95.
- **CI now enforces the gates that previously existed only on paper** (`.github/workflows/ci.yml`, `.semgrep/empire-rules.yaml`, `ci/mypy_dirty_allowlist.txt`, `Makefile`, `.githooks/pre-push`): the 4 import-linter architecture contracts (written in `.importlinter` but never installed or run) are now a blocking CI step and `make arch`; the Empire semgrep coding rules are vendored in-repo and blocking (0 findings at introduction; the raw-httpx rules exclude `gateway/core/http_client.py`, the wrapper itself); mypy gates on a dirty-file allowlist — any of the 149 currently-clean files gaining type errors fails the build, while the 37 known-dirty files are grandfathered until cleaned; a per-file coverage ratchet guards the Alpaca order money path (router ≥88%, provider ≥45%); the Heber wire-contract tests run as their own named step; a PR guard fails when `pyproject.toml` changes without `uv.lock` (full `uv lock --check` runs in the pre-push hook, which now also checks lockfile freshness); and the pip cache is actually keyed on `pyproject.toml`/`uv.lock` instead of a nonexistent requirements file.
- **Downstream remediation handoff notes for Orion and Kairos** (`docs/ORION_KAIROS_REMEDIATION_HANDOFF_2026-07-11.md`): captured July 5-11 Gateway log evidence for Orion Alpaca option rejects and the Kairos rate-limit burst, including counts, affected symbols, caller client IDs, and representative file:line references.

### Fixed

- Resumable `flow_alerts` subscriptions now start their prepared replay immediately after the subscription acknowledgement reaches the client. A misplaced replay-launch hook previously left authenticated Kairos connections waiting forever without `replay_begin` or `replay_complete`.
- Resumed `flow_alerts` connections now remain behind a per-connection handoff barrier until historical replay completes. Events captured inside the replay high-water mark are emitted exactly once by replay, while newer live events are buffered and released only after `replay_complete`, preventing reconnect-time reordering, duplicates, and loss.
- **The backfill stream gets its own MAXLEN — a bulk job can no longer self-evict** (`gateway/core/redis_sink.py`): all streams shared one 500K cap, so the 2026-07-19 976K-record bars backfill evicted the un-read UW feeds published just before it on the very stream (#35) built to isolate eviction. `heber:events:backfill` now caps at `GATEWAY_BACKFILL_STREAM_MAX_LEN` (default 1M ≈ 3GB at ~3KB/entry, sized against the 4GB instance); live `heber:events` stays at the shared cap. Jobs beyond the cap must pace (publish → drain → publish). Tests in `tests/test_redis_sink.py`.
- **Sink Redis eviction policy: `allkeys-lru` → `volatile-lru`** (`docker-compose.yml`): cache/dedup keys all carry TTLs (2,983/3,000 sampled) and stay evictable; the persistent keys — both heber streams, consumer groups, and `heber:watch:*` live watch state — are no longer silently evictable under memory pressure. At-cap writes with only persistent keys left now error into the sink's failover buffer instead of eating data.
- **`iv_term_structure` no longer stores 0/null IVs on every row** (`gateway/providers/uw/market.py`): the UW term-structure endpoint returns the ATM IV in `volatility`, but the mapper looked for `iv`/`implied_volatility` (never present) — so every Silver row carried `iv=0` and 100%-null `call_iv`/`put_iv` (~1.4k null-warnings/day in Heber). `volatility` is now the primary source; `call_iv`/`put_iv` stay `None` honestly (the endpoint has no per-side IVs).
- **Delisted MATIC/USD dropped from the default crypto pairs** (`gateway/core/crypto_poller.py`): Alpaca's latest-bar endpoint forever re-serves MATIC's final 2023-06-23 bar, which minted bogus `dt=2023-06-23` partitions in Heber whenever consumer dedupe rotated.
- **CI type checking actually runs now** (`pyproject.toml`): the CI mypy step had failed with `mypy: command not found` on every run — silently masked by `continue-on-error` — because the dev dependencies shipped `pyright` (which nothing invoked) instead of `mypy`. The dev extra now installs `mypy>=1.19`; the unused pyright dependency is removed (the `[tool.pyright]` editor config stays).
- **Gateway rate-limit and Alpaca timeout logs carry caller context** (`gateway/api/middleware/ratelimit.py`, `gateway/api/alpaca/trading.py`): per-client rate-limit warnings now include method/path/retry timing, and trading-call timeout/backpressure logs can include caller/path context for the high-volume account/orders/positions read routes, making post-mortems actionable without re-correlating access logs.
- **UW provider failure logs now include endpoint context** (`gateway/providers/uw/{transient.py,_base.py,flow.py,options.py}`): empty-body, JSON parse, and upstream 5xx failures now log the UW endpoint/path, symbol when known, status code when available, and a short response-body preview while preserving the existing raised exceptions.
- **UW poller's market-hours and EOD-state checks no longer stall the event loop** (`gateway/core/uw_poller.py`): the single-threaded poll loop called `_is_market_hours()` (which runs `exchange_calendars`' synchronous pandas work) and the EOD-state file-lock checks (`_should_poll_eod` → `should_defer`, and `claim`, which do blocking `Path.read_text()` + `fcntl.flock`) directly on the asyncio loop. A 2026-07-06 postmortem tied loop stalls (up to ~80s) partly to these blocking calls. Their invocations inside `_poll_loop`/`_poll_eod_snapshots` now run off the loop via `asyncio.to_thread`, so a slow calendar/pandas lookup or a contended EOD state file can no longer freeze streaming dispatch, WebSocket heartbeats, or other pollers. The helpers stay synchronous (so their sync callers — `_get_darkpool_interval`, the quotes/trades pollers, and the existing tests — are untouched) and the existing 30s market-hours cache still applies.
- **Producer-timeout events are spilled to the failover buffer instead of dropped** (`gateway/core/data_sink.py`, `gateway/core/redis_sink.py`, `gateway/core/metrics.py`, `config/prometheus_alerts.yml`): when the bounded dispatch queue is full AND workers can't drain it within `data_sink_producer_block_timeout_seconds` (opening-bell backpressure), the timed-out event was previously dropped and logged `critical` — silent data loss to Heber even though `RedisStreamsSink` already has a bounded failover buffer. The producer-timeout path now routes the event into that buffer via `buffer_event`/`schedule_drain`; a spill is logged at `warning` (recoverable) while a true loss (a sink with no buffer) is logged at `critical` and counted in the new `gateway_sink_producer_timeout_loss_total` metric. The page-on-any-drop alert now fires on true loss (`SinkProducerTimeoutLoss`), with a separate `SinkProducerTimeoutDropsSustained` warning for sustained backpressure that spilled but did not lose data.

- **UW `oi_change` open-interest rows no longer collapse at Heber** (`empire-schemas` `NormalizedOIChange`, mirrored in `ci/empire_schemas`): the schema omitted `option_symbol`, so Pydantic (`extra="ignore"`) silently dropped it before the `event_id` was computed — even though `FEED_UNIQUE_FIELDS["oi_change"]` leads its dedup key with exactly that field. Two distinct OCC contracts on the same underlying/date that shared `call_oi_change` (often 0) hashed to one `event_id`, and all but one dropped at Heber's Bronze dedup. `NormalizedOIChange` now carries `option_symbol` (plus `volume`/`trades`/`avg_price`/`prev_oi`, which the provider already produced but were being discarded). This affected the **live EOD poller**, not just backfill.
- **UW `historic_option_volume` snapshots no longer collapse or duplicate** (`gateway/providers/uw/options.py`): the provider read the expiry from `expiry` while UW's `volume-oi-expiry` endpoint returns `expires`, and stamped `datetime.now()` once per fetch when UW omitted a timestamp. So every per-expiry row shared the same `(symbol, "", None)` unique fields *and* a wall-clock `ts_event`: all per-expiry rows collapsed to one `event_id` that drifted on each re-fetch — dropping rows and duplicating on the reconcile/backfill path. It now maps `expires`→`expiry` and anchors `ts_event` to the snapshot day (the requested date, or today for the live poller), never `now()`. This affected the **live EOD poller**, not just backfill.
- **Account activities no longer 502 after the alpaca-py upgrade** (`gateway/providers/alpaca/trading.py`): alpaca-py ≥0.41 removed `TradingClient.get_account_activities` (it now lives only on `BrokerClient`, which needs Broker-API credentials the gateway doesn't hold), so once the container picked up alpaca-py 0.43.4 (the pin is `>=0.34`), every `/api/v1/alpaca/account/activities` call raised `AttributeError` → `502 GW-E5002`. This ran continuously from 2026-06-27, breaking the close-reconciliation Orion's execution engine runs each cycle to recover fills the 200-row poll window missed. The provider now calls the Trading API `/account/activities` endpoint directly via the SDK's low-level client (returning the raw activity dicts unchanged), keeping the Trading-API credentials and staying robust to where the SDK relocates the convenience wrapper.
- **UW news headlines can now be ticker-filtered through Data Gateway** (`gateway/api/uw/misc.py`): `/api/v1/uw/news/headlines?ticker=TSLA` now forwards the ticker as the official UW SDK `search_term`, includes the applied ticker/search filter in response metadata, and uses a ticker-aware cache key. Previously the route ignored `ticker`, so Kairos could only get generic UW headlines through the approved gateway path while the official UW MCP returned ticker-specific news.
- **Option OI-change now reports real call vs put open interest** (`gateway/providers/uw/options.py`): `/api/v1/uw/options/{ticker}/oi-change` hardcoded `put_oi`/`put_oi_change` to 0 and stuffed every contract's OI into the call side, and used UW's `oi_change` *ratio* (truncated to int) instead of the absolute change. Each row is now classified call/put by its OCC `option_symbol` and reports the absolute `oi_diff_plain`, so a sum over rows is a genuine call-vs-put net. Consumers that summed these rows (e.g. opening-confirmation, OI-trend signals) were silently call-biased and used a meaningless magnitude.
- **Short interest now returns actual short-interest data** (`gateway/providers/uw/market.py`): `get_short_interest` called UW's borrow/rebate endpoint (`shorts.get_data`) and read short-interest keys that don't exist there, so `short_interest` / `days_to_cover` / `short_percent_float` came back empty for every ticker. It now calls the interest-float endpoint and maps `si_float_returned` / `days_to_cover_returned` / `percent_returned`. Note: this endpoint is a single latest snapshot (no history/date param), and returns an all-UNSET record on UW tiers that don't include it (observed empty even for high-SI names) — in that case the gateway now returns no rows rather than a misleading all-zero short-interest row. Daily short *volume* + short-volume ratio remain available with history via `/{symbol}/short-volume`.

### Changed

- **Sink Redis is now durable and localhost-only** (`docker-compose.yml`): `data-gateway-redis` runs with AOF persistence (`appendonly yes`, `appendfsync everysec`, named volume `data-gateway-redis-data`) so a restart or power loss no longer destroys the event streams, DLQ, and consumer-group offsets — the 2026-07-12 outage restarted it empty. The unauthenticated port is now published on `127.0.0.1` only instead of LAN-wide `0.0.0.0` (containers still reach it via the compose network / `host.docker.internal`).
- **Backfill API now publishes to the dedicated `heber:events:backfill` stream by default** (`gateway/core/backfill.py`): `POST /api/v1/backfill` jobs previously flooded the live `heber:events` stream, the exact MAXLEN-eviction vector that destroyed un-consumed live events during the Jul 5–10 UW bulk backfill. Bulk backfill now lands on the isolated stream consumed by `heber-backfill-consumer` (same design as `uw_backfill_driver.py`); override with `GATEWAY_BACKFILL_STREAM` if the old behavior is needed.
- **Documentation refreshed to match the actual codebase** (root docs + `docs/`): fixed broken links (`docs/ARCHITECTURE.md`/`API_REFERENCE.md` → real filenames), corrected the install instructions to `uv sync --extra local --extra dev` (the previously documented `pip install -e ".[dev]"` produced a broken environment), documented the ownership-prefixed `c-<client_id>-dg-<uuid>` order-id format, the `key_hash` client-auth scheme, the PIT-universe ticker file, the Massive provider, and the real CI pipeline steps; removed dead references (black, `GATEWAY_LOG_LEVEL`, Shared-MCP-Server, TheOracle). Added `docs/README.md` as the documentation map with an explicit precedence rule (hand-written root docs win over the auto-generated `docs/` set, which now carries AUTO-GENERATED banners). Archived the stale remediation plan and moved loose vendor reference files under `docs/reference/`.
- **Log writes no longer happen on the gateway's asyncio event loop** (`empire-core` `empire_core/logger.py`, consumed here; `gateway/main.py`): `empire_core.setup_logging` now places a `QueueHandler` in front of a `QueueListener` background thread that owns the real stdout + rotating-file handlers, so `logger.info(...)` only enqueues a record and the blocking work (file flush, midnight rollover, disk writes) runs off the caller's thread — removing one contributor to the loop stalls tracked in the 2026-07-06 postmortem. The graceful-shutdown sequence's final step now calls the new `empire_core.logger.shutdown_logging()` to drain the queue so a process exiting mid-burst doesn't lose buffered log records (also registered via `atexit`). Log destinations (same files, same stdout) are unchanged.
- **Loop-stall watchdog now samples stacks repeatedly through a stall and captures GC stats** (`gateway/core/loop_watchdog.py`): the watchdog previously captured a single all-thread `faulthandler` dump at the moment a stall was first detected (~5s in). A 2026-07-06 postmortem's adversarial review showed that lone late snapshot lands well into a long stall and catches whatever frame the loop happens to be on rather than the frame that consumed the time. It now fires `on_stall` once per `check_interval` while the stall persists — bounded at `_max_stall_samples` (default 5) — so a long freeze yields several dumps spanning the whole window. Each `event_loop_stall_detected` log line now also carries `gc_stats` (`gc.get_stats()`) alongside the existing `gc_counts`, plus a `sample_index`/`max_samples` pair identifying which dump of the stall it is; `stalled_seconds` itself increases across the successive samples so a reader can see how far into the freeze each dump was taken. Detection and recovery semantics are unchanged.
- **`on_message_slow` warning threshold raised 0.1s → 1.0s** (`gateway/core/stream.py`): the WebSocket receive loop logged `on_message_slow` whenever a handler took longer than 0.1s, which fired ~2,600×/day on benign opening-bell batches (median ~0.15s, p95 ~1s) and added synchronous log I/O to the receive hot path. The threshold is now 1.0s — it keeps the genuinely-slow tail that risks Alpaca's slow-client (407/1006) disconnect while cutting the log flood ~95%.

### Added

- **UW company financial statements are now backfillable to Heber** (`gateway/core/envelope.py`, `scripts/uw_backfill_driver.py`): the `income_statement`/`balance_sheet`/`cash_flow` provider endpoints added in #50 are now wired end-to-end into the historical backfill path. (1) Added `FEED_UNIQUE_FIELDS` entries keyed `(ticker, fiscal_date_ending, report_type)` so the ~102 fiscal-period rows a single call returns each hash to a distinct `event_id` instead of collapsing to one (the failure mode fixed for `oi_change`/`insider_trades`) — one call returns both quarterly and annual rows. (2) Added `fiscal_date_ending` to `wrap_event`'s `ts_event` field chain so event_ids are stable across re-fetches and anchored to the fiscal period, not `now()`. (3) Added the three feeds to `uw_backfill_driver.py` as tier1 (one call/ticker → full history) + the proven-feed self-check. Verified end-to-end against live AAPL data: 102 rows → 102 distinct event_ids → 102/102 normalize to typed Silver in Heber. Regression tests in `tests/test_envelope_financials.py` + a contract case in `tests/test_envelope_heber_contract.py`. Field names verified against the live endpoint 2026-07-03. Pairs with Heber's matching typed-Silver ingestion change.
- **81 previously-unreachable UnusualWhales endpoints are now exposed through Data Gateway** (`gateway/providers/uw/{volatility_ext,options_ext,fundamentals,congress_ext,crossasset,predictions,private_markets}.py`, matching routers under `gateway/api/uw/`, plus a shared `UWBaseMixin._raw_get` primitive): an audit of UW's live OpenAPI spec (190 operations) against the gateway found ~92 wired vs 98 candidate-missing; after verifying against the actual gateway routes (dropping false positives already exposed under a different path — `iv-rank`, `top-net-impact`, `net-flow/expiry`), 81 genuinely-missing REST endpoints were added, spanning: options intelligence (per-ticker flow-alerts / flow-per-strike / flow-per-expiry, GEX levels, spot-exposures-by-expiry-strike v2, group greek-flow, options-pulse, option-trades, lit-flow, market OI-change & movers); the full volatility-analytics suite (anomaly / character / stats / variance-risk-premium per ticker, market-wide tops, VIX term structure); stock fundamentals & corporate (balance-sheets, cash-flows, income-statements, full financials, fundamental-breakdown, technical-indicator, ownership, company profile / dividends / splits / earnings-estimates / transcripts, IPO calendar, ticker-exchange directory); congress / institutional / shorts / analytics (congress-trader, politician list, unusual congressional trades + by-ticker / chart / stats, institutional activity v2, politician-portfolio disclosures, short screener, interest-float v2, sliding/fixed-window analytics); cross-asset (crypto whale trades / OHLC / state, FX rate / history / intraday, commodities, economic indicators, digital-currency series); and prediction-markets + private/pre-IPO-markets. UW added these to their API after the vendored SDK (v5.1) was cut, so 73 call the documented paths directly via the new `_raw_get` helper (authenticated passthrough; list params emitted as repeated `key[]=` query args); the rest use SDK-backed paths. All 81 return the standard `{"success", "data", "meta"}` envelope under `/api/v1/uw/…`. The 14 UW WebSocket channels (`/api/socket/*`) were deliberately left out — they need a streaming producer, not a REST wrapper. Note: many of these endpoints are gated to UW Advanced+/Enterprise tiers upstream, which the gateway does not enforce (it passes the upstream response through).
- **Per-request trace/correlation id** (`gateway/api/middleware/correlation.py`): a new outermost `CorrelationIdMiddleware` reads an inbound `x-trace-id` (or mints one), binds it into the structlog context via `empire_core.bind_context` so every log line for the request carries it, echoes it back on the response, and clears it on the way out. A single REST request — and its downstream sink publishes — can now be followed across the JSON logs by id, where previously correlation relied on timestamp+symbol guesswork. The `bind_context`/`clear_context` plumbing existed but had zero call-sites.
- **Provider-layer tests for the Alpaca money path** (`tests/test_alpaca_trading_provider_methods.py`): the router tests stub the provider, so the real `AlpacaTradingMixin` → alpaca-py request construction was untested (29% line coverage). New tests pin `replace_order`, `cancel_all_orders`, `close_all_positions` (incl. the `cancel_orders` default), `get_positions`, `exercise`/`do_not_exercise_option`, and the error branches of `create_order` (submit failure, unsupported type) and `close_position` (404 `POSITION_NOT_FOUND`). A wrong field name here would otherwise ship a malformed live order with nothing to catch it.
- **bandit and mypy run in CI** (`.github/workflows/ci.yml`): bandit was pip-installed but never invoked (SAST was effectively absent while appearing present); it now runs as a real blocking step (`bandit -c pyproject.toml -r gateway/`, currently 0 findings). mypy runs as an advisory step (`continue-on-error`) so type regressions surface in PRs without blocking on pre-existing errors.
- **Local pre-push gate** (`.githooks/pre-push`, `make install-hooks`): because the repo is private on the free GitHub plan, server-side branch protection / required checks are unavailable and a passing CI run is only advisory. The pre-push hook runs ruff + the capital/wire-contract-guarding tests before a push reaches the remote. Activate with `make install-hooks`.

- **EOD capture now targets the Atlas PIT universe instead of random dynamic tickers** (`gateway/core/uw_poller.py`, `gateway/config.py`): when the PIT universe export file exists (`GATEWAY_UW_UNIVERSE_FILE`, default `config/uw_pit_universe.json`), the EOD poller uses its ~989 survivorship-free active symbols as the capture set and **disables the daily screener-driven dynamic ticker rotation** — replacing the old ~33 mega-cap core + 20 rotating "random" tickers. A missing or malformed file falls back to the previous core-tickers/defaults behavior so it can never break startup. This is the "stop pulling random tickers, capture the universe we actually use" swap; takes effect on gateway restart. Note: ~989 tickers × 8 EOD feeds ≈ 8k calls/day of forward capture — raise `GATEWAY_UW_EOD_CONCURRENCY` (5 → 10-15) so the fan-out finishes inside the after-close window.
- **UW PIT-universe historical backfill driver** (`scripts/uw_backfill_driver.py`, `scripts/export_pit_universe.py`, `config/uw_pit_universe.json`): a standalone, resumable, budget-metered backfill tool that pulls UnusualWhales history for the Atlas V2 survivorship-free PIT liquidity universe (989 currently-active / 1,333 trailing-780-day symbols) and publishes it to the same `heber:events` stream the gateway uses. Runs as its own process (own provider + Redis sink) so backfill load never touches the live gateway event loop. Tier-1 (`greek_exposure` with `timeframe=3Y`, `ftds`, `short_volume`, `earnings`, `congress_trades`, `insider_trades`) pulls full multi-year history in ~1 call/ticker — the whole universe is ~8,000 calls, one day under the new 40k/day limit. Tier-3 (`oi_change`, `historic_option_volume`, `darkpool`) is sampled per-day (top-N symbols × N days) to stay in budget — the `oi_change`/`historic_option_volume` event_id fixes above are included, so their per-contract/per-expiry rows no longer collapse (`oi_change` dedup depends on empire-schemas' `NormalizedOIChange.option_symbol`, this PR's cross-repo dependency). Congress/insider payloads get their `ts_event` stamped from the filing date (matching the live EOD poller) so re-published rows dedup instead of duplicating. Only backfills feeds proven end-to-end (EOD poller publishes them + Heber has a Silver normalizer); a `--self-check` guard asserts the feed set never drifts. Partial/failed publishes leave the unit un-marked so a rerun retries it (no silent gaps); the sink uses a large `MAXLEN` so backfill never trims un-consumed live events off the shared `heber:events` stream. Progress persists to a state file so a run resumes where it stopped.
- **UW provider supports wider historical windows** (`gateway/providers/uw/options.py`): `get_greek_exposure` now forwards a `timeframe` param (UW default 1Y; `2Y`/`3Y` verified to return the full multi-year daily series in one call — passing `date` alone returns empty for historical dates), and `get_options_volume` forwards `limit` (UW caps ~500 trailing days ≈ 2yr). These let the backfill driver collapse a multi-year series into a single call.
- **Four UW EOD feeds are now backfillable** (`gateway/core/backfill.py`): `oi_change`, `iv_rank`, `iv_term_structure`, and `historic_option_volume` — the per-ticker end-of-day UnusualWhales feeds — can now be recovered through the backfill API (`POST /api/v1/backfill` with `provider=unusual_whales`), where previously only the live EOD poller could produce them, so a missed or failed EOD run was unrecoverable. The two expiry-bearing feeds (`iv_term_structure`, `historic_option_volume`) are wrapped with the same `equity:{symbol}` instrument-key override the poller uses, so their rows are not misclassified as option contracts and dropped by Heber. Note: `iv_term_structure`'s provider method is snapshot-only (no date parameter), so a backfill of it returns the *current* term structure regardless of the requested date range; the other three resolve the requested trading day.
- **Darkpool dedup-distinctness guard** (`tests/test_envelope.py`): a test that 200 darkpool prints with distinct `tracking_id`s produce 200 distinct `event_id`s (and a true duplicate still collapses). Guards `FEED_UNIQUE_FIELDS['darkpool']` — if `tracking_id` were ever dropped from the dedup key, distinct prints would silently collapse and be deduped away, the exact latent darkpool silent-loss the live `published:0 / duplicates:200` pattern hinted at.
- **Direct tests for the streaming→Heber dispatch** (`tests/test_stream_sink_dispatch.py`): `_on_stream_envelope` — the single callback through which every streaming event reaches the Redis sink — had zero direct coverage. New tests pin the happy path, the no-registry no-op, publish-failure swallowing (must not propagate and kill the stream), CancelledError propagation, and the serialize-error fallback to the raw envelope. They also lock the frozen wire contract (topic `heber:events` + feed forwarding) so a future change to either fails loudly.
- **Per-feed poller publish/duplicate metrics** (`gateway/core/metrics.py`, `gateway/core/uw_poller.py`): `gateway_feed_published_total{feed}` counts events that actually reached the Heber sink (post-dedup) and `gateway_poller_duplicates_total{feed}` counts dedup-suppressed events, both wired at the single `_publish_envelopes` chokepoint so every UW feed is covered. This makes a feed silently going to zero during market hours — e.g. darkpool prints overflowing the 200-record fetch window — visible per feed on a dashboard, where it was previously hidden behind a 100%-deduped INFO line. (Alert thresholds intentionally deferred: darkpool's expected baseline is `published==0` outside active trading, so a publish-based zero alert needs real market-hours data to tune.)
- **Event-loop stall and circuit-breaker-state metrics** (`gateway/core/metrics.py`, `gateway/core/loop_watchdog.py`, `gateway/core/circuit_breaker.py`): two failure signals that were log-only are now Prometheus series so they can be alerted, not just grepped after the fact. `gateway_event_loop_stalls_total` / `gateway_event_loop_stall_seconds_total` count the loop-stall watchdog's detections (the class of stall that silently delays WebSocket heartbeats and the Heber sink drain). `gateway_circuit_breaker_state{name}` exports each breaker's state (0=closed, 1=half_open, 2=open) on every transition, so the Redis-sink breaker tripping OPEN — the leading edge of Heber data loss — is visible immediately instead of only once the failed-event buffer starts evicting.
- **Pinned `compute_event_id` parity guard** (`tests/test_envelope.py`): a test that hard-pins the gateway's local `compute_event_id` output and asserts it is sensitive to Decimal precision. `empire-schemas` ships a divergent duplicate that stringifies Decimals as `str(float(d))` ("150.5") instead of `str(d)` ("150.50"); the gateway uses only its local copy today, but a future "dedupe the duplication" refactor swapping to the shared impl would silently change every event_id and reset Heber's dedup bloom filter. This guard makes that swap fail loudly.
- **Event-loop stall watchdog** (`gateway/core/loop_watchdog.py`, `gateway/main.py`, `gateway/config.py`): an off-loop watcher thread watches an asyncio heartbeat and, when the single event loop is blocked longer than `GATEWAY_LOOP_WATCHDOG_STALL_THRESHOLD_SECONDS` (default 5s), dumps every thread's stack (`faulthandler`) plus GC counts — capturing the exact synchronous frame behind the otherwise-silent multi-second `on_message_slow` stalls and WS `heartbeat_timeout_disconnect` storms. One dump per stall, with a recovery line recording the total stalled time. Enabled by default; toggle via `GATEWAY_LOOP_WATCHDOG_ENABLED`.

### Changed

- **The UW historical backfill publishes to a dedicated `heber:events:backfill` stream, not the live `heber:events`** (`scripts/uw_backfill_driver.py`): a multi-year backfill (e.g. `ftds`/`greek_exposure`, ~740k+ records) scattered across the live stream could fill it to MAXLEN and evict un-read **live** capital-adjacent events — silent data loss observed in Heber's dataflow-health across the week of 2026-06-29. The driver now defaults its publish target to `heber:events:backfill` (override with `--stream` / `HEBER_BACKFILL_STREAM`), consumed by a separate Heber `heber-backfill-consumer`, so the live and backfill streams have independent MAXLEN caps and consumers — a backfill flood can now only trim re-runnable backfill data. **Deploy ordering:** the Heber `heber-backfill-consumer` must be running before the driver publishes, else backfill records land in a stream nobody reads and eventually trim. `--max-stream-len` now caps the backfill stream only. Pass `--stream heber:events` to restore the old shared-stream behavior.
- **`make test` now runs the real suite** (`Makefile`): the `unit` marker was only ever applied to one file, so `make test` (`pytest -m unit`) ran ~4 tests — false confidence before a push. It now runs `pytest -m "not perf and not integration"` (the full fast suite).
- **Coverage floor raised 43 → 58** (`pyproject.toml`): the floor sat ~17 points below the real ~60.8%, so the entire order path could have lost all coverage and CI would still pass. Now ~3 points under measured; the stale "48% as of 2026-06-10" comment is refreshed.
- **Per-test timeout** (`pyproject.toml`): default `addopts` now includes `--timeout=60` so a single hung test (e.g. a broken production-timeout patch) fails fast instead of stalling the whole CI job. Per-test, so it never trips the ~3s perf benchmarks.

### Fixed

- **CI was silently dead — restored** (`.github/workflows/ci.yml`): the flagship workflow used `if: secrets.SONAR_TOKEN != ''` in a **job-level `if:`**, but the `secrets` context is not permitted there — this invalidated the entire workflow, which failed at validation in 0 seconds on every run for ~4 months. No lint, no tests, no coverage floor, no provider-contract check, and no real-Redis integration ran on any merge to master. The SonarCloud job now exposes the secret as a job `env` and gates each step on `env.SONAR_TOKEN` (the valid pattern), so the rest of the pipeline executes again.
- **Live trading calendar no longer degrades silently** (`gateway/api/calendar.py`): a failure fetching the authoritative Alpaca calendar caught `Exception` and silently fell through to the hardcoded static calendar (e.g. wrong early-close days) with no log. The degradation now emits a `calendar_alpaca_fallback` warning with the error before falling through.
- **`do_not_exercise_option` had no HTTP timeout** (`gateway/providers/alpaca/trading.py`): the one trading method that bypasses the alpaca-py SDK used a raw `httpx.post` with no timeout, so a hung trading endpoint would block the calling thread forever — the exact leak the SDK-session timeout guards against, on the path that escapes it. It now passes the configured `alpaca_trading_http_timeout_seconds`.
- **Flood-prone log sites are now throttled** (`gateway/core/redis_sink.py`, `gateway/main.py`): the per-item / per-chunk Redis batch errors and the per-event WebSocket `stream_send_error` fired once per failed item/event in hot paths — the flood class behind past "Too many connections"/DLQ incidents. They now use `LogThrottle` (one line per key per 10s) and report a `suppressed_since_last` count, so a Redis outage or one broken downstream client can no longer bury the error log.
- **Per-client rate limits are now honored, not silently defaulted** (`gateway/api/middleware/ratelimit.py`, `gateway/core/auth.py`): the rate-limit middleware runs before the auth dependency sets `request.state.client`, so it fell back to the default 600 req/min for every client identified by key — a client with a higher configured `rate_limit` (e.g. `3roses` at 6000) was throttled 10× too aggressively. The middleware now resolves the client via a new audit-silent `ClientAuthenticator.client_for_key()` and applies its actual `rate_limit`.
- **Two provider data-correctness bugs that silently corrupted bars/flow** (`gateway/providers/yfinance.py`, `gateway/core/option_capture.py`): (1) the yfinance bar fallback called `Ticker.history()` with the library default `auto_adjust=True`, returning split+dividend-adjusted closes, while Alpaca returns split-only — so an Alpaca→yfinance failover silently switched the price basis and put a discontinuity in the series mid-stream; it now passes `auto_adjust=False` to match Alpaca's split-only basis. (2) Per-contract `option_trades` set `size` to the contract's whole-day `volume` whenever `last_trade_size` was missing, minting one fake "trade" of size = full-day volume that double-counted flow and corrupted VWAP; an unknown size is now `0` (a zero-weight trade), never the day's volume.
- **Order-ownership prefix collisions rejected at startup** (`gateway/core/auth.py`): per-client order isolation uses a `c-{client_id}-` marker, and the access check is a raw `startswith` — so if a client id were ever a hyphen-prefix of another (e.g. `heber` alongside `heber-watch`, where `c-heber-` prefixes `c-heber-watch-…`), the shorter-id client could cancel/replace the longer-id client's orders. No such pair exists today, but `_load_clients` now fails loud at startup on any prefix-colliding id pair rather than silently cross-wiring two trading clients' order isolation.
- **GEX by-strike/by-expiry rows no longer silently dropped before Heber** (`gateway/api/middleware/envelope.py`): the REST sink middleware marks `/api/v1/uw/gex/...` greek-exposure responses sink-eligible, but the `/strike` and `/expiry` sub-routes return rows carrying per-row `strike`/`expiry`, so `_infer_instrument_type` tagged each as an option and built a malformed `option:{SYMBOL}` key (no OCC) that Heber rejects — 100% of by-strike/by-expiry rows vanished while the caller still got a 200. Greek-exposure is per-underlying analytics; the middleware now overrides those envelopes to the `equity:{SYMBOL}` key (mirroring the poller's `iv_term_structure`/`historic_option_volume` handling).
- **Account-wide live-trading actions now require super_admin** (`gateway/api/deps.py`, `gateway/api/alpaca/trading.py`): flattening ALL positions (`DELETE /positions`) and changing account configuration (`PATCH /account/configurations`) affect every client's book on the shared Alpaca account, yet were gated only by the `trading` capability — so any one trading client could liquidate or halt the entire fleet. They now require the `super_admin` role (no automated client has it; an operator grants it deliberately). Per-symbol close (`DELETE /positions/{symbol}`) stays at the trading gate — that is how the trading systems flatten only their own positions, so no kill-switch is weakened. `close_all_positions` now defaults `cancel_orders=False` (it no longer cancels every client's open orders unless explicitly asked), and every close is audit-logged with the caller id + symbol.
- **UW EOD run claim is released on shutdown-cancel** (`gateway/core/uw_poller.py`): when a restart cancelled the EOD task mid-run, the `CancelledError` path re-raised without releasing the persistent `running` claim, so the next startup deferred the whole day's EOD snapshots until the 2-hour stale window expired (silently missing from Bronze if no later restart). The cancel path now rolls back the in-memory date guard and releases the claim (release preserves a `completed` claim), so the next startup re-runs today's EOD.
- **Insider/congress/OI-change event_id collisions fixed — root cause of the `insider_trades` silent loss** (`gateway/core/envelope.py`): `FEED_UNIQUE_FIELDS` for `insider_trades` (`ticker, owner_name, transaction_date`), `congress_trades` (`ticker, name, transaction_date`), and `oi_change` (`symbol, date, call_oi_change`) carried no per-row discriminator, so multiple distinct same-day rows hashed to the **same** `event_id` and all but one were deduped away — the gateway-side root cause of the `insider_trades` 200→~2 Bronze collapse previously thought to be consumer-side. Added the row's stable discriminator to each hash: `id` (insider), `txn_type` + `amounts` (congress), and `option_symbol` (oi_change). True duplicates still collapse. Heber trusts the gateway `event_id` (it does not recompute), so no Heber change is required; the change re-publishes affected rows once under their new ids. A parametrized uniqueness contract test now guards the whole class.
- **Paper/live trading no longer fails OPEN to real capital** (`gateway/providers/alpaca/_base.py`, `gateway/config.py`): live-vs-paper was decided by `"paper" in APCA_API_BASE_URL`, so an empty or typo'd value resolved to *live* trading on the shared Alpaca account — one config slip from routing every order to real money. Paper is now the fail-safe default: live requires BOTH an explicit `GATEWAY_ALLOW_LIVE_TRADING=true` opt-in AND the exact recognized live URL, and going live emits a loud `alpaca_live_trading_enabled` WARN. A missing/empty/typo'd URL always resolves to paper. (Per the monorepo rule that paper/noop must be the default.)
- **Broken `MemoryPressure` alert restored + new stall/breaker alerts** (`config/prometheus_alerts.yml`): the `MemoryPressure` alert keyed on `gateway_memory_pressure`, a metric that no longer exists (the always-zero gauge was removed), so it never fired — a memory leak toward an OOM-kill produced no page until the process crashed. Re-keyed on the real `gateway_process_memory_percent`. Added `EventLoopStalls` (any loop stall in 5m) and `SinkCircuitBreakerOpen` (the Redis-sink breaker OPEN for 30s — the leading edge of Heber loss) alerts on the new metrics.
- **Misleading dead `GATEWAY_LOG_LEVEL` removed from Docker** (`docker-compose.yml`): the deployed container set `GATEWAY_LOG_LEVEL=DEBUG`, but the runtime reads `EMPIRE_LOG_LEVEL` (via `empire_core.logger`) and ignored it — so the gateway ran at INFO while operators believed DEBUG was on, and "turn on DEBUG" did nothing. Replaced with an explicit `EMPIRE_LOG_LEVEL=INFO` (preserving the actual current behavior) plus a comment; the key failure signals are Prometheus metrics now, not DEBUG logs.
- **Failed-buffer drain re-buffering now counts evictions** (`gateway/core/redis_sink.py`): during a Redis-outage recovery, events that failed to re-publish were re-appended to the failed-event buffer via a raw `deque.append`, which silently evicts past capacity without incrementing `gateway_sink_buffer_evictions_total` — defeating the eviction alert exactly when loss is happening. The drain now re-buffers through the accounted `_buffer_failed_event` path so any eviction during recovery is counted and logged.
- **Prometheus per-path metric cardinality leak closed** (`gateway/core/metrics.py`): request-metric path normalization collapsed plain alpha tickers (`AAPL`) to `{symbol}` but let crypto/forex pairs (`BTC-USD`), dotted share classes (`BRK.B`), and OCC-style alphanumerics through unchanged — so each distinct pair/contract that hit a metered route created its own permanent `gateway_requests_total{path=...}` series, an unbounded cardinality (and memory) leak. The `_looks_like_symbol` heuristic now also matches those shapes; an uppercase-lead requirement keeps lowercase route names (`bars`, `quotes`, `health`) from being collapsed.
- **Poller dedup cache hard-capped** (`gateway/core/base_poller.py`): the in-process `_seen_ids` dedup cache was TTL-only, and the TTL sweep runs just once per poll cycle — so a high-volume burst (× the 24h TTL window) could grow it to hundreds of thousands of entries between sweeps. Added a 200k LRU cap; evicting an old id only risks a rare re-publish, which Heber dedups by `event_id` anyway.
- **Two streaming-path memory leaks bounded** (`gateway/core/stream.py`): (1) `SequenceTracker._counters` grew one permanent entry per `(symbol, feed)` — with OPRA options streaming that is one key per OCC contract, accumulating forever (the `reset()` method was never called in-process) toward a slow RSS climb and eventual OOM restart. It is now an LRU `OrderedDict` capped at 50,000 keys; evicting a stale contract harmlessly restarts its sequence at 1. (2) `StreamMultiplexer._tasks` was a list appended on every eager-start and dormant-stream revival but never pruned, so each reconnect cycle pinned a completed `Task` forever over a multi-day run. It is now a set with an `add_done_callback` discard so finished tasks are released automatically.
- **Silent stream/poller drops are now metered per feed** (`gateway/core/stream.py`, `gateway/core/uw_poller.py`, `gateway/core/metrics.py`): two paths that silently removed records from the Heber stream were log-only. A streaming bar/quote/trade that fails validation (e.g. an Alpaca format change that trips the validator for a whole feed) and a UW record that fails envelope construction (UW schema drift) now both increment `gateway_messages_dropped_total`, which an existing alert already watches. The counter gained a `feed` label so "drops are spiking" becomes "feed X is vanishing", turning a systematic silent feed loss into a page within minutes instead of a buried INFO/WARNING log.
- **UW EOD per-ticker envelope building moved off the event loop** (`gateway/core/uw_poller.py`): the earlier off-loop fix covered the darkpool/flow/tide pollers but not the end-of-day fan-out — the 8 per-ticker EOD feeds and the congress/insider feeds still built their envelopes (`model_dump` + `wrap_event` + BLAKE2b) inline on the loop. A live 16:30 ET EOD run was observed throwing a 5.1s event-loop stall while building greek-exposure envelopes across the ticker universe. The builds now run via `asyncio.to_thread` through a shared `_wrap_eod_results` helper. Behavior (including the equity-key overrides for the expiry-bearing feeds) is unchanged.
- **Backfill envelope building moved off the event loop** (`gateway/core/backfill.py`): `_publish_items` sorted and wrapped a full chunk of records (`wrap_event` + BLAKE2b per record) synchronously on the event loop before publishing — for a large historical chunk this blocks the loop the same way the UW darkpool build did, stalling live ingest while a backfill runs. The sort+wrap now runs via `asyncio.to_thread` (extracted into a pure `_wrap_items` helper); the publish stays async on the loop. Behavior unchanged.
- **UW REST envelope building moved off the event loop** (`gateway/core/uw_poller.py`): the darkpool, flow, market-tide, and sector-tide pollers built up to 200 EventEnvelopes per cycle (`model_dump` + `wrap_event` + BLAKE2b) synchronously on the single asyncio event loop. Runtime observation caught a 5.34s event-loop stall coincident with a darkpool publish — a stall on the one loop simultaneously starves WebSocket heartbeats (triggering 1006 disconnect storms), live ingest, and the Heber sink drain. The per-cycle build now runs via `asyncio.to_thread`, mirroring the existing `option_capture`/`bulk` offloads. Behavior is unchanged; only the work location moves off the loop.
- **Finnhub & Alpha Vantage no longer log client-caused 4xx as ERROR** (`gateway/providers/finnhub.py`, `gateway/providers/alphavantage.py`, `gateway/providers/_errors.py`): both providers logged every upstream failure at ERROR, so a client-caused 4xx (bad/index symbol → 400, rate limit → 429) produced ERROR-log noise indistinguishable from a real server outage — the same flood pattern already fixed in the Alpaca provider. All 65 provider error-log sites now route through a shared `log_provider_http_error()` helper that logs 4xx at WARNING and 5xx / transport failures at ERROR. The exception still re-raises unchanged; only log severity changed. AlphaVantage's existing rate-limit/premium WARNING handling is untouched.
- **Per-ticker UW EOD feeds now retry transient upstream errors** (`gateway/core/uw_poller.py`): the eight per-ticker end-of-day fetches (greek exposure, IV rank, IV term structure, OI change, historic option volume, short interest, short volume, FTDs) called the UW provider directly with no retry, so a single transient 5xx zeroed that feed for that ticker for the whole day — a scattered hole in Heber Gold. They now route through the same bounded-retry helper (`_fetch_with_retry`, 3 attempts with exponential backoff on transient upstream errors) already used for the congress/insider EOD fetches.
- **Failed UW EOD runs now retry the same day instead of being lost** (`gateway/core/uw_poller.py`, `gateway/core/uw_eod_state.py`): when the end-of-day per-ticker snapshot run raised after the in-memory date guard was stamped and the persistent run claim was written, both guards stayed set — blocking any same-day retry and blocking a restart from re-claiming until the 2-hour stale window expired, so a transient failure lost the whole day's EOD snapshots (greek exposure, IV rank, OI change, short interest, FTDs, etc.). On failure the poller now rolls the in-memory date guard back to its prior value and the state store exposes `release()` to clear a *running* claim (a completed claim is preserved), so the next poll tick — or a restart — re-runs today's EOD.
- **Backfill partial sink failures are now detectable instead of silent gaps** (`gateway/core/backfill.py`): historical backfill published chunks via the aggregate-count `publish_all_batch`, which under a partial Redis pipeline failure (some XADDs fail at arbitrary indices) returned an undercount with no way to tell which events landed — so the chunk was marked complete and the missing rows became an undetectable hole in Heber. Backfill now uses the per-message `publish_all_batch_results` API, counts only confirmed publishes, logs `backfill_publish_partial`, and records the shortfall in the job's error list so an operator can re-run the affected chunk.
- **Dormant upstream streams now self-revive while clients stay subscribed** (`gateway/core/stream.py`): when an Alpaca upstream WebSocket exhausted its reconnect budget or hit a non-recoverable error (e.g. "connection limit exceeded"), it set itself dormant and waited for a *new* client subscription to restart — but long-lived trading bots subscribe once and never re-subscribe, so the stream delivered zero ticks to clients and zero events to the Heber sink until the process was restarted. The multiplexer now runs a supervisor coroutine that every 20 seconds revives any stream that is dormant *and* still has active subscriptions, logging `stream_supervisor_reviving_dormant`. Each connection's own backoff keeps a persistently-failing upstream on a slow retry cycle rather than a tight loop.
- **Failed-event retry buffer is now operator-tunable and defaults larger** (`gateway/core/redis_sink.py`, `gateway/config.py`, `gateway/main.py`): the in-memory buffer that holds events through a Redis outage (re-published on reconnect) was a fixed 10,000 entries. At OPRA quote volume the circuit breaker's 15-second open window filled it in under a second, silently evicting the bulk of an outage to a metered-but-lost deque. Capacity is now configurable via `GATEWAY_DATA_SINK_FAILED_BUFFER_CAPACITY` and the default is raised to 50,000 (~50 MB); deployments still seeing `gateway_sink_buffer_evictions_total` climb during a flap can raise it further.
- **SIGHUP config reload no longer 401-locks every client on a bad edit** (`gateway/core/auth.py`, `gateway/main.py`): `ClientAuthenticator.reload()` cleared all key maps *before* re-parsing `clients.yaml`, so a single malformed edit (bad YAML, invalid client id) + SIGHUP left the maps empty and 401-locked every trading client (Kairos/Cerberus/3Roses/Orion) until a process restart — the same failure class as the documented key-rotation split-brain. Reload is now atomic: it builds fresh maps and swaps them in only on a fully successful load, rolling back to the last-good clients and logging `clients_reload_failed_kept_previous` on any error. The SIGHUP handler also catches reload failures so a broken config can't propagate out of the signal handler.
- **Heber watch UW enrichment permissions restored** (`config/clients.yaml`): the `heber-watch` client can now read the UW endpoints it uses for alert feature enrichment (`iv-rank`, `gex`, max pain, and market tide), preventing `403 Provider access denied: uw` responses that left Gold feature rows with null Greek/tide columns.
- **Heber watch Gateway authentication restored** (`config/clients.yaml`): aligned the `heber-watch` client hash with the key loaded by the running Heber watch service so its option quote polls authenticate instead of returning `401`.
- **UW greek exposure date-only rows normalize to UTC** (`gateway/providers/uw/options.py`): date-only `date` values from Unusual Whales now become UTC-aware midnight timestamps before strict schema validation, preventing live `uw_greek_exposure_failed` errors.
- **Benign order-cancel races no longer logged as errors** (`gateway/providers/alpaca/trading.py`): a cancel that loses the race with the order's own terminal transition (Alpaca 422, code 42210000 "order is already in … state") is now logged at INFO (`alpaca_order_cancel_noop`) instead of ERROR. The cancel still re-raises so callers learn the true order state (e.g. a fill); this removes ~1,300 non-actionable ERROR lines per trading day from the error log. Matching requires both the terminal code and the "already in … state" message, so unrelated reuses of the code still surface as ERROR.
- **Option-chain snapshot processing no longer stalls the event loop** (`gateway/core/option_capture.py`): each capture cycle synchronously serialized (Pydantic `model_dump`), sorted, and ranked the full SPY/QQQ/IWM option chains (~13k+ contracts each) directly on the event loop, contributing to multi-second freezes that starve WS heartbeats and the Heber sink drain. The per-symbol crunch now runs via `asyncio.to_thread`, and the chain is serialized once instead of twice (the snapshot payload and WS-contract selection share a single serialization pass). All events stay fully awaited — no fan-out or dispatch change.
- **Bulk option-chain filtering no longer stalls the event loop** (`gateway/core/bulk.py`): `_filter_option_contracts` ran per-contract `model_dump` + filtering synchronously on the loop for bulk options jobs; it now runs via `asyncio.to_thread`, mirroring the existing backfill normalization offload.

### Changed

- **`heber:events` stream cap and Redis memory raised together to survive an EOD burst into a down consumer** (`docker-compose.yml`): `GATEWAY_DATA_SINK_MAX_STREAM_LEN` 300K→500K and Redis `--maxmemory` 2GB→4GB, moved in lockstep. On 2026-06-25 a ~114k-message EOD batch published while heber-consumer was down; the stream was sitting at the 300K cap, so every new XADD trimmed the earliest (unread) entries and the first-published EOD feeds (oi_change, iv_rank, iv_term_structure, historic_option_volume) were evicted before the consumer returned — permanent loss. The larger cap gives an EOD burst headroom above the steady-state backlog so it no longer evicts unread feeds during a brief consumer outage. The cap and maxmemory MUST move together (raising the cap alone caused the 2026-05-05 outage); **deploy prereq: confirm the Docker VM has the RAM to back Redis at 4GB.** This is mitigation, not a guarantee — a long-enough consumer outage still overflows any fixed cap; the durable fix is self-healing replay from UW via the backfill API.
- **Gateway sink drain capacity raised and Redis pools tied to worker count** (`gateway/config.py`, `gateway/main.py`): the default `data_sink_worker_count` is raised 16 → 24 (still within the 32-connection pool) to widen Heber sink drain throughput and reduce producer-timeout event drops during bursts. Both the dedup cache and the XADD sink now size their Redis pools to the worker count, so raising the worker count can never leave workers serializing on connection acquisition ("Too many connections").

### Removed

- **Dead `old_key_hashes` rotation grace-period writes removed** (`gateway/cli.py`): `rotate-key` accumulated each rotated client's old key hash into an `old_key_hashes` list in `clients.yaml`, but `auth.py` never read it — the intended grace period was dead, and key rotation has always been a hard cutover (old key dies immediately, fail-closed). Honoring those hashes without an expiry would make old keys valid indefinitely (a security hole), so the dead writes are removed and any existing `old_key_hashes` is cleared on the next rotation; a real grace period needs a deliberate expiring-key design.
- **Dead `catalog_registry.py` removed** (`gateway/api/catalog_registry.py`, 1,906 lines): an in-process feed/provider registry with zero importers across the gateway, tests, and consumer repos. It advertised catalog routes that no router mounted, so it was pure carrying cost. The live catalog API (`gateway/api/catalog.py`) does not depend on it.
- **Dead config settings removed** (`gateway/config.py`): deleted 10 settings that nothing in the runtime reads — `app_name` (the FastAPI title is hardcoded), the `stream_reconnect_*` trio (the multiplexer hardcodes its reconnect schedule), `alpaca_base_url`/`uw_api_key` (the SDK and UW provider read `APCA_API_BASE_URL`/`UNUSUAL_WHALES_API_KEY` directly from the environment), `ws_max_duration`, and the unused memory-budget trio (`memory_target_mb`, `memory_hard_limit_mb`, `gc_threshold_pct`). These were silent no-ops: setting the corresponding `GATEWAY_*` env vars changed nothing.
- **Dead memory-pressure metric removed** (`gateway/core/metrics.py`): deleted the `gateway_memory_pressure` gauge and its `update_memory_pressure()` setter — never called, so the gauge sat permanently at zero and read as a false all-clear on dashboards.
- **Unreachable `LogSink` removed** (`gateway/core/redis_sink.py`): a debug-only `DataSink` that was never registered or instantiated anywhere.
- **Root-level ad hoc probes removed after the Ponytail over-engineering audit**: deleted loose manual scripts (`test_bars.py`, `test_crypto_endpoint.py`, `test_crypto_error.py`, `verify_backfill.py`, `scripts/remediation_workflow.js`) that lived outside the maintained pytest/smoke-test paths.
- **Unused core compatibility modules removed after the Ponytail over-engineering audit**: deleted the dead API-key balancer, standalone normalizer, and unused gateway error hierarchy (`gateway/core/balancer.py`, `gateway/core/normalizer.py`, `gateway/core/errors.py`) plus the balancer self-test. Live providers normalize in their own provider modules, request limiting is handled by the active rate-limiter stack, and no runtime code imported these modules.
- **Historical audit backlog pruned after the Ponytail over-engineering audit**: removed stale performance/technical-debt audit reports and old Redis resilience plans from `docs/audits/` and `docs/plans/`. Current operational release-readiness and live-smoke reports stay in place.
- **Unused quality API slice removed after the Ponytail over-engineering audit**: deleted `/quality/*`, its standalone analyzer, and quality-only tests after a live cross-repo search found no consumers. Heber and Orion keep their own quality checks.
- **Dead rate-limit knobs removed after the Ponytail over-engineering audit**: deleted the unused `rate_limit_enabled` setting and unused `max_connections_per_ip` middleware constructor argument. The active global and per-client rate-limit middleware remains mounted.

### Security

- **Admin IP blocklist is now actually enforced** (`gateway/api/admin.py`, `gateway/api/middleware/global_ratelimit.py`): `POST /api/v1/admin/security/blocklist` wrote the IP into a metadata-only dict that nothing on the request path ever read — so "blocking" an IP did nothing. The `GlobalRateLimitMiddleware` (which *does* enforce a blocklist at request time) now registers a process-wide singleton, and the admin endpoint pushes the block into it via `block_ip(...)` with the requested duration. The response now reports whether the block was actually `enforced`.
- **Over-long request URLs now rejected with 414** (`gateway/api/middleware/validation.py`): the `url_max_bytes` (8 KB) limit was defined but never enforced, so a request target (path + query string) of unbounded size was parsed before any per-route check ran. `InputValidationMiddleware` now rejects an over-long target with 414 before route/query parsing. (The related symbol-list count cap the audit flagged was already enforced per-client via `_enforce_symbol_limits` — every client is capped at ≤1100 — so no additional count cap was needed.)
- **UW provider symbol path-interpolation hardened against SSRF** (`gateway/providers/uw/options.py`, `gateway/providers/uw/flow.py`): a few UW endpoints build their upstream URL by f-string-interpolating a caller-supplied symbol/sector directly into the path (`/api/stock/{symbol}/iv-rank`, `/api/etf/{symbol}/tide`, `/api/market/{sector}/sector-tide`). A symbol containing path separators could inject extra path segments and steer the gateway's privileged UW key at a different upstream endpoint. Those segments are now percent-encoded via `urllib.parse.quote(..., safe="")` (the `/` becomes `%2F`), matching the existing Massive-provider pattern; SDK-based calls were already safe.
- **All gateway client keys rotated to SHA-256 hashes; fine-grained `trading` capability added** (`config/clients.yaml`, `gateway/core/auth.py`, `gateway/api/deps.py`): every real client (`cerberus`, `3roses`, `orion`, `atlas`, `orbit`, `kairos`, `heber-watch`) now authenticates via `key_hash: sha256:...` instead of a committed plaintext key (only the powerless `test` fixture client keeps a plaintext key; it has role `client` and no trading/admin rights). Key comparison uses `hmac.compare_digest`. Order- and position-mutating Alpaca routes (`POST/PATCH/PUT/DELETE` under `/orders`, `/positions`, `/account/configurations`, `/watchlists`) now require an explicit `trading: true` permission in addition to the trader role — granted only to the order-placing systems `kairos`, `cerberus`, `3roses`, and `orion` (orion places orders through the gateway). Trader-role clients without the capability (`atlas`, `orbit`, `heber-watch`) get a 403 `GW-E2009` on those routes, so a data/research client can no longer move money or alter broker account settings.

### Added

- **Dedicated behavioural unit tests for the streaming and request-dedup core modules** (`tests/test_stream.py`, `tests/test_dedup.py`): added focused tests that exercise real branches not covered by the existing stream/dedup suites. `test_dedup.py` covers the error path (a failing fetcher propagates the same exception to every coalesced waiter and the pending slot is released so failures are not cached), in-flight pending-count tracking, sequential re-fetch after completion, the `dedup_rate` ratio and zero-division guard, the `lock_stripes` floor, and the module singleton. `test_stream.py` covers `AlpacaStreamType` feed/endpoint/instrument-type routing, the subscription-request OCC symbol-shape filtering (the 2026-05-01 RCA where wrong-shape symbols silently kill the whole upstream subscribe) and the options-stream "no bars" rule, and fallback-fanout client isolation (a raising or hung client must not block delivery to the other subscribed clients).

### Changed

- **`.gitignore` now covers generated artifacts by glob, not just by exact filename** (`.gitignore`): added `*.scip` (alongside the existing `index.scip`), `*.coverage` (alongside `.coverage`/`.coverage.*`), and `gateway_errors_*.log` (alongside the existing `gateway_errors_24h.log`) so freshly-named coverage dumps, SCIP indexes, and rotated gateway error logs stay untracked instead of leaking into commits. No previously-tracked files needed untracking — `git ls-files` listed no `*.scip`, `.aider*`, or `*.log` artifacts. Note: the tracked `PRD.md` (~174KB) and `unusualwhales_openapi.yaml` (~538KB) are git-LFS candidates (left in place — referenced by other docs/tooling), and `CHANGELOG.md` (~211KB) is a future archive candidate.

- **`DataProvider.get_quotes` multi-symbol error contract is now documented and uniformly enforced** (`gateway/core/provider.py`, `gateway/providers/finnhub.py`, `gateway/providers/alphavantage.py`, `tests/test_finnhub_provider.py`, `tests/test_alphavantage_provider.py`): the per-symbol failure behavior of the multi-quote path was undocumented and looked divergent between providers. The contract is now stated on the ABC method docstring — per-symbol failures are skipped and partial results returned, an empty input returns `[]`, and total failure (every symbol fails) returns `[]` rather than raising. Both the Finnhub and Alpha Vantage `get_quotes` implementations already behaved this way; conformance tests were added per provider to pin the partial-result, total-failure, and empty-input cases so the contract can't silently drift.

- **Cache read/write failures are now logged and counted separately from misses** (`gateway/api/middleware.py`, `gateway/core/metrics.py`): the response-cache middleware swallowed both cache-get and cache-set exceptions at DEBUG, so a degraded or saturated cache backend silently slowed every request to an upstream call with nothing in the error logs. Both handlers now log `cache_read_error` / `cache_write_error` at WARNING and increment `gateway_cache_errors_total{cache_type,operation}`. The read path additionally records a cache miss on the error branch (which previously recorded neither hit nor miss), keeping the hit-rate denominator honest while preserving an alertable backend-error signal.

- **Redis pool size can be validated against the data_sink worker count** (`gateway/core/cache.py`, `gateway/core/redis_sink.py`): the dedup `RedisCache` and the `RedisStreamsSink` both build a `BlockingConnectionPool`, while the data_sink registry runs `data_sink_worker_count` concurrent publishers/dedup callers against them — a pool smaller than the worker count serializes workers and contributed to the "Too many connections" flood. Both constructors now accept an optional `worker_count`; when provided, the pool size is raised to at least the worker count (logging a warning if it was smaller) so the two can no longer silently mismatch. The relationship is documented inline in both constructors. Default behavior is unchanged when `worker_count` is omitted.

- **API keys rotated to hashed storage** (`config/clients.yaml`, `gateway/core/auth.py`): the seven plaintext gateway client keys (cerberus, 3roses, orion, atlas, orbit, kairos, heber-watch) were rotated to fresh strong values and their `clients.yaml` entries converted from plaintext `key:` to `key_hash: sha256:<hex>`, so a leaked clone/branch/CI artifact no longer exposes a working key. Key verification now uses `hmac.compare_digest` against stored hashes to remove a timing side-channel. The `test` client stays enabled (it is the test-suite fixture credential) but is powerless — role `client`, plaintext key, no trading/admin rights. The new key material and old→new mappings are recorded in the gitignored `config/ROTATION_HANDOFF.md`; consumers must pick up their new keys and the gateway must be reloaded for the new hashes to take effect.

- **Fine-grained `trading` capability required for Alpaca state mutations** (`gateway/core/auth.py`, `gateway/api/deps.py`, `config/clients.yaml`): mutating Alpaca routes (POST/PATCH/PUT/DELETE under `/api/v1/alpaca/orders`, `/positions`, `/account/configurations`, and `/watchlists`) now require a new `trading: true` permission in addition to the trader role. A trader-role client lacking the capability gets `403 GW-E2009` on those routes while keeping read-only access to account/clock/calendar/positions/portfolio/watchlists. `kairos`, `cerberus`, `3roses`, and `orion` are granted the capability (they place orders through the gateway); the other trader clients (atlas, orbit, heber-watch) are read-only for trading.

- **Envelope-wrap failures are now loud and never ship malformed keys** (`gateway/core/envelope.py`, `gateway/api/middleware/envelope.py`, `gateway/config.py`, `tests/test_envelope.py`, `tests/test_middleware_streaming.py`): on a `wrap_event` failure the lenient path previously returned a fallback envelope with `instrument_key="unknown:{symbol}"` and continued — Heber's writer-side validator rejects `unknown:` keys, so those records dropped silently on Bronze→Silver, and the strict-mode check that should have surfaced the failure was wrapped in a `try/except` that silently swallowed when settings were unavailable. `wrap_event` now always increments an alerting counter (`record_message_dropped(reason="envelope_wrap_error")`) on failure and never returns an `unknown:`/malformed envelope; option-shaped payloads that would produce `option:{symbol}` without an `OCC:` contract are rejected too. It raises instead, so the single bad event is dropped loudly while live callers degrade gracefully without crashing. The REST envelope middleware now also honors `strict_envelopes=True` instead of swallowing its own re-raise and returning an unwrapped 200, and list/symbol-keyed response envelopes use a bounded `aggregate:{provider}:{feed}` key instead of trying to infer one option contract for an aggregate payload. The silent settings-unavailable swallow is removed. `strict_envelopes` (default `False`) now controls only the error surface — `True` propagates the original exception, `False` raises a tagged `EnvelopeWrapError`. New tests cover the counter increment, strict/lenient raise behavior, settings-unavailable raise, malformed option-key rejection, strict middleware surfacing, aggregate response keys, and that no `unknown:` envelope is ever returned.

- **Option-capture quality metrics no longer carry a per-symbol Prometheus label** (`gateway/core/metrics.py`, `tests/test_metrics.py`): `record_option_capture_symbol_metrics` previously created per-SYMBOL gauge series (`gateway_option_capture_symbol_contracts`, `gateway_option_capture_quality_ratio`, `gateway_option_capture_snapshot_age_seconds`), so Prometheus cardinality grew unbounded as the tracked option universe expanded. The per-symbol gauges are replaced with bounded aggregates: histograms over symbols for contracts, coverage ratios, and snapshot age (exposing P50/P95/P99 via Prometheus quantiles plus a running sum/count) and a single `gateway_option_capture_symbols_tracked` gauge. Per-symbol detail remains available via the in-memory admin snapshot (`get_option_capture_snapshot`); the caller signature is unchanged.

- **The 1,496-line `gateway/api/middleware.py` module is now a package** (`gateway/api/middleware/`): the monolithic module was split into one module per middleware — `metrics.py` (`RequestMetricsMiddleware`), `validation.py` (`InputValidationMiddleware`), `ratelimit.py` (`RateLimitBucket`, `RateLimitMiddleware`), `cache.py` (`CacheEntry`, `CacheMiddleware`), `envelope.py` (`EventEnvelopeMiddleware` and its route-mapping constants), `security_headers.py` (`SecurityHeadersMiddleware`), and `global_ratelimit.py` (`IPConnectionTracker`, `GlobalRateLimitMiddleware`). The package `__init__.py` re-exports every public class and helper, so existing imports (`from gateway.api.middleware import CacheMiddleware`, etc.) are unchanged. This is a pure structural move — no middleware behavior changed.

### Fixed

- **Low-priority REST envelope publishing no longer crowds the live Heber stream** (`gateway/config.py`, `gateway/api/middleware/envelope.py`, `gateway/core/data_sink.py`, `gateway/core/metrics.py`, `gateway/api/health.py`, `gateway/api/admin.py`, `config/prometheus_alerts.yml`, `tests/test_rest_sink_gating.py`, `tests/test_data_sink.py`, `tests/test_config.py`, `tests/test_metrics.py`, `tests/test_health.py`, `tests/test_admin_status.py`): REST responses still return normal EventEnvelope-wrapped HTTP bodies, but the background sink publish path now skips high-volume UW REST feeds already owned by live pollers (`greek_exposure`, `iv_rank`, `iv_term_structure`, `short_data`, `ftd`, `flow_alerts`, `darkpool`). Alpaca REST bars/trades remain allowed when the sink queue is healthy. The middleware also checks the optional `DataSinkRegistry.can_accept_low_priority()` hook before scheduling low-priority REST publishes and sheds them at or above the configured queue-utilization threshold instead of adding pressure to `heber:events`. That pressure check now counts queued plus worker-in-flight publishes over `queue_size + worker_count`, and rejects low-priority work when the target sink is missing or disabled. Sink producer-timeout CRITICAL logs now include event identifiers (`event_id`, `feed`, `provider`, `instrument_key`), queue/backpressure snapshots are exposed in health and admin status, and Prometheus tracks queue utilization, per-feed/source producer drops, and low-priority REST shedding.

- **UW EOD poller run state now survives Gateway restarts** (`gateway/core/uw_eod_state.py`, `gateway/core/uw_poller.py`, `gateway/config.py`, `tests/test_uw_eod_state.py`, `tests/test_uw_poller.py`): the EOD snapshot poller now writes a small atomic JSON state file under `/app/logs/state/uw_eod_state.json` when it claims a trading-day run and marks it completed after the endpoint sweep finishes. The claim path uses an exclusive sibling lock file around the read-check-write decision, so two Gateway processes cannot both claim the same trading day. Same-day completed runs and non-stale active claims are skipped after restart and during scheduler checks, while stale running markers can be retried after the configured timeout.

- **`GET /alpaca/orders` now rejects naive or inverted `after`/`until` windows** (`gateway/api/alpaca/trading.py`, `tests/test_alpaca_trading_router.py`): a timezone-naive `after`/`until` was silently serialized as UTC, shifting the `submitted_at` window for any caller intending a different zone and quietly missing fills; an `after` later than `until` produced a degenerate window. Both are now rejected up front with `400 GW-E4007` and never reach the broker. Valid tz-aware pairs pass through unchanged.

- **Re-subscribing the same UW flow symbol at the subscription cap is now idempotent** (`gateway/api/websocket.py`, `tests/test_websocket_flow_routing.py`): the WS subscribe quota was computed from the caller's `flow:<symbol>` alias, but subscriptions are stored under the canonical `flow_alerts:<symbol>` key, so an idempotent re-subscribe looked like a brand-new slot and was wrongly rejected at the cap. The quota now normalizes flow feeds to the stored key, so re-subscribing a symbol you already hold is free.

- **Empty-symbols UW flow unsubscribe clears all flow accounting** (`gateway/api/websocket.py`, `tests/test_websocket_flow_routing.py`): a firehose (empty-symbols) unsubscribe drops every flow bucket in the fan-out, but connection accounting only removed the `flow_alerts:*` sentinel — leaving stale `flow_alerts:<symbol>` entries from earlier per-symbol subscribes that double-counted against the quota and misreported status. The empty unsubscribe now clears all `flow_alerts:*` and `flow_alerts:<symbol>` entries, mirroring the fan-out's full drop.

- **Redis batch publishing no longer crashes on `CancelledError` gather results** (`gateway/core/redis_sink.py`, `tests/test_redis_sink.py`): `asyncio.gather(..., return_exceptions=True)` can return `CancelledError`, which inherits from `BaseException` rather than `Exception`. The batch and indexed-batch result loops treated only `Exception` as a failure, then tried to add or iterate the `CancelledError`, crashing the caller instead of counting the chunk as failed. Both paths now treat any `BaseException` result as a failed chunk and record failed publish metrics for its messages.

- **WebSocket broadcasts serialize Decimal-bearing flow envelopes** (`gateway/core/connections.py`, `tests/test_flow_fanout.py`): the UW flow fan-out can route envelopes whose payloads include `Decimal` values through `ConnectionManager.broadcast_to_connection_ids`; the shared broadcast serializer used bare `orjson.dumps`, which raises on `Decimal` before any send occurs. Broadcast serialization now mirrors the Redis sink's JSON fallback with `default=str`, so real flow envelopes can be delivered over WebSockets.

- **UW poller marks dedup state only after confirmed publish success, retries transient EOD fetch failures, and produces content-stable insider/congress event IDs** (`gateway/core/uw_poller.py`, `tests/test_uw_poller.py`): five silent-loss bugs in the Unusual Whales poller. (1) `_publish_envelopes` marked every event as seen *before* publishing, so when a batch publish raised (e.g. a Redis pipeline failure) the events were already deduped and a later cycle could never re-send them — permanent loss if the sink's failed-event buffer also failed. It now publishes first and marks only exactly-confirmed successes via the indexed batch API; if only a count-only partial result is available, it marks none rather than guessing the first N and risking permanent suppression of an un-published event. Content-derived event IDs keep Heber-side dedup effective on any re-publish. (2) The EOD `congress_trades` and `insider_trades` fetches had no retry — `congress_trades` returned zero rows for all of 2026-06-10 after a single upstream 503. Both fetches now retry transient upstream errors (5xx, network resets) with bounded exponential backoff. (3) `congress_trades`/`insider_trades` payloads carry no event-time field, so `wrap_event` stamped `ts_event=now()` and the same trade re-fetched on a later run got a fresh `event_id`, producing duplicate Bronze rows (observed 2026-06-09). The poller now derives a stable `timestamp` from the filing date (fallback transaction date) before wrapping, so re-published rows reuse the same content-stable `event_id`. (4) `historic_option_volume` rows carry an `expiry` but are per-underlying analytics, not OCC contracts; the poller now uses the same equity instrument override as `iv_term_structure`, avoiding malformed `option:{ticker}` keys. (5) strict-envelope failures now skip only the malformed UW record instead of aborting the whole fetched batch. New tests pin the publish-then-mark ordering, the no-mark-on-failure re-publish path, ambiguous partial-batch behavior, exact indexed successes, transient-fetch retry, cross-run event-id stability, per-underlying option-volume keys, and per-record skip behavior.

- **UW `/{ticker}/spot-exposures` now returns the real per-strike greek exposures instead of `data:[]`** (`gateway/providers/uw/options.py`, `gateway/providers/uw/_base.py`, `tests/test_uw_provider.py`): `GET /api/v1/uw/{ticker}/spot-exposures` silently returned an empty list on every call even though UW returns full data, verified against the live API on 2026-06-09. The generated SDK model `SpotGreekExposuresByStrike` is a *single* strike-row shape, but the endpoint returns `{"data":[…many strike rows…]}`; the SDK's `_parse_response` wraps the list and calls `from_dict`, which finds none of its declared fields at the top level and dumps the whole `{"data":[…]}` into `additional_properties` — producing a model with **no `.data` attribute**. The provider read the rows via `_get_data_safe` (which only checks `.data`), got `None`, and returned `[]`. It now reads via `_extract_data` (the same helper `get_greek_exposure` already uses successfully, which checks `additional_properties['data']` first). Separately, the per-strike output keys were wrong — the old mapping looked for `gamma_exposure`/`gex`/`call_volume`/`put_volume`/`call_oi`/`put_oi`, none of which this endpoint returns, so even a correct parse would have yielded all-`None` rows. The mapping now emits the real UW fields (`strike`, `price`, `date`, `timestamp`, and `call`/`put` × `gamma`/`charm`/`vanna`/`delta` × `oi`/`vol`), coerced to float via a new `_safe_float` helper. Verified end-to-end against live SPY data: 50 strike rows, all populated. New test reproduces the broken single-row model shape and pins the field mapping. (Orion had already sidestepped this by repointing its greek_exposure connector to the working `/api/v1/uw/gex/{ticker}` route in commit 45f27ac, so this was not blocking Orion — but the `/spot-exposures` route is now fixed for any other consumer.)

- **Streaming option quotes now build a valid `option:OCC:...` instrument key** (`gateway/core/envelope.py`, `tests/test_envelope.py`): the high-frequency streaming path `fast_wrap_streaming_event` built option keys as `option:{symbol}` (e.g. `option:QQQ260609C00690000`, no `OCC:` infix) — a *separate* code path from `wrap_event`. Heber's writer-side validator requires `option:OCC:[A-Z]{1,6}\d{6}[CP]\d{8}`, so 100% of Alpaca OPRA option quotes/trades were rejected into `heber:events:dlq`. On 2026-06-09 this flooded the DLQ to 767K+ entries at hundreds/sec, drove the single `heber-writers` consumer ~300K messages behind, and (because the stream is MAXLEN-capped) evicted other feeds' un-consumed records before they could be written — surfacing as Heber "feed appears dark" liveness alerts for darkpool and flow_alerts. The option branch now passes `contract_symbol=symbol` to `make_instrument_key` (OPRA sends the OCC contract as the symbol), yielding `option:OCC:{symbol}`. The OPTIONS-stream subscription filter already admits only `[A-Z]{1,6}\d{6}[CP]\d{8}`-shaped symbols, so every streamed option now produces a Heber-valid key. New parametrized test covers 1–6 char roots and index weeklies (SPXW). NOTE: this stops *new* rejections; draining the existing DLQ backlog and recovering consumer lag are operational (Heber-side).

- **UW darkpool, insider, and short-volume feeds now actually store data** (`gateway/providers/uw/flow.py`, `gateway/providers/uw/institutional.py`, `gateway/providers/uw/market.py`, `tests/test_uw_provider.py`): three Unusual Whales feeds were silently publishing ~zero records to `heber:events` (with `errors=0`, so they never surfaced in error logs), confirmed against the live UW API on 2026-06-08. **Darkpool** (`get_darkpool_recent`) read trades from `response.additional_properties['data']`, but the SDK puts them on `DarkpoolTradeResponse.data` — so the primary path returned `[]` on every call and trades were only ever captured by the raw-HTTP fallback (which fires only on an SDK exception); Jun 8 logs showed 726 SDK calls = 0 trades vs 15 fallback calls = 100% of the data, and full-day zeros on quiet-network days. It now reads via `_extract_data` (both shapes). **Insiders** (`get_insiders`) called `market.get_insider_trades` = `/api/market/insider-buy-sells`, a market-wide aggregate whose rows have no `ticker`/`owner_name`/`transaction_code`, so 100% were dropped by the null-field filter (`skipped=200 kept=0` daily); it now calls `insider.get_transactions` = `/api/insider/transactions`. **Short volume** (`get_short_volume`) returned rows under the `si` key (fields `market_date`/`short_volume`/`short_volume_ratio`), but the provider read `.data`/`['data']` via `_extract_data` and the old `date`/`short_ratio` field names — yielding `[]` every call; it now reads the `si` key with the correct field names. Three new tests reproduce each real response shape.

- **UW options-flow envelopes now build a valid `option:OCC:...` instrument key** (`gateway/core/envelope.py`, `tests/test_envelope.py`): flow-alert (and `flow`) events were published to `heber:events` with `instrument_key=option:{symbol}` (e.g. `option:SPX`) because `wrap_event` only looked for the OCC contract under `contract_symbol`/`contract`, while UW carries it under `option_chain`. Heber's writer-side validator requires `option:OCC:[A-Z]{1,6}\d{6}[CP]\d{8}`, so every flow alert failed `is_valid_instrument_key()` and was dropped before Bronze. `wrap_event` now falls back to `option_chain` when building the key, so `option_chain=SPX260918P07500000` yields the valid `option:OCC:SPX260918P07500000`. Verified against 8/8 live flow alerts (previously 0/8 valid). New test pins the OCC-key construction.

- **`position_intent` now applied to stop and stop-limit orders** (`gateway/providers/alpaca/trading.py`, `tests/test_alpaca_trading_position_intent.py`): The `create_order` reduce-only safety field (`buy_to_open` / `buy_to_close` / `sell_to_open` / `sell_to_close`) was validated and forwarded only on market and limit orders — it was silently dropped when building `StopOrderRequest` and `StopLimitOrderRequest`. A caller setting `sell_to_close` on a stop or stop-limit order believed the order was reduce-only, but Alpaca received no intent and could open or extend a position (e.g. convert an intended close into a naked short). Both stop request types now forward `position_intent` exactly as market/limit do. New parametrized tests assert propagation across all four order types and all four `PositionIntent` values.

- **Data-sink dedup Redis checks now use a bounded blocking pool** (`gateway/core/cache.py`, `gateway/main.py`, `tests/test_cache.py`, `tests/test_config.py`): The prior fail-open/throttle fix made `redis_cache_set_nx_error` non-destructive and non-flooding, but the high-volume dedup cache still created its Redis client with redis-py's default unbounded `ConnectionPool` (`max_connections=2147483648` in local redis 7.1.1). During opening-bell bursts, every stream envelope performs a dedup `SET NX` before entering the bounded sink queue, so the dedup layer could fan out Redis connections independently of the `RedisStreamsSink` worker pool and trigger the `"Too many connections"` spike seen June 1-5. `RedisCache` now accepts optional bounded-pool settings and uses `BlockingConnectionPool` when provided; the data-sink dedup cache is wired to `data_sink_redis_pool_size` and `data_sink_operation_timeout_seconds`, matching the stream sink's backpressure model instead of bypassing it.

- **Dedup-cache backend errors no longer silently drop events to Heber** (`gateway/core/cache.py`, `gateway/core/data_sink.py`, `tests/test_cache.py`, `tests/test_data_sink.py`): When the sink dedup Redis pool was saturated, `RedisCache.set_nx` caught the error, logged `redis_cache_set_nx_error`, and returned `False` — indistinguishable from "key already exists". `DataSinkRegistry.publish_all` then treated `False` as a confirmed duplicate and `return`ed without publishing, so every event whose dedup check errored was silently classified as a duplicate and never reached the `heber:events` stream. The `dedup_cache_error` fail-open branch was dead code because `set_nx` never re-raised. This week's logs showed up to ~197K `redis_cache_set_nx_error` (`"Too many connections"`) per day starting 2026-06-01 — roughly that many events/day potentially lost to the lakehouse, mislabeled as duplicates. `set_nx` now returns a three-state verdict: `True` (newly set) / `False` (confirmed duplicate) / `None` (backend unavailable — could not determine); closed/teardown caches return `None` instead of a false `False`. `publish_all` skips only on a confirmed `False` and fails open (publishes) on `None`. New tests pin all three set_nx verdicts and the publish_all fail-open-vs-skip-duplicate behavior.

- **High-frequency error/critical logs throttled to one per minute** (`gateway/core/log_throttle.py`, `gateway/core/cache.py`, `gateway/core/data_sink.py`, `tests/test_log_throttle.py`, `tests/test_cache.py`, `tests/test_data_sink.py`): The `redis_cache_set_nx_error` WARNING (above) and the `data_sink_producer_timeout_drop` CRITICAL page were each emitted once per affected event — 197,760 set_nx warnings (~95% of the entire error log) and 5,638 CRITICAL pages in a single day this week (2026-06-05, to 14:30) — burying genuine errors and storming the pager. A new `LogThrottle` collapses repeats of the same key to one log per 60s and reports a `suppressed_since_last` count so the hidden volume stays visible. set_nx errors are throttled per exception type; producer-timeout drops per sink. Every drop is still counted in `gateway_sink_producer_timeout_drops_total` and the publish stats — only the log/page is throttled, never the accounting. (The root trigger — why the dedup pool reports `"Too many connections"` under load when current code builds an unbounded client pool on redis-py 7.1.1 — still needs confirming against the live container's deps/runtime env; these fixes make both failure modes non-destructive and non-flooding regardless of that answer.)

### Added

- **`FeedZeroEnvelopesMarketHours` Prometheus alert** (`config/prometheus_alerts.yml`): warns when a continuously-polled feed (`flow_alerts`, `darkpool`) creates zero `EventEnvelope`s over a 15-minute window while the US market is open — a silent data-loss signal that the upstream endpoint is failing, the poller stalled, or normalization is dropping every record (as on the 2026-06-09 malformed-option-key incident). The rule reads the existing per-feed `gateway_envelopes_created_total{provider,feed}` counter and gates on a conservative 15:00-20:00 UTC weekday window to avoid DST edges. It can only fire for feeds whose metric series already exists (i.e. produced at least once historically).

- **Envelope→Heber instrument-key contract test** (`tests/test_envelope_heber_contract.py`): a dedicated regression net for the three prior option-key DLQ incidents (streaming OPRA quotes, UW flow alerts, per-underlying analytics — all once emitted malformed `option:{symbol}` keys with no `OCC:` infix that Heber's writer-side validator rejects, dropping 100% of those records). The test mirrors Heber's five instrument-key validators (`equity`/`crypto`/`forex`/`option`/`macro`) verbatim and asserts the `instrument_key` produced by both `wrap_event` and `fast_wrap_streaming_event` validates for representative payloads of *every* feed in `FEED_UNIQUE_FIELDS` and every instrument type. Payloads and override kwargs mirror the real gateway call sites (UW flow's OCC contract in `option_chain`, the `iv_term_structure`/`historic_option_volume` equity overrides, the treasury poller's `macro:` overrides), so the contract tests what the gateway actually ships. A guard test fails if a new feed is added to `FEED_UNIQUE_FIELDS` without a case, and a negative control asserts the historical bug shape (`option:` without `OCC:`) fails validation — proving the net has teeth. Test-only; no production code changed.

- **Real-Redis integration test tier** (`.github/workflows/ci.yml`, `tests/integration/test_redis_sink_integration.py`, `tests/conftest.py`): a new `integration-tests` CI job spins up a Redis 7 service container and runs `pytest -m integration` against it, exercising the `RedisStreamsSink` and dedup cache against a live Redis instead of a mock — publish → `XLEN` increment, `set_nx` dedup first-wins + TTL, and the failed-event buffer fill → evict → drain path (plus reconnect-drain). Tests target `GATEWAY_TEST_REDIS_URL` (default `redis://localhost:6379/15`, flushed before/after each test) and `pytest.skip` gracefully when no Redis is reachable, so the default local run is never broken. Additive `redis_probe` / `redis_sink` / `redis_cache` fixtures were added to `conftest.py`.

- **New `massive` provider for historical OHLCV bars** (`gateway/providers/massive.py`, `config/providers.yaml`, `tests/test_massive_provider.py`): adds Massive (formerly Polygon.io) as a data provider exposing `get_bars` against the aggregates endpoint (`/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`), mapping Massive's `o/h/l/c/v/vw/t/n` fields to `NormalizedBar` and following `next_url` pagination. Reads the API key from `MASSIVE_API_KEY`; supports an `adjusted` kwarg (defaults `True`) so raw unadjusted bars can be requested. Concurrency is bounded (default 2) and an optional `min_request_interval_seconds` paces requests globally (set ~12s for the free tier). Hardened for use as a trading data source: `get_bars` **fails loud** — auth/rate-limit/5xx/malformed-payload errors propagate instead of being silently returned as empty bars; `next_url` pagination is host-pinned to `base_url` (the Bearer token is never sent off-host) and raises on exceeding `max_pages` or detecting a loop rather than truncating history; tickers are validated and URL-encoded before path interpolation; malformed rows raise instead of fabricating values (e.g. a missing volume never becomes `0`); naive datetimes are rejected; and `health_check` inspects the provider JSON `status`. Tests use `httpx.MockTransport` to cover auth headers, params, pagination cap/loop, hostile `next_url`, error status, and malformed rows. The provider loads and is directly callable but is intentionally **not** wired into the live `stocks` route — it will not serve trading traffic until verified against a real key. Targeted at building a survivorship-free historical equity dataset (delisted-ticker and flat-file bulk download to follow).

- **`POST /api/v1/alpaca/orders` accepts an optional `position_intent`** (`gateway/api/alpaca/trading.py`, `gateway/providers/alpaca/trading.py`, `tests/test_alpaca_trading_router.py`, `tests/test_alpaca_trading_position_intent.py`): additive query param (`buy_to_open` / `buy_to_close` / `sell_to_open` / `sell_to_close`) threaded from the endpoint into the Alpaca `MarketOrderRequest`/`LimitOrderRequest`. Lets callers force reduce-only semantics so a close is never converted into an opening (e.g. naked short) position — the counterpart to the related 2026-05-29 flood where ~3,235 close orders/day were rejected as cash-secured-put opens (`40310000`) because the caller had no live long and sent no intent. Invalid values raise a 400. Omitted → `None` (unchanged behavior).

- **Trading-route authorization characterization tests** (`tests/test_alpaca_trading_router.py`): a regression net pinning today's observable authz behavior on the Alpaca trading endpoints before a finer trading permission lands. Exercised through the full FastAPI stack (auth → middleware → router) against a real authenticator with two clients: a granted trader (role `trader` plus a forward-compatible `trading` permission marker) CAN place / cancel / close orders (mocked provider, 200 success shapes pinned), and a clearly-denied `client`-role key gets 403 `GW-E2008` on the write endpoints with the provider never reached. The granted client carries both a trader role and a `trading` marker so the success assertions stay valid after the permission gate is tightened. Test-only; no production code changed.

### Removed

- **Dead nested `trading-bot/` project removed** (`trading-bot/`, `pyproject.toml`): the abandoned standalone trading-bot application (23 files with its own README and requirements) lived inside the Data-Gateway repo and is not part of the gateway. Deleted along with the now-unneeded ruff `exclude = ["trading-bot"]`.

- **Dead legacy provider modules removed** (`gateway/providers/uw_legacy.py`, `gateway/providers/alpaca_legacy.py`): the pre-split monolithic UnusualWhales and Alpaca provider implementations were superseded by the `gateway/providers/uw/` and `gateway/providers/alpaca/` packages that `config/providers.yaml` and `gateway/providers/__init__.py` actually load. Nothing imported the legacy modules; they are deleted.

### Fixed

- **Alpaca order rejection logs now identify the caller and order shape** (`gateway/api/alpaca/common.py`, `gateway/api/alpaca/trading.py`, `tests/test_alpaca_common.py`, `tests/test_alpaca_trading_router.py`): This week's logs showed 10,566 rejected `POST /api/v1/alpaca/orders` attempts, but the existing `provider_request_failed` records did not include the authenticated `client_id`, symbol, side, order type, time-in-force, or idempotency source. `execute_alpaca_provider_call` now accepts optional `log_context`, and `create_order` supplies those RCA fields so the next invalid-order flood can be traced to the responsible trading system without guessing.
- **Option contracts sent to the Alpaca stock-bars route now fail before calling Alpaca** (`gateway/api/alpaca/stock.py`, `tests/test_alpaca_stock_router.py`): The past-week error logs had 1,130 `alpaca_bars_error` / `provider_request_failed` records from OCC option symbols like `CLF260717C00013000` being sent to `/stocks/{symbol}/bars`. The stock-bars route now recognizes option contracts and returns a local 400 `GW-E4007` that points callers to `/api/v1/alpaca/options/{contract}/bars`, preventing a guaranteed upstream 400 and reducing warning-log noise.
- **Alpaca provider-layer 4xx errors no longer flood the ERROR log** (`gateway/providers/alpaca/market.py`, `tests/test_alpaca_provider.py`): `get_bars` and `get_quotes` logged `alpaca_bars_error` / `alpaca_quotes_error` at ERROR for *any* upstream `HTTPStatusError`, including client-caused 4xx — e.g. requesting an index symbol like `SPX` from `/v2/stocks/bars` returns 400, and a single misconfigured client produced 4,052 ERROR-level `alpaca_bars_error` lines in one ~7h window (24h error-log snapshot). These are not retried (`http_retry` only retries 429/502/503/504), so the volume was real client traffic and the wrong severity buried genuine 5xx upstream failures in the WARNING+ errors log. Both call sites now mirror the API-layer convention already in `gateway/api/alpaca/common.py`: 4xx → `logger.warning`, 5xx → `logger.error`. Four new parametrized tests in `tests/test_alpaca_provider.py` pin the split for both methods. (The same anti-pattern exists across ~40 sites in `alphavantage.py`/`finnhub.py` but is not flooding; deferred to a dedicated follow-up PR rather than swept here.)
- **Gateway-side guards for 2026-05-21 error-log findings** (`gateway/api/alpaca/trading.py`, `gateway/providers/uw/institutional.py`, `gateway/config.py`, `gateway/core/stream.py`, `tests/test_alpaca_trading_router.py`, `tests/test_uw_provider.py`, `tests/test_config.py`): `DELETE /api/v1/alpaca/positions/{symbol}` now rejects negative `qty` values before calling Alpaca, preventing caller bugs like `qty=-8000.0` from reaching the broker. UW Congress EOD polling now reads the raw `/api/congress/recent-trades` payload instead of the generated SDK parser, so unexpected `txn_type` strings are preserved and published instead of failing the whole feed. Data-sink defaults were raised for opening-bell load (`data_sink_queue_size=16384`, `data_sink_worker_count=16`, `data_sink_redis_pool_size=32`) after today's logs showed producer-timeout drops and Redis connection pressure on `heber:events`. Stream handling now skips validator lookup only when there are neither downstream clients nor sink side effects, preserving production validation while avoiding pointless work for discarded messages.

### Added

- **`DataSinkRegistry.publish_all_batch_results` / `RedisStreamsSink.publish_batch_results` return per-message booleans** (`gateway/core/data_sink.py`, `gateway/core/redis_sink.py`, `tests/test_batch_publish_partial_failure.py`): A new batch-publish primitive that returns ``list[bool]`` aligned 1:1 with the input messages, so callers can tell exactly which entries succeeded under partial pipeline failure. The legacy ``publish_all_batch`` / ``publish_batch`` (int return) is preserved for back-compat but now delegates to the new method and is documented as unsafe for dedup-marking under partial failure.

### Fixed

- **Date-based feeds now hash event time, not wall-clock time, into `event_id`** (`gateway/core/envelope.py`, `gateway/core/treasury_poller.py`, `tests/test_envelope_date_stability.py`): `wrap_event` extracted ``ts_event`` from only ``timestamp`` / ``t`` / ``published_at`` / ``datetime`` fields, then fell through to ``datetime.now(UTC)`` if none matched. Date-based feeds (``iv_rank`` -- no time field, ``congress_trades`` with ``transaction_date``, ``insider_trades`` with ``filing_date`` / ``transaction_date``, ``treasury_yields`` with ``date``) hashed the wall-clock time into their ``event_id`` -- so retries and replays produced a *different* id each time, defeating Heber's three-layer dedup. Two changes: (1) extended the field list ``wrap_event`` checks to include ``date``, ``transaction_date``, ``filing_date``, ``report_date``, ``effective_date``, ``period_end_date`` (YYYY-MM-DD strings are promoted to midnight-UTC datetime); (2) added an explicit ``ts_event_override`` kwarg for callers that already have the canonical timestamp in hand. The treasury poller now passes its parsed-from-date ``ts_event`` via ``ts_event_override`` instead of patching ``envelope["ts_event"]`` *after* the event_id hash was computed (the previous code path silently produced a fresh id on every poll). Regression pinned by 9 tests in `test_envelope_date_stability.py`: same-input event_id stability for iv_rank (via override), congress_trades (via transaction_date), insider_trades (via filing_date), treasury_yields (via date), plus naive-datetime UTC promotion and date-object handling.

- **UW `historic_option_volume` envelopes now carry a valid `equity:{TICKER}` instrument_key** (`gateway/core/uw_poller.py`, `tests/test_uw_eod_instrument_keys.py`): Each per-expiry row in `historic_option_volume` carries an `expiry` field (volume bucketed per expiration of the *underlying*). `_infer_instrument_type` in `gateway/core/envelope.py` flagged any payload with `expiry` as `instrument_type=option` and built `instrument_key=option:{ticker}` -- a malformed OCC key (no contract suffix). Heber's writer-side `is_valid_instrument_key()` rejected 100% of rows on Bronze→Silver normalization. Fix mirrors `_poll_eod_iv_term_structure` (already correct): pass `instrument_type_override="equity"` + `instrument_key_override=f"equity:{ticker.upper()}"` to `wrap_event()`. Audited all per-underlying EOD pollers (`_poll_eod_greek_exposure`, `_poll_eod_iv_rank`, `_poll_eod_oi_change`, `_poll_eod_short_interest`, `_poll_eod_short_volume`, `_poll_eod_ftds`) and confirmed their Pydantic models do not surface `expiry`/`strike` in `model_dump()` (pinned by `test_per_underlying_uw_models_dont_carry_expiry_in_dump`); only `historic_option_volume` (which returns raw dicts) and `iv_term_structure` carry the offending field, and both now use the override.

- **UW EOD snapshot run no longer blocks darkpool / extended-hours polling** (`gateway/core/uw_poller.py`, `tests/test_uw_eod_non_blocking.py`): `_poll_loop` awaited `_poll_eod_snapshots` inline. The EOD fan-out runs across hundreds of tickers × 8 feeds (greek_exposure, iv_rank, iv_term_structure, oi_change, historic_option_volume, short_interest, short_volume, ftds) plus market-wide congress/insider, so the await could stretch for many minutes — and the darkpool poller (which is explicitly meant to keep firing during EOD because dark-pool trades continue into extended hours) was starved for that entire window. The fix tracks an `_eod_task: asyncio.Task | None` field, spawns the EOD work via `asyncio.create_task(self._run_eod_with_logging(sink_registry))` when `_should_poll_eod()` first returns True, and stamps `_last_eod_date` *before* spawning so a second loop tick within the same minute doesn't double-schedule. Each subsequent loop tick checks `self._eod_task.done()` to surface any task exception into the error log and clear the reference. `stop()` cancels an in-flight EOD task (2s budget) before tearing down the rest of the poller so shutdown doesn't leave a fan-out wedging the event loop. Regression pinned by `test_eod_runs_as_background_task` (asserts darkpool fires while EOD is in flight) and `test_eod_not_double_scheduled_while_running` (re-entry guard).

- **UW poller batch dedupe no longer marks failed events as seen** (`gateway/core/uw_poller.py`, `tests/test_batch_publish_partial_failure.py`): Follow-up to the quotes/trades/crypto/news fix below. The UW poller's "mark all seen before publish" pattern relied on `RedisStreamsSink._buffer_failed_event` / `_drain_buffer` to replay failed events -- but that buffer is only populated by the single-publish path (`publish`), NOT the batch path (`_publish_chunk`). Partial pipeline failures on batched UW publishes silently lost events because they were marked seen optimistically with no replay mechanism. UW now uses the same `publish_all_batch_results` per-message pattern as the Alpaca pollers.

- **`DataSinkRegistry.publish_all_batch` semantics restored to sum-across-sinks** (`gateway/core/data_sink.py`): When the per-message-results API landed, `publish_all_batch` was rewritten to delegate to `publish_all_batch_results` and return `sum(any_success)`. With multiple registered sinks this changed the legacy contract: old behavior counted N successful sink publishes per message (N sinks × M messages → up to N×M), new behavior collapsed all sinks into a per-message bool (up to M). Re-implemented `publish_all_batch` directly to preserve the original sum-across-sinks count for backward compatibility with `option_capture` and `backfill` callers that depend on the legacy semantic.

- **Batch-publish dedupe no longer marks failed events as seen** (`gateway/core/quotes_poller.py`, `gateway/core/trades_poller.py`, `gateway/core/crypto_poller.py`, `gateway/core/news_poller.py`, `gateway/core/data_sink.py`, `gateway/core/redis_sink.py`, `tests/test_batch_publish_partial_failure.py`, `tests/test_trades_poller.py`, `tests/test_crypto_poller.py`, `tests/test_news_poller.py`): The four pollers (quotes, trades, crypto, news) called ``sink_registry.publish_all_batch(messages)`` and then marked ``to_publish[:published]`` as seen, on the assumption that the first ``published`` messages succeeded. A Redis pipeline executed with ``transaction=False`` (the default in ``RedisStreamsSink.publish_batch``) can fail at arbitrary indices, so the "first N succeeded" assumption was wrong — unpublished events at low indices were being marked seen by the dedup cache (in-memory OrderedDict + optional Redis ``set_many``) and permanently suppressed on subsequent polls. The fix swaps the call site to the new ``publish_all_batch_results`` API (``list[bool]`` aligned 1:1 with input) and iterates ``zip(to_publish, results, strict=True)``, marking only entries with ``ok=True`` as seen. Pollers fall back to the legacy ``publish_all_batch`` only if the registry doesn't expose the new method, and in that fallback path treat the batch as all-or-nothing (count == len(messages) → mark all seen; partial count → mark none seen, retry on next poll) so the failure mode is "duplicate publish on retry" (idempotent at the sink) rather than "silently drop". The UW poller already used a safer "mark seen before publish" pattern and is unchanged. Regression pinned by `test_batch_publish_partial_failure.py::TestDataSinkRegistryRoundTrip::test_index_2_of_5_failure` (the exact bug scenario) plus four poller-level tests asserting that only the failed index is re-published on the next poll.

- **Shutdown sink-close no longer drops poller tail events** (`gateway/main.py`, `CLAUDE.md`, `tests/test_shutdown_sink_ordering.py`): The 8-step graceful shutdown ran `sink_registry.close_all()` (step 6) BEFORE stopping producers (step 7 — pollers, backfill, provider registry). Once `close_all` flipped `DataSinkRegistry._enabled=False`, any tail event emitted by a poller's final iteration silently no-op'd in `publish_all`'s `if not self._enabled` early return. Pollers stopping mid-cycle (UW EOD, treasury, quotes/trades/crypto/news, etc.) routinely emit one last batch on shutdown — those envelopes were vanishing without reaching Redis. New step order: (1) mark shutting down, (2) notify WS clients, (3) drain period, (4) stop streaming producers (option capture + multiplexer), (5) stop polling producers + backfill engine + provider registry, (6) close client WS connections, (7) drain queues + `sink_registry.close_all()`, (8) reset coordinator. The sink now stays open for the entire producer-stop sequence so every final poller publish gets a chance to reach Redis. Regression pinned by `test_pollers_stop_before_sink_close` (source-level ordering check on `lifespan`) and `test_late_publish_lands_when_sink_open` (runtime check that a publish issued during a producer's final iteration reaches the sink).

### Fixed

- **Async-correctness review followups (codex)** (`gateway/core/circuit_breaker.py`, `gateway/core/connections.py`, `gateway/core/dedup.py`, `gateway/core/stream.py`, plus their tests): Followups identified by codex review of the five async-correctness fixes below. (1) A cancelled HALF_OPEN probe used to leave `_half_open_in_progress = True` forever (CancelledError bypasses `except Exception`), wedging the breaker until manual reset; `call()` now releases the slot in a dedicated `except asyncio.CancelledError` branch and re-raises. (2) `StreamMultiplexer.start()` in eager / selective-eager modes did `create_task(conn.start())` without flipping `conn._running = True` synchronously, so a subscriber arriving in the same scheduler tick could still race into `_ensure_connected` and spawn a duplicate `conn.start()`. Both eager branches now set `_running = True` before scheduling, closing the same race the lazy-restart fix closed for dormant streams. (3) The slow-WS-client timeout fix closed the socket but left the connection authenticated in `_connections` / `_client_map`, so subsequent broadcasts kept re-paying the timeout cost per event; the timeout handler now flips `connection.authenticated = False` first, so target selection skips zombies on every later broadcast. (4) Dedup followers `await future` directly, so cancelling one follower would cancel the shared future and poison every other follower; followers now `await asyncio.shield(future)`. Four new regression tests pin each of these.

- **Events queued before the circuit opens are no longer silently dropped** (`gateway/core/data_sink.py`, `tests/test_data_sink.py`): `DataSinkRegistry.publish_all` checks the circuit state BEFORE enqueue and routes to `buffer_event` if OPEN, but the breaker can open AFTER enqueue and BEFORE the worker calls `_safe_publish`. The `except CircuitOpenError` branch in `_safe_publish` logged + recorded the failure metric but did NOT call `sink.buffer_event` — events queued during the in-flight window were lost. This regressed PR #32's "single buffer site" invariant. The branch now calls `_safe_buffer_event(sink, topic, data, reason="circuit_open_in_flight")` so the event lands in the sink's failed buffer alongside the existing dispatch-time and worker-cancellation buffer paths. Regression pinned by `test_safe_publish_buffers_on_circuit_open_error`.

- **Slow WebSocket client can no longer stall the entire broadcast + upstream read loop** (`gateway/core/connections.py`, `tests/test_connections_helpers.py`): `ConnectionManager.broadcast` and `broadcast_to_connection_ids` did unbounded `await connection.websocket.send_*(payload)` inside an `asyncio.gather`. The stream multiplexer awaits `broadcast_to_connection_ids` inline, so one client that stopped draining its send buffer (slow consumer, frozen process, dropped TCP) backpressured every other client AND the upstream Alpaca read loop — risking Alpaca's keepalive timeout (1006 disconnect for the whole gateway). Per-send wraps now use `asyncio.wait_for(..., timeout=_BROADCAST_SEND_TIMEOUT_SECONDS)` (default 2.0s). On timeout the slow client is force-closed (code 1011 "slow consumer") with a structured warning, so subsequent broadcasts don't keep waiting on it. Regression pinned by `test_broadcast_slow_client_does_not_stall_other_clients` and `test_broadcast_to_connection_ids_slow_client_does_not_stall_fanout`.

- **Lazy-connect race no longer spawns duplicate Alpaca WebSocket connections** (`gateway/core/stream.py`, `tests/test_multiplexer.py`): `StreamMultiplexer._ensure_connected` checked `conn._running` to decide whether to restart a dormant upstream, but `_running` is only assigned inside `conn.start()` — *after* the `create_task` returns. Two concurrent subscribers both observed `_running == False` and both scheduled `conn.start()`, producing two competing reconnect loops on the same Alpaca endpoint. Added a per-connection `asyncio.Lock` (`_start_lock`); the dormant-restart sequence now runs under the lock with a double-check on `is_connected` and `_running`, and `_running` is flipped synchronously before scheduling the task so any racer that reaches the lock next falls into the "already running" branch. Regression pinned by `test_lazy_connect_does_not_spawn_duplicate_start_tasks_under_race`.

- **Half-open circuit no longer admits unlimited concurrent probes** (`gateway/core/circuit_breaker.py`, `tests/test_circuit_breaker.py`): `CircuitBreaker._check_state` only consulted `_half_open_in_progress` inside the `state == OPEN` branch. Once the first probe flipped OPEN → HALF_OPEN, every subsequent caller saw `state == HALF_OPEN` and fell through without raising — N callers hit `await func(...)` in parallel, amplifying load against an upstream that was only tentatively recovered. Added an `elif state == HALF_OPEN` branch that treats `_half_open_in_progress` as a single-slot semaphore: concurrent callers raise `CircuitOpenError`, and `_on_success` now releases the slot on every probe completion (not only on full close) so the next *sequential* probe can run. Regression pinned by `test_half_open_rejects_concurrent_probes`.

- **Cancelled dedup leader no longer hangs all followers** (`gateway/core/dedup.py`, `tests/test_request_dedup.py`): `RequestDeduplicator.dedupe` wraps the leader's `await fetcher()` in `try/except Exception`. `asyncio.CancelledError` is a `BaseException` (not `Exception`), so when the leader was cancelled mid-fetch the shared `future` was never resolved and every follower coalesced behind it stayed wedged on `await future` forever. The fetch loop now catches `CancelledError` explicitly, cancels the shared future so followers observe `CancelledError` instead of hanging, then re-raises so cancellation still propagates to the leader's caller. Regression pinned by `test_request_deduplicator_leader_cancel_propagates_to_followers`.

### Fixed

- **Six schema-integrity blockers in one PR — naive datetimes, missing positivity, silent provider mis-stamps, corporate-action ex_date crashes, fake `supports_bars=True`, wrong UW realized-vol endpoint** (`gateway/schemas/__init__.py`, `gateway/schemas/_strict.py`, `gateway/schemas/_validators.py`, `gateway/providers/alpaca/corporate.py`, `gateway/providers/alphavantage.py`, `gateway/providers/yfinance.py`, `gateway/providers/uw/market.py`, plus tests): gateway-side strict subclasses now reject naive datetimes (UTC contract enforced), non-positive prices/strikes, and negative volume/OI/sizes across NormalizedBar/Quote/Trade/ForexRate/FlowAlert/OptionContract/GreekExposure/MarketTide/SectorTide/NetPremiumTick/DarkpoolTrade/InsiderTrade/PoliticianTrade/NewsArticle; `ResponseMeta.provider` is required (no more silent `"alpaca"` default that mis-stamped lineage); seven Alpaca corporate-action branches now derive the schema-required `ex_date` from the closest semantic vendor field (effective_date / process_date / payable_date) with structured warnings when synthesized; AlphaVantage and yfinance now actually implement `get_bars(symbols, timeframe, start, end)` as canonical dispatchers (they advertised `supports_bars=True` but generic callers hit `NotImplementedError`); UW `get_realized_volatility` now uses the dedicated `/api/stock/{ticker}/volatility/realized` endpoint with client-side log-return fallback (it was wired to `get_candles` and produced 100% empty `NormalizedVolatilityStats`). Cross-repo follow-up: tighten the same invariants in `empire-schemas` itself once Heber / Cerberus / 3Roses / Kairos / Orion / Atlas / Orbit roll forward, and add `quality_flags: list[str]` to `NormalizedCorporateAction` so synthesized ex_date can be typed.

### Fixed

- **Per-client Alpaca order ownership isolation — two live-capital BLOCKERs against the shared upstream account** (`gateway/api/alpaca/trading.py`, `gateway/core/auth.py`, `tests/test_alpaca_trading_router.py`): Multiple Empire trading systems (Cerberus, 3Roses, Kairos, Orion, Atlas, Orbit) authenticate to the gateway with distinct API keys but SHARE A SINGLE upstream `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY`. Before this change, any auth'd trading client could (1) list, mutate (PATCH), or cancel (DELETE) any other client's open orders against the broker account (BLOCKER 1 — the four endpoints `replace_order`, `cancel_order`, `get_order`, `get_orders` never bound `order_id` to `client.id`); and (2) collide with, probe, replay, or enumerate another client's `client_order_id` via PR #32's just-landed idempotency plumbing (BLOCKER 2 — `_validate_client_order_id` did no per-client scoping). Fix: the gateway now PREFIXES every effective `client_order_id` going to Alpaca with `c-{client.id}-` (`c-cerberus-...`, `c-atlas-...`, etc.). The prefix IS the ownership marker — no separate persisted `(order_id → client_id)` map is required for the common ownership checks. Auto-generated keys are now `c-{client.id}-dg-{uuid4hex}` (was `dg-{uuid4hex}`); caller-supplied keys are transparently prefixed and the FULL prefixed string is what the caller sees in `meta.client_order_id` (so retry tooling round-trips correctly). Alpaca's 128-char ceiling is enforced AFTER prefixing — the per-caller budget shrinks by `len(prefix)` chars (114 for "test-client", 117 for "cerberus") and the 400 GW-E4006 error message surfaces the exact post-prefix budget. The four list/lookup/mutation paths are gated by ownership: `get_orders` filters out orders whose `client_order_id` doesn't start with the caller's prefix (silently — no signal that filtering happened, and `meta.count` reflects the filtered count); `GET /orders:by_client_order_id` returns 404 GW-E4404 for a foreign-prefix lookup BEFORE hitting Alpaca (404 not 403, to avoid enumeration confirmation); `get_order`, `replace_order`, and `cancel_order` use the stateless Option A pattern — fetch the order from Alpaca via `provider.get_order(order_id)`, then check the returned `client_order_id` for the caller's prefix and 404 on mismatch. The ownership pre-check on `replace_order` / `cancel_order` runs BEFORE the mutating SDK call so a cross-client attempt never reaches Alpaca's executor thread (no broker side-effect). Every cross-client attempt logs a structured WARNING with code `GW-A4001` carrying `client_id_attempted` and `order_owner_actual` (parsed from the prefix) — operators alert on any non-zero rate (misconfiguration vs active enumeration both deserve immediate attention against a shared broker account). Defense in depth: `gateway/core/auth.py` validates every `client.id` against `[A-Za-z0-9_-]+` at `ClientAuthenticator._load_clients()` time and raises `InvalidClientIdError` before the gateway accepts traffic, so a misconfigured `config/clients.yaml` fails loudly at startup rather than emitting malformed `client_order_id`s mid-burst; the trading router also re-validates on every prefix build and raises 500 GW-E5008 if a bad id ever leaks past the auth gate. 13 new tests in `tests/test_alpaca_trading_router.py` pin the contract: auto-generated key prefix shape, caller-key prefix shape, post-prefix budget error message, `get_orders` filter (mixed-owner provider with 6 entries → 2 owned returned), `by_client_order_id` foreign-prefix 404 + no upstream call, `get_order` foreign-owner 404, `replace_order` foreign-owner 404 + no SDK invocation, `cancel_order` foreign-owner 404 + no SDK invocation, cross-client collision attempt (B reusing A's caller-key yields distinct wire keys), GW-A4001 audit log for `get_order` and `by_client_order_id` foreign attempts, owned-order success counter-test, unsafe-client-id 500 short-circuit; plus 6 existing tests updated for the new prefixed key shape and the 2 HTTP-layer tests (`create_order_504_via_http_layer`, `replace_order_504_via_http_layer`) now assert the prefix surfaces in the 504 body and configure the mock provider's `get_order` to return an owned order so the ownership pre-check passes through to the timeout path. Backward compat: existing trading callers pass un-prefixed `client_order_id` and see a different `client_order_id` in `meta` (the prefixed string) but the API contract is otherwise identical — callers using `meta.client_order_id` to retry continue to work; callers caching their own un-prefixed key for retry must switch to the prefixed value (the foreign-prefix 404 enforces this).

### Changed

- **Trading-call timeout split: writes get 25s, reads stay at 15s** (`gateway/config.py`, `gateway/api/alpaca/trading.py`, `tests/test_alpaca_trading_router.py`): The 2026-05-15 opening-bell window logged 67 trading-call timeouts against the shared 15s `alpaca_trading_call_timeout_seconds` ceiling — 37 × `get_account`, 17 × `get_orders`, 9 × `get_positions`, 3 × `create_order`, 1 × `close_position`. Reads can safely retry a 504 (they're idempotent at the broker), but writes timing out surfaces a 504 that forces the caller through the idempotency-retry contract (GET `/orders:by_client_order_id`, GET `/positions/<symbol>`, etc.) — strictly more expensive than letting a merely-slow successful write complete. New `alpaca_trading_write_call_timeout_seconds` (default 25s, env `GATEWAY_ALPACA_TRADING_WRITE_CALL_TIMEOUT_SECONDS`) gives writes — `create_order`, `replace_order`, `cancel_order`, `cancel_all_orders`, `close_position`, `close_position_all` — more wall-clock budget so opening-bell broker slowdowns no longer 504 spuriously. The existing `alpaca_trading_call_timeout_seconds` remains the source of truth for reads (15s default unchanged); a new `_WRITE_TRADING_OPERATIONS` frozenset in `gateway/api/alpaca/trading.py` lists which operations consult the write knob. The HTTP-level safety net (`alpaca_trading_http_timeout_seconds`, default 30s) is still >= the write timeout so the executor thread releases on either path. Pool sizing (`alpaca_trading_thread_pool_size=16`, `alpaca_trading_max_inflight=24`) is unchanged: the real upstream concurrency ceiling is `alpaca_max_concurrent_requests=25` (the per-provider semaphore held during the full SDK call), so bumping the trading pool past 25 yields no additional throughput. The 67 timeouts on 5/15 were Alpaca-latency-driven, not pool-saturation-driven — `alpaca_trading_backpressure_reject` (the 503 path that fires when the inflight cap is hit) did not log, so the cap was not the bottleneck. 19 new tests in `tests/test_alpaca_trading_router.py`: 6 parametrized write ops verify they consult the write timeout; 11 parametrized read ops verify they consult the read timeout; 1 asymmetry smoke test (1s sleep: read 504s with 0.5s read budget, write succeeds with 2s write budget); 1 defaults sanity test (writes >= reads, HTTP timeout >= writes).

### Fixed

- **`replace_order` 504 timeouts could lead to double-modified orders on naive retry** (`gateway/api/alpaca/trading.py`, `tests/test_alpaca_trading_router.py`): Same root cause as the `create_order` 504 issue — the trading-path `asyncio.wait_for` (default 15s) cancels the asyncio task on timeout, but the underlying executor thread keeps running until the Alpaca SDK call returns. With no idempotency key on PATCH, a caller seeing a 504 had no safe retry path: retrying naively against a replaced order would either no-op (the original `order_id` is now in `replaced` status and not re-replaceable) OR replace again with a new server-minted key, double-modifying the position. Alpaca's `replace_order_by_id` accepts a `client_order_id` on the *replacement* order with the same dedup semantics as `submit_order`, so the fix mirrors the `create_order` contract verbatim — reusing `_validate_client_order_id`, `_generate_client_order_id`, and `_merge_idempotency_context_into_5xx` from the `create_order` implementation. The PATCH endpoint now: (1) validates caller-supplied `client_order_id` (rejecting empty/whitespace/oversize with 400 `GW-E4006`); (2) auto-generates `dg-<uuid4hex>` when omitted; (3) returns the effective key in `meta.client_order_id` + `meta.client_order_id_source` on success; (4) carries the key in `detail.client_order_id` on every 5xx (503 backpressure, 504 timeout, non-timeout 5xx via the `_merge_idempotency_context_into_5xx` wrapper). The PATCH-specific wrinkle is the retry hint, which points at BOTH lookup paths — `GET {ALPACA_ROUTER_PREFIX}/orders/{order_id}` (the original order transitions to `replaced` status when the replacement applies) AND `GET {ALPACA_ROUTER_PREFIX}/orders:by_client_order_id` (the replacement is keyed by the supplied `client_order_id`). Together these let callers resolve "did my replacement land?" without re-issuing the PATCH. 16 new tests in `tests/test_alpaca_trading_router.py` mirror the `create_order` set verbatim: gateway-generated key for naked calls, caller-supplied key preserved, 504 detail carries the key (gateway or caller-sourced), 503 backpressure surfaces the key, empty/whitespace/oversize keys rejected with 400, max-length (128 char) passes through, non-timeout 5xx preserves the retry contract, retry-hint URL drift is pinned to `ALPACA_ROUTER_PREFIX`, and one HTTP-layer test via FastAPI `TestClient` exercises the full middleware/router stack against the real mounted PATCH URL.

### Changed

- **`DataSinkRegistry` dispatch now uses a bounded queue + worker pool instead of a drop-on-saturation semaphore** (`gateway/core/data_sink.py`, `gateway/core/redis_sink.py`, `gateway/config.py`, `gateway/core/metrics.py`, `gateway/main.py`, `gateway/api/admin.py`, `config/prometheus_alerts.yml`): The previous registry guarded concurrent publishes with an `asyncio.Semaphore` sized by `data_sink_max_inflight_per_sink` (default 512, briefly bumped to 2048 in commit `744bba2` as an interim cap-headroom measure). When the cap was reached, new events were silently DROPPED at the acquire site (`data_sink_backpressure_drop` WARNING) — they were not queued, retried, or buffered, so every saturation event was permanently lost to Heber. Operators observed 32 757 such drops on 2026-05-15 (peak 19 790 in one minute at 08:59 ET / pre-open and another 11 400 at 09:36 ET / post-open) and 5 872 drops on 2026-05-18 at the same hour; the cap bump bought headroom but did not fix the architecture. The new dispatch path gives each sink a bounded `asyncio.Queue` (`data_sink_queue_size`, default 4096) drained by a small worker pool (`data_sink_worker_count`, default 8). Producers `put` with a short timeout (`data_sink_producer_block_timeout_seconds`, default 100ms) so backpressure is propagated to the caller instead of silently dropping. Drops happen ONLY when the producer-side timeout fires — i.e. the queue is full AND workers cannot drain it within 100ms — and surface as a new emergency Counter `gateway_sink_producer_timeout_drops_total{sink}` plus a CRITICAL `data_sink_producer_timeout_drop` log line. A new `SinkProducerTimeoutDrops` Prometheus alert fires immediately on any non-zero rate (1-minute lookback, no debounce) with severity critical — every drop is a permanently lost event. The stream-to-sink dispatcher in `gateway/main.py` now `await`s `registry.publish_all(...)` inline: the outer fire-and-forget task set (`_stream_sink_publish_tasks`) and its 512-task cap that previously dropped events with `stream_sink_publish_backpressure_drop` BEFORE they ever reached the registry queue has been removed, so the registry's bounded queue is now the *single* in-process gate for sink dispatch. The obsolete env vars `GATEWAY_DATA_SINK_STREAM_PUBLISH_MAX_INFLIGHT` and `GATEWAY_DATA_SINK_STREAM_PUBLISH_MAX_PENDING` have been removed (use `GATEWAY_DATA_SINK_WORKER_COUNT` / `GATEWAY_DATA_SINK_QUEUE_SIZE` instead). `DataSinkRegistry.close_all()` now also flushes the per-sink queues (via `queue.join()`) before tearing down workers and sinks, so events queued in the final seconds of operation get a publish attempt instead of vanishing on shutdown. Workers cancelled mid-publish (e.g. `drain_queues` timeout) now route the in-flight event through `sink.buffer_event` before re-raising `CancelledError`; `RedisStreamsSink.publish` does NOT catch `CancelledError` itself so the single registry-worker buffer site stays the only one (avoiding the double-buffer corner case where the same event lands in `_failed_buffer` twice and gets replayed on next reconnect drain). All existing behaviour is preserved: circuit-breaker check before enqueue, dedup-cache integration, sink-side `buffer_event` failover when the circuit is OPEN, and the per-sink failed-event retry buffer. Three new gauges (`gateway_sink_queue_size`, `gateway_sink_queue_capacity`, `gateway_sink_worker_count`) give operators dashboard visibility into queue health.

### Fixed
- **Idempotency retry-contract followups: wrong retry URLs, empty client_order_id, lost context on non-timeout 5xx** (`gateway/api/alpaca/trading.py`, `gateway/api/alpaca/common.py`, `gateway/api/alpaca/__init__.py`, `tests/test_alpaca_trading_router.py`): Three followups to the idempotency series landed in this changelog above. (1) The 504 `retry_hint` strings for `create_order` and `close_position` referenced `/api/alpaca/trading/...` — wrong on two counts (missing the `v1`, plus an extra `trading` segment) — so callers retrying with the URL embedded in the error body got a 404. Fix: introduced a single `ALPACA_ROUTER_PREFIX = "/api/v1/alpaca"` constant in `gateway/api/alpaca/common.py` consumed by both the parent router (`alpaca/__init__.py`) and the retry-hint string builders, plus a regression test that asserts `parent_router.prefix == ALPACA_ROUTER_PREFIX` so the two can't drift again. (2) A caller passing `client_order_id=""` (or whitespace-only) silently got a gateway-minted UUID labelled `"caller"` — so each retry minted a new UUID and Alpaca-side dedup was defeated. Fix: empty/whitespace AND oversize (>128 char Alpaca limit, per their REST API docs — the installed `alpaca-py` SDK has no length validator on `OrderRequest.client_order_id` so the gateway is the only place this check happens before the wire) keys now raise 400 `GW-E4006` instead of falling through to auto-generation. (3) Non-timeout 5xx (e.g. `APIError` → 503, `httpx.HTTPStatusError`) flowed through `execute_alpaca_provider_call`'s error remapper, which replaced the structured `detail` with a plain string and lost the `client_order_id` / `symbol` retry key — leaving callers ambiguously 5xx'd with no idempotency contract. Fix: new `_merge_idempotency_context_into_5xx()` helper in the route handler re-wraps the HTTPException on ANY 5xx, promoting a string `detail` to a dict carrying `client_order_id` / `symbol` / `retry_with` / `retry_hint`. Seven new tests in `tests/test_alpaca_trading_router.py` pin the contract: retry hints reference the actual mounted prefix; old wrong-shape `/api/alpaca/trading/` strings never appear; empty/whitespace/oversize keys are rejected; max-length (128 char) keys pass through; non-timeout 5xx for `create_order` (synthetic 503 via `httpx.HTTPStatusError`) and `close_position` (synthetic 502 via bare `Exception`) preserve the retry contract; one HTTP-layer test via FastAPI `TestClient` exercises the full middleware/router stack against the real mounted URL so future URL drift fails loudly.

- **`close_position` 504 timeouts gave callers no path to reconcile broker state** (`gateway/api/alpaca/trading.py`): Same root cause as the `create_order` timeout issue (executor thread keeps running after `asyncio.wait_for` fires), but with a different fix because Alpaca's `ClosePositionRequest` does NOT accept a `client_order_id` — there's no Alpaca-side dedup key the gateway can pre-mint. Instead, the 504 body now carries `detail.symbol` (upper-cased), `detail.retry_with: "get_position"`, and a `detail.retry_hint` pointing the caller at `GET /api/v1/alpaca/positions/{symbol}`. The reconciliation contract: 404 POSITION_NOT_FOUND → the close succeeded (or position never existed), do NOT retry; 200 with position data → the close did NOT take effect, safe to retry. Double-close is naturally bounded — Alpaca rejects close-of-nonexistent-position with 40410000 and the provider already translates that into a clean 404 (see `_POSITION_NOT_FOUND_CODES`). The success-path meta now also includes `meta.symbol` (upper-cased) so the caller's reconciliation logic gets the canonical key without re-deriving. Two new tests in `tests/test_alpaca_trading_router.py` pin the 504 detail shape and the success-meta shape; the 504 test intentionally passes lowercase `aapl` to assert the symbol normalizes to `AAPL` in the error body.

- **`create_order` 504 timeouts could lead to double-placed orders on naive retry** (`gateway/api/alpaca/trading.py`): The trading-path `asyncio.wait_for` (default 15s, see `alpaca_trading_call_timeout_seconds`) cancels the asyncio task on timeout, but the underlying executor thread keeps running until the Alpaca SDK call returns — there is no clean way to cancel a sync HTTP call mid-flight. Net result: when the gateway returned a 504 `GW-E5004` to the caller, the order MAY have already placed at Alpaca. With no idempotency key, a caller that retried POST naively could double-place. Evidence from `logs/data-gateway_errors_2026-05-13.log` 2026-05-15: 3 `create_order` timeouts during opening-bell broker slowdown. Fix: `POST /api/v1/alpaca/orders` now auto-generates a `dg-<uuid4hex>` `client_order_id` when the caller doesn't supply one. The effective key is returned in `meta.client_order_id` (with `client_order_id_source: "gateway"|"caller"`) on success AND in the 504 error body (`detail.client_order_id`, `detail.retry_with: "client_order_id"`, `detail.retry_hint`). Alpaca natively dedupes `submit_order` by client_order_id — so a caller seeing a 504 can either GET `/api/v1/alpaca/orders:by_client_order_id?client_order_id=<key>` to verify whether the order placed, or retry POST with the same key (idempotent at Alpaca). The 503 backpressure path (`GW-E5005`) also surfaces the key, even though that path runs *before* the call hits Alpaca, in case the caller's reconciliation logic is racing. Six new tests in `tests/test_alpaca_trading_router.py` pin the contract: gateway-generated key for naked calls, caller-supplied key preserved, 504 detail carries the key (gateway or caller-sourced), 503 detail carries the key, and the 400-on-ValueError path is unaffected by the new plumbing.

- **`close_position` ValidationError → 502 left callers' positions open** (`gateway/providers/alpaca/trading.py`): Calling `DELETE /api/alpaca/trading/positions/<symbol>` with neither `qty` nor `percentage` constructed a `ClosePositionRequest(qty=None, percentage=None)`; Alpaca's SDK requires exactly one to be non-None and raised `pydantic_core.ValidationError`. The exception surfaced upstream as a 502 to the caller — but the position remained open at the broker. Callers reading the 5xx as "close failed" left positions open in the wild. Fix: a naked close request now defaults to `percentage=100.0` (close the entire position — the principle of least surprise for "close this position"). Also fixed the downstream `if qty else None` / `if percentage else None` falsy-checks to use `is not None` so that `qty=0` / `percentage=0` round-trip to Alpaca verbatim instead of being silently rewritten back to None (which would have re-triggered the same ValidationError the naked-default guard prevents). Four new tests in `tests/test_alpaca_provider.py` pin: naked → 100%, explicit qty forwards verbatim, explicit percentage forwards verbatim, and `qty=0` reaches Alpaca as a literal `"0"` rather than being silently rewritten.

### Added
- **Prometheus metrics + alerts for failed-event buffer evictions** (`gateway/core/metrics.py`, `gateway/core/redis_sink.py`, `config/prometheus_alerts.yml`, 2026-05-07): Direct followup to the 2026-05-05 outage. The previous instrumentation only logged a `redis_sink_buffer_eviction` WARNING per evicted event — operators had no aggregated signal that the bounded retry buffer was overflowing, so the DG sink dropped events for 32 hours before anyone noticed. Two new metrics: `gateway_sink_buffer_evictions_total{sink}` (Counter) and `gateway_sink_buffer_size{sink}` (Gauge). `_buffer_failed_event` now increments both. Two new alerts: `SinkBufferEvictionsActive` (critical, fires after 2 minutes of any eviction rate — the silent-data-loss signal) and `SinkBufferNearCapacity` (warning, fires when buffer >90% full so operators can act before evictions start). One new test (`test_buffer_eviction_increments_prometheus_counter`) pins the metric wiring as a regression guard.

### Fixed
- **Redis-sink memory exhaustion → 32-hour silent data outage** (`docker-compose.yml`, 2026-05-06): The compose file overrode `GATEWAY_DATA_SINK_MAX_STREAM_LEN` from the in-code default of 100 000 up to 500 000 ("for backfill jobs >300K records") while Redis was still capped at `--maxmemory 1gb`. With `~3 KB`/entry the heber:events stream alone could reach `~1.5 GB` — well past the cap. On 2026-05-05 the stream crossed 200K entries, Redis hit memory pressure, XADD started timing out (>5 s `operation_timeout_seconds`), DG's `data_sink:redis_streams` circuit breaker opened, and every subsequent publish was buffered locally. The bounded 10 000-entry failed-event buffer overflowed within minutes; over the next 32 hours `redis_sink_buffer_eviction` fired 451 000+ times before anyone noticed, dropping every UW alert / quote / trade fanout the bus carried. Heber-consumer's last Silver flush was 2026-05-05T13:37:18Z; Kairos Scout fetched `count=0` flow alerts on 5/06 and skipped a full trading day. Two tuning changes in `docker-compose.yml`: (1) lowered `GATEWAY_DATA_SINK_MAX_STREAM_LEN` from 500 000 → 300 000 (still 3× the in-code default, fits under the new cap with `~900 MB` worst case for the stream itself); (2) raised Redis `--maxmemory` from `1gb` → `2gb` to give 4× headroom for the stream + dedup cache + heber:watch / snapshots + opening-bell burst margin. Live Redis was set via `CONFIG SET maxmemory 2gb`; compose change persists across recreates. Recovery sequence verified: `XLEN heber:events` dropped from 225K → 49K when Redis was restarted earlier, DG's circuit breaker closed automatically once XADD latency normalized, today's `dt=2026-05-06` flow_alerts partition is now populated (553 rows). Followup worth scoping (not in this change): DG's failed-event buffer drops events when full instead of paging to disk; the existing 2026-04-30 backpressure alerts only fire on the dispatch metric, not on the `redis_sink_buffer_eviction` counter — operators have no signal that events are being lost in real-time.

- **Mixed-shape upstream subscribes silently dropped by Alpaca** (`gateway/core/stream.py`, 2026-05-01): When a downstream client subscribed to `stock_quotes` with an OCC option contract mixed in (or `option_quotes` with an equity ticker), `UpstreamConnection._sanitize_subscription_request()` only filtered the OPTIONS-stream-doesn't-support-bars case. Everything else got faithfully forwarded to Alpaca's STOCKS_SIP / OPRA endpoints, which silently reject mismatched-shape symbols, drop the entire subscribe payload, and (for OPRA) close the connection 2–5 minutes later with code 1006. Downstream clients still received `subscription_ack: ok` with no indication anything was wrong, so daemons sat connected with 0 ticks for hours. New `_filter_symbols_by_stream_shape()` drops OCC-shaped symbols from STOCKS_SIP/IEX subscribes and non-OCC symbols from OPTIONS subscribes, surfaces a `stream_symbol_shape_mismatch` warning, and includes the dropped count in the `subscription_ack.warnings` returned to the client. CRYPTO/NEWS streams are unchanged. Four new tests in `tests/test_option_stream_options.py` cover OPTIONS-drops-equities, SIP-drops-OCC, IEX-drops-OCC, and the empty-after-filter no-send case. Companion client-side fix landed in Kairos `LiveExitMonitor._build_subscribe_messages()`; this DG-side filter is defense-in-depth so other clients can't poison upstream subscribes the same way. Takes effect on next DG container restart.

- **`exchange_calendars` is now a real dependency, with extended forward range** (`pyproject.toml`, `gateway/core/calendar.py`): The package was *imported* by `gateway/core/calendar.py` but never declared in `dependencies`, so every `uv sync` left it uninstalled and the `exchange_calendars_unavailable` warning fired at startup. The hardcoded fallback only covered 2024-2026 and would have started returning wrong holiday/early-close answers in 2027 — silently. Added `exchange-calendars>=4.13.1` (matching Orion's pin; Kairos and Heber use the older >=4.5 which is range-compatible) and explicit `pandas>=2.0` (calendar.py imports it directly via `import pandas as pd`). Also updated `_get_nyse_calendar()` to pass `start=today-5y, end=today+10y` to `xcals.get_calendar()` — the package's default range is only 1 year forward so any 2027+ query was hitting the same fallback even with the package installed. Verified: holiday detection now correct for 2026-12-25 (Christmas), 2027-12-25 (Christmas), 2030-07-04 (July 4th), 2032-01-01 (New Year), 2033-11-24 (Thanksgiving), and early-close detection correct for Black Friday and Christmas Eve through 2036. 19 tests in `tests/test_calendar.py` and `tests/test_calendar_api.py` pass.

### Added

- **Prometheus alerts for stream-to-sink backpressure drops** (`config/prometheus_alerts.yml`): Two new alert rules — `StreamSinkBackpressureDrops` (critical, fires after any drops sustained 2 minutes) and `StreamSinkBackpressureSustained` (critical, fires when drops average > 1/sec for 10 minutes). The `gateway_stream_sink_dispatch_events_total{status="dropped_backpressure"}` metric was already being recorded but had no alert wiring, meaning silent data loss in the Heber pipeline could occur indefinitely without any operator signal. Identified by a multi-persona predict analysis on 2026-04-30 as the highest-priority finding.

### Changed

- **CORS configuration no longer combines wildcard origin with credentials** (`gateway/main.py`): The previous setup used `allow_origins=["*"]` (in debug mode) AND `allow_credentials=True` unconditionally. The CORS spec forbids this combination — browsers reject it, but non-browser clients (curl, custom HTTP) bypass silently. Since the gateway uses `X-Gateway-Key` header authentication (not cookies), `allow_credentials` is unnecessary when using a wildcard origin. The new logic sets credentials to False whenever origins includes `"*"` and adds a startup assertion that fails fast on any future configuration that re-introduces the forbidden combination.

- **Process note: `docs/audits/PERF_GATE_PROCESS_NOTE.md`** documenting CI perf-gate erosion and recommended discipline. The most recent five master commits before this branch were all CI perf-gate adjustments — the gate has been drifting in the looser direction without any feedback signal that real perf work is needed. New doc explains the pattern, recommends splitting CI gates (smoke) from prod-truth benchmarks (regression detection), and gives concrete audit commands. No code change — pure process documentation.

### Changed

- **Stream-sink dispatch and Redis-pool capacity defaults raised to absorb opening-bell bursts** (`gateway/config.py`, `gateway/core/redis_sink.py`): Previous defaults (`data_sink_stream_publish_max_inflight=32`, `data_sink_stream_publish_max_pending=512`, `data_sink_redis_pool_size=8` capped at `le=32`) provisioned for ~6.4K events/sec sustained throughput — well below the ~50K/sec aggregate at opening bell across SIP equities + OPRA options + crypto + news. New defaults: max_inflight=64, max_pending=1024, pool_size cap raised to `le=128` (default still 8 — operators must explicitly tune for higher load). The dead `min(64, pool_size)` clamp inside `RedisStreamsSink._create_client` was removed since Settings now validates the upper bound. New defaults push sustained capacity to ~12.8K events/sec at the dispatch layer; the new backpressure-drop alerts (added in commit 9dd32c4) fire before silent data loss occurs at higher burst rates so operators have a feedback signal for further tuning.

- **`fast_wrap_streaming_event` now uses a content-derived BLAKE2b hash for `event_id`** (`gateway/core/envelope.py`): Previously the streaming hot path generated `event_id` via `os.urandom(16).hex()` — a random ID that defeated Heber's three-layer deduplication (bloom filter at consumer in `heber/bus/dedupe.py`, dict at writer in `heber/writer/consumer.py`, dedup at compactor in `heber/writer/compactor.py`), all of which key on `event_id`. Same upstream Alpaca event delivered twice (e.g. on reconnect with replayed messages, or upstream retransmits after slow-client closes) produced two distinct envelopes that Heber stored as separate records. New implementation extracts feed-specific unique fields (`bp/ap/bs/as` for quotes, `trade_id` for trades, etc. — same `FEED_UNIQUE_FIELDS` table as the slow path), combines them with `(provider, feed, instrument_key, ts_event_str, sequence)`, and BLAKE2b-hashes to a stable 32-char hex. Verified properties: same event → same ID (idempotent), different price → different ID (no false collisions), different sequence → different ID (replay-resilient). Benchmarked at 2.15µs/event on M1 Pro — earlier "5-10µs" comment was stale, actual cost is ~10% of one core at 50K events/sec sustained.

- **`GATEWAY_STRICT_ENVELOPES` opt-in flag promotes silent envelope errors to loud failures** (`gateway/config.py`, `gateway/core/envelope.py`, `gateway/api/middleware.py`): Two long-standing silent-fallback paths in the data layer — `wrap_event` returning a `quality_flags=["error"]` minimal envelope on exception, and `EventEnvelopeMiddleware` shipping the unwrapped original body — produced corrupt-but-routable data instead of detectable failures. New `EnvelopeWrapError` is raised by `wrap_event` and the middleware now re-raises (FastAPI returns 500) when `GATEWAY_STRICT_ENVELOPES=true`. Default `false` preserves legacy lenient behavior. As an additional safety in lenient mode, `EventEnvelopeMiddleware` now sets `x-gateway-envelope: false` on the unwrapped fallback response so consumers can distinguish "this endpoint never wraps" from "wrap failed silently". Recommended for staging/prod where corruption is harder to recover from than a clear error.

- **`required: true` provider field with prod-mode enforcement** (`gateway/core/registry.py`, `config/providers.yaml`, `gateway/main.py`): Previously when `Alpaca.initialize()` raised (bad creds, network blip), the gateway booted with no Alpaca provider, `/health` returned 200, and runtime calls failed with confusing "Provider access denied" — operators chased the wrong issue. New `required: true` field per provider in `providers.yaml` marks load-critical providers. New `RequiredProviderInitError` raised by `ProviderRegistry.load_from_config(strict_required=True)` aborts startup if a required provider fails. `gateway/main.py` passes `strict_required=not settings.debug`, so production fails fast and local dev (`GATEWAY_DEBUG=true`) keeps the lenient behavior — developers can run without all 7 provider keys. The route-referenced providers (alpaca, yfinance, unusual_whales) are now marked `required: true` in `config/providers.yaml`. Verified: lenient + working = boots; strict + broken required = raises; lenient + broken = boots empty.

- **WebSocket oversize messages now close the connection with code 1009 instead of looping** (`gateway/api/websocket.py`): Previously when a WebSocket frame exceeded `ws_max_message_size`, the handler emitted an error response and `continue`d the loop, allowing the same client to repeat the attack indefinitely on the same connection. Each oversize frame was fully buffered into Python memory by starlette before the application-level check ran, so a coordinated stream of 100MB frames from many connections could exhaust process memory. New behavior: log `ws_message_oversize` warning, send the existing GW-E8005 error response (best-effort), close with code 1009 ("Message Too Big"), and exit the loop. Operators should ALSO configure uvicorn `--ws-max-size` (or equivalent) at the ASGI layer so oversize frames are rejected before reaching Python — the application-level check exists for clean error codes within the ASGI cap.

- **EventEnvelopeMiddleware no longer buffers streaming response bodies** (`gateway/api/middleware.py`): The cache layer correctly excluded `text/event-stream` and `application/x-ndjson` from caching, but the envelope-wrap layer accumulated the entire body for every JSON response — including streaming ones — into `body_chunks` before wrapping. For an NDJSON bulk endpoint, this meant the entire response was held in memory before a single byte reached the client, defeating the streaming contract. Now mirrors the cache layer's content-type check and skips wrapping for `text/event-stream` / `application/x-ndjson`. Per-request memory floor for these routes drops back to per-chunk.

- **`gateway/main.py` stream-sink shutdown drain timeout raised from 2s to 6s** (`gateway/main.py`): The drain timeout was shorter than the redis_sink `operation_timeout_seconds` default (5s), so any in-flight publish that took >2s got cancelled mid-write. Cancelled publishes do NOT re-enter `_failed_buffer` (the buffer-on-fail logic is inside the publish retry path which never returns). New default is 6s, leaving 1s headroom over the operation timeout. Long publishes that genuinely hang past 6s still get cancelled and the existing `stream_sink_publish_drain_timeout` warning fires.

- **`RedisStreamsSink.close()` now drains the in-memory failed-event buffer before disconnecting** (`gateway/core/redis_sink.py`): Previously `close()` awaited in-flight drain tasks but did not trigger a final drain attempt against the buffer. Events accumulated during the last seconds of operation could be lost on graceful shutdown if Redis was still reachable. New `close()` calls `_do_drain` with a 5s timeout before tearing down the client. If the buffer is still non-empty after the attempt (e.g. Redis itself is down at shutdown), a `redis_sink_close_buffer_nonempty` WARNING reports the lost-event count so operators have a number for post-mortem. The buffer is still in-memory only — disk persistence is a larger separate change.

- **`GATEWAY_TRUSTED_PROXY_CIDRS` setting + rightmost-untrusted X-Forwarded-For resolution** (`gateway/config.py`, `gateway/api/middleware.py`, `gateway/main.py`): Previously when `behind_trusted_proxy=True`, the gateway parsed the LEFTMOST entry from `X-Forwarded-For` as the client IP — but the leftmost entry is the most-distant claimed origin and is fully attacker-controlled. A request with `X-Forwarded-For: 1.1.1.1, real-ip` was rate-limited under bucket `1.1.1.1`, allowing trivial bypass of per-IP limits and IP block lists. New `trusted_proxy_cidrs: str` setting (comma-separated CIDRs, validated at config load time) enables proper rightmost-to-leftmost walk that returns the first IP NOT in the trusted set. Without `trusted_proxy_cidrs` configured, behavior falls back to the legacy leftmost (with a startup `trusted_proxy_misconfig` warning telling operators to set the CIDR list). With it configured, spoofing is blocked. Default empty string preserves existing behavior so no environment changes are required to upgrade.

- **Loud warning at startup when any client uses a plaintext `key:` in clients.yaml** (`gateway/core/auth.py`): The authenticator already supported both plaintext (`key:`) and hashed (`key_hash:`) entries but silently accepted plaintext, which is one mistake from leaking via any clone, branch, or CI artifact. New `clients_plaintext_keys_in_use` WARNING fires once at load time listing every affected `client_id` and the migration command. Behavior unchanged — plaintext still works for backward compat — but operators are no longer surprised.

### Fixed

- **DEBUG `auth_check_start` log no longer includes the first 4 chars of API keys** (`gateway/core/auth.py`): Companion to the failed-auth fix below. The DEBUG log emitted on every authentication attempt previously included `key_prefix=api_key[:4]` which exposed `gw_X` (the `gw_` prefix plus one secret char). Even DEBUG logs ship to remote aggregators in some setups, so the same SHA256 fingerprint approach is applied here too.

- **Failed-authentication logs no longer leak the first 10 chars of API keys** (`gateway/core/auth.py`): On invalid key, `auth_failed_invalid_key` events previously included `key_prefix=<first 10 chars>` in both log files and the audit trail. Since keys are generated as `f"gw_{secrets.token_urlsafe(32)}"`, the first 10 chars exposed `gw_` plus 7 random secret chars — partial credential leakage on any log breach AND a regulatory disclosure issue in compliance audit trails. Replaced with `key_fingerprint=<sha256[:12]>` and `key_length=<n>` which preserves correlation ability with zero plaintext leakage.

### Added

- **Background REST-fallback pollers for Alpaca quotes, trades, crypto, news + per-contract option-trade emission** (`gateway/main.py` + 4 new `gateway/core/*_poller.py` modules + `gateway/core/option_capture.py`): The Heber data audit on 2026-04-29 confirmed 5 stalled feeds — `alpaca/quotes` (equity) stopped 2026-03-31, `alpaca/trades` stopped intraday today, `alpaca/crypto_bars`/`crypto_trades` stopped ~2026-02-14, `alpaca/option_trades` only ever wrote 1 partition (2026-02-13), and `alpaca/news` stopped 2026-03-11. Root cause: the `StreamMultiplexer` is demand-driven (only subscribes upstream when a downstream client subscribes via `/ws`), and Heber is a Redis consumer rather than a stream client. The pre-existing `AlpacaQuotesPoller` (`gateway/core/quotes_poller.py:321`) was the documented "REST-based fallback that runs on a schedule…ensuring quote data flows to Heber regardless of client subscription state" — but `start_quotes_poller()` was never invoked from the lifespan, so it never ran. The April 1st `b56e9a8` schemas/providers/pollers refactor (-9,317 lines) appears to have dropped the wiring. Fixes:
  - **`start_quotes_poller()`** now wired into `lifespan`. Toggle `GATEWAY_QUOTES_POLLER_ENABLED` (default `true`); 30s default interval; market-hours gated.
  - **New `AlpacaTradesPoller`** (`gateway/core/trades_poller.py`) mirrors quotes_poller against `provider.get_latest_trades`; same gating/dedup pattern. `GATEWAY_TRADES_POLLER_ENABLED`.
  - **New `AlpacaCryptoPoller`** (`gateway/core/crypto_poller.py`) polls `get_crypto_latest_bars` + `get_crypto_latest_trades` for the configured pair list (default BTC/ETH/SOL/DOGE/AVAX/LINK/MATIC/DOT vs USD). 24/7 — no market-hours gate. Forces canonical `crypto:BASE-QUOTE` `instrument_key` so Heber's envelope validator accepts it. `GATEWAY_CRYPTO_POLLER_ENABLED`.
  - **`OptionCaptureService` extended** with `publish_per_contract_trades=True` (config `GATEWAY_OPTION_CAPTURE_PUBLISH_PER_CONTRACT_TRADES`). On each capture cycle the chain-snapshot is fetched as today; the new path also emits one `feed=option_trades` envelope per contract that has a non-zero last-price. Zero additional REST calls — reuses snapshot data already on hand.
  - **New `AlpacaNewsPoller`** (`gateway/core/news_poller.py`) polls `provider.get_news` market-wide (or for a configured symbol set) every 120s, deduped by article_id with a 24h TTL. `GATEWAY_NEWS_POLLER_ENABLED`.
  All five publishers ride the existing data-sink registry, dedup contract, and shutdown sequence (8-step PRD graceful shutdown). 30 new unit tests across the four poller test files; 14 option_capture tests still green. Heber verified the contract names match its `CONTRACTED_RAW_FEEDS` (quotes/trades/crypto_bars/crypto_trades/option_trades/news).

- **Selective eager-connect for upstream Alpaca streams via `GATEWAY_STREAM_EAGER_CONNECT_TYPES`** (`gateway/config.py`, `gateway/core/stream.py`, `gateway/main.py`): The multiplexer's `lazy_connect=True` default deferred upstream Alpaca connection until the first client subscribe. On 2026-04-29 the 3Roses bot subscribed at exactly 9:30:00 ET and observed a 31-second handshake (TLS + auth + first-bar drain) plus a 110-second initial bar-staleness backlog as the upstream caught up — the bot missed the entire gap-and-go window. The new `stream_eager_connect_types` config (default `"stocks"`) names the subset of stream types that connect at Gateway startup regardless of `lazy_connect`. By 9:30 ET the stocks WS is already authenticated, subscriptions take effect on a hot socket, and the cold-start cost is paid once at Gateway startup instead of once per market open. Comma-separated list; supports `stocks` (matches whichever of SIP/IEX is active), `stocks_sip`, `stocks_iex`, `options`, `crypto`, `news`. Set to empty string to keep all streams lazy on Basic-plan deployments.

- **`/health/ready` now verifies eager upstream streams are authenticated** (`gateway/api/health.py`, `gateway/core/stream.py`): The readiness probe previously returned `ready` as soon as the FastAPI app was up, which let trading bots open their own WebSocket before the Gateway's upstream Alpaca connection was actually streaming. New `StreamMultiplexer.is_stream_ready(stream_type)` method exposes per-stream connected+authenticated state; the `/health/ready` handler now reports each `stream_eager_connect_types` entry under `checks.streams` and refuses to return `ready` until all eager streams are connected. Trading bots polling `/health/ready` before opening their WS now get an honest readiness signal.

- **Default HTTP-level timeout on the alpaca-py trading SDK session** (`gateway/providers/alpaca/_base.py`, `gateway/config.py`): The alpaca-py SDK creates a bare `requests.Session()` with no timeout, so when our async `wait_for` fired at 15s on `alpaca_trading_call_timeout` (4 events / 2 days, all `get_account` and `get_order`), the user got HTTP 504 immediately but the underlying thread kept blocking on `requests.get(...)` until the kernel/OS gave up — leaking a slot from the 16-thread pool. The `_install_session_default_timeout` helper now wraps `session.request` to inject a default `timeout=` (controlled by `alpaca_trading_http_timeout_seconds`, default 30s) whenever the caller doesn't pass one. The default is intentionally larger than `alpaca_trading_call_timeout_seconds` (15s) so user-facing behavior is unchanged — the HTTP timeout exists purely as a safety net to release leaked threads. Explicit `timeout=` overrides from callers are preserved.

- **`on_message_slow` warning when an upstream Alpaca handler holds the receive loop too long** (`gateway/core/stream.py`): The receive loop's docstring already promised this instrumentation but the code didn't actually do it. Now wraps each `on_message` call with `perf_counter` and logs a structured WARNING (`stream`, `duration_seconds`, `message_type`) when handling exceeds 100ms. Slow handlers stall TCP reads, fill Alpaca's outbound buffer, and trigger their slow-client (407) → 1006-without-close-frame disconnect path. This is diagnostic instrumentation to tell whether the 314 code-1006 closes/2d we see during peak hours are upstream saturation drops or our own processing falling behind.

### Fixed

- **Buffered events on Redis reconnect could be silently lost when the drain task was garbage-collected mid-flight** (`gateway/core/redis_sink.py`): `_ensure_connected` scheduled the post-reconnect drain via `asyncio.create_task(self._drain_buffer())` and discarded the task handle. Python only keeps weak references to tasks, so a task with no other reference can be collected before completing — the official asyncio docs explicitly call this out as a footgun. Verified by `grep -rn "^\s*asyncio\.create_task(" gateway/` that this was the *only* unstored `create_task` in the gateway codebase; every other call site (`data_sink.py:198-200`, `main.py:243`, `option_capture.py:164`, etc.) holds a strong reference. The drain task now lives in a new `_drain_tasks: set[asyncio.Task]` member with a `discard` done-callback, and `RedisStreamsSink.close()` awaits/cancels any pending drains (2s timeout) before tearing down the client/pool so they don't run against a closed connection. The recent 1 GiB Redis `maxmemory` and 500K stream MAXLEN bumps are wasted if events vanish in the gateway before reaching Redis.

- **`/api/v1/calendar/next-trading-day` and `/api/v1/calendar/clock` returned wrong-day results when the host TZ wasn't ET** (`gateway/api/calendar.py`, `gateway/core/calendar.py`): Both routes computed `query_date = date.today()` (host-local) before passing into the calendar. On a UTC-hosted gateway after 8pm ET (UTC date is already tomorrow in ET terms), `next-trading-day` skipped a day and `clock` reported the wrong "today". The earlier fix that made `TradingCalendar.next_trading_day(from_date=None)` default to ET didn't help because the routes always passed an explicit host-local date. Added a public `TradingCalendar.today()` helper that returns `datetime.now(self._tz).date()`, and switched both routes to use it. Other `date.today()` callers in `gateway/api/quality.py` and `gateway/core/bulk.py` have the same class of bug and should be migrated in a follow-up.

- **Test runs polluted production-shaped log files** (`tests/conftest.py`): `gateway.main` calls `empire_core.logger.setup_logging("data-gateway")` at import time, which installs daily-rotating file handlers writing to `./logs/data-gateway_*.log`. Pytest imports `gateway.main` via `conftest.py`, so every test run added test-fixture errors (`raise RuntimeError("boom")`, mocked Redis "Redis is loading the dataset" errors, fake test-net IPs like `203.0.113.50`) into the same files an operator would grep for production triage. Audit of `logs/data-gateway_errors_2026-04-29.log` showed 100% of its 165 entries were test artifacts. Conftest now sets `EMPIRE_LOG_DIR=$(mktemp -d)` and `EMPIRE_LOG_LEVEL=WARNING` before any gateway import (using `os.environ.setdefault` so CI overrides win), redirecting per-pytest-run logs to a temp dir that macOS auto-reaps from `/var/folders/.../T/`.

- **UW poller could mark the wrong events as deduplicated under partial Redis pipeline failure** (`gateway/core/uw_poller.py`): `_publish_envelopes` called `publish_all_batch` and then ran `for ... in to_publish[:published]: self._mark_seen(...)`, assuming the *first N* envelopes succeeded. But Redis Streams pipeline-with-`transaction=False` can fail at arbitrary indices — `published` is the count of successes, not their identities. Under partial failure (e.g., one oversized payload), the poller would mark items 0..N-1 as seen even though item k actually failed and item N was the one that succeeded — leaving the failed event stuck (never re-emitted because dedup says seen) and the actually-succeeded one re-emitted next poll. Refactored to mark all candidate envelopes as seen *before* publishing. This is consistent under all failure modes; the sink-level retry buffer (`RedisStreamsSink._buffer_failed_event` + `_drain_buffer`) handles the actual delivery guarantee.

- **Duplicate EOD-poll block in UW poller main loop** (`gateway/core/uw_poller.py`): `_poll_loop` had two identical `if self.eod_enabled and self._should_poll_eod(): ... await self._poll_eod_snapshots(...)` blocks back-to-back. Today this is dead code — `_poll_eod_snapshots` sets `_last_eod_date` on success so the second `_should_poll_eod()` returns False — but it's a copy-paste artifact that would silently double-poll if a future change moved the date update earlier. Deleted.

- **`TradingCalendar.next_trading_day()` used host-local "today" instead of ET** (`gateway/core/calendar.py`): The default branch was `from_date = date.today()`, which returns a naive date based on the host's TZ. For a UTC-hosted gateway after 8pm ET (when UTC's "today" has already rolled over to ET's "tomorrow"), this produced an off-by-one in the next-trading-day lookup. Sister method `is_market_open()` already correctly converts to `self._tz` first; aligned `next_trading_day()` to do the same: `from_date = datetime.now(self._tz).date()`.

- **Alpaca trading client `requests.Session` not closed on shutdown** (`gateway/providers/alpaca/_base.py`): `shutdown()` aclose'd the httpx market-data client but only nulled out `_trading_client`, leaving the alpaca-py SDK's `requests.Session` (which we monkey-patched at init via `_install_session_default_timeout`) for GC to reap eventually. The pooled HTTPS sockets to `paper-api.alpaca.markets` could linger across gateway restarts. Now explicitly calls `_trading_client._session.close()` in shutdown.

- **`heartbeat_send_failed` and `broadcast_send_failed` benign-close races no longer slip through to WARNING when the exception has an empty `str()`** (`gateway/core/connections.py`): The `is_benign_ws_close_error` helper introduced in commit 56cc1c1 only matched on substrings of `str(exc)`, but `WebSocketDisconnect()` and `ConnectionClosed(None, None)` both have `str(exc) == ""`, so every benign client-disconnect-during-heartbeat race produced a WARNING with `error=""`. Recent audits saw 93 `heartbeat_send_failed` and 24 `broadcast_send_failed` WARNs over 2 days that all matched this empty-string pattern. Helper now also matches by exception type (`isinstance(exc, ConnectionClosed | WebSocketDisconnect)`), so these benign races route to DEBUG via the existing `*_closed` log lines.

- **UW upstream errors classified as transient now log at WARNING instead of ERROR** (`gateway/providers/uw/transient.py`, `gateway/providers/uw/flow.py`, `gateway/providers/uw/options.py`, `gateway/core/uw_poller.py`): Recent audits saw 26 ERROR entries over 2 days for `uw_market_tide_failed`, `uw_greek_exposure_failed`, `uw_greek_exposure_strike_failed`, `uw_spot_exposures_strike_failed`, `uw_flow_alerts_failed`, `uw_ticker_flow_failed`, `uw_poller_flow_error`, `uw_poller_darkpool_error`, and `uw_poller_market_tide_error` that were all routine upstream brownouts: empty JSON bodies (`Expecting value: line 1 column 1 (char 0)`), `Server disconnected without sending a response`, `[SSL: UNEXPECTED_EOF_WHILE_READING]`, HTTP 5xx, `httpx.ReadTimeout`, and `httpx.ConnectError`. Added a shared `is_transient_upstream_error` classifier; every UW catch site now downgrades these to WARNING (still re-raises so retry/backoff logic upstream is unaffected) and reserves ERROR for genuine bugs. Persistent issues like `ValueError`/`KeyError`/application bugs continue to log at ERROR with `exc_info=True`.

- **Alpaca `provider_request_failed` ERROR logs now include the `endpoint` field** (`gateway/api/alpaca/common.py`): The terminal `except Exception:` branch in `execute_alpaca_provider_call` (and the APIError, HTTPStatusError <500, and HTTPStatusError >=500 branches) previously emitted `provider_request_failed` log lines with no context — recent audits saw 85 generic ERROR entries over 2 days with no symbol, no operation, and no route. All four call sites now attach the failing inner call's qualified name (e.g. `get_stock_bars.<locals>._call`) as `endpoint`, so operators can immediately see which Alpaca route timed out without grepping tracebacks. The same `endpoint` parameter is also wired through the duplicate `_handle_alpaca_error` helper.

- **Alpaca trading routes now log a meaningful `endpoint` instead of `<lambda>`** (`gateway/api/alpaca/trading.py`): Follow-up to the previous fix. Every trading route (`get_account`, `cancel_order`, `get_orders`, `replace_order_by_id`, `close_position`, etc.) funnels through `_execute_trading_call` / `_execute_trading_cached_call`, which built an inline `lambda` and handed it to `execute_alpaca_provider_call`. Python sets a lambda's `__qualname__` from its lexical position, not its closure, so all of those routes collapsed to `endpoint=_execute_trading_call.<locals>.<lambda>` — exactly the trading routes the prior CHANGELOG entry promised to disambiguate ("4 events / 2 days, all `get_account` and `get_order`"). Both helpers now lift the closure to a named `async def call` and set `call.__qualname__ = f"trading.{operation}"` before passing it, so failures log as `endpoint=trading.get_account`, `endpoint=trading.cancel_order`, etc.

- **`GET /api/v1/alpaca/orders` now returns 400 for invalid filter values instead of 502** (`gateway/api/alpaca/trading.py`): Callers passing unsupported values like `status=filled` (the individual order status, not the list filter) triggered `ValueError: 'filled' is not a valid QueryOrderStatus` deep in the provider, which bubbled up as a generic `provider_request_failed` / HTTP 502 with no hint as to what went wrong. The endpoint now validates `status` (open, closed, all), `direction` (asc, desc), and `side` (buy, sell) up-front and returns HTTP 400 with a message listing the allowed values, so clients learn immediately that they used an invalid filter rather than retrying against a fake upstream error.

- **WebSocket "Cannot call send once a close message has been sent" no longer logs at ERROR/WARNING** (`gateway/core/connections.py`, `gateway/api/websocket.py`): When a client disconnects while a heartbeat, broadcast, or response write is in flight, the websockets protocol raises `Cannot call "send" once a close message has been sent.` from the send side. The existing benign-close filter only matched `1006` / `transfer_data_task` / `disconnect`, so 253 `broadcast_send_failed` warnings and 3 `websocket_error` ERROR lines per day fell through to noisy levels even though nothing was actually wrong. The detection is now factored into a shared `is_benign_ws_close_error` helper and recognizes the `once a close message` and `is not connected` patterns too, silencing the reconnect-tail noise.

- **WebSocket auth receive errors during unclean client drops downgraded from ERROR to INFO** (`gateway/api/websocket.py`): `_wait_for_auth` wrapped `websocket.receive_json()` in `except Exception` and logged every failure at ERROR, so clients dropping before sending credentials (code 1006, TCP reset, tab closed) produced `auth_receive_error` errors with `"(1006, None)"` attached. These short-lived, pre-auth disconnects are client-side noise and now log as `auth_client_disconnected` at INFO; genuine receive errors still log at ERROR.

- **Test mock stubs caused false ERROR and WARNING log entries** (`tests/conftest.py`): `mock_provider.get_bars` returned a dict instead of `list[NormalizedBar]`, causing `AttributeError: 'str' object has no attribute 'model_dump'` in `stock.py` during test runs. `mock_provider.get_calendar` was an `AsyncMock` but is called synchronously via `asyncio.to_thread`, returning a coroutine object and raising `'coroutine' object is not subscriptable`. Both stubs corrected: `get_bars` now returns `[]`, `get_calendar` uses `MagicMock(return_value=[])`.

- **Upstream Alpaca stream reconnect bug doubled connection churn and lost bars after every flap** (`gateway/core/stream.py`): Apr 17, 2026 investigation into why 3Roses, Cerberus, and Orion all saw simultaneous "0 bars in last 60s" warnings followed by stale-data rejections revealed that `UpstreamConnection._connect_and_run()` and `_reconnect_with_backoff()` both independently called `connect()` → `authenticate()` → `subscribe()`. After a disconnect the backoff helper would re-establish a working connection and return, and then the outer loop would fall through to its next iteration and call `connect()` again — which closes the just-reconnected WebSocket (via `_close_ws()`, because Alpaca allows only 1 concurrent connection per endpoint) and opens yet another. Every single upstream flap produced TWO full connect cycles, doubling Alpaca-side reconnection churn and opening a 1-3 second gap after each recovery during which bars were silently dropped. On Apr 17 the stocks_sip + options streams flapped 45 times in 2 hours, so the amplified churn left downstream clients continuously tripping the 90s staleness guard and never trading. Refactored `_connect_and_run()` into a single connect-once-per-iteration loop with inline exponential backoff, and deleted the redundant `_reconnect_with_backoff()` helper. Non-recoverable error detection (`connection limit exceeded`) preserved and now covers the initial connect as well.

- **WebSocket keepalive ping timeouts disconnecting all streaming clients during Alpaca slowdowns** (`gateway/api/alpaca/trading.py`, `gateway/config.py`, `Dockerfile`, `scripts/com.empire.data-gateway.plist`): On Apr 15 and Apr 16 high-volatility sessions, every `alpaca_trading_call_timeout` log was followed ~1s later by simultaneous "keepalive ping timeout" disconnects across all connected streaming clients (3roses, cerberus, orion). Root cause: when the 8-thread trading pool saturated, new calls queued in the executor's unbounded internal queue, leaving asyncio `wait_for` timers and pending coroutines piled up on the loop while the upstream market-data stream was also pushing peak traffic. Event-loop CPU contention delayed server pong frames past the client's 20s default `ping_timeout`, dropping all connections together. Three fixes: (1) doubled the dedicated trading thread pool from 8 to 16 so burst load is absorbed without queueing; (2) added `alpaca_trading_max_inflight` (default 24) — a bounded semaphore that fast-fails with `503 / GW-E5005` when the cap is reached, so surplus traffic bounces immediately instead of piling up and starving the loop; (3) raised uvicorn `--ws-ping-interval` to 30s and `--ws-ping-timeout` to 90s (up from the 20s/20s default) in both Dockerfile CMD and launchd plist to match the client-side tolerance already landed in 3Roses `f58fdc2`, giving both sides enough slack to absorb transient event-loop lag without terminating healthy connections.

- **Option capture timeouts causing 228 failures/day** (`gateway/core/option_capture.py`, `gateway/config.py`, `docker-compose.yml`): SPY (13,422 contracts, 14 API pages), QQQ (10,100 contracts, 11 pages), and IWM (4,932 contracts, 5 pages) frequently exceeded the 30s per-symbol timeout, causing 228 `option_capture_symbol_failed` warnings per trading day (136 SPY, 67 QQQ, 25 IWM). All failures logged with `error=""` because `TimeoutError` produces an empty string representation, making diagnosis impossible from logs alone. Three fixes applied: (1) raised default snapshot timeout from 30s to 90s and added per-symbol overrides in docker-compose (SPY:120s, QQQ:90s) to match actual fetch times; (2) increased capture interval from 60s to 300s since minute-level capture of 28K+ contracts created unnecessary Alpaca connection pressure (45 of 200 daily ConnectError retries were from `get_option_snapshots`); (3) added 2s inter-symbol stagger delay to reduce connection contention. Also fixed the empty error log by falling back to the exception type name when `str(exc)` is empty.

- **UW historic_option_volume: volume=0 treated as null** (`gateway/providers/uw/options.py`): The `get_historic_option_volume()` method used `if get("volume") else None` to guard `_safe_int` calls, which treated `volume=0` as falsy and converted it to `None`. This caused 8 DLQ messages and 6 `silver_validation_failed` events in Heber because `volume` is a required non-null field for the `historic_option_volume` Silver schema. Changed all falsy guards in this method to explicit `is not None` checks. Also defaulted volume to 0 when UW returns None (a record with no volume data is more useful at 0 than rejected). Added a `timestamp` field to the payload so `wrap_event` produces a proper `ts_event` instead of defaulting to `now()`.
- **Stale position close now returns terminal 404 instead of retryable error** (`gateway/providers/alpaca/trading.py`, `gateway/providers/alpaca_legacy.py`): When Alpaca returns error code `40410000` (position does not exist), the gateway now returns HTTP 404 with `"code": "POSITION_NOT_FOUND"` in the error body instead of bubbling up as a generic error. This prevents callers from retrying close requests for positions that no longer exist (e.g., expired SPY put contracts). The log level for these events is downgraded from ERROR to WARNING since they represent expected terminal states.
- **Downgraded WebSocket dead-connection errors from ERROR/WARNING to DEBUG** (`gateway/api/websocket.py`, `gateway/core/connections.py`): Orion reconnect cycles (close code 1006, missing `transfer_data_task`) generated ~300 errors and ~500 warnings per day. These are normal reconnect behavior and now log at DEBUG, eliminating the noise while preserving genuine error visibility.
- **Added top-level IV-rank route alias to eliminate 404 flood** (`gateway/api/uw/volatility.py`): Heber-watch and Kairos called `/api/v1/uw/{symbol}/iv-rank` but the endpoint only existed at `/api/v1/uw/options/{symbol}/iv-rank`, causing ~3,390 404s/day. Added a convenience alias at the top-level path that shares the same cache key and provider call.
- **Filtered empty insider_trades records before publishing** (`gateway/providers/uw/institutional.py`): The UW API returns filing-date index records with null ticker, owner_name, and transaction_code alongside real insider transactions. These 405 empty-payload events were being published to Redis and failing Heber Silver validation (missing insider_name, trade_date, trade_type), flooding the DLQ. Records missing ticker, owner_name, or transaction_code are now skipped at the provider level with a warning log.
- **Increased option capture snapshot timeout from 10s to 30s** (`gateway/config.py`, `gateway/core/option_capture.py`): SPY/QQQ/IWM option chains are extremely large and consistently timed out at the 10s default, causing ~100 `option_capture_symbol_failed` warnings per day. Default raised to 30s via `GATEWAY_OPTION_CAPTURE_SNAPSHOT_TIMEOUT_SECONDS`.

### Added

- **Makefile for local development** — `make setup` runs `uv sync --extra local --extra dev` (the required invocation); also includes `test`, `lint`, `format`, `typecheck`, `run`, and `clean` targets. Bare `uv sync` leaves the venv broken because empire-core, empire-schemas, and the UW SDK are in optional extras.

- **Per-symbol snapshot timeout overrides for option capture** (`gateway/config.py`, `gateway/core/option_capture.py`): New `GATEWAY_OPTION_CAPTURE_SYMBOL_TIMEOUT_OVERRIDES` env var accepts comma-separated `SYMBOL:SECONDS` pairs (e.g. `SPY:45,QQQ:45`) for symbols that need longer timeouts than the default.

### Changed

- **Increased Redis maxmemory from 512mb to 1gb** (`docker-compose.yml`): Redis was at 346MB/512MB usage with the stream buffer recently increased to 500K entries. The tight headroom caused OOM-driven restarts (~8/day), which forced the Heber consumer group to be recreated each time. Doubling to 1gb provides adequate room for stream growth.

### Fixed

- **Increased sink backpressure limit from 256 to 512** (`gateway/core/data_sink.py`, `gateway/config.py`): 296 events were silently dropped when the in-flight publish cap was exceeded during peak data flow. Default raised to 512 and made configurable via `GATEWAY_DATA_SINK_MAX_INFLIGHT_PER_SINK`.
- **Backfill engine loop-variable closure bug** (`gateway/core/backfill.py`): The `_bounded_chunk` async function defined inside the date-chunk loop captured `chunk_start` and `chunk_end` by reference. Because tasks are scheduled asynchronously, all tasks for a given symbol batch could see the final loop values rather than the intended chunk boundaries. Fixed by binding both variables as default arguments so each closure captures the correct chunk range at definition time.
- **Stress test dead code cleaned up** (`scripts/stress_test.py`): Removed unused `statistics` import, unused `placed_order_ids` list, and two unused local variable assignments (`data`, `resp`) left over from a partially-removed order-ID-tracking feature.

### Changed

- **Backfill engine now iterates date-first for partition locality** (`gateway/core/backfill.py`): Previously symbols were processed concurrently with each symbol iterating its own date chunks, interleaving events from different dates in the Redis stream. Now the outer loop iterates date chunks and the inner loop processes symbols concurrently within each chunk. This ensures Heber's consumer receives all events for a given date together, enabling larger partition flushes and fewer tiny parquet files.
- **Backfill events sorted by timestamp before publishing** (`gateway/core/backfill.py`): Items within each chunk are sorted by timestamp before wrapping in envelopes, further improving downstream partition locality.
- **Increased Redis stream MAXLEN to 500K** (`docker-compose.yml`): Previous 100K limit caused trimming during 300K+ record backfills before Heber could consume them.

### Added

- **Dedicated thread pool for Alpaca trading calls** (`gateway/api/alpaca/trading.py`, `gateway/config.py`): Trading SDK calls previously used Python's default `ThreadPoolExecutor` (~16 workers shared across all async tasks). With 5 trading systems polling concurrently, threads exhausted and requests hit the timeout. Added a dedicated 8-thread pool (`GATEWAY_ALPACA_TRADING_THREAD_POOL_SIZE`) exclusively for Alpaca trading calls, preventing thread exhaustion under concurrent load.

### Changed

- **Increased trading call timeout from 10s to 15s** (`gateway/config.py`): `alpaca_trading_call_timeout_seconds` default raised from 10.0 to 15.0 to accommodate slower responses under heavy concurrent load.
- **Uvicorn now runs with 2 workers** (`Dockerfile`): Added `--workers 2` to the CMD entrypoint for better concurrency handling when multiple trading systems poll simultaneously.

### Fixed

- **Redis sink connection warnings flooding test output and logs** (`gateway/core/redis_sink.py`, `gateway/core/circuit_breaker.py`, `gateway/core/data_sink.py`): Intermediate retry resets in `RedisStreamsSink.publish()` logged at WARNING for every attempt, producing 14+ `redis_sink_connection_reset` and 8+ `redis_sink_publish_error` warnings per test run. Intermediate retry resets now log at DEBUG; only the final exhaustion logs at WARNING. Circuit breaker `circuit_opened` events for data sinks downgraded from ERROR to WARNING (code `GW-W1013`) since the sink layer has its own retry/buffer logic. `DataSinkRegistry._safe_publish` no longer emits ERROR+traceback for sinks that handle their own metrics (avoids duplicate logging with RedisStreamsSink).
- **No specific handling for "Redis is loading" state** (`gateway/core/redis_sink.py`): When Redis is restarting and still loading its dataset, connections succeeded but operations failed with "Redis is loading the dataset in memory". Added `_is_redis_loading()` detection and a 2-second backoff between retries (vs. the default 0.1s) to give Redis time to finish loading before the next attempt.

- **Treasury poller hitting Alpha Vantage rate limits repeatedly** (`gateway/core/treasury_poller.py`, `gateway/providers/alphavantage.py`): The treasury poller fired API calls for each maturity in rapid succession with no inter-request delay, easily exceeding Alpha Vantage's free-tier limit of 5 calls/min. Added a 15-second delay between maturity fetches, exponential backoff (60s base, 5min max, 3 retries) on rate-limit errors, and graceful handling of premium-endpoint errors (permanently skips maturities that require a paid subscription). Also introduced `AlphaVantageRateLimitError` and `AlphaVantagePremiumError` exception types so callers can distinguish transient rate limits from permanent subscription failures. The poller now checks for a configured API key at startup and refuses to start without one, eliminating repeated `alphavantage_api_key_not_set` errors every poll cycle.

- **Validation/symbology warnings polluting error log** (`gateway/core/validator.py`, `gateway/core/symbology.py`, `gateway/core/stream.py`, `gateway/api/alpaca/crypto.py`): Downgraded `data_validation_failed`, `unknown_symbol_format`, `stream_validation_failed`, and `alpaca_invalid_crypto_pair` from WARNING to INFO. These are expected operational events (bad ticks rejected, unknown user-supplied symbols, invalid crypto pairs returning 400) not actionable errors. Eliminates ~48 spurious entries per test run from the error log while keeping them visible in the main log for monitoring.

- **Live websocket bars dropped after successful subscribe and trading calls could hang indefinitely** (`gateway/core/connections.py`, `gateway/main.py`, `gateway/core/stream.py`, `gateway/api/alpaca/trading.py`, `gateway/config.py`): The stream multiplexer tracked downstream subscriptions by WebSocket `connection_id`, but the optimized broadcast path delivered by authenticated `client.id`, so bar events could authenticate and subscribe cleanly and still vanish before reaching 3Roses. Added an explicit `broadcast_to_connection_ids()` path and wired the multiplexer to use it. Also made lazy stream subscribe return an error when the upstream stream is still not authenticated instead of pretending success, and wrapped sync Alpaca trading SDK calls in a bounded timeout with a `504 / GW-E5004` response so stuck `/orders` requests no longer hang the Gateway thread pool indefinitely.
- **`unusualwhales-python-client` missing from `local` optional deps and `uv.sources`** (`pyproject.toml`): Running `uv sync` (without `--all-extras`) uninstalled the vendored UW SDK, causing `ModuleNotFoundError: No module named 'unusualwhales'` on next startup. Re-added `unusualwhales-python-client` to the `local` extras group and `[tool.uv.sources]` so `uv sync --extra local --extra dev` restores it correctly.
- **ruff scanning `trading-bot/` sub-project** (`pyproject.toml`): `ruff check .` reported 100+ violations from the `trading-bot/` directory (a separate sub-project co-located inside Data-Gateway). Added `exclude = ["trading-bot"]` to `[tool.ruff]` so only `gateway/` source is linted.

### Added

- **Upstream concurrency semaphore for Alpaca** (`gateway/core/rate_limiter.py`, `gateway/config.py`, `gateway/api/alpaca/stock.py`, `gateway/api/alpaca/common.py`): Added `GATEWAY_ALPACA_MAX_CONCURRENT_REQUESTS` setting (default 25) that limits how many requests are simultaneously in-flight to the Alpaca API. At market open, 3Roses dispatches 8 workers × 248 symbols × 3 calls each = ~750 concurrent requests, which overwhelmed Alpaca and caused 81 HTTP 502 errors. The semaphore is applied to all Alpaca stock endpoints (bars, quotes, trades, snapshot) and all 39 usages of `execute_alpaca_provider_call`. Requests beyond the concurrency limit block until a slot opens, spreading the burst over time instead of hitting Alpaca all at once.

### Fixed

- **SIGHUP handler logs misleading "Not supported on this platform" during tests** (`gateway/main.py`): `signal.signal(signal.SIGHUP)` raises `ValueError` when called from a non-main thread (e.g. during pytest), not `OSError` for unsupported platforms. Split the exception handler to log `ValueError` at DEBUG level with an accurate message, reserving the WARNING for actual platform incompatibility. Eliminates ~25 spurious warning-level log entries per test run.
- **Validator rejects valid stock symbols with dots, hyphens, or 6 characters** (`gateway/core/validator.py`): `STOCK_PATTERN` was `^[A-Z]{1,5}$`, which rejected class shares (`BRK.A`, `BRK.B`), preferred shares (`BAC-PL`), and 6-character tickers. Expanded pattern to `^[A-Z]{1,6}(?:[.\-][A-Z]{1,2})?$`.
- **Duplicate `_parse_alpaca_time` function in calendar API** (`gateway/api/calendar.py`): The function was defined twice (lines 42 and 83), with the second copy shadowing the first. Removed the duplicate.

- **UW SDK fallback paths flooding error log with warnings** (`gateway/providers/uw/flow.py`): Downgraded `uw_sector_tide_sdk_missing` (214/day), `uw_darkpool_recent_sdk_failed` (25/day), and `uw_etf_tide_sdk_missing` from `logger.warning` to `logger.debug`. These are expected SDK limitations with working raw HTTP fallbacks, not actionable errors.

- **RedisCache operations after shutdown cause "Event loop is closed" errors** (`gateway/core/cache.py`): Added a `_closed` flag to `RedisCache` that prevents any reconnection attempts after `close()` is called. All public methods (`get`, `set`, `mget`, `set_many`, `set_nx`, `delete`, `exists`) now bail out immediately when closed. This eliminates the ~150/day `redis_cache_get_error` "Event loop is closed" warnings caused by dedup checks racing with shutdown. Also fixed deprecation warning by using `aclose()` when available.
- **RedisStreamsSink reconnects after close during shutdown** (`gateway/core/redis_sink.py`): Added a `_closed` flag to prevent `_ensure_connected()` from creating new connections after `close()` is called. Fixed `close()` to properly disconnect the underlying connection pool (previously only closed the client handle, leaving TCP sockets open).
- **DataSinkRegistry never closes dedup cache** (`gateway/core/data_sink.py`): `close_all()` closed registered sinks but not the dedup `RedisCache`, which continued accepting operations on a closing event loop. Now explicitly closes the dedup cache before sinks.
- **Treasury poller never started at application boot** (`gateway/main.py`): The `start_treasury_poller()` function existed but was never called during the FastAPI lifespan, so treasury yields were never polled or published to Heber. Wired the poller into startup (gated on data sink enabled + AlphaVantage provider with API key) and shutdown (step 7).
- **AlphaVantage health check wrapped in unnecessary `@http_retry`** (`gateway/providers/alphavantage.py`): The health check catches all exceptions internally and returns `HealthStatus`, so the tenacity retry decorator was dead code that could mask unexpected errors. Removed it.
- **Treasury yield events missing dedup fields** (`gateway/core/envelope.py`): `treasury_yields` was absent from `FEED_UNIQUE_FIELDS`, so `compute_event_id` skipped field extraction and produced unstable hashes for treasury data. Added entry with `date`, `maturity`, and `yield_pct` fields.
- **Treasury poller maturity list not configurable via env** (`gateway/config.py`): `Settings` had no field for treasury maturities, making it impossible to override the default 2-year/10-year list. Added `GATEWAY_TREASURY_POLLER_MATURITIES` setting (comma-separated) with automatic filtering of invalid values and a safe default fallback.
- **`NormalizedOptionContract.underlying_price` always None** (`gateway/schemas/__init__.py`): The inline schema definition in `__init__.py` was missing the `underlying_price` field present in `gateway/schemas/options.py`. AlpacaProvider correctly extracted the value from API responses but the model silently dropped it.
- **UP047 lint errors in generic functions** (`gateway/api/alpaca/common.py`, `gateway/providers/uw.py`): Three generic functions were using legacy `TypeVar` syntax instead of Python 3.12 type parameters, causing ruff UP047 violations.

- **UW options endpoints returning 404 (2K+ daily)** (`gateway/api/uw/options.py`, `options_data.py`): The UW options routers had no prefix, causing `/{symbol}/max-pain` and similar routes to be shadowed by other routers' `/{symbol}` catch-all. Added `prefix="/options"` to both routers so `/api/v1/uw/options/SPY/max-pain` resolves correctly. Removed redundant `/options/{symbol}/iv-rank` alias.
- **Invalid order `side` silently defaults to SELL** (`gateway/providers/alpaca/trading.py`): Any value other than `"buy"` — including typos — mapped to `OrderSide.SELL`. Added explicit validation that raises `ValueError` for invalid side values.
- **`replace_order` truncates fractional shares** (`gateway/providers/alpaca/trading.py`): `int(qty)` silently dropped fractional quantities. Removed the cast so Alpaca receives the exact quantity.
- **`compute_event_id` hash instability from Decimal→float** (`gateway/core/envelope.py`): `str(float(field))` produced non-deterministic representations for certain Decimal values, causing duplicate events to get different hashes. Changed to `str(field)` for exact Decimal serialization.
- **Crypto misclassification of equities containing "USD"** (`gateway/core/envelope.py`): The `_infer_instrument_type` substring check for `"USD"` matched equity tickers. Tightened to require prefix/suffix match on `BTC`, `ETH`, `USDT` only.
- **REST events incorrectly tagged as "cached"** (`gateway/core/envelope.py`): All REST-sourced events received a `"cached"` quality flag regardless of whether data came from cache. Removed the unconditional flag.
- **`UnboundLocalError` in Redis sink health check** (`gateway/core/redis_sink.py`): If `_ensure_connected()` raised before `client` was assigned, the except handler referenced an unbound variable. Initialized `client = None` before the try block.
- **Non-atomic drain lock check** (`gateway/core/redis_sink.py`): `_drain_buffer` checked `.locked()` then acquired — allowing double-drain or missed drain. Simplified to always acquire the lock.
- **`ttl=0` silently uses default TTL** (`gateway/core/cache.py`): The truthiness check `if ttl` treated `0` as falsy. Changed to `if ttl is not None`.
- **`datetime.replace(hour=24)` overflow at midnight** (`gateway/core/replay.py`): Mock data generator crashed at hour 23, minute 59. Replaced manual arithmetic with `timedelta(minutes=1)`.
- **Market close boundary off-by-one** (`gateway/core/calendar.py`): `is_market_open` used `<=` on closing time, counting 16:00:00 as open. Changed to strict `<`.
- **`cancel_job` never cancels the background task** (`gateway/core/bulk.py`): Set status to FAILED but left the asyncio task running. Now calls `task.cancel()`.
- **UW `realized_vol_60d` typo** (`gateway/providers/uw/market.py`): Guard checked `realized_60d` (missing `_vol_`), so the field was always `None`.
- **UW `get_market_correlations` ignores date params** (`gateway/providers/uw/market.py`): `start_date` and `end_date` were accepted but never forwarded to the API.
- **UW `get_intraday_option_data` passes `None` instead of UNSET** (`gateway/providers/uw/options.py`): `date_str` was passed directly instead of via `_or_unset()`.
- **UW `min_premium` truncated to int** (`gateway/providers/uw/options.py`): `int(min_premium)` discarded fractional values. Removed the cast.
- **SEC `get_filings` under-delivers when `form_type` is set** (`gateway/providers/sec.py`): Loop bound was `min(len(forms), limit)`, skipping filtered entries counted against the limit. Now iterates the full list, breaking only when enough matching filings are collected.
- **Alpha Vantage health check inverted logic** (`gateway/providers/alphavantage.py`): Declared healthy when `"Note"` key was absent, even for error payloads. Now requires `"Global Quote"` present AND no `"Note"` or `"Information"` keys.
- **`check_permission` default-allows unlisted paths** (`gateway/core/security.py`): New endpoints were accessible without auth. Changed to default-deny.
- **Catalog endpoints publicly accessible** (`gateway/api/catalog.py`): Duplicate `router` definition at line 339 replaced the authenticated router with an unauthenticated one. Removed the duplicate.
- **`get_asset` passes un-normalized symbol** (`gateway/api/alpaca/trading.py`): Provider received raw lowercase input instead of the uppercased `normalized_symbol`.
- **`size % 100` TypeError on null trade size** (`gateway/core/quality.py`): Added type guard for `None` and non-numeric sizes.
- **`date.today()` uses local time for option expiry** (`gateway/core/symbology.py`): Replaced with `datetime.now(UTC).date()` for consistent UTC behavior.
- **Debug log fires every poll cycle** (`gateway/core/uw_poller.py`): Removed `INFO`-level `uw_poller_debug_first_alert` log that serialized the first alert on every 5-minute poll.
- **Sink registry never closed in shutdown** (`gateway/main.py`): Added `sink_registry.close_all()` to the shutdown sequence to close Redis connection pool.
- **Plaintext API key echoed to stdout** (`gateway/cli.py`): `hash-key` command now masks the key as `xxxx...xxxx`.
- **Missing `exc_info=True` on replay failure** (`gateway/core/replay.py`): Stack trace was lost on replay errors.
- **Type annotation `RequestDeduplicator = None`** (`gateway/core/dedup.py`): Fixed to `RequestDeduplicator | None = None`.
- **Calendar holiday names not resolved when `exchange_calendars` is installed** (`gateway/core/calendar.py`): `_resolve_holiday_name()` was iterating `cal.regular_holidays` directly, but `HolidayCalendar` is not iterable. Fixed to iterate `cal.regular_holidays.rules`.
- **Import sort order across all gateway modules** (`gateway/api/`, `gateway/core/`, `gateway/providers/`): 85 import blocks reordered, 14 unused imports removed. Auto-fixed with `ruff --fix`.

- **`uv sync` without extras uninstalls `unusualwhales-python-client`** (`pyproject.toml`, `CLAUDE.md`): The `unusualwhales-python-client`, `empire-core`, and `empire-schemas` packages are declared under `[project.optional-dependencies].local`. Running `uv sync` without `--extra local` silently uninstalls them, causing `uw_sector_tide_sdk_missing` warnings on every poll cycle and potential runtime failures. Updated the `CLAUDE.md` `Commands` section to use `uv sync --extra local --extra dev` and added a callout note explaining why this is required for local development and CI.

### Added

- **Centralized logger shim** (`gateway/core/logger.py`): All ~80 gateway modules now import from a single shim that delegates to `empire_core.logger`, enabling structured JSON output, daily log rotation, and trace/correlation ID propagation across the entire service.
- **Dynamic trading calendar via `exchange_calendars`** (`gateway/core/calendar.py`): Holiday and early-close detection now uses the NYSE calendar from `exchange_calendars` (XNYS) instead of hardcoded dicts. The 2024–2026 dicts remain as fallback. Holidays for 2027+ now resolve correctly.
- **WebSocket dead-client detection** (`gateway/api/websocket.py`): Heartbeat loop now tracks `last_received` timestamp from client messages and disconnects after 90s of silence, catching clients that keep TCP open but stop responding.
- **Circuit breaker HALF_OPEN single-probe guard** (`gateway/core/circuit_breaker.py`): Added `_half_open_in_progress` flag preventing multiple concurrent probes during HALF_OPEN state.

- **Standard pytest markers registered** (`pyproject.toml`): Added `unit`, `integration`, `e2e`, and `slow` marker definitions to `[tool.pytest.ini_options]` to align with the monorepo standard. Previously only `perf` was declared, causing `-m "unit"` to deselect all 827 tests. Markers are currently reserved; no tests are tagged yet.

- **Kairos client** (`config/clients.yaml`): Added API client entry for the Kairos options swing trading system with trader role and permissions for `alpaca`/`uw` providers, bars/quotes/trades/options/option_quotes/flow/flow_alerts/greek_exposure feeds, 100 max symbols, 600 req/min rate limit.

### Changed

- **empire-core dependency** (`uv.lock`): Locked `empire-core` v1.1.0 now declares `pandas>=2.0` as a required dependency, reflected in the resolved lock file.

### Fixed

- **`get_etf_tide` fails with `AttributeError` when SDK lacks `market.get_etf_tide`** (`gateway/providers/uw/flow.py`): Same class of bug as the `get_sector_tide` fix — the local SDK version does not expose `market.get_etf_tide`, causing `uw_etf_tide_failed` errors on every EOD poll cycle. Added a `hasattr` guard that falls back to a raw HTTP call to `/api/etf/{symbol}/tide` when the SDK attribute is absent. Extracted `_parse_etf_tide_items()` helper to deduplicate parsing logic between the SDK and raw-HTTP paths.

- **`data-gateway-redis` ephemeral** (`docker-compose.yml`): Disabled AOF/RDB persistence and capped memory at 512mb with LRU eviction. Eliminates the 15-second startup penalty that caused flow alert data loss during the 576+ container restarts.
- **Events buffered through circuit breaker OPEN state** (`gateway/core/data_sink.py`, `redis_sink.py`): When the Redis sink's circuit breaker opens, events are now routed to the sink's 10K-event buffer instead of being silently dropped. They are replayed automatically when Redis recovers.
- **`uw_poller_no_sink` promoted to WARNING** (`gateway/core/uw_poller.py`): Previously logged at DEBUG, making Redis sink outages invisible in production logs.

### Added

- `sink_available` field in UW poller runtime snapshot (`gateway/core/uw_poller.py`)
- `data_sink` component status in `/health/status` endpoint (`gateway/api/health.py`) for monitoring integration

### Fixed

- **`get_sector_tide` fails with `AttributeError` when running outside Docker** (`gateway/providers/uw/flow.py`): The local conda environment installs `unusualwhales-python-client` 5.0.1 from PyPI which lacks `market.get_sector_tide`. The Docker container correctly installs the vendored 5.1 SDK, but local runs produced 22 `uw_sector_tide_failed` / `uw_poller_sector_tide_error` errors per poll cycle. Added a `hasattr` guard that falls back to a raw HTTP call to `/api/market/{sector}/sector-tide` when the SDK attribute is absent, matching the existing darkpool raw-HTTP fallback pattern. Also extracted `_parse_sector_tide_items()` helper to deduplicate parsing logic between the SDK and raw-HTTP paths.

- **WebSocket bar relay completely broken — zero bars delivered to downstream clients** (`gateway/core/connections.py`): `ConnectionManager.broadcast()` looked up client IDs in `_client_map` (keyed by application-level client IDs like `"3roses"`), but the stream multiplexer's `SubscriptionManager` stores and passes **connection UUIDs**. Every lookup returned `None`, so `targets` was always empty and zero bars were ever sent. Added fallback: when a provided ID is not found in `_client_map`, check `_connections` directly (which is keyed by connection UUID). Confirmed fix with live market test — SPY and AAPL bars now flow correctly.


- **`_safe_int` undefined in four UW provider modules** (`gateway/providers/uw/earnings.py`, `market.py`, `flow.py`, `institutional.py`): All four modules called `_safe_int()` but only imported `ERR_NOT_INITIALIZED` and `_or_unset` from `._base`. At runtime this would raise `NameError: name '_safe_int' is not defined` for any endpoint that processes volume, open interest, short interest, or OI data. Added `_safe_int` to the import in each affected file.

- **Unused import `ERR_PROVIDER_NOT_INITIALIZED`** (`gateway/providers/alpaca/trading.py`): `ERR_PROVIDER_NOT_INITIALIZED` was imported but never referenced in the module. Removed it to keep the import clean.

- **Incomplete historical data from crypto, options, and forex endpoints** (`gateway/providers/alpaca/crypto.py`, `options.py`, `forex.py`): Five historical data methods were making a single API request and silently truncating results at the `limit` parameter instead of paginating through all available data. Added `next_page_token` pagination loops (matching the pattern already used by stock bars/trades/quotes) to: `get_crypto_bars()`, `get_crypto_trades()`, `get_option_bars()`, `get_option_trades()`, and `get_forex_rates_historical()`. Also normalized per-request limits to `max(1, min(limit, 10000))` for consistency.

- **Option chain and snapshots missing pagination** (`gateway/providers/alpaca/options.py`): `get_option_chain()` and `get_option_snapshots()` made single requests, silently truncating chains at 1000 contracts. AAPL can have 5000+ contracts. Added `next_page_token` pagination loops to both methods.

- **News endpoint missing pagination** (`gateway/providers/alpaca/news.py`): `get_news()` made a single request capped at 50 articles. Added pagination loop with caller-specified `limit` as the total cap.

- **`do_not_exercise_option` hitting wrong API URL** (`gateway/providers/alpaca/trading.py`, `_base.py`): The do-not-exercise REST call was using `self._base_url` (`data.alpaca.markets`, the data API) instead of the trading API (`api.alpaca.markets`). Added `_trading_base_url` field to `AlpacaBaseMixin` and fixed the method to use it. Also fixed the guard clause to check `_trading_client` instead of `_client`.

- **Missing `@http_retry` on paginated methods** (`gateway/providers/alpaca/crypto.py`, `options.py`): `get_historical_crypto_quotes()` and `get_historical_option_quotes()` were missing the `@http_retry` decorator, meaning transient failures mid-pagination were not retried.

- **Pagination `limit` parameter ignored as total cap** (`gateway/providers/alpaca/market.py`, `crypto.py`, `options.py`, `forex.py`): All paginated methods used the caller's `limit` as the per-page size only — the pagination loop continued fetching ALL data regardless. For example, `limit=100` would still fetch all 10,000+ bars across multiple pages. Added total-result capping to all 8 paginated methods: `get_bars()`, `get_trades()`, `get_historical_quotes()`, `get_crypto_bars()`, `get_crypto_trades()`, `get_historical_crypto_quotes()`, `get_option_bars()`, `get_option_trades()`, `get_historical_option_quotes()`, and `get_forex_rates_historical()`. The loop now breaks when `len(results) >= limit` and trims to exact limit.

- **Streaming bars missing `timeframe` field** (`gateway/core/stream.py`): Alpaca WebSocket bar messages don't include a `timeframe` field. The streaming handler passed raw messages to `fast_wrap_streaming_event` without injecting it, so Heber wrote all streaming bars with `timeframe=null` — making it impossible to distinguish 1Min bars from other timeframes in Silver. Added `timeframe="1Min"` injection for streaming bars.

- **UW `bool()` treats string `"false"` as True** (`gateway/providers/uw/_base.py`): Seven boolean fields (`is_sweep`, `is_unusual`, `all_opening_trades`, `has_floor`, `has_multileg`, `has_singleleg`, `canceled`) used Python's `bool()` which treats any non-empty string as True. If the UW API returns `"false"` (string), trades would be incorrectly marked as sweeps, canceled, etc. Added `_safe_bool()` helper that parses string boolean representations correctly.

- **OPRA options streaming `ts_event` broken by msgpack.Timestamp** (`gateway/core/envelope.py`): `fast_wrap_streaming_event` failed to convert `msgpack.Timestamp` objects from the OPRA options stream to ISO strings. The fallback `str(ts_event)` produced `"Timestamp(seconds=..., nanoseconds=0)"` which caused Heber's Pydantic validation to fail, sending all OPRA events to the DLQ. Added explicit handling for `msgpack.Timestamp` (via `to_datetime().isoformat()`) and epoch integers.

- **Trading API defaults to LIVE instead of paper** (`gateway/providers/alpaca/_base.py`): `TRADING_BASE_URL` was set to `https://api.alpaca.markets` (live trading), while `config.py` defaults to `https://paper-api.alpaca.markets`. When `APCA_API_BASE_URL` env var is unset, the provider would connect to the live trading API instead of paper, violating the "paper/noop mode must be the default" safety rule. Changed the constant to default to paper.

- **AlphaVantage `adjusted` close price ignored for daily/weekly bars** (`gateway/providers/alphavantage.py`): When `adjusted=True`, `get_daily` and `get_weekly` used field `"4. close"` (raw close) instead of `"5. adjusted close"` (dividend-adjusted). The `adjusted` parameter was effectively non-functional for close prices on these timeframes. Monthly was already correct. Fixed daily and weekly to use `"5. adjusted close"` when adjusted data is available.

- **NaN/Inf prices bypass data validator** (`gateway/core/validator.py`): The `_to_float` method converted `"NaN"` to `float('nan')`, which silently passed all price validation checks because NaN comparisons always return False in IEEE 754 (e.g., `nan <= 0` is False). Bars with NaN/Inf prices flowed through to Heber as corrupt data. Added `math.isnan`/`math.isinf` rejection in `_to_float` and explicit non-finite detection before price validation.

- **Treasury yield poller `maturity` parameter silently ignored** (`gateway/providers/alphavantage.py`, `gateway/core/treasury_poller.py`): The `get_economic_indicator` method didn't accept a `maturity` parameter, but the treasury poller passed `maturity=maturity` as a kwarg. This caused a `TypeError` caught by the generic exception handler, silently skipping ALL maturities. All treasury yield polls produced zero data. Added `maturity` parameter to `get_economic_indicator` with proper passthrough to the Alpha Vantage API.

- **Bulk bars `format=parquet` silently returns JSON** (`gateway/core/bulk.py`, `gateway/api/bulk.py`): The bulk bars endpoint accepted `format=parquet` as valid, but no parquet serialization code exists. Users requesting parquet silently received JSONL data. Removed `parquet` from accepted formats until implementation exists.

- **UW provider `int()` crash on float strings** (`gateway/providers/uw/*.py`, `_base.py`): The UW API returns numeric fields as float strings (e.g., `'12345.67'` for volume), but 47 callsites across the UW provider used bare `int(get(...))` which crashes with `ValueError` on float strings. Added `_safe_int()` helper that uses `int(float())` to handle both integer and float string formats, and applied it across all UW provider files (options, flow, market, institutional, earnings, _base).

- **WebSocket symbol validation rejects crypto/options** (`gateway/core/security.py`): `validate_symbols_array()` defaulted to stock validation (`^[A-Z]{1,5}$`) when called without `symbol_type`, rejecting valid crypto pairs (`BTC/USD`) and option contracts (`AAPL250117C00200000`). This blocked WebSocket clients from subscribing to crypto and options feeds. Fixed to accept any known symbol format when no type is specified.

- **AlphaVantage `max_points` parameter ignored** (`gateway/providers/alphavantage.py`): Three methods (`get_crypto_daily`, `get_forex_daily`, `get_indicator`) accepted `max_points` as a parameter but hardcoded `limit=100` internally, silently ignoring the caller's request. Fixed to use `max_points`.

- **Rate limiter phantom request inflation** (`gateway/core/rate_limiter.py`): `ProviderRateLimiter.try_acquire()` recorded timestamps in the per-second bucket before checking per-minute/per-day limits. When per-minute rejected the request, the per-second bucket was already inflated with a phantom entry. Over time this caused premature throttling. Refactored to two-phase check-then-record: all buckets are checked for capacity first, then all are recorded atomically.

- **Inconsistent `model_dump()` serialization across API endpoints** (`gateway/api/alpaca/crypto.py`, `options.py`, `news.py`, `screener.py`, `corporate.py`, `market.py`): Stock endpoints used `model_dump(mode="json")` producing JSON-safe types (strings for Decimal/datetime), while crypto, options, news, screener, corporate, and market endpoints used bare `model_dump()` producing Python-native types. Standardized all API response serialization to use `mode="json"`.

- **DataSinkRegistry backpressure test timeout** (`gateway/core/data_sink.py`, `tests/perf/test_perf_stream_sink.py`): The 2-second `slot_wait_timeout` introduced in the UW burst fix caused three perf tests to time out (>30s) or produce incorrect peak-task assertions when tested with a permanently-blocking sink. Added a `slot_wait_timeout` parameter to `DataSinkRegistry.__init__` (default `2.0s` preserves production burst-tolerance) and updated the three perf tests that exercise blocked/slow sinks to use `slot_wait_timeout=0.0` (immediate drop), matching their original intended behavior. All 819 unit tests now pass.

- **Dev environment bootstrap** (`pyproject.toml`): `unusualwhales-python-client` (a local path package) was missing from `uv.sources` and `[project.optional-dependencies] local`, causing `ModuleNotFoundError: No module named 'unusualwhales'` on fresh `uv sync`. Package added to the `local` extras group alongside `empire-schemas` so `uv sync --all-extras` installs it correctly.
- **Import sort in `gateway/main.py`**: Sorted stdlib/third-party/local import blocks to satisfy ruff `I001`.

### Changed

- **Option chain snapshot payload contract** (`gateway/core/option_capture.py`, `gateway/schemas/__init__.py`): Added optional `underlying_price` to normalized option contracts and publish it at the top level of `option_chain_snapshot` envelopes so downstream storage and replay do not need to infer spot from per-contract prices.
- **Option capture quality telemetry** (`gateway/core/option_capture.py`, `gateway/core/metrics.py`, `gateway/api/admin.py`): Added per-symbol snapshot quality stats for contract count, Greeks coverage, IV coverage, non-zero open-interest coverage, bid/ask coverage, snapshot age, and websocket add/remove counts. These now show up in the option capture runtime snapshot for admin status and in Prometheus metrics.
- **OPRA-first option streaming** (`gateway/config.py`, `gateway/core/stream.py`, `gateway/main.py`, `docker-compose.yml`): Added `stream_options_feed` / `GATEWAY_STREAM_OPTIONS_FEED` with `opra` as the default, and pass the configured options feed into the Alpaca multiplexer at startup.
- **Budgeted option websocket universe** (`gateway/config.py`, `gateway/core/option_capture.py`): Added `option_capture_ws_contract_limit_per_symbol` with a default budget of 40 contracts per underlying. Full chain snapshots still land in Heber, while websocket `quotes`/`trades` subscriptions are capped to the nearest-expiry, near-ATM, tighter-spread, more-liquid contracts per symbol.

### Fixed

- **UW OI change parsing crash** (`gateway/providers/uw/options.py`): The UW API returns OI change values as float strings (e.g., `'0.73297002724795640327'`), which caused `int()` to raise `ValueError: invalid literal for int() with base 10`. Replaced bare `int()` casts with `int(float(...))` for all numeric fields in `get_oi_change()` — `call_oi`, `put_oi`, `call_oi_change`, `put_oi_change`, `prev_oi`, `volume`, and `trades`. This was causing 100% failure on the EOD OI change poller (29/29 tickers every cycle).

- **OPRA option REST alignment** (`gateway/providers/alpaca.py`, `tests/test_alpaca_provider.py`): Option chain snapshots, option quotes, option trades, and option snapshot REST calls now use the configured options feed instead of being hardcoded to `indicative`. The provider defaults to `opra`, honors explicit overrides, and now coerces string trade conditions into the normalized list form required by `NormalizedTrade`.
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
