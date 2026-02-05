# Technical Debt Audit — Data Gateway

Date: 2026-02-05

This audit is a static code review (no runtime tests executed). It surfaces technical debt, creates a prioritized remediation backlog, and documents what was audited. Vendor/third‑party code was not audited.

**Method**
- Manual review of first‑party source and project docs.
- Pattern scans (auth usage, cache key behavior, pagination) across API subrouters and providers.
- No runtime or test execution.

## Executive Summary

The codebase is feature‑rich but carries substantial technical debt concentrated in access control enforcement, cache isolation, duplicated subsystems, and several “stub” features that return mock or empty data. Core security and correctness controls are defined but not enforced. Operational behavior in containerized deployments likely diverges from intent due to configuration mismatches and disabled Redis/data sink features.

Top risks to address first:
1. Cache isolation and authorization enforcement (potential cross‑client data leakage).
2. Permissions, RBAC, and per‑client limits are defined but not enforced.
3. WebSocket disconnects do not clean up upstream subscriptions.
4. Multiple endpoints expose stub/mock data while appearing production‑ready.
5. Pagination and cache key bugs that can corrupt API results.

## Audit Coverage

**Deep review (line‑by‑line)**
- `README.md`
- `PRD.md`
- `codebase.md`
- `pyproject.toml`
- `Dockerfile`
- `docker-compose.yml`
- `SECURITY.md`
- `production.md`
- `Audit_Checklist.md`
- `coverage.json`
- `gateway/main.py`
- `gateway/config.py`
- `gateway/cli.py`
- `gateway/api/deps.py`
- `gateway/api/middleware.py`
- `gateway/api/websocket.py`
- `gateway/api/admin.py`
- `gateway/api/health.py`
- `gateway/api/metrics.py`
- `gateway/api/bulk.py`
- `gateway/api/replay.py`
- `gateway/api/quality.py`
- `gateway/api/corporate.py`
- `gateway/api/calendar.py`
- `gateway/api/symbology.py`
- `gateway/api/catalog.py`
- `gateway/api/news.py`
- `gateway/api/sec.py`
- `gateway/api/yf.py`
- `gateway/api/alpaca/__init__.py`
- `gateway/api/alpaca/stock.py`
- `gateway/api/alpaca/options.py`
- `gateway/api/alpaca/crypto.py`
- `gateway/api/alpaca/forex.py`
- `gateway/api/alpaca/news.py`
- `gateway/api/alpaca/corporate.py`
- `gateway/api/alpaca/metadata.py`
- `gateway/api/alpaca/trading.py`
- `gateway/api/alpaca/account.py`
- `gateway/api/alphavantage/__init__.py`
- `gateway/api/alphavantage/common.py`
- `gateway/api/alphavantage/timeseries.py`
- `gateway/api/alphavantage/indicators.py`
- `gateway/api/alphavantage/economic.py`
- `gateway/api/finnhub/__init__.py`
- `gateway/api/finnhub/common.py`
- `gateway/api/finnhub/quotes.py`
- `gateway/api/finnhub/news.py`
- `gateway/api/uw/common.py`
- `gateway/api/uw/flow.py`
- `gateway/api/uw/market.py`
- `gateway/api/uw/etf.py`
- `gateway/api/uw/etf_extended.py`
- `gateway/core/auth.py`
- `gateway/core/cache.py`
- `gateway/core/redis_cache.py`
- `gateway/core/registry.py`
- `gateway/core/multiplexer.py`
- `gateway/core/stream.py`
- `gateway/core/connections.py`
- `gateway/core/rate_limiter.py`
- `gateway/core/corporate_actions.py`
- `gateway/core/adjustments.py`
- `gateway/core/replay.py`
- `gateway/core/bulk.py`
- `gateway/core/security.py`
- `gateway/core/validator.py`
- `gateway/core/quality.py`
- `gateway/core/metrics.py`
- `gateway/core/uw_poller.py`
- `gateway/core/calendar.py`
- `gateway/core/dedup.py`
- `gateway/core/envelope.py`
- `gateway/core/normalizer.py`
- `gateway/core/circuit_breaker.py`
- `gateway/core/balancer.py`
- `gateway/core/data_sink.py`
- `gateway/core/redis_sink.py`
- `gateway/core/symbology.py`
- `gateway/providers/alpaca.py`
- `gateway/providers/alpaca_stream.py`
- `gateway/providers/alpaca_crypto_stream.py`
- `gateway/providers/alpaca_options_stream.py`
- `gateway/providers/alpaca_news_stream.py`
- `gateway/providers/alphavantage.py`
- `gateway/providers/finnhub.py`
- `gateway/providers/uw.py`
- `gateway/providers/sec.py`
- `gateway/providers/yfinance.py`
- `gateway/providers/news.py`
- `gateway/api/_legacy/uw_monolithic.py`
- `gateway/schemas/__init__.py`
- `config/clients.yaml`
- `config/providers.yaml`
- `clients.yaml` (symlink to `config/clients.yaml`)
- `providers.yaml` (symlink to `config/providers.yaml`)

**Pattern review (structure + key risks, not every line)**
- Remaining `gateway/api/alpaca/*` files not listed above
- Remaining `gateway/api/alphavantage/*` files not listed above
- Remaining `gateway/api/finnhub/*` files not listed above
- Remaining `gateway/api/uw/*` files not listed above

**Not audited (future runs)**
- `tests/` (no test review)
- `unusualwhales_sdk/` and `vendor/unusualwhales_sdk/` (third‑party code not audited)
- `unusualwhales_openapi.yaml` and related docs

## Findings (Prioritized)

### Critical

**TD‑001: Cache key is not scoped by client or authorization**
Evidence: `gateway/api/middleware.py` `_cache_key` uses only method/path/query.
Impact: Cross‑client data leakage is possible for any response that varies by permissions or client context (admin, replay, bulk, etc.).
Recommendation: Include client identity and auth scope in cache keys, or disable caching for authenticated/sensitive routes. Maintain an allowlist for cacheable routes.
**Status:** Fixed (2026-02-05)

**TD‑002: Permissions, RBAC, and per‑client limits are defined but not enforced**
Evidence: `gateway/core/auth.py`, `gateway/core/security.py`, `gateway/api/deps.py`, `gateway/api/websocket.py`.
Impact: Provider/feeds/max‑symbol limits, RBAC matrix, and input validation policies are not applied in REST or WebSocket paths.
Recommendation: Enforce `ClientPermissions` and RBAC at the dependency or middleware layer. Apply `InputValidator` to REST params and WS payloads. Enforce `max_symbols` and `ws_subscriptions_max`.
**Status:** Fixed (2026-02-05)

**TD‑003: WebSocket disconnects do not clean up upstream subscriptions**
Evidence: `gateway/api/websocket.py` only calls `ConnectionManager.disconnect`; `StreamMultiplexer.client_disconnect` is never invoked.
Impact: Subscriptions leak, causing stale upstream subscriptions, memory growth, and increased data fanout overhead.
Recommendation: On disconnect, call `multiplexer.client_disconnect(connection_id)` and clear any per‑connection tracking.
**Status:** Fixed (2026-02-05)

**TD‑004: Bulk and replay jobs are not scoped to clients**
Evidence: `gateway/core/bulk.py` and `gateway/core/replay.py` store global job/session state; API endpoints expose all jobs by ID or list.
Impact: Any authenticated client can access or download other clients’ jobs or sessions.
Recommendation: Add client ownership to jobs/sessions and enforce access checks on all endpoints.
**Status:** Fixed (2026-02-05)

### High

**TD‑005: WebSocket news subscriptions are effectively unsupported**
Evidence: `gateway/core/stream.py` only tracks bars/quotes/trades. News messages are mapped but there is no subscription path for `news`.
Impact: Documented `news` feed cannot be subscribed to; data will never be routed to clients.
Recommendation: Add `news` to subscription tracking and update `websocket.py` subscribe/unsubscribe mapping.
**Status:** Fixed (2026-02-05)

**TD‑006: Multiple endpoints expose stub/mock data while appearing production‑ready**
Evidence: `gateway/core/replay.py` uses mock generator, `gateway/core/bulk.py` default fetcher returns empty, `gateway/api/bulk.py` has stub options endpoint, `gateway/api/quality.py` returns hard‑coded quality, `gateway/core/corporate_actions.py` uses `KNOWN_SPLITS` only, `gateway/core/calendar.EarningsCalendar` stub.
Impact: Downstream systems will treat these endpoints as reliable and get misleading or empty data.
Recommendation: Integrate with real provider data, or feature‑flag and clearly mark responses as stub/experimental.
**Status:** Fixed (2026-02-05)

**TD‑007: Admin and trading endpoints lack RBAC enforcement**
Evidence: `gateway/api/admin.py` only uses `require_api_key`; `gateway/api/alpaca/trading.py` and `gateway/api/alpaca/account.py` are accessible to any key.
Impact: Any client can access admin data or place trades if Alpaca credentials are configured.
Recommendation: Enforce RBAC roles for admin/trading routes (e.g., require admin or trading scope).
**Status:** Fixed (2026-02-05)

**TD‑008: Pagination cursor is broken for UW endpoints**
Evidence: `gateway/api/uw/common.paginate_response` decodes cursor but does not apply offset; always returns `data[:limit]`.
Impact: `cursor` does not advance results and provides misleading pagination behavior.
Recommendation: Apply offset when slicing and include offset in cache keys.
**Status:** Fixed (2026-02-05)

**TD‑009: Cache key builders drop falsy values, causing collisions**
Evidence: `gateway/api/alphavantage/common.cache_key`, `gateway/api/finnhub/common.cache_key`, `gateway/api/sec._cache_key`, `gateway/api/yf._cache_key` all use `if a` filtering.
Impact: Requests with `False`, `0`, or empty strings collapse to same cache key (e.g., `adjusted=False` vs `adjusted=True`).
Recommendation: Include all args explicitly; use sentinel strings for `None`.
**Status:** Fixed (2026-02-05)

**TD‑010: Calendar logic is incorrect and will drift**
Evidence: `gateway/core/calendar.py` uses UTC without ET conversion, manual date increment logic, and hard‑coded holiday tables only through 2026.
Impact: Market‑open checks can be wrong; trading days computation can skip or miscompute dates; holidays go stale yearly.
Recommendation: Use timezone‑aware ET logic and `date + timedelta(days=1)` for iteration. Integrate a market‑calendar provider or library.
**Status:** Fixed (2026-02-05)

**TD‑011: API path mismatches vs docs and internal conventions**
Evidence: `gateway/api/symbology.py` uses `/symbology` (not `/api/v1/symbology`), `gateway/api/corporate.py` uses `/corporate-actions` and `/adjustment-factors` (not `/api/v1/*`). README lists `/api/v1/symbology/*`.
Impact: Clients relying on documented paths will fail; inconsistent routing patterns.
Recommendation: Align routes to `/api/v1/*` or update docs and deprecate old paths.
**Status:** Fixed (2026-02-05)

### Medium

**TD‑012: Metrics are defined but not instrumented**
Evidence: `gateway/core/metrics.py` defines `record_request`, `record_cache_hit`, `record_provider_request`, `init_uptime`, but no middleware calls them; uptime metrics never initialized.
Impact: Prometheus metrics are incomplete; SLOs are not observable.
Recommendation: Add metrics middleware and provider wrappers; call `init_uptime` at startup and `update_uptime` periodically.
**Status:** Fixed (2026-02-05)

**TD‑013: Duplicate cache implementations and exports**
Evidence: `gateway/core/cache.py` and `gateway/core/redis_cache.py` both implement Redis/Hybrid caches; `gateway/core/__init__.py` exports the legacy cache while API deps use the newer implementation.
Impact: Confusion, inconsistent behavior, and accidental misuse.
Recommendation: Consolidate to one cache module and update imports/exports.
**Status:** Fixed (2026-02-05)

**TD‑014: Duplicate streaming subsystems**
Evidence: `gateway/core/stream.py` multiplexer plus `gateway/providers/alpaca_stream*.py` implement separate WebSocket stacks.
Impact: Increased maintenance, unclear source of truth, risk of divergence.
Recommendation: Choose one streaming implementation and delete or deprecate the other.
**Status:** Fixed (2026-02-05)
Note: Legacy `gateway/providers/alpaca_stream*.py` handlers were removed; `gateway/core/stream.py` is the only streaming path.

**TD‑015: WebSocket protocol schema mismatch**
Evidence: `gateway/schemas/__init__.py` defines `SubscribeMessage` with `feeds`, but `gateway/api/websocket.py` expects `feed` singular.
Impact: Clients built against schema may fail.
Recommendation: Align schema and implementation (prefer list of feeds to match PRD).
**Status:** Fixed (2026-02-05)

**TD‑016: Auth is missing on some endpoints**
Evidence: `gateway/api/corporate.py`, `gateway/api/quality.py`, `gateway/api/symbology.py`, `gateway/api/catalog.py`, `gateway/api/metrics.py`, `gateway/api/health.py` lack `require_api_key`.
Impact: Security policy mismatch; potential exposure if not intended public.
Recommendation: Either require auth or explicitly document these as public endpoints.
**Status:** Fixed (2026-02-05)

**TD‑017: Cache middleware can leak data across auth boundaries**
Evidence: Cache keys do not vary by `X‑Gateway‑Key`, headers, or permissions; EventEnvelopeMiddleware and CacheMiddleware read entire response bodies.
Impact: Data leak risk and memory pressure for large responses.
Recommendation: Add Vary‑like behavior to cache key and introduce size limits or streaming bypass.
**Status:** Fixed (2026-02-05)

**TD‑018: Client permission model is not applied to WebSocket subscriptions**
Evidence: `gateway/api/websocket.py` never checks `client.permissions.providers/feeds/max_symbols`.
Impact: A client can subscribe to feeds it should not have.
Recommendation: Enforce permissions and per‑client subscription limits before routing to multiplexer.
**Status:** Fixed (2026-02-05)

**TD‑019: Subscription state tracking is incomplete**
Evidence: `gateway/core/connections.py` has `subscriptions` but it is never updated; admin/status endpoints report misleading data.
Impact: Observability and operational diagnostics are wrong.
Recommendation: Track subscriptions on subscribe/unsubscribe or remove unused fields.
**Status:** Fixed (2026-02-05)

**TD‑020: Input validation and data validation are defined but not used**
Evidence: `gateway/core/security.InputValidator` and `gateway/core/validator.DataValidator` are not invoked by endpoints or stream processing.
Impact: Invalid inputs and malformed data can pass through.
Recommendation: Add validation hooks in API endpoints and stream ingestion.
**Status:** Fixed (2026-02-05)

**TD‑021: Provider capability declarations are inconsistent with implementations**
Evidence: `gateway/providers/finnhub.py` declares `supports_streaming=True` but no streaming implementation.
Impact: Provider routing and feature flags can be misleading.
Recommendation: Align capability flags with actual methods or implement missing features.
**Status:** Fixed (2026-02-05)

**TD‑022: News provider and docs mismatch**
Evidence: `gateway/providers/news.py` uses NewsAPI.org while README/PRD mention EventRegistry.
Impact: Mismatched expectations and API contracts.
Recommendation: Update docs or swap provider implementation.
**Status:** Fixed (2026-02-05)

**TD‑023: News get‑by‑ID endpoint is not supported**
Evidence: `gateway/providers/news.py.get_article` returns `{}` and logs warning; `gateway/api/news.py` returns 404 for empty result.
Impact: `/api/v1/news/articles/{id}` will always 404, despite being documented.
Recommendation: Remove endpoint, mark as unsupported, or implement with a different provider.
**Status:** Fixed (2026-02-05)

**TD‑024: UW health check does not validate upstream connectivity**
Evidence: `gateway/providers/uw.py.health_check` returns healthy when client exists without making API calls.
Impact: Health status can be false‑positive.
Recommendation: Make a lightweight API request or validate token.
**Status:** Fixed (2026-02-05)

**TD‑025: UW endpoints fetch full datasets then paginate in memory**
Evidence: `gateway/api/uw/*` often calls provider without limit or cursor and then paginates.
Impact: Expensive and non‑scalable for large datasets.
Recommendation: Use provider pagination or pass limit/cursor into provider API calls.
**Status:** Fixed (2026-02-05)

**TD‑026: Alpaca feed parameter is ignored**
Evidence: `gateway/api/alpaca/stock.py` accepts `feed` query param but `gateway/providers/alpaca.py.get_bars` uses `self._feed` from config only.
Impact: Client‑requested feed does not take effect.
Recommendation: Pass feed to provider and/or document that feed is fixed by config.
**Status:** Fixed (2026-02-05)

**TD‑027: SEC User‑Agent is hard‑coded**
Evidence: `gateway/providers/sec.py` uses a fixed email in `SEC_USER_AGENT`.
Impact: Compliance and operational flexibility issues.
Recommendation: Make User‑Agent configurable via env.
**Status:** Fixed (2026-02-05)

### Low

**TD‑028: Duplicate config files add maintenance overhead**
Evidence: Root `clients.yaml` and `providers.yaml` duplicated `config/clients.yaml` and `config/providers.yaml`.
Impact: Risk of drift and confusion about source of truth.
Recommendation: Keep a single source of truth and adjust Settings and Docker mounts.
**Status:** Fixed (2026-02-05)

**TD‑029: Admin error buffer is unused**
Evidence: `gateway/api/admin.log_error` is never called.
Impact: Admin logs endpoint does not reflect real errors.
Recommendation: Integrate with logging or remove.
**Status:** Fixed (2026-02-05)

**TD‑030: As‑of, labels, lineage, and ring buffer are unused**
Evidence: `gateway/core/asof.py`, `labels.py`, `lineage.py`, `ring_buffer.py` are not referenced by runtime paths.
Impact: Dead code increases maintenance cost and confusion.
Recommendation: Wire into the pipeline or remove/deprecate.
**Status:** Fixed (2026-02-05)

**TD‑031: Cache middleware and envelope wrapping re‑build responses**
Evidence: `gateway/api/middleware.py` reads full body and returns a new Response.
Impact: Memory pressure for large responses and breaks streaming responses.
Recommendation: Add size caps or bypass for large/streaming responses.
**Status:** Fixed (2026-02-05)

## Remediation Plan (Suggested Starting Point)

**Phase 0 — Safety and correctness (1–2 days)**
- Fix cache key isolation or disable caching for authenticated endpoints.
- Enforce RBAC and `ClientPermissions` in REST and WS.
- Fix WebSocket disconnect cleanup and subscription tracking.
- Fix UW pagination cursor logic.
- Fix cache key builders to include falsy values.
- Align routes with `/api/v1/*` conventions or update docs.

**Phase 1 — Feature completeness (1–2 weeks)**
- Integrate Bulk job fetcher with provider routing.
- Replace Replay mock data with real provider loader.
- Implement corporate actions provider integration.
- Wire Quality endpoints to real data.
- Use ProviderRegistry for News provider and deprecate unsupported endpoints.

**Phase 2 — Consolidation and scale (2–4 weeks)**
- Consolidate caching implementations.
- Choose a single streaming subsystem and retire duplicates.
- Move shared state to Redis or DB for multi‑worker deployments.
- Add metrics middleware and provider instrumentation.

**Phase 3 — Test hardening (ongoing)**
- Expand coverage for auth, caching, multiplexer, and provider routing.
- Add integration tests for WebSocket and bulk/replay flows.

## Open Questions for Future Audit Runs

- Remaining: Are UW, Finnhub, AlphaVantage, SEC, and yfinance endpoint implementations fully aligned with PRD error codes and schemas?
- Resolved (2026-02-05): `gateway/core/auth.py` remains the API key/RBAC source of truth; `gateway/core/security.py` remains for input validation and utility models.
- Resolved (2026-02-05): Path/auth policy is now explicit:
  - `/api/v1/symbology/*` authenticated (with `/symbology/*` legacy alias retained)
  - `/catalog/*` authenticated
  - `/metrics` authenticated
  - `/health/*` public
- Resolved (2026-02-05): Legacy `gateway/providers/alpaca_stream*.py` stack is retired and removed.
