export const meta = {
  name: 'data-gateway-remediation',
  description: 'Execute the Data-Gateway audit remediation plan (M0-M3) with opus subagents across phased barriers',
  phases: [
    { title: 'M0 Safety Net', detail: 'contract test, real-Redis integration tier, coverage floor, trading characterization tests', model: 'opus' },
    { title: 'M1 Critical Fixes', detail: 'key rotation + fine-grained trading authz, dedup-after-publish + EOD retry, strict envelopes, cache visibility', model: 'opus' },
    { title: 'M2 High-Leverage', detail: 'delete legacy providers, unify poller handlers, provider error contract, metrics cardinality', model: 'opus' },
    { title: 'M3 Polish', detail: 'split middleware, stream/dedup tests, repo slimming', model: 'opus' },
    { title: 'Verify', detail: 'full lint + type + test + integration matrix', model: 'opus' },
  ],
}

const REPORT = {
  type: 'object',
  additionalProperties: false,
  properties: {
    task: { type: 'string' },
    status: { type: 'string', enum: ['done', 'partial', 'blocked'] },
    files_changed: { type: 'array', items: { type: 'string' } },
    tests_command: { type: 'string', description: 'exact pytest/ruff command run' },
    tests_pass: { type: 'boolean' },
    summary: { type: 'string', description: 'what was changed and why, 3-6 sentences' },
    deviations: { type: 'string', description: 'anything done differently from the brief, or audit claims found to be wrong/already-handled' },
    blockers: { type: 'string', description: 'unresolved issues needing human input, or empty' },
  },
  required: ['task', 'status', 'files_changed', 'tests_pass', 'summary'],
}

const RULES = `
SHARED RULES (this is LIVE trading infrastructure — Kairos trades through it right now):
- Do NOT git commit, git push, git stash, or run \`make deploy\`.
- Do NOT restart/stop/recreate any Docker container or bounce any service.
- The 2026-06-10 audit that produced your task was partly inaccurate. VERIFY the current
  code state before changing it; if a mechanism the brief says to "add" already exists, do
  not duplicate it — adapt or improve it, and record that in \`deviations\`.
- Own ONLY the files listed in YOUR FILES. Do not edit files owned by another task.
- Match existing style. For any behavior change, append a \`CHANGELOG.md\` \`## [Unreleased]\`
  entry to the correct group (Added/Changed/Fixed/Removed) — append only, never reorder.
- Run targeted checks only (this repo is shared with parallel agents):
  \`ruff check <your files>\`, \`ruff format <your files>\`, and \`pytest <your test file(s)>\`.
  Do NOT run \`ruff format .\` or the whole suite — a final Verify phase does that.
- Repo root: /Users/jacobmcmillan/Empire/Data-Gateway. Use \`uv run pytest ...\`.
- Return your structured report. Be honest in tests_pass; if you couldn't run something, say so in blockers.
`

// ----------------------------------------------------------------------------
phase('M0 Safety Net')

const m0 = await parallel([
  () => agent(`${RULES}
TASK M0.1 — Envelope→Heber contract test (test-only, no production edits).
YOUR FILES: NEW tests/test_envelope_heber_contract.py (you may READ gateway/core/envelope.py but not edit it).

Heber's writer-side validator (heber/models/envelope.py) accepts instrument keys matching EXACTLY:
  equity: ^equity:[A-Z0-9]+(?:[.-][A-Z0-9]+)*$
  crypto: ^crypto:[A-Z]{2,10}-[A-Z]{2,10}$
  forex:  ^forex:[A-Z]{3}-[A-Z]{3}$
  option: ^option:OCC:[A-Z]{1,6}\\d{6}[CP]\\d{8}$
  macro:  ^macro:[a-z][a-z0-9_]*(?::[a-z0-9_]+)*$
Write a test that compiles these regexes locally and asserts that the instrument_key produced by
gateway.core.envelope.wrap_event AND fast_wrap_streaming_event validates for representative payloads
of every feed in FEED_UNIQUE_FIELDS and every instrument_type (equity/option/crypto/forex). Include a
negative control: an option key built WITHOUT the OCC contract (the historical bug) must FAIL validation,
proving the test has teeth. This is the regression net for the three prior option-key DLQ incidents.
Acceptance: \`uv run pytest tests/test_envelope_heber_contract.py\` passes; negative control fails when OCC infix removed.`,
    { label: 'M0.1 contract-test', phase: 'M0 Safety Net', model: 'opus', schema: REPORT }),

  () => agent(`${RULES}
TASK M0.2 — Real-Redis integration test tier.
YOUR FILES: .github/workflows/ci.yml ; NEW tests/integration/test_redis_sink_integration.py ; tests/conftest.py (ADDITIVE integration fixtures only — do not modify existing fixtures; do NOT touch pyproject.toml).
Read gateway/core/redis_sink.py and gateway/core/data_sink.py first. Add a CI job (or step) that runs a Redis
service container and executes \`pytest -m integration\`. Write integration tests (marked @pytest.mark.integration)
against a REAL redis (env GATEWAY_TEST_REDIS_URL, default redis://localhost:6379/15, flush the test DB) covering:
publish→XLEN increment, dedup TTL behavior, and the failed-event buffer fill→evict→drain path. Skip gracefully
(pytest.skip) if no Redis is reachable so the default local run isn't broken. The \`integration\` marker already
exists in pyproject.
Acceptance: tests pass against a local redis (start one if available: \`docker run -d -p 6379:6379 redis\` is NOT allowed —
instead detect data-gateway-redis on localhost:6379 and use DB 15); CI yaml is valid.`,
    { label: 'M0.2 integration-tier', phase: 'M0 Safety Net', model: 'opus', schema: REPORT }),

  () => agent(`${RULES}
TASK M0.3 — Coverage floor + apply pytest markers.
YOUR FILES: pyproject.toml ONLY.
First measure current coverage: \`uv run pytest --cov=gateway --cov-report=term -m 'not perf' -q\` (this may take a few min).
Then add \`[tool.coverage.report] fail_under = N\` where N is ~5 points BELOW the measured total (a floor that won't
make CI immediately red — record measured value and chosen N in summary). Apply the existing \`slow\`/\`integration\`
markers where obviously appropriate is OUT OF SCOPE for other agents' files — only document in summary which test
files should later be marked. Do not edit test files.
Acceptance: pyproject parses; \`uv run pytest --cov=gateway --cov-fail-under=<N> -m 'not perf' -q\` passes at the floor.`,
    { label: 'M0.3 coverage-floor', phase: 'M0 Safety Net', model: 'opus', schema: REPORT }),

  () => agent(`${RULES}
TASK M0.4 — Trading-route characterization tests (pin CURRENT behavior before M1 authz changes).
YOUR FILES: tests/test_alpaca_trading_router.py (APPEND new tests only; do not alter existing tests).
Read gateway/api/alpaca/trading.py and gateway/api/deps.py. The current authz: require_api_key →
_enforce_trading_role requires client.role in (trader/admin/super_admin) for /api/v1/alpaca/orders etc.
Add characterization tests that LOCK today's observable behavior: a trader-role key CAN place/cancel/close orders
(mocked provider), a non-trader/role-'client' key gets 403, and the success/error response shapes are pinned.
These must still pass AFTER M1 introduces a finer 'trading' permission for the granted clients, so assert via a
client that WILL retain trading rights (e.g. one with role trader AND the soon-to-exist trading permission) plus a
clearly-denied client. Keep them robust to the upcoming permission addition.
Acceptance: \`uv run pytest tests/test_alpaca_trading_router.py\` passes.`,
    { label: 'M0.4 trading-characterization', phase: 'M0 Safety Net', model: 'opus', schema: REPORT }),
])

log(`M0 complete: ${m0.filter(Boolean).map(r => r.task + '=' + r.status).join(', ')}`)

// ----------------------------------------------------------------------------
phase('M1 Critical Fixes')

const m1 = await parallel([
  () => agent(`${RULES}
TASK M1-SEC — API key hashing + rotation (incl. consumer repos) + fine-grained trading authz.
YOUR FILES: gateway/core/auth.py, config/clients.yaml, gateway/api/deps.py, gateway/api/alpaca/trading.py (only if a route-level permission check is the cleanest place),
  consumer .env files: /Users/jacobmcmillan/Empire/{Kairos,Cerberus,3Roses,Orion,Atlasv2,Orbit}/.env,
  NEW /Users/jacobmcmillan/Empire/Data-Gateway/config/ROTATION_HANDOFF.md (add it to .gitignore — append to existing .gitignore, that single line is allowed).

CURRENT STATE (verified — do not duplicate): deps.py already has _enforce_admin_role (gates all /api/v1/admin*,
roles admin/super_admin) and _enforce_trading_role (gates /api/v1/alpaca/{account,orders,positions,portfolio,
watchlists,assets,clock,calendar}, roles trader/admin/super_admin). The 'test' client has default role 'client'
and is ALREADY denied trading. clients.yaml: 7 clients use plaintext \`key:\`, only 'drogon' uses \`key_hash: sha256:<hex>\`.
Read auth.py to see EXACTLY how a key_hash is computed and matched (replicate drogon's scheme).

DO ALL OF:
1) KEY ROTATION: For each plaintext client (cerberus, 3roses, orion, atlas, orbit, kairos, heber-watch — NOT test, NOT drogon),
   generate a fresh strong key (python: \`secrets.token_urlsafe(24)\` prefixed \`gw_<id>_\`), replace its clients.yaml entry's
   plaintext \`key:\` with \`key_hash: sha256:<sha256hex-of-new-key>\` computed the SAME way auth.py verifies. Write every
   old→new mapping to config/ROTATION_HANDOFF.md. Then update each consumer's .env with its new key by replacing the OLD
   plaintext value wherever it appears (grep the repo for the old value; known var names: Kairos KAIROS_DATA_GATEWAY_API_KEY,
   Cerberus CERBERUS_GATEWAY_KEY, 3Roses GATEWAY_API_KEY, Orbit ORBIT_GATEWAY_API_KEY; Orion & Atlasv2 — grep for the old
   gw_*_key value and replace in-place). For heber-watch (a service, not a repo): grep /Users/jacobmcmillan/Empire/Heber and
   Data-Gateway docker-compose*.yml for gw_heber_watch_key_44931 and update it; if not found, just note it in the handoff.
   Do NOT restart anything. Do NOT commit.
2) DISABLE test client: set \`enabled: false\` on the 'test' client. (Many tests use it via clients.yaml; if disabling breaks
   the test fixture, instead KEEP it enabled but move it behind a clearly-marked test-only block AND ensure it has role 'client'
   with no trading — verify tests still pass. Record your choice in deviations.)
3) FINE-GRAINED TRADING AUTHZ: order-/position-MUTATING routes (POST /api/v1/alpaca/orders, DELETE order, POST close position,
   replace order) must require a NEW \`trading\` capability, NOT just role==trader. Add \`trading: true\` to permissions for
   kairos, cerberus, 3roses ONLY; add an enforcement (extend _enforce_trading_role or a dedicated dependency) that returns 403
   for a trader-role client lacking \`trading\` on those mutating paths. Leave READ-ONLY trading routes (account, clock, calendar,
   positions GET, portfolio GET) at the existing trader-role gate. Use hmac.compare_digest where you touch key comparison.
Acceptance: M0.4 characterization tests still pass; new test proves a trader-WITHOUT-trading key gets 403 on POST /orders while
kairos/cerberus/3roses succeed; \`uv run pytest tests/test_auth.py tests/test_alpaca_trading_router.py\` passes; ROTATION_HANDOFF.md
lists all 7 mappings; ROTATION_HANDOFF.md is gitignored.`,
    { label: 'M1-SEC keys+authz', phase: 'M1 Critical Fixes', model: 'opus', schema: REPORT }),

  () => agent(`${RULES}
TASK M1-POLL — Dedup-after-publish + EOD retry + content-stable insider/congress event_ids + zero-row alert.
YOUR FILES: gateway/core/uw_poller.py, gateway/core/quotes_poller.py, gateway/core/trades_poller.py,
  gateway/core/treasury_poller.py, gateway/core/crypto_poller.py, gateway/core/news_poller.py,
  gateway/providers/uw/institutional.py, config/prometheus_alerts.yml. (Do NOT edit envelope.py — that's M1-ENV.)
Read uw_poller.py _publish_envelopes (~line 199) first. Current code MARKS dedup BEFORE publishing (optimistic), relying on
the sink's failed-event buffer to avoid loss; on batch publish exception it logs warning and returns published=0 with events
already marked seen → permanent loss if the buffer also fails.
DO:
1) Change _publish_envelopes to mark dedup ONLY after a successful publish; on failure, do NOT mark (or unmark) so a later
   cycle re-publishes. Keep content-derived event_ids so Heber-side dedup absorbs any duplicate re-publish. Audit the other
   pollers for the same pattern and fix consistently.
2) EOD fetch retry: congress_trades got 0 rows on 2026-06-10 because UW returned a single 503 with no retry. Add bounded
   retry/backoff (reuse core/http_client http_retry or tenacity already in deps) to the EOD congress + insider fetches.
3) Content-stable event_ids: insider_trades and congress_trades payloads have NO timestamp field, so wrap_event stamps
   ts_event=now() and the SAME trade re-fetched gets a new event_id every run (duplicate Bronze rows observed 2026-06-09).
   In institutional.py / the EOD poller, derive ts_event from filing_date (fallback transaction_date) before wrapping so IDs
   are content-stable across runs. Verify against FEED_UNIQUE_FIELDS expectations.
4) Zero-row alert: add a prometheus alert rule (config/prometheus_alerts.yml) firing when a per-feed published count is 0 for
   an ENABLED feed during market hours (use existing envelope/published metrics; if a suitable metric doesn't exist, note it
   in blockers rather than inventing a new metric in metrics.py — that file is owned by M3.3).
Acceptance: \`uv run pytest tests/test_uw_poller.py tests/test_uw_provider.py\` passes (add/adjust tests for the new ordering,
retry, and ts_event derivation); prometheus_alerts.yml is valid YAML.`,
    { label: 'M1-POLL pollers', phase: 'M1 Critical Fixes', model: 'opus', schema: REPORT }),

  () => agent(`${RULES}
TASK M1-ENV — Strict envelopes by default (stop silently emitting malformed keys to the sink).
YOUR FILES: gateway/core/envelope.py, gateway/config.py.
Read envelope.py wrap_event (lines ~291-455) and config.py (strict_envelopes already exists, default False, line ~193).
Today on wrap failure the lenient path returns an envelope with instrument_key="unknown:{symbol}" and continues; the strict
check itself is wrapped in a try/except that SILENTLY swallows when settings are unavailable (lines ~430-439). Heber rejects
unknown: keys → another silent drop.
DO: make the safe default loud. Either (a) flip strict_envelopes default to True, or (b) keep lenient but ensure the fallback
NEVER ships an unknown:/malformed key to the sink and ALWAYS increments an alerting counter (record_envelope_error or similar).
Remove the silent settings-unavailable swallow — if settings truly can't load, prefer raising. Whichever you choose, ensure
existing callers (main.py dispatch, pollers, middleware) handle a possible raise without crashing the process on a single bad
event (they should drop+count that one event loudly, not die). Update tests in tests/test_envelope.py.
Acceptance: \`uv run pytest tests/test_envelope.py\` passes; a malformed event produces a loud counter increment and is NOT
published with an unknown: key.`,
    { label: 'M1-ENV strict-envelopes', phase: 'M1 Critical Fixes', model: 'opus', schema: REPORT }),

  () => agent(`${RULES}
TASK M1-CACHE — Cache-failure visibility + Redis pool tuning.
YOUR FILES: gateway/core/cache.py, gateway/core/redis_sink.py, and ONLY the CacheMiddleware methods of gateway/api/middleware.py
  (do not restructure middleware.py — M3.1 splits it later; make minimal in-place edits to the cache read/write handlers).
Read cache.py and the CacheMiddleware in middleware.py (cache read ~line 448, write ~line 520 log at debug only).
DO:
1) Raise cache read/write failure logs from debug → warning, and increment a counter metric (use existing metrics if present;
   if you must add one, add it in metrics.py is NOT allowed — instead use an existing counter or emit via the existing logger
   and note the missing metric in blockers).
2) Pool tuning: redis_sink.py and cache.py use BlockingConnectionPool(max_connections=32, timeout=5). The recent
   "Too many connections" flood came from the dedup cache fanning out connections. Document (comment) the relationship between
   pool size and data_sink worker count, and make the pool size derive from / be validated against the worker count so they
   can't silently mismatch. Do not change runtime behavior beyond making the sizing explicit and safe.
Acceptance: \`uv run pytest tests/test_cache.py tests/test_data_sink.py\` passes.`,
    { label: 'M1-CACHE cache+pool', phase: 'M1 Critical Fixes', model: 'opus', schema: REPORT }),
])

log(`M1 complete: ${m1.filter(Boolean).map(r => r.task + '=' + r.status).join(', ')}`)

// ----------------------------------------------------------------------------
phase('M2 High-Leverage')

const m2 = await parallel([
  () => agent(`${RULES}
TASK M2.1 — Delete dead legacy providers.
YOUR FILES: gateway/providers/uw_legacy.py, gateway/providers/alpaca_legacy.py (DELETE both).
First PROVE they're unused: grep the whole Data-Gateway repo and config/*.yaml for imports/references to uw_legacy / alpaca_legacy
and the class names they define. Also check config/providers.yaml does not point module paths at them. If ANY reference exists,
do NOT delete — report it in blockers. If clean, delete both files (use \`git rm\`), and grep tests for references.
Acceptance: files removed, \`uv run pytest -q -k "provider"\` still passes, no dangling imports (\`uv run python -c "import gateway.main"\` succeeds).`,
    { label: 'M2.1 delete-legacy', phase: 'M2 High-Leverage', model: 'opus', schema: REPORT }),

  () => agent(`${RULES}
TASK M2.3 — Unify UW poller feed handlers (refactor; behavior-preserving).
YOUR FILES: gateway/core/uw_poller.py ONLY. (M1-POLL already edited this file in the prior phase — build ON its current state.)
The handlers _poll_flow_alerts / _poll_darkpool / _poll_market_tide / _poll_sector_tides (~lines 456-616) are copy-pasted
fetch→loop-with-timestamp→wrap_event→publish→log. Extract ONE parameterized helper (feed name, fetch coroutine, unique-field/
timestamp handling, log labels) and route all four through it WITHOUT changing observable behavior. Preserve the dedup-after-publish
ordering M1-POLL just introduced. This is a pure refactor — same envelopes, same publish calls, same logs (labels may be
parameterized).
Acceptance: \`uv run pytest tests/test_uw_poller.py\` passes unchanged; diff shows net LOC reduction; no behavior change.`,
    { label: 'M2.3 unify-pollers', phase: 'M2 High-Leverage', model: 'opus', schema: REPORT }),

  () => agent(`${RULES}
TASK M2.4 — Provider error contract.
YOUR FILES: gateway/core/provider.py, gateway/providers/finnhub.py, gateway/providers/alphavantage.py.
Read the DataProvider ABC (provider.py) and the get_quotes implementations: finnhub returns None on single-symbol failure,
alphavantage returns partial results — divergent, undocumented. DECIDE one contract (recommend: per-symbol failures are skipped
and partial results returned; total failure raises), DOCUMENT it on the relevant ABC method docstrings, and ALIGN finnhub +
alphavantage to it. Add a small conformance test per provider asserting the agreed behavior (use mocked transports — no network).
Acceptance: \`uv run pytest tests/test_finnhub_provider.py tests/test_alphavantage_provider.py\` (or the existing equivalents) pass;
ABC docstring states the contract.`,
    { label: 'M2.4 provider-contract', phase: 'M2 High-Leverage', model: 'opus', schema: REPORT }),

  () => agent(`${RULES}
TASK M3.3 — Bound Prometheus metric cardinality.
YOUR FILES: gateway/core/metrics.py ONLY.
record_option_capture_symbol_metrics (~lines 965-983) creates per-SYMBOL gauge labels (OPTION_CAPTURE_SYMBOL_CONTRACTS,
OPTION_CAPTURE_QUALITY_RATIO, OPTION_CAPTURE_SNAPSHOT_AGE) — unbounded cardinality as the option universe grows. Replace the
per-symbol gauges with bounded aggregates (histograms / summary quantiles over symbols, plus a symbols_tracked gauge) that
preserve observability (P50/P95/P99 of quality ratio and snapshot age, total contracts) WITHOUT a label per symbol. Update any
callers in metrics.py and adjust the metrics test.
Acceptance: \`uv run pytest tests/test_metrics.py\` passes; no metric carries a per-symbol label.`,
    { label: 'M3.3 metrics-cardinality', phase: 'M2 High-Leverage', model: 'opus', schema: REPORT }),
])

log(`M2 complete: ${m2.filter(Boolean).map(r => r.task + '=' + r.status).join(', ')}`)

// ----------------------------------------------------------------------------
phase('M3 Polish')

const m3 = await parallel([
  () => agent(`${RULES}
TASK M3.1 — Split the 1,496-line middleware module into a package (mechanical, behavior-preserving).
YOUR FILES: gateway/api/middleware.py → NEW gateway/api/middleware/ package; update import sites in gateway/main.py and anywhere
that does \`from gateway.api.middleware import ...\`. (M1-CACHE edited CacheMiddleware in the prior phase — split its CURRENT content.)
Create gateway/api/middleware/__init__.py re-exporting every public middleware class so existing imports keep working, then move
each middleware class into its own module (metrics.py, validation.py, ratelimit.py, cache.py, envelope.py, security_headers.py,
global_ratelimit.py). Do NOT change behavior. Keep the public import surface identical.
Acceptance: \`uv run python -c "import gateway.main"\` succeeds; \`uv run pytest tests/test_middleware.py\` (and any middleware tests)
pass; \`from gateway.api.middleware import CacheMiddleware\` etc still work.`,
    { label: 'M3.1 split-middleware', phase: 'M3 Polish', model: 'opus', schema: REPORT }),

  () => agent(`${RULES}
TASK M3.2 — Dedicated unit tests for stream.py and dedup.py (the two biggest untested core modules).
YOUR FILES: NEW tests/test_stream.py, NEW tests/test_dedup.py. (Read-only on gateway/core/stream.py, gateway/core/dedup.py.)
Write real behavior tests (not just import-smoke): for dedup.py cover add/seen/TTL/eviction and the duplicate-detection path;
for stream.py cover what's unit-testable without a live Alpaca socket — subscription bookkeeping, fanout to multiple fake clients,
a slow/raising client not blocking others, and reconnect/sequence handling via injected fakes. Assert observable behavior.
Acceptance: \`uv run pytest tests/test_stream.py tests/test_dedup.py\` passes and exercises real branches (not trivially green).`,
    { label: 'M3.2 stream+dedup-tests', phase: 'M3 Polish', model: 'opus', schema: REPORT }),

  () => agent(`${RULES}
TASK M3.4 — Repo slimming (low-risk hygiene).
YOUR FILES: .gitignore, and git-tracked-file management only. Do NOT delete source or docs content.
DO: ensure .gitignore covers *.scip, .aider*, *.coverage, gateway_errors_*.log, sonar_scan.log (append missing lines). If any of
these large artifacts are git-TRACKED (check \`git ls-files\`), \`git rm --cached\` them (keep on disk). For the 177KB PRD.md and
550KB unusualwhales_openapi.yaml (both tracked): do NOT move them (other tooling/docs may reference them) — instead just note in
summary that they're candidates for git-LFS. Record a short note in summary about CHANGELOG.md size (204KB) as a future archive
candidate; do not modify CHANGELOG history.
Acceptance: \`git status\` clean of newly-ignored artifacts; \`git ls-files\` no longer lists any *.scip/.aider*/*.log that were tracked.`,
    { label: 'M3.4 repo-slimming', phase: 'M3 Polish', model: 'opus', schema: REPORT }),
])

log(`M3 complete: ${m3.filter(Boolean).map(r => r.task + '=' + r.status).join(', ')}`)

// ----------------------------------------------------------------------------
phase('Verify')

const verify = await agent(`${RULES}
TASK VERIFY — consolidated quality gate over ALL changes from M0-M3. You MAY run whole-repo commands now (you run alone).
Run and report each, with the tail of any failure output:
1) \`ruff check .\`
2) \`ruff format --check .\`
3) \`uv run mypy .\` (note: many modules have overrides; report only NEW errors vs the known baseline if distinguishable)
4) \`uv run pytest -q -m 'not perf'\` (full default suite)
5) \`uv run pytest -q -m integration\` (will skip if no Redis; report skip vs fail)
6) \`uv run python -c "import gateway.main"\` (import sanity after middleware split + legacy deletion)
Produce a pass/fail matrix in summary. List EVERY failing test/file with a one-line cause. Do NOT attempt fixes — just report.
Set tests_pass=true only if 1,2,4,6 all pass (3 and 5 may have known/no-op issues — describe them).`,
  { label: 'VERIFY gate', phase: 'Verify', model: 'opus', schema: REPORT })

return {
  m0: m0.filter(Boolean),
  m1: m1.filter(Boolean),
  m2: m2.filter(Boolean),
  m3: m3.filter(Boolean),
  verify,
}
