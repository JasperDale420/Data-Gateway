# Data-Gateway Remediation Plan

Derived from the 2026-06-10 audit + Heber-ledger verification. Encoded as a
multi-phase agent workflow (`scripts/` run artifact). Phases are barriers:
the safety net must land before correctness/security changes, which must land
before refactors that rely on the new tests.

**Operating rules for every agent in this plan:**
- Do **not** `git commit`, `git push`, or run `make deploy`.
- Do **not** restart, stop, or recreate any Docker container (Kairos is live).
- Own only the files listed for your task; do not edit files owned by another
  agent in the same phase.
- After editing, run the targeted tests for your area and report pass/fail with
  the command used. Run `ruff check` + `ruff format` on files you changed.
- Match existing code style. Add a `CHANGELOG.md` `## [Unreleased]` entry for
  any behavior change (append to the correct group; do not reorder existing
  entries).

---

## Phase M0 — Safety net (parallel; disjoint files)

| Task | Files owned | Acceptance |
|------|-------------|-----------|
| **M0.1** Envelope→Heber contract test | NEW `tests/test_envelope_heber_contract.py` (reads `gateway/core/envelope.py`; copies Heber's instrument-key validator regex `option:OCC:[A-Z]{1,6}\d{6}[CP]\d{8}` and equity/crypto/forex equivalents) | Every feed in `FEED_UNIQUE_FIELDS` + every streaming instrument type produces a key the validator accepts; a deliberately broken OCC build fails the test |
| **M0.2** Real-Redis integration tier | `.github/workflows/ci.yml`; NEW `tests/integration/test_redis_sink_integration.py`; `tests/conftest.py` (additive integration fixtures only) | New CI job runs Redis as a service container and `pytest -m integration` covers publish, dedup TTL, failed-buffer fill/evict/drain |
| **M0.3** Coverage floor + markers | `pyproject.toml` | `[tool.coverage.report] fail_under` set to current measured %; CI fails on regression; `slow`/`integration` markers actually applied |
| **M0.4** Trading-route characterization tests | `tests/test_alpaca_trading_router.py` (append only) | Current order placement/cancel/close behavior pinned BEFORE authz changes, so M1.2 can't silently alter it |

## Phase M1 — Critical fixes (parallel; disjoint files)

| Task | Files owned | Acceptance |
|------|-------------|-----------|
| **M1-SEC** Key hashing + rotation + fine-grained trading authz | `gateway/core/auth.py`, `config/clients.yaml`, `gateway/api/deps.py`, consumer `.env` files (`../Kairos`, `../Cerberus`, `../3Roses`, `../Orion`, `../Atlasv2`, `../Orbit`, heber-watch), NEW gitignored `config/ROTATION_HANDOFF.md` | **Note: authz partly exists already** — `_enforce_admin_role` gates all `/api/v1/admin*` (admin/super_admin) and `_enforce_trading_role` gates `/api/v1/alpaca/{orders,account,...}` (trader/admin); `test` client (role `client`) is already denied. Real work: (1) all `clients.yaml` entries → `key_hash` (match auth.py's sha256 scheme, as `drogon` does); new keys written to each consumer `.env` + handoff file; (2) **fine-grained** `trading` permission gating only order-/position-MUTATING routes, granted to Kairos+Cerberus+3Roses only (leave read-only account/clock at trader role); (3) disable `test` client; (4) verify admin router mounts under `/api/v1/admin` so the central gate covers logs/error-summary/providers; tests prove a trader-without-`trading` gets 403 on POST /orders and M0.4 characterization still passes |
| **M1-POLL** Dedup-after-publish + EOD retry + insider ts_event + alerts | `gateway/core/uw_poller.py`, other `*_poller.py`, `gateway/providers/uw/institutional.py`, `config/prometheus_alerts.yml` | Mark dedup only after successful publish (or unmark failures); EOD fetches retry with backoff on 5xx (fixes congress 503); insider/congress `ts_event` derived from `filing_date`/`transaction_date` so event_ids are content-stable; alert on per-feed `published==0` during market hours |
| **M1-ENV** Strict envelopes default | `gateway/core/envelope.py`, `gateway/config.py` | `strict_envelopes` defaults true OR lenient fallback never emits an `unknown:`/malformed key to the sink and always increments an alerting counter; the silent settings-unavailable swallow removed |
| **M1-CACHE** Cache failure visibility + Redis pool tuning | `gateway/core/cache.py`, `gateway/core/redis_sink.py`, cache methods of `gateway/api/middleware.py` | Cache read/write failures log WARNING+ with a counter metric; pool size vs worker count documented and tuned per the M0.2 saturation test |

## Phase M2 — High-leverage (parallel; disjoint files)

| Task | Files owned | Acceptance |
|------|-------------|-----------|
| **M2.1** Delete legacy providers | `gateway/providers/uw_legacy.py`, `gateway/providers/alpaca_legacy.py` (delete) | Cross-repo import check clean (Sourcegraph/grep), files removed, suite green |
| **M2.3** Unify poller feed handlers | `gateway/core/uw_poller.py` | flow/darkpool/market_tide/sector_tide handlers go through one parameterized fetch→wrap→publish helper; behavior preserved (M0.1 + M1-POLL tests green) |
| **M2.4** Provider error contract | `gateway/core/provider.py`, `gateway/providers/finnhub.py`, `gateway/providers/alphavantage.py` | ABC documents partial-vs-None-vs-raise; finnhub/alphavantage aligned; conformance test per provider |
| **M3.3** Prometheus cardinality | `gateway/core/metrics.py` | Per-symbol option-capture gauges replaced with bounded histograms |

## Phase M3 — Polish (parallel; disjoint files)

| Task | Files owned | Acceptance |
|------|-------------|-----------|
| **M3.1** Split middleware module | `gateway/api/middleware.py` → NEW `gateway/api/middleware/` package; import sites in `gateway/main.py` | One module per middleware; imports updated; suite green |
| **M3.2** stream.py + dedup.py tests | NEW `tests/test_stream.py`, `tests/test_dedup.py` | Dedicated unit coverage for fanout/reconnect and dedup |
| **M3.4** Repo slimming | `.gitignore`, root megafiles, `logs/` retention note | `*.scip`/`.aider*` gitignored; PRD/openapi spec moved out of git or LFS-noted; CHANGELOG history archive plan recorded |

## Phase VERIFY

Run `ruff check .`, `ruff format --check .`, `mypy .`, full `pytest` (default
markers) + `pytest -m integration` against a local Redis. Report a consolidated
pass/fail matrix and any remaining failures with output.

## Post-workflow (driven by me, not agents)

1. Review the complete combined diff.
2. **Codex adversarial review** (`codex` skill, model gpt-5.5, reasoning
   xtrahigh) over all changes; apply Codex's fixes.
3. Commit in logical groups; `make deploy`; smoke-check `/health`, confirm
   Kairos reconnects with its rotated key and streams flow.
