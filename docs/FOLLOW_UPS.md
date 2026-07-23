# Deferred Follow-Ups

Items deliberately deferred by the 2026-07-19 repo-hygiene pass. Everything
here either requires editing `gateway/`/`config/` (bind-mounted into the
running container — needs a coordinated restart window) or lives in another
repo.

## Requires a deploy window (source edits)

- ~~mypy stages 2–3~~ **DONE 2026-07-22** (branch `debt/mypy-protocols`):
  feature mixins got a type-checking-only base, the router and provider
  `disable_error_code` overrides were dropped, and the allowlist is empty
  (863 → 0 errors). Remaining: legacy-core and stream-provider override
  groups in `pyproject.toml` still disable codes for their modules.
- **Latent runtime bugs surfaced by the 2026-07-22 mypy pass** — each site
  carries a targeted `# type: ignore` with a comment; the code paths were
  broken before and after that pass (they raise `ImportError`/
  `AttributeError`/`TypeError` when hit). Fix or delete deliberately:
  - Vendored UW SDK v5.1 lacks operations referenced by
    `providers/uw/options.py` (`get_implied_volatility_surface`,
    `get_put_call_ratio`, `get_option_volume_levels`, `get_volume_profile`),
    `providers/uw/flow.py` (`get_greek_flow_expiry`, `get_net_flow_by_expiry`,
    `get_top_net_premium`, and `get_full_tape.sync` — module only ships
    `sync_detailed`), `providers/uw/market.py` (`get_sector_etfs`), and
    `providers/uw/institutional.py` (`get_trader`). Port them to the
    `_raw_get` primitive like the 2026-07 endpoint expansion did.
  - `providers/alpaca/trading.py`: `get_portfolio_history` and
    `set_account_configurations` pass kwargs that alpaca-py does not accept
    (it takes `GetPortfolioHistoryRequest` / a full `AccountConfiguration`
    model) — both endpoints TypeError on every call; tests mock the SDK
    client so they never noticed.
  - `api/admin.py` `admin_reload_provider`: calls
    `registry.reload_provider(...)`, which does not exist on
    `ProviderRegistry` — the `except AttributeError` turns every call into
    a 404, so the admin reload endpoint has never worked.
- **`heber:events` constant consolidation**: 9 duplicated topic literals
  (`gateway/main.py:210` + 6 pollers + `option_capture` + backfill +
  `api/middleware/envelope.py`) could share one constant. Value-neutral —
  the string is frozen forever — so only worth folding into some other
  deploy, never as a standalone change.
- **`stock_bars` → `bars` label unification**: the wire label (`bars`) is
  correct; `stock_bars` survives in capability names, the client-facing WS
  default, catalog metadata, and replay mock data. Renaming touches WS
  clients — coordinate with consumers if ever done.
- ~~72 grandfathered semgrep findings~~ **DONE 2026-07-22** (branch
  `debt/semgrep-exceptions`): all 76 findings paid down — real swallows
  narrowed/logged (heaviest: the silent circuit-breaker pre-checks in
  `core/data_sink.py`), legitimate boundaries annotated with justified
  `nosemgrep` — and both rules are now blocking in CI (the `--exclude-rule`
  flags are gone; semgrep pin bumped 1.157.0 → 1.170.0).
- ~~`get_trades` severity split~~ **DONE 2026-07-22** (same branch): all six
  market methods that logged every status at ERROR (`get_trades`,
  `latest_bars`, `latest_trades`, `historical_quotes`, `snapshots`,
  `auctions`) now split 4xx→WARNING / 5xx→ERROR like `get_bars`/`get_quotes`.

## Other repos

- **empire-core log retention is broken** (`empire_core/logger.py`): the
  custom `namer` renames rotated files to `{service}_{date}.log`, which
  stdlib `TimedRotatingFileHandler.getFilesToDelete()` never matches, so
  `backupCount` deletes nothing. Data-Gateway accumulated 2.3 GB / 206
  files before the 2026-07-19 manual prune. Fix: override
  `getFilesToDelete()` (a spawn-task chip was filed for this).
- **empire-schemas envelope reconciliation**: its `compute_event_id` /
  `FEED_UNIQUE_FIELDS` / `make_instrument_key` diverge from the canonical
  `gateway/core/envelope.py` (see DEVELOPER_NOTES.md). Either update
  empire-schemas to match the gateway byte-for-byte (affects any repo that
  imports it) or deprecate its copy with a warning. Do NOT point the
  gateway at it.

## Deployment model

- **Baked-image deploys**: `docker-compose.yml` bind-mounts `./gateway` and
  `./config` into the running container, so the working tree IS production.
  Moving to baked images (build + tag + recreate) would decouple local file
  state from the live service. Big operational change — its own project.

## Nice-to-have

- **Codex-session worktrees**: 18 stale detached-HEAD worktrees under
  `~/.codex/worktrees/*/Data-Gateway` (plus similar for Heber) were left in
  place — removing them may break resuming old Codex CLI sessions.
- **Old stashes**: 4 stashes from March–June 2026 (`git stash list`) —
  including one from the 2026-06-11 Orion incident referencing clients.yaml
  hashing WIP. Review and drop deliberately.
- ~~Perf budget too thin on `test_replay_run_large_message_batch_memory_profile`~~
  **DONE 2026-07-19**: re-baselined 0.35s → 0.50s (user-approved config/ edit)
  after 5 boundary failures against a 0.34–0.43s runner spread.
- **`data_dictionary.yaml` has no regeneration command**: the `.gitignore`
  comment says "regenerated from provider specs", but no script or Makefile
  target produces it — the untracked copy in `docs/` is currently the only
  one. Either write the generator or re-track the file.
- **Ratchet the money-path coverage floors** (`.github/workflows/ci.yml`,
  "Money-path coverage floor") as provider tests land; same for the global
  `fail_under = 58` in `pyproject.toml`.
