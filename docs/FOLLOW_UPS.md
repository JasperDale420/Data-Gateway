# Deferred Follow-Ups

Items deliberately deferred by the 2026-07-19 repo-hygiene pass. Everything
here either required editing `gateway/`/`config/` (formerly bind-mounted into the
running container — needs a coordinated restart window) or lives in another
repo.

## Requires a deploy window (source edits)

- **mypy stages 2–3**: shrink `ci/mypy_dirty_allowlist.txt` (37 files,
  ~860 grandfathered errors — heaviest: `providers/uw/options.py`,
  `providers/uw/market.py`, `providers/uw/institutional.py`,
  `providers/uw/flow.py`, `providers/alpaca/trading.py`), then drop the
  per-module `disable_error_code` overrides in `pyproject.toml` one module
  at a time, and finally re-enable the pre-commit mypy hook.
- **`heber:events` constant consolidation**: 9 duplicated topic literals
  (`gateway/main.py:210` + 6 pollers + `option_capture` + backfill +
  `api/middleware/envelope.py`) could share one constant. Value-neutral —
  the string is frozen forever — so only worth folding into some other
  deploy, never as a standalone change.
- **`stock_bars` → `bars` label unification**: the wire label (`bars`) is
  correct; `stock_bars` survives in capability names, the client-facing WS
  default, catalog metadata, and replay mock data. Renaming touches WS
  clients — coordinate with consumers if ever done.
- **72 grandfathered semgrep findings** (`empire-no-bare-exception`,
  `empire-no-return-none-for-failure`): these two rules had invalid
  patterns upstream and never actually ran anywhere; fixed in the vendored
  `.semgrep/empire-rules.yaml`, they surface 72 pre-existing violations in
  `gateway/` (heaviest: `core/http_client.py` ×16). Excluded from the
  blocking CI scan via `--exclude-rule` — clean them up in a deploy window,
  then remove the excludes.
- **`get_trades` severity split** (`gateway/providers/alpaca/market.py`):
  logs all statuses at ERROR while `get_bars`/`get_quotes` split 4xx→WARNING.

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
