# Deferred Follow-Ups

Living ledger of deliberate deferrals. Two big passes have drained it:
the 2026-07-19 repo-hygiene pass and the 2026-07-22/23 debt-paydown +
baked-image cutover. Paid items are pruned; git history has the details.

## Open

- **A failed `OrderOwnershipGuard.freeze()` leaves an ambiguous claim
  reusable once Redis recovers** — `freeze()` is the only mechanism that
  durably blocks further use of a symbol after an ambiguous broker
  mutation (e.g. a 504 on `create_order` where the order may or may not
  have reached Alpaca). If the `freeze()` write itself fails because
  Redis is unavailable at that moment, `_freeze_after_ambiguous_mutation`
  /`_freeze_before_fence_release` correctly reject *that* request (503,
  `GW-E5301`), but the claim in Redis is left unmarked. Once Redis comes
  back, a later `authorize_submission`/`authorize_close` call for the
  *same* owner sees an ordinary (non-frozen) claim and falls back to
  plain broker-state reconciliation — which passes if the broker's
  reported state looks self-consistent, with no explicit gate forcing
  manual reconciliation of the original ambiguous mutation first. Raised
  by the 2026-08-07 adversarial review (`gpt-5.6-terra`) of the
  `tests/test_order_ownership.py` Redis-failure-path test additions
  (high severity). Needs its own design (a durable or service-wide
  fail-closed hold that survives a failed freeze) and adversarial review
  of that plan before implementation — out of scope for a test-only
  change.
- **Sink Redis has no measured RSS envelope for `appendfsync always` +
  `maxmemory 6gb`** — the compose Redis now carries both the synchronous
  AOF added for order-ownership claim durability and the 6GB cap raised
  after the 2026-08-04 eviction outage. `maxmemory` bounds the logical
  dataset, not process RSS: allocator overhead plus AOF-rewrite
  copy-on-write can push peak RSS well above 6GB against a Docker VM of
  11.67GB, and the same outage already recorded two Redis restarts under
  AOF-fsync stalls. A stall or OOM at that ceiling fails `XADD`, opens the
  `data_sink:redis_streams` breaker, and overflows the 50K failover
  buffer — the exact failure this cap was raised to prevent. Compose
  declares no `mem_limit`/`mem_reservation` for the service, so nothing
  enforces the headroom. Before the next deploy that carries this pair:
  measure peak Redis RSS through a full AOF rewrite at representative
  market-hours ingress, then either set an explicit container memory
  limit with proven headroom or lower the cap. Raised by the adversarial
  review of the 2026-08-06 branch-hygiene merge.
- **Latent runtime bugs surfaced by the 2026-07-22 mypy pass** (a
  spawn-task chip is filed) — each site carries a `cast(Any, ...)` with a
  comment; the code paths were broken long before that pass (they raise
  `ImportError`/`AttributeError`/`TypeError` when hit):
  - Vendored UW SDK v5.1 lacks operations referenced by
    `providers/uw/options.py` (`get_implied_volatility_surface`,
    `get_put_call_ratio`, `get_option_volume_levels`, `get_volume_profile`),
    `providers/uw/flow.py` (`get_greek_flow_expiry`, `get_net_flow_by_expiry`,
    `get_top_net_premium`, and `get_full_tape.sync` — module only ships
    `sync_detailed`), `providers/uw/market.py` (`get_sector_etfs`), and
    `providers/uw/institutional.py` (`get_trader`). Port them to the
    `_raw_get` primitive like the 2026-07 endpoint expansion did.
  - `providers/alpaca/trading.py`: `get_portfolio_history` and
    `set_account_configurations` pass kwargs alpaca-py does not accept
    (it takes `GetPortfolioHistoryRequest` / a full `AccountConfiguration`
    model) — both TypeError on every call; tests mock the SDK client so
    they never noticed. `set_account_configurations` needs get-merge-set.
  - `api/admin.py` `admin_reload_provider`: calls
    `registry.reload_provider(...)`, which does not exist on
    `ProviderRegistry` — the endpoint has never worked.
- **Stream supervisor treats the post-deploy Alpaca connection-limit race
  as non-recoverable** (observed 2026-07-23): a container recreate can
  authenticate before the old container's SIP WebSocket slot is released;
  Alpaca replies "connection limit exceeded", the supervisor classifies it
  as an auth failure and stops retrying, leaving `/health/ready` 503 until
  a manual `RESTART=1 make deploy`. Treat that specific error as retryable
  with backoff (~30-90s covers the slot-expiry window).
- **`heber:events` constant consolidation**: 9 duplicated topic literals
  (main.py + 6 pollers + option_capture + backfill + middleware) could
  share one constant. Value-neutral — the string is frozen forever — fold
  into some other change, never standalone.
- **`stock_bars` full rename**: the subscribe surface now ACCEPTS the
  canonical `bars`/`quotes`/`trades` as aliases (2026-07-23, PR #61);
  retiring the `stock_*` spellings entirely would still break Cerberus
  (`FEED_NAME_MAP` hard-dep) and Orbit live subscriptions — only after
  those clients migrate, if ever.
- **mypy legacy override groups** (`pyproject.toml` `[tool.mypy]`): the
  legacy-core group (10 disabled codes) and stream-provider group remain
  grandfathered. Shrink module-by-module when touching those files.
- **Expose the deployed image tag via `/health`**: with baked-image
  deploys, "what code is running" is one `docker inspect` away — but
  surfacing `data-gateway:YYYYMMDD-sha` in the health payload would make
  staleness observable to dashboards and downstream consumers.
- **`data_dictionary.yaml`**: resolved as deleted (no generator ever
  existed, zero consumers; recoverable from git history pre-`f1c4624`).
  If it's ever wanted again, write the generator first.
- **3 codex worktrees with real (ancient) uncommitted docs-WIP** kept under
  `~/.codex/worktrees/{01ec,1929,b04d}/Data-Gateway` (March–April 2026,
  against long-gone file layouts) — review and delete deliberately. The
  other 15 Data-Gateway + all Heber codex worktrees were removed 2026-07-23.

## Done (kept one line each; details in git history)

- 2026-07-23: baked-image deploys (PR #58) — working tree is no longer production
- 2026-07-23: mypy 863→0, allowlist emptied, router/provider overrides retired (PR #60)
- 2026-07-23: semgrep 76 findings paid, both rules blocking, pin → 1.170 (PR #62)
- 2026-07-23: Alpaca market log-severity sextet 4xx→WARNING (PR #62)
- 2026-07-23: WS feed aliases bars/quotes/trades (PR #61)
- 2026-07-23: empire-schemas envelope byte-synced (parity-verified) + dead
  gateway.schemas envelope re-exports removed (PR #59, empire-schemas `b3be701`)
- 2026-07-23: empire-core log retention fixed (custom-namer getFilesToDelete
  override, merged + shipped in the baked image) — verify logs/ prunes to ≤30
  dated files after the next midnight rollover
- 2026-07-23: stashes dropped (4→0, March WIP archived to session scratchpad)
- 2026-07-19: perf budget re-baselined 0.35s → 0.50s
- 2026-07-19: coverage ratchets introduced; raised 2026-07-23 to 60 global /
  88 router / 97 provider
