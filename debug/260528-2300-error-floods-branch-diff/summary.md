# Debug Summary — 2026-05-28 error-floods + branch-diff

## Setup
- **Mode:** Targeted (error-log floods + branch regression check)
- **Scope:** entire `gateway/`
- **Depth:** 13 / 15 iterations used (bounded)
- **After:** Find-and-fix, then codex review
- **Inputs:** `gateway_errors_24h.log` (2026-03-05 snapshot, 11,658 lines) + branch diff vs `master`

## Result

| | Count |
|---|---|
| Live bugs found | 1 (MEDIUM) |
| Live bugs fixed | 1 |
| Deferred patterns reported | 1 (LOW, ~40 sites) |
| Floods verified already-fixed | 4 |
| Branch changes verified clean | 4 |
| Hypotheses tested | 11 (2 confirmed, 9 disproven) |

## The one live bug (fixed)

**`gateway/providers/alpaca/market.py:55,82`** logged `alpaca_bars_error` / `alpaca_quotes_error` at ERROR for *any* HTTP status, including client-caused 4xx (e.g. index symbol `SPX` → 400). This was the **single remaining source of the ERROR-log flood** (4,052 lines in a ~7h window) after commit `a12483e` fixed the API layer but missed the provider layer.

**Fix:** mirror `gateway/api/alpaca/common.py` — 4xx → `logger.warning`, 5xx → `logger.error`. Added 4 parametrized tests. Full provider + branch suite: **110 passed**, ruff clean.

## Key insight

Most floods in the snapshot were already fixed; the snapshot predated the fix commit. The value of this run was finding the **one place the fix didn't reach** (the provider layer) by pattern-matching the already-applied convention — and confirming the branch's 4 changes introduced no regressions.

## Deferred (separate PR recommended)

The same "ERROR-for-4xx" anti-pattern exists at ~40 sites in `gateway/providers/alphavantage.py` and `gateway/providers/finnhub.py`. Not flooding (absent from the log), so deferred rather than swept — a dedicated PR with a shared `log_provider_http_error()` helper + tests is the right vehicle.

## Files changed this session
- `gateway/providers/alpaca/market.py` — severity split (bars + quotes)
- `tests/test_alpaca_provider.py` — +4 parametrized severity tests
- `CHANGELOG.md` — `[Unreleased] > Fixed` entry
