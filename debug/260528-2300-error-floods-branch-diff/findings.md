# Debug Findings — 2026-05-28 error-floods + branch-diff

**Mode:** Targeted (error-log floods + branch regression check)
**Scope:** entire `gateway/`
**Depth:** 11 hypothesis iterations (bounded: 15)
**Baseline:** ruff clean; error log = `gateway_errors_24h.log` (11,658 lines, snapshot 2026-03-05 14:12–20:59 UTC)

## Error-log cluster analysis (24h snapshot)

| Event | Count | Logger | Current status |
|-------|-------|--------|----------------|
| `provider_request_failed` | 5190 | api.alpaca.common | ✅ Already fixed — split 4xx→warning / 5xx→error |
| `alpaca_bars_error` | 4052 | providers.alpaca (`market.py`) | ❌ **STILL ERROR-level for 4xx** (Bug 1) |
| `alpaca_order_create_error` | 1138 | providers.alpaca (`trading.py`) | ✅ Already fixed — `logger.warning` |
| `redis_sink_publish_error` | 256 | core.redis_sink | ✅ `logger.warning` + buffered |
| `data_sink_publish_failed` | 256 | core.data_sink | ✅ `logger.debug` for self-metric sinks |
| `circuit_opened` | 252 | core.circuit_breaker | ✅ data_sink circuits → `logger.warning` (GW-W1013) |

The 2026-03-05 snapshot predates commit `a12483e "Fix gateway error-log regressions"`, which downgraded most of these. The **one remaining ERROR-level flood source is the provider layer** (`market.py`), which the fix did not reach.

---

## [MEDIUM] Bug 1 — Alpaca provider-layer logs 4xx client errors at ERROR (remaining flood source)

- **Location:** `gateway/providers/alpaca/market.py:55` (`get_bars`) and `gateway/providers/alpaca/market.py:82` (`get_quotes`)
- **Hypothesis:** `alpaca_bars_error` / `alpaca_quotes_error` are logged at `logger.error` unconditionally on any `httpx.HTTPStatusError`, regardless of whether the status is a client-caused 4xx (e.g. requesting index symbol `SPX` from `/v2/stocks/bars` → 400) or a genuine upstream 5xx.
- **Evidence:**
  ```python
  # market.py:54-56
  except httpx.HTTPStatusError as e:
      logger.error("alpaca_bars_error", status=e.response.status_code, error=str(e))
      raise
  ```
  The sibling API layer was already fixed to split severity by status (`common.py:139-155`: 4xx→`warning`, 5xx→`error`). The provider layer was not. A single SPX 400 is therefore **double-logged**: ERROR at `market.py` + WARNING at `common.py`.
  - `http_retry` retries only `{429,502,503,504}` (`http_client.py:159`), so 4xx is **not** retried → 4052 log lines = 4052 real client requests, not retry amplification. The flood magnitude is real client behavior; the **severity is the bug**.
- **Reproduction:** `GET /api/v1/alpaca/stocks/bars?symbols=SPX&timeframe=1Day` → Alpaca 400 → ERROR-level `alpaca_bars_error`. Repeat → ERROR log flood that drowns genuine 5xx errors.
- **Impact:** Alert fatigue / operational hazard. The WARNING+ errors log (`logs/{service}_errors_*.log`) is what operators watch; 4052 client-symbol 400s at ERROR bury real upstream failures. This is the exact regression class the branch set out to fix — the fix was incomplete.
- **Root cause:** Severity-by-status convention was applied at the API layer (`common.py`) but not propagated to the provider layer (`market.py`).
- **Suggested fix:** Mirror the `common.py` convention in `market.py` for both `get_bars` and `get_quotes` — 4xx → `logger.warning`, 5xx → `logger.error`.

---

## [LOW] Bug 2 — Same ERROR-for-4xx anti-pattern pervasive in other providers (DEFERRED — not flooding)

- **Locations:** `gateway/providers/alphavantage.py` (~20 sites) and `gateway/providers/finnhub.py` (~20 sites) — every `logger.error("<provider>_<op>_failed", ..., error=str(e))` in an except block.
- **Status:** **Report only, do NOT fix in this loop.** These are not present in the error-log snapshot (no flood evidence), and changing ~40 call sites across two providers is a large, unrequested refactor. The prior 2026-04-28 scan reached the same conclusion about sweeping cross-file log changes ("touches too many files for an atomic change. Recommend a separate dedicated PR").
- **Recommendation:** Separate dedicated PR that introduces a small helper (e.g. `log_provider_http_error(logger, event, exc)` that splits severity by status) and applies it uniformly across all providers, with tests.

---

## Branch diff verification (`codex/data-gateway-error-log-fixes` vs `master`) — NO regressions

| File | Change | Verdict |
|------|--------|---------|
| `api/alpaca/trading.py:790` | `close_position` rejects `qty < 0` with 400 GW-E4006 | ✅ Valid guard; covered by new tests in `test_alpaca_trading_router.py` |
| `config.py:202,222,223` | pool 8→32, queue 4096→16384, worker 8→16 | ✅ worker 16 ≤ pool 32; queue 16384 ≤ Field max 65536; within bounds |
| `core/stream.py:1223` | validation runs only if `clients or self._on_envelope` | ✅ Skips only when no consumer; `if not result.valid` is inside same guard (no unbound var) |
| `providers/uw/institutional.py:82-98` | `get_congress_trades` SDK call → direct httpx `/api/congress/recent-trades` | ✅ `_call_sync(func)` matches `(self, func, *args, **kwargs)`; `raise_for_status()` propagates to outer `except` |

---

## Dead code noted (not modified — per surgical-change policy)

- `gateway/providers/alpaca_legacy.py:243,1848` has the same logging patterns, but `alpaca_legacy` is imported nowhere (`providers.yaml` loads `gateway.providers.alpaca`). Leave untouched.

## Summary

| ID | Severity | Action | File | One-liner |
|----|----------|--------|------|-----------|
| 1 | MEDIUM | **Fix now** | `providers/alpaca/market.py:55,82` | 4xx client errors logged at ERROR → flood; split severity by status |
| 2 | LOW | Defer (separate PR) | `alphavantage.py`, `finnhub.py` | Same anti-pattern, ~40 sites, not flooding |
