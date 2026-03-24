# Debug Findings — Data-Gateway Silent Failures

**Session:** 2026-03-17 19:57
**Scope:** Entire gateway/ directory
**Mode:** Hunt all bugs + fix
**Status:** All bugs fixed, 806 tests passing

---

### [CRITICAL] Bug: do_not_exercise_option hits wrong API server
- **Location:** `providers/alpaca/trading.py:351-374`, `providers/alpaca/_base.py:36`
- **Evidence:** `self._base_url` is `https://data.alpaca.markets` (data API), but `/v2/positions/*/do-not-exercise` is a trading endpoint on `https://api.alpaca.markets`
- **Impact:** Any call to do-not-exercise an option position would hit the wrong server, likely returning 404
- **Root cause:** `_base_url` was the only URL stored on self; trading URL was a local variable in `initialize()`
- **Fix:** Added `_trading_base_url` field, stored trading URL during init, fixed the method to use it. Also fixed guard clause from `_client` to `_trading_client`.

### [HIGH] Bug: Option chain missing pagination (x2 methods)
- **Location:** `providers/alpaca/options.py:22-88` (get_option_chain), `providers/alpaca/options.py:316-338` (get_option_snapshots)
- **Evidence:** Single HTTP request with `limit=1000`, no `next_page_token` loop. AAPL has 5000+ option contracts.
- **Impact:** Option chains silently truncated to first 1000 contracts
- **Fix:** Added pagination loops to both methods

### [HIGH] Bug: News endpoint missing pagination
- **Location:** `providers/alpaca/news.py:19-97`
- **Evidence:** Single request capped at 50 articles, no `next_page_token` loop
- **Impact:** Callers requesting >50 articles only get 50 with no indication of truncation
- **Fix:** Added pagination loop respecting caller's `limit` as total cap

### [HIGH] Bug: Incomplete historical data (5 methods, fixed earlier)
- **Location:** `providers/alpaca/crypto.py`, `options.py`, `forex.py`
- **Methods:** `get_crypto_bars`, `get_crypto_trades`, `get_option_bars`, `get_option_trades`, `get_forex_rates_historical`
- **Impact:** Historical data silently truncated at first page (1000 results)
- **Fix:** Added pagination loops matching stock provider pattern

### [MEDIUM] Bug: Missing @http_retry on paginated methods
- **Location:** `providers/alpaca/crypto.py:146`, `providers/alpaca/options.py:191`
- **Methods:** `get_historical_crypto_quotes`, `get_historical_option_quotes`
- **Impact:** Transient failures (timeouts, 5xx) not retried — entire fetch fails on first error
- **Fix:** Added `@http_retry` decorator

### [MEDIUM] Bug: Inconsistent model_dump serialization
- **Location:** `api/alpaca/crypto.py`, `options.py`, `news.py`, `screener.py`, `corporate.py`, `api/market.py`
- **Evidence:** Stock endpoints use `model_dump(mode="json")` (strings), crypto/options/etc use bare `model_dump()` (Python objects)
- **Impact:** Downstream consumers see different types for the same fields depending on asset class. Decimal objects instead of strings could break JSON serialization in some paths.
- **Fix:** Standardized all API response serialization to `model_dump(mode="json")`

---

## Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 1 |
| HIGH | 4 |
| MEDIUM | 2 |
| **Total** | **7** |

All bugs fixed. 806 tests passing. Changes span 12 files across providers and API layers.
