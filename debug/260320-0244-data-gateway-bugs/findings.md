# Debug Findings — Data-Gateway Bug Hunt

**Date:** 2026-03-20
**Scope:** All 155 source files in gateway/
**Tests:** 811 passed (before and after all fixes)

## Confirmed Bugs (All Fixed)

### [HIGH] Bug: Division by zero in BulkJob.eta_seconds
- **Location:** `gateway/core/bulk.py:225`
- **Evidence:** `rate = self.symbols_complete / elapsed` — no zero-guard on `elapsed`
- **Root cause:** Guard only checked `symbols_complete == 0`, not `elapsed <= 0`
- **Impact:** ZeroDivisionError crash when querying status of fast-completing bulk jobs
- **Fix:** Added `if elapsed <= 0: return None` guard

### [HIGH] Bug: Finnhub rate limit per-second contradicts per-minute
- **Location:** `gateway/core/rate_limiter.py:59`
- **Evidence:** `requests_per_second=30` allows 30-request burst, but per-minute is only 60
- **Root cause:** Copy-paste from Alpaca's higher limits
- **Impact:** Finnhub API hammered with bursts, causing upstream 429 errors
- **Fix:** Changed `requests_per_second` from 30 to 1

### [HIGH] Bug: X-Forwarded-For header spoofing bypasses per-IP rate limits
- **Location:** `gateway/api/middleware.py:1254`
- **Evidence:** `_get_client_ip()` trusts user-controlled header unconditionally
- **Root cause:** No trusted proxy configuration existed
- **Impact:** Attacker bypasses per-IP limits by spoofing different IPs
- **Fix:** Added `behind_trusted_proxy` config (default: False), only reads header when enabled

### [HIGH] Bug: Backfill error swallowing via asyncio.gather
- **Location:** `gateway/core/backfill.py:574`
- **Evidence:** `await asyncio.gather(*tasks, return_exceptions=True)` without inspecting results
- **Root cause:** Exceptions from `_bounded_process` silently returned as values, never logged
- **Impact:** Symbol processing failures go unnoticed; jobs report COMPLETED when some symbols failed
- **Fix:** Added result inspection loop that logs unhandled errors and appends to job.errors

### [MEDIUM] Bug: Naive datetime in UW flow provider
- **Location:** `gateway/providers/uw/flow.py:418, 466, 512`
- **Evidence:** `datetime.now().strftime("%Y-%m-%d")` — server-local time, not market time
- **Root cause:** Missing timezone import; local `from datetime import datetime` shadowed awareness
- **Impact:** Wrong date sent to UW API near midnight — fetches wrong day's data
- **Fix:** Added `_ET = ZoneInfo("America/New_York")`, all 3 instances use `datetime.now(_ET)`

### [LOW] Bug: EventEnvelopeMiddleware lazy _background_tasks init
- **Location:** `gateway/api/middleware.py:905`
- **Evidence:** `getattr(self, "_background_tasks", set())` instead of `__init__` initialization
- **Root cause:** Attribute added post-design, never added to constructor
- **Impact:** Code smell; prevents proper cleanup/introspection of background tasks
- **Fix:** Added to `__init__`, removed lazy getattr

### [HIGH] Bug: Crypto symbol normalization breaks multi-char base currencies
- **Location:** `gateway/core/envelope.py:86-107`
- **Evidence:** 3-char base assumption: `DOGEUSD` → `DOG-EUSD`, `LINKUSD` → `LIN-KUSD`
- **Root cause:** Hardcoded 3-char split for crypto pairs without quote currency matching
- **Impact:** Silently generates wrong instrument keys, breaking dedup and downstream lookups
- **Fix:** Replaced with known quote currency matching (3-char first: USD/EUR/BTC/ETH/GBP, then 4-char: USDT/USDC/BUSD)

### [MEDIUM] Bug: String-based type check for CircuitOpenError
- **Location:** `gateway/core/execution.py:67`
- **Evidence:** `type(e).__name__ == "CircuitOpenError"` instead of `isinstance()`
- **Root cause:** Missing import, worked around with string comparison
- **Impact:** Subclasses or module path changes would bypass circuit-open detection, causing confusing error logs
- **Fix:** Added proper import and `isinstance(e, CircuitOpenError)` check

### [MEDIUM] Bug: String-based type check for DataSinkRegistry (3 instances)
- **Location:** `gateway/core/uw_poller.py:279`, `gateway/core/quotes_poller.py:219`, `gateway/core/backfill.py:686`
- **Evidence:** `type(sink_registry).__name__ == "DataSinkRegistry"` in all 3 files
- **Root cause:** Circular import workaround using fragile string comparison
- **Impact:** Subclasses or name changes silently fall through to less efficient individual publish path
- **Fix:** Replaced with `hasattr(sink_registry, "publish_all_batch")` (duck typing, import-safe)

### [MEDIUM] Bug: Alpha Vantage naive datetime timestamps
- **Location:** `gateway/providers/alphavantage.py:337, 398, 452, 657`
- **Evidence:** `datetime.fromisoformat(...)` without timezone info — 4 instances
- **Root cause:** Alpha Vantage returns intraday timestamps in US/Eastern; daily as date-only strings
- **Impact:** Naive datetimes violate codebase timezone-aware contract; intraday times wrong by 4-5 hours if assumed UTC
- **Fix:** Intraday: `.replace(tzinfo=_ET).astimezone(UTC)`; daily: `.replace(tzinfo=UTC)`

### [MEDIUM] Bug: Validator crashes on naive timestamps
- **Location:** `gateway/core/validator.py:339-351`
- **Evidence:** `_parse_timestamp` returns naive datetime for strings without timezone; `_is_future_timestamp` compares with timezone-aware `now_utc` → `TypeError`
- **Root cause:** `fromisoformat` preserves whatever timezone info (or lack thereof) is in the input string
- **Impact:** Any data with naive timestamps crashes the validator, dropping the data without proper validation
- **Fix:** `_parse_timestamp` now assumes UTC for naive datetimes (both datetime objects and parsed strings)

## Disproven Hypotheses

| # | Hypothesis | Why Disproven |
|---|-----------|---------------|
| 1 | Division by zero in option_capture.py:558 | Guard at line 527: `if total_contracts <= 0: return empty_snapshot()` |
| 5 | Race condition in SubscriptionManager | Synchronous method in asyncio event loop — no interleaving |
| 6 | Race condition in main.py global set | All mutations synchronous in event loop callbacks |
| 9 | cache.py zip strict=False | Redis mget always returns same-length list |
| 10 | news.py sentiment division by zero | Guard at line 280: `if total > 0` |
| 11 | replay.py speed could be zero | Validated: `speed <= 0` rejected at config validation |
| 15 | Adjustment factor could be zero | Impossible in real stock splits |

## Noted Risks (Not Fixed — Low Priority)

| Risk | Location | Rationale for Not Fixing |
|------|----------|-------------------------|
| CORS `allow_origins=["*"]` in debug mode | main.py:495 | Only active when `debug=True` (defaults False); browsers ignore `Access-Control-Allow-Credentials` with wildcard origin |
| `follow_redirects=True` SSRF vector | http_client.py:114,136 | Requires compromised upstream API; base URLs are hardcoded to provider domains |
| API key prefix logged (10 chars) | auth.py:182 | Necessary for operational debugging; prefixes alone insufficient to reconstruct keys |

### [LOW] Bug: Backfill test mocks incompatible with duck typing fix
- **Location:** `tests/test_backfill.py:136, 638`
- **Evidence:** `MagicMock()` responds `True` to `hasattr(mock, "publish_all_batch")`, causing `await mock.publish_all_batch(...)` to fail with `TypeError: object MagicMock can't be used in 'await' expression`
- **Root cause:** Duck typing fix in `backfill.py:686` (`hasattr` check) interacts with MagicMock's auto-attribute behavior — tests never set `publish_all_batch` as `AsyncMock`
- **Impact:** 2 test failures (`test_job_executes_and_publishes`, `test_concurrent_symbol_processing`)
- **Fix:** Added `sink_registry.publish_all_batch = AsyncMock(return_value=1)` to both mock setups; updated assertion to check `publish_all_batch`

## Statistics

- **Hypotheses tested:** 24
- **Bugs confirmed & fixed:** 12 (5 HIGH, 4 MEDIUM, 1 LOW, 2 additional LOW)
- **Hypotheses disproven:** 7
- **Risks noted:** 3
- **Files modified:** 14
- **Tests:** 811/811 passing after all fixes (8 deselected)
- **Lint:** All clean (ruff)
