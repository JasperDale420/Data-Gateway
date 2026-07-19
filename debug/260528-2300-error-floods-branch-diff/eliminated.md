# Eliminated Hypotheses — 2026-05-28

Disproven hypotheses are as valuable as findings — they prevent re-investigation.

## Floods already fixed before this session (verified in current code)

The `gateway_errors_24h.log` snapshot is from **2026-03-05**, predating commit `a12483e`. The following ERROR-level floods in that snapshot are already downgraded in current code:

1. **`provider_request_failed` (×5190) still ERROR for 4xx** — DISPROVEN. `gateway/api/alpaca/common.py:82-94` and `:124-155` now split: 4xx → `logger.warning`, 5xx → `logger.error`.
2. **`alpaca_order_create_error` (×1138) still ERROR** — DISPROVEN. `gateway/providers/alpaca/trading.py:187` is `logger.warning`. (Insufficient-buying-power 403 is a normal business rejection.)
3. **`circuit_opened` (×252) still ERROR for sink circuits** — DISPROVEN. `gateway/core/circuit_breaker.py:222-229` logs data_sink circuits at `logger.warning` (GW-W1013); only non-sink circuits use ERROR.
4. **`data_sink_publish_failed` (×256) still ERROR/exception** — DISPROVEN. `gateway/core/data_sink.py:473-479` uses `logger.debug` for sinks that record their own metrics (RedisStreamsSink), avoiding duplicate ERROR+traceback spam.
5. **`redis_sink_publish_error` (×256)** — DISPROVEN as a bug. `gateway/core/redis_sink.py:500` is `logger.warning` with `buffered=True`; events are buffered for drain on reconnect. Expected behavior during a transient Redis outage.

## Retry amplification

6. **SPX 400s amplified by `http_retry` (inflating the 4052 count)** — DISPROVEN. `gateway/core/http_client.py:159` retries only `{429, 502, 503, 504}`; 4xx is not retried. The 4052 count = 4052 real client requests.

## Branch diff (`codex/data-gateway-error-log-fixes` vs `master`)

7. **`trading.py` `close_position` `qty<0` guard regresses callers** — DISPROVEN. New 400 GW-E4006 guard is correct and test-covered.
8. **`config.py` tuning out of safe bounds** — DISPROVEN. worker_count 16 ≤ pool_size 32 (comment invariant holds); queue_size 16384 ≤ Field max 65536.
9. **`stream.py` validation guard skips needed validation** — DISPROVEN. Validation is skipped only when there are neither clients nor an `on_envelope` sink (no consumer); the `if not result.valid` check is inside the same guard, so no unbound-variable path.
10. **`uw/institutional.py` SDK→httpx swap regresses** — DISPROVEN. `_call_sync(_request_recent_trades)` matches `_call_sync(self, func, *args, **kwargs)`; `response.raise_for_status()` propagates to the outer `except Exception`; payload dict/list/else handling is defensive.

## Test-safety

11. **Existing tests assert the ERROR level (fix would break them)** — DISPROVEN. No test references `alpaca_bars_error`/`alpaca_quotes_error` or asserts log level. Fix is additive.
