# Eliminated Hypotheses — Data-Gateway Debug

These hypotheses were tested and disproven. Documenting them prevents re-investigation.

## 1. Division by zero in option_capture.py:558
**Claim:** `total_contracts` could be 0 causing ZeroDivisionError
**Evidence against:** Line 527-528 explicitly guards: `if total_contracts <= 0: return self._empty_symbol_snapshot()`. The division at line 558 only executes when `total_contracts > 0`.

## 2. Race condition in SubscriptionManager (stream.py:189)
**Claim:** Multiple concurrent calls to `subscribe()` could corrupt `_subscriptions` and `_index` dicts
**Evidence against:** `subscribe()` is synchronous (no `await` points). In asyncio's cooperative multitasking model, synchronous code runs atomically within a single event loop iteration. No other coroutine can interleave during execution.

## 3. Race condition in main.py global set (line 136)
**Claim:** `_stream_sink_publish_tasks` set mutations from `_schedule_stream_sink_publish()` and `_on_stream_sink_publish_done()` callback could corrupt the set
**Evidence against:** Same as above — all mutations are synchronous operations in the same event loop thread. Done callbacks execute synchronously. No true concurrency.

## 4. cache.py zip strict=False (line 283)
**Claim:** `zip(keys, values, strict=False)` could silently lose data if lists have different lengths
**Evidence against:** `redis.mget(keys)` always returns a list of exactly the same length as input keys, with `None` for missing keys. Length mismatch is impossible by Redis protocol contract.

## 5. news.py sentiment division by zero (line 281)
**Claim:** `(positive_count - negative_count) / total` could divide by zero
**Evidence against:** Line 280 has explicit guard: `if total > 0:`. The `else` branch at line 282 sets `sentiment_score = 0.0`.

## 6. replay.py speed could be zero (line 165)
**Claim:** `market_duration / self.speed` could divide by zero if `speed == 0`
**Evidence against:** `ReplayConfig.validate()` at line 71 rejects `speed <= 0`, and the API endpoint at `api/replay.py:149` calls `config.validate()` before creating the session. `resume()` at line 194 also guards with `speed > 0`.

## 7. Adjustment factor could be zero (adjustments.py:235)
**Claim:** `adjusted_bar["volume"] / float(factor)` could divide by zero
**Evidence against:** Stock split factors are ratios (e.g., 2.0 for 2:1 split, 0.5 for reverse). A factor of 0 is impossible in real corporate actions. The factor defaults to `Decimal("1.0")` when no adjustment applies.
