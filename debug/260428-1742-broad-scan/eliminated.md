# Eliminated Hypotheses — 2026-04-28 broad-scan

Hypotheses that looked suspicious but were proven not to be bugs after investigation. Logged so future debug runs don't waste time re-investigating.

---

## option_capture: per-symbol stagger compounds with timeout to exceed cycle interval

- **Hypothesis:** With 5 symbols × 120s SPY timeout + 4 × 2s stagger = 608s, can exceed `interval_seconds=300`.
- **Verdict:** Disproven. Loop is bucket-based — `int(now.timestamp()) // self.interval_seconds` advances naturally. A long cycle just means the next cycle starts when the bucket changes, no double-cycle. By design, not a bug.

---

## option_capture: `_to_float(x) or 0.0` treats real 0.0 as falsy

- **Hypothesis:** Same class of bug as the recently fixed `historic_option_volume` `if get("volume") else None` issue.
- **Verdict:** Disproven. `_to_float` returns `None` or `float`. Both `0.0 or 0.0` and `None or 0.0` yield `0.0`. Idempotent — no bug.

---

## RateLimitMiddleware: race on bucket creation

- **Hypothesis:** Two concurrent requests for the same `client_id` could both pass the `not in` check and both create new buckets, with the second silently overwriting the first.
- **Verdict:** Disproven. There's no `await` between the check and the assignment, and asyncio is single-threaded — no scheduler-driven interleave. Safe.

---

## auth.py: timing attack via `dict.get` on plaintext API keys

- **Hypothesis:** `dict.get(api_key)` is O(1) hash-table lookup; not constant-time, possibly leaking key prefix via timing.
- **Verdict:** Practical impact essentially zero. CPython dict lookup time is dominated by hash computation; comparison only happens on hash collisions (vanishingly rare for high-entropy random keys). Severity below the noise floor.

---

## auth.py: short API key prefix could log full key

- **Hypothesis:** `key_preview = api_key[:10] + "..." if len(api_key) > 10 else api_key` would log the *full* key for inputs ≤10 chars.
- **Verdict:** True statement, but real keys generated via `gateway/cli.py` are 32+ chars. The branch only fires for malformed/empty input, where logging the full string is reasonable for debugging. Not a bug worth fixing.

---

## websocket subscribe: rollback unsubs all symbols even if some never subscribed

- **Hypothesis:** Rollback issues `client_unsubscribe(symbols=symbols)` for the entire input symbol list, including symbols that may have failed to subscribe upstream.
- **Verdict:** Multiplexer's `client_unsubscribe` handles "not subscribed" gracefully (idempotent). Slightly wasteful but correct. Local `connection.subscriptions` is *not* updated until the success path (line 519), so no stale-state leak.

---

## websocket heartbeat: `heartbeat_task` is fire-and-forget

- **Hypothesis:** `heartbeat_task = asyncio.create_task(_heartbeat_loop(...))` could be GC'd.
- **Verdict:** Stored in local variable held throughout `try/finally` scope; cancelled in `finally`. Reference held for entire task lifetime. Safe.

---

## data_sink.py: `_safe_publish_with_release` slot leak on cancellation

- **Hypothesis:** If the task is cancelled before its body runs, the acquired sink semaphore slot would never be released.
- **Verdict:** Disproven. `try/finally` in the coroutine still executes `finally` when `CancelledError` propagates through. Even a never-started task that gets cancelled raises `CancelledError` on its first scheduled awaitable, which still runs the `finally`. Slot is released.

---

## data_sink.py: race on `_sink_semaphores` dict in `_try_acquire_sink_slot`

- **Hypothesis:** Concurrent first-access for the same `sink_name` could create two semaphores; the loser leaks a permit.
- **Verdict:** `register()` populates `_sink_semaphores` synchronously, so steady-state hits the existing entry. The race only fires for unregistered sinks (which shouldn't be publishable anyway). Practical risk: nil.

---

## connections.py: `websocket.accept()` under lock serializes handshakes

- **Hypothesis:** Holding `self._lock` during `await websocket.accept()` would block other connect attempts.
- **Verdict:** True but `accept()` is fast (just sends the ASGI accept frame). Doesn't materialize as a real perf issue.

---

## cache.py: `asyncio.get_event_loop()` deprecation

- **Hypothesis:** Python 3.12 deprecates `get_event_loop()` outside a running loop; could fail.
- **Verdict:** `_loop_is_closed` is only called from async methods (running loop always present), where `get_event_loop()` returns the running loop without warning. Safe in current call sites.

---

## stream.py: `_get_connection` could return None for unmapped types

- **Hypothesis:** Subscribe path could pass an unsupported `AlpacaStreamType`.
- **Verdict:** Caller checks `if not conn: return error` (line 948). Handled.

---

## option_capture: bucket logic re-runs on backward NTP correction

- **Hypothesis:** Wall-clock backward step could make `current_bucket < self._last_bucket`, retriggering `run_cycle`.
- **Verdict:** Yes, technically. But NTP step corrections >5min are essentially never seen in monitored hosts; smaller corrections don't cross bucket boundaries. Theoretical only.
