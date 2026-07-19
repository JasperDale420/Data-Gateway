# Debug Findings — 2026-04-28 broad-scan

Scope: entire `gateway/` codebase (autonomous bug hunt, ~41 hypothesis iterations).
Baseline: 900 tests pass, ruff clean, mypy clean before investigation.

---

## [HIGH] Bug 1 — `_drain_buffer` task can be garbage-collected mid-flight

- **Location:** `gateway/core/redis_sink.py:151`
- **Hypothesis:** `asyncio.create_task(self._drain_buffer())` is fire-and-forget — Python only keeps a *weak* reference to tasks, so a task with no other reference can be GC'd before completion.
- **Evidence:**
  ```python
  # gateway/core/redis_sink.py:149-151
  # After reconnect, drain any buffered events (outside the lock)
  if is_reconnect and self._failed_buffer:
      asyncio.create_task(self._drain_buffer())
  ```
  The result of `create_task` is discarded. The same module already demonstrates the correct pattern elsewhere (`data_sink.py:198-200` stores tasks in `self._background_tasks` with a `discard` callback). This is the **only** unstored `create_task` in `gateway/` (verified: `grep -rn "^\s*asyncio\.create_task(" gateway/`).
- **Reproduction:** Trigger Redis reconnect under memory pressure with non-empty `_failed_buffer`. `_drain_buffer` schedules but may be collected before all chunks publish. Worst case: events that exhausted retries during a transient outage stay in the deque (or get evicted by `maxlen=10_000` as new failures arrive) and never reach Redis.
- **Impact:** Silent event loss to Heber on Redis reconnect — the exact case the buffer was added to protect against. The bigger Redis pipeline + 1 GiB `maxmemory` change in the recent CHANGELOG is wasted if events never make it past the gateway.
- **Root cause:** Misunderstanding of Python asyncio task lifecycle (weak references for tasks).
- **Suggested fix:**
  ```python
  if is_reconnect and self._failed_buffer:
      task = asyncio.create_task(self._drain_buffer())
      self._drain_tasks.add(task)  # type: set[asyncio.Task] in __init__
      task.add_done_callback(self._drain_tasks.discard)
  ```

---

## [MEDIUM] Bug 2 — UW poller dedup marking assumes contiguous prefix of successful publishes

- **Location:** `gateway/core/uw_poller.py:259-269`
- **Hypothesis:** After `publish_all_batch` returns `published=N`, the poller iterates `to_publish[:published]` and marks those `event_id`s as seen. But Redis Streams pipeline-with-`transaction=False` can have partial failures at *arbitrary indices* — `published=N` only tells you the *count* of successes, not *which ones*.
- **Evidence:**
  ```python
  # gateway/core/uw_poller.py:243-269
  if hasattr(sink_registry, "publish_all_batch"):
      published = await sink_registry.publish_all_batch(messages)
  ...
  if published > 0:
      redis_items: list[tuple[str, Any]] = []
      for _envelope, event_id, cache_key in to_publish[:published]:  # <-- assumes prefix
          if event_id:
              self._mark_seen(event_id)
              if self._redis_dedupe is not None and cache_key:
                  redis_items.append((cache_key, True))
  ```
  And in `redis_sink.py:_publish_chunk`:
  ```python
  for i, result in enumerate(results):
      if isinstance(result, Exception):
          ...
      else:
          published += 1
  ```
  The success count is correct; the index identity is not propagated.
- **Reproduction:** Construct a UW batch where item 2 fails (e.g., topic name issue, oversized payload). RedisStreamsSink returns `published=N-1`. UW poller marks items 0..N-2 as seen — but item 2 actually failed and item N-1 actually succeeded. Item 2 gets stuck (will never re-poll because dedup says seen); item N-1 will be re-emitted next poll.
- **Impact:** Silent data loss for the failed item (locally cached as seen, never retried) AND duplicate emission for the actually-succeeded last item. In practice, Redis pipeline partial failures are uncommon (transport blips usually fail the whole batch), but the correctness gap is real.
- **Root cause:** Sink contract returns aggregate count, not per-message success vector. Caller infers identity from index.
- **Suggested fix:** Either (a) have `publish_batch`/`publish_all_batch` return a `list[bool]` aligned with input order, or (b) have UW poller use single `publish_all` per envelope so each success is unambiguous (slower but correct). Option (a) is the right long-term fix.

---

## [LOW] Bug 3 — Duplicate EOD poll block in UW poller loop

- **Location:** `gateway/core/uw_poller.py:440-447`
- **Hypothesis:** Two identical `if self.eod_enabled and self._should_poll_eod():` blocks back-to-back are likely a copy-paste / merge artifact.
- **Evidence:**
  ```python
  # gateway/core/uw_poller.py:439-447
  # EOD snapshot polling (once per trading day after market close)
  if self.eod_enabled and self._should_poll_eod():
      logger.info("uw_poller_starting_eod_snapshots")
      await self._poll_eod_snapshots(sink_registry)

  # EOD snapshot polling (once per trading day after market close)
  if self.eod_enabled and self._should_poll_eod():
      logger.info("uw_poller_starting_eod_snapshots")
      await self._poll_eod_snapshots(sink_registry)
  ```
  Currently dead code: `_poll_eod_snapshots` sets `self._last_eod_date = today` on success, so the second `_should_poll_eod()` returns False. If the first call raises, the outer `except Exception` catches and breaks out of *both* blocks anyway.
- **Impact:** No runtime impact today. But:
  1. If a future change makes `_should_poll_eod` set the date upfront (instead of inside `_poll_eod_snapshots`), the duplicate fires twice and double-publishes.
  2. Code-review confusion / wasted reader cycles.
- **Suggested fix:** Delete lines 444-447.

---

## [LOW] Bug 4 — `next_trading_day` uses host-local date instead of ET

- **Location:** `gateway/core/calendar.py:520-532`
- **Hypothesis:** `date.today()` returns a naive date based on the host TZ. For a host running in UTC, "today" is UTC — but trading calendar lookups should be against ET dates.
- **Evidence:**
  ```python
  # gateway/core/calendar.py:520-523
  def next_trading_day(self, from_date: date | None = None) -> date:
      """Get the next trading day."""
      if from_date is None:
          from_date = date.today()
  ```
  Compare to `is_market_open` (line 498-507) which correctly converts to `self._tz` (ET) before extracting the date.
- **Reproduction:** Host in UTC, current time 2026-04-29 02:00 UTC = 2026-04-28 22:00 ET. `date.today()` returns 2026-04-29; `next_trading_day()` looks for the next trading day after 04-29 (so 04-30) — but the user probably wanted "next trading day after today (ET)" = 04-29 itself if 04-29 is a trading day.
- **Impact:** Off-by-one trading-day lookups during ~4 hours of the day in UTC-hosted deployments. The Docker container in this repo doesn't pin TZ but logs are in UTC, so it's likely UTC by default. Used in informational paths, not order routing.
- **Suggested fix:** `from_date = datetime.now(self._tz).date()`.

---

## [LOW] Bug 5 — Alpaca trading client session never closed on shutdown

- **Location:** `gateway/providers/alpaca/_base.py:154-167`
- **Hypothesis:** `shutdown()` closes the httpx market-data client but only nulls out `_trading_client`. The trading client's `requests.Session` (which was monkey-patched by `_install_session_default_timeout` at init) is left to GC.
- **Evidence:**
  ```python
  # gateway/providers/alpaca/_base.py:154-167
  async def shutdown(self) -> None:
      if self._client:
          await self._client.aclose()
          self._client = None

      # SDK TradingClient doesn't need explicit cleanup
      self._trading_client = None
      ...
  ```
  The comment is wrong — `requests.Session` does need explicit `.close()` to release pooled HTTPS connections immediately.
- **Impact:** During gateway restart, ~16 idle HTTPS sockets to `paper-api.alpaca.markets` linger until OS or GC reaps them. Doesn't affect functionality; minor resource hygiene.
- **Suggested fix:**
  ```python
  if self._trading_client is not None:
      try:
          self._trading_client._session.close()
      except Exception as e:
          logger.debug("alpaca_trading_session_close_failed", error=str(e))
      self._trading_client = None
  ```

---

## [LOW] Bug 6 — Pervasive `time.time()` for windowed timing instead of `time.monotonic()`

- **Locations:** `gateway/api/middleware.py:105-138, 157-227, 268-278, 514, 1259-1406`; `gateway/core/circuit_breaker.py:161,204`; `gateway/core/cache.py:61,76,139,165`; `gateway/core/rate_limiter.py:103-557`.
- **Hypothesis:** All windowed timing logic (rate-limit buckets, cache TTL expiration, circuit-breaker last-failure window) uses `time.time()` — wall-clock time. NTP step corrections (or hibernate / VM pause) can move wall-clock backward, breaking deque-based sliding windows that assume monotonicity.
- **Evidence:**
  ```python
  # gateway/api/middleware.py:110-114
  def _cleanup(self, now: float) -> None:
      cutoff = now - self._window
      while self._timestamps and self._timestamps[0] <= cutoff:
          self._timestamps.popleft()
  ```
  If `now` jumps backward, `cutoff` shrinks, all old timestamps remain, and the bucket appears full forever (until the wall clock catches back up).
- **Impact:** Rare in practice (NTP corrections are usually <1s, smaller than typical windows). On VM resume after suspend, the impact is brief degraded rate limiting. Severity is principle, not active fire.
- **Suggested fix:** Switch all internal-only timing to `time.monotonic()`. Keep `time.time()` only for fields that *must* serialize as wall-clock (e.g. `X-RateLimit-Reset` header — Unix timestamp).

---

## Summary

| ID | Severity | File | Line | One-liner |
|----|----------|------|------|-----------|
| 1 | HIGH | redis_sink.py | 151 | `_drain_buffer` task fire-and-forget — GC risk |
| 2 | MEDIUM | uw_poller.py | 259-269 | Dedup marks index-prefix; partial-failure mismatch |
| 3 | LOW | uw_poller.py | 444-447 | Duplicate EOD poll block (dead code) |
| 4 | LOW | calendar.py | 522 | `next_trading_day` uses host-local TZ |
| 5 | LOW | providers/alpaca/_base.py | 154-167 | Trading client session not closed on shutdown |
| 6 | LOW | middleware/cache/rate_limiter | many | `time.time()` for monotonic windows |

The pre-existing `bug_003` from the cloud `/ultrareview` (Alpaca trading endpoint label) was already fixed earlier in this session in `gateway/api/alpaca/trading.py`.
