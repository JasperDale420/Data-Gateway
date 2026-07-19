# Gateway Stall + Sink-Spill Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the lost sink-spill data-protection to committed code and eliminate the market-hours event-loop stalls, so the gateway can be safely restarted without reintroducing silent data loss.

**Architecture:** Three workstreams landed together, then one image rebuild + container recreate. (A) Re-wire `data_sink`'s producer-timeout drop into `redis_sink`'s existing bounded failover buffer (the `spilled_to_buffer` behavior that today runs only in the 5-day-old process's RAM and is on no disk). (B) Move the three synchronous blocking call sites off the event loop and make the stall watchdog actually diagnostic. (C) Move `empire_core` file-logging behind a `QueueHandler`/`QueueListener` so log I/O leaves the loop entirely.

**Tech Stack:** Python 3.12, asyncio, FastAPI, redis-py async, structlog + stdlib logging, uv, pytest (asyncio_mode=auto). `empire-core` is a shared library consumed by every Empire service.

---

## ⚠️ Deploy caveat (read before starting)

- The running container **bind-mounts `gateway/`, `config/`, `logs/`** but **not** `empire_core` (baked into the image). Workstreams A+B are in `gateway/` and reload on restart; **Workstream C changes `empire_core`, so the deploy is an image REBUILD from the monorepo root, not just a restart** (`docker build -f Data-Gateway/Dockerfile -t data-gateway .`).
- `empire_core.setup_logging` is called by **every** Empire repo. The C change keeps the public signature identical (internal handler wiring only), so other services are unaffected until they redeploy. Do **not** change `setup_logging`'s parameters.
- The current container is running uncommitted phantom code (see `project_gateway_postmortem_jul06` memory). Once Workstream A is committed, the rebuild is safe. Do not restart/rebuild until A is committed.

## Root-cause recap (from the 2026-07-06 postmortem)

- **Sink spill:** on producer-block timeout, on-disk `master` `_enqueue_for_sink` (`gateway/core/data_sink.py:412-434`) drops the event and logs `critical`. The running process instead spills to `redis_sink`'s failover buffer and logs `spilled_to_buffer=true` — code that exists nowhere on disk or in git. A rebuild reverts to drop-on-saturation → silent opening-bell loss returns.
- **Loop stalls:** 39 stalls (true `total_stalled_seconds`: sum 607s, max 79.9s) from CPU-bound work monopolizing the single loop thread + GC (23/39 stalls had gen2 sweeps). No container throttling (cgroup limits all zero). Genuine sync-on-loop offenders: `uw_poller.py:829` (`should_defer` — sync file read + `fcntl.flock`), `uw_poller.py:193` (`is_market_open` — `exchange_calendars` pandas), and synchronous `TimedRotatingFileHandler` flush on the WS receive hot path.

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `gateway/core/data_sink.py` | `DataSink` ABC + registry dispatch | Add ABC `buffer_event`/`schedule_drain` hooks; spill on producer-timeout; separate true-loss counter |
| `gateway/core/redis_sink.py` | Redis Streams sink + failover buffer | `buffer_event` returns bool; add `schedule_drain()` |
| `gateway/core/metrics.py` | Prometheus metrics | Add `sink_producer_timeout_loss` counter |
| `config/prometheus_alerts.yml` | Alerts | Point the critical page at true-loss, not every backpressure spill |
| `gateway/core/uw_poller.py` | UW polling loop | `is_market_open` + EOD-state reads off-loop via `asyncio.to_thread`; TTL-cache `is_market_open` |
| `gateway/core/stream.py` | WS receive loop | Raise `on_message_slow` threshold 0.1→1.0s (kill hot-path log flood) |
| `gateway/core/loop_watchdog.py` | Stall detector | Repeated bounded snapshots through a stall + `gc.get_stats()`; clarify duration |
| `empire-core/empire_core/logger.py` | Shared logging setup | Wrap handlers behind `QueueHandler`/`QueueListener`; `shutdown_logging()` + atexit |
| `gateway/core/shutdown.py` | Graceful shutdown | Call `shutdown_logging()` last (flush the queue) |

---

## Workstream A — Restore sink drop-protection (data integrity)

### Task A1: Spill producer-timeout events into the failover buffer

**Files:**
- Modify: `gateway/core/data_sink.py:48` (ABC), `:412-434` (`_enqueue_for_sink` timeout branch)
- Modify: `gateway/core/redis_sink.py:334` (`buffer_event`), add `schedule_drain`
- Modify: `gateway/core/metrics.py`, `config/prometheus_alerts.yml`
- Test: `tests/test_data_sink.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_data_sink.py` add (use the existing bounded-queue fixtures/helpers in that file for forcing a full queue):

```python
async def test_producer_timeout_spills_to_buffer_when_sink_supports_it(registry_with_full_queue):
    registry, sink, blocked_event = registry_with_full_queue  # sink is a fake with buffer_event->True
    await registry.publish("heber:events", {"event_id": "e1", "feed": "flow"}, source="poller", feed="flow")
    assert sink.buffered == [("heber:events", {"event_id": "e1", "feed": "flow"})]
    assert sink.drain_scheduled is True
    assert registry.get_publish_stats()["dropped_producer_timeout"] == 1
    # spilled events are NOT counted as true loss
    assert registry.get_publish_stats().get("producer_timeout_loss", 0) == 0


async def test_producer_timeout_is_true_loss_when_sink_has_no_buffer(registry_with_full_queue_no_buffer):
    registry, sink = registry_with_full_queue_no_buffer  # buffer_event returns False (default ABC)
    await registry.publish("heber:events", {"event_id": "e2", "feed": "flow"}, source="poller", feed="flow")
    assert registry.get_publish_stats()["producer_timeout_loss"] == 1
```

- [ ] **Step 2: Run it, verify it fails**

Run: `uv run pytest tests/test_data_sink.py -k producer_timeout_spills -q`
Expected: FAIL (`buffer_event`/`schedule_drain` not on ABC; no `producer_timeout_loss` stat).

- [ ] **Step 3: Add the ABC hooks**

In `gateway/core/data_sink.py`, inside `class DataSink(ABC):` (near line 48), add:

```python
    def buffer_event(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        """Spill an undeliverable event into a retry buffer.

        Returns True if the event was buffered for later drain, False if this
        sink has no buffer (the event is lost). Default: no buffer.
        """
        return False

    def schedule_drain(self) -> None:
        """Best-effort: flush the retry buffer once transient backpressure clears.

        Default: no-op. Buffered sinks override this. A producer-timeout spill
        happens while the connection is healthy (queue full, not Redis down),
        so nothing else would drain it until the next reconnect.
        """
        return None
```

- [ ] **Step 4: Make `RedisStreamsSink` support the hooks**

In `gateway/core/redis_sink.py`, change `buffer_event` (line 334) to return `bool`:

```python
    def buffer_event(self, topic: str, data: dict[str, Any] | str | bytes) -> bool:
        """Buffer an event from the registry when the queue is saturated or the
        circuit breaker is OPEN. Serializes and delegates to _buffer_failed_event
        so it drains through the same reconnect/drain path. Returns True (buffered)."""
        if isinstance(data, str):
            payload = data.encode()
        elif isinstance(data, bytes):
            payload = data
        else:
            payload = orjson.dumps(data, default=str)
        self._buffer_failed_event(topic, payload)
        return True
```

Add `schedule_drain` next to `_drain_buffer` (near line 348):

```python
    def schedule_drain(self) -> None:
        """Kick a buffer drain when connected and events are pending.

        Serialized by _drain_lock; _do_drain no-ops on an empty buffer, so a
        redundant schedule is harmless. Skips when disconnected (drain then
        happens on reconnect via _ensure_connected)."""
        if self._redis is None or not self._failed_buffer:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._drain_buffer())
        self._drain_tasks.add(task)
        task.add_done_callback(self._drain_tasks.discard)
```

- [ ] **Step 5: Spill on timeout in `_enqueue_for_sink`**

In `gateway/core/data_sink.py`, replace the `except TimeoutError:` branch (lines 412-434) with:

```python
        except TimeoutError:
            buffered = sink.buffer_event(topic, data)
            if buffered:
                sink.schedule_drain()
            self._publish_stats["dropped_producer_timeout"] += 1
            if not buffered:
                self._publish_stats["producer_timeout_loss"] = (
                    self._publish_stats.get("producer_timeout_loss", 0) + 1
                )
                record_sink_producer_timeout_loss(sink.name, source=source, feed=partition_feed)
            self._record_partitioned_publish_stat(
                "dropped_producer_timeout",
                data,
                source=source,
                feed=partition_feed,
            )
            record_sink_producer_timeout_drop(sink.name, source=source, feed=partition_feed)
            allowed, suppressed = _PRODUCER_DROP_LOG_THROTTLE.should_emit(sink.name)
            if allowed:
                # Spilled = recoverable (warning); true loss = page-worthy (critical).
                log_fn = logger.warning if buffered else logger.critical
                log_fn(
                    "data_sink_producer_timeout_drop",
                    sink=sink.name,
                    topic=topic,
                    queue_size=self._queue_size,
                    producer_block_timeout_seconds=self._producer_block_timeout_seconds,
                    suppressed_since_last=suppressed,
                    spilled_to_buffer=buffered,
                    **_event_log_context(data),
                )
            if not sink.record_publish_metrics:
                record_sink_publish(sink=sink.name, topic=topic, success=False)
            return
```

Add `"producer_timeout_loss": 0` to the `self._publish_stats = {...}` initializer.

- [ ] **Step 6: Add the true-loss metric**

In `gateway/core/metrics.py`, mirror `record_sink_producer_timeout_drop` with a `gateway_sink_producer_timeout_loss_total` counter and a `record_sink_producer_timeout_loss(sink, *, source, feed)` function. Import it in `data_sink.py` alongside the existing `record_sink_producer_timeout_drop`.

- [ ] **Step 7: Repoint the critical alert**

In `config/prometheus_alerts.yml`, change the "page on any producer-timeout drop" rule (from commit `99b93e7`) to fire on `gateway_sink_producer_timeout_loss_total` (true loss) instead of `..._drop_total`. Add a separate **warning** on sustained `..._drop_total` rate (backpressure without loss).

- [ ] **Step 8: Run tests + lint, verify pass**

Run:
```bash
uv run pytest tests/test_data_sink.py tests/test_metrics.py -q
ruff check gateway/core/data_sink.py gateway/core/redis_sink.py gateway/core/metrics.py tests/test_data_sink.py
```
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add gateway/core/data_sink.py gateway/core/redis_sink.py gateway/core/metrics.py config/prometheus_alerts.yml tests/test_data_sink.py
git commit -m "fix(data-sink): spill producer-timeout events to failover buffer instead of dropping"
```

---

## Workstream B — Eliminate event-loop stalls

### Task B1: Move UW poller blocking calls off the loop

> **⚠️ CORRECTION (2026-07-09):** The original plan below assumed the blocking calls sit directly in the async loop. They don't — they're behind **sync helpers** (`_is_market_hours` @187 which *already* has a 30s cache, `_is_extended_hours` @197, `_should_poll_eod` @818). Those helpers are also called from a **sync** method (`_get_darkpool_interval:395`), from `quotes_poller.py`/`trades_poller.py`, and from sync tests — so converting them to `async` is out. **Corrected approach:** keep the helpers sync; in `async def _poll_loop`, wrap the invocations at lines 535/542/549/555 (`_is_market_hours`/`_is_extended_hours`) and 565 (`_should_poll_eod`) with `await asyncio.to_thread(self._is_market_hours)` etc., and wrap the `self._eod_state.claim(...)` call at ~line 876 (already async context) with `asyncio.to_thread`. This runs the existing sync methods off the loop with **no signature changes** (transparent to the sync monkeypatched tests) and reuses the existing cache. Do NOT add a second `_market_open_cached`/cache. Test by asserting the helper runs off the loop thread (`threading.get_ident()` differs), in the style of the existing `tests/test_uw_eod_non_blocking.py`. The obsolete original steps are kept below for context only.

**Files:**
- Modify: `gateway/core/uw_poller.py:193` (`is_market_open`), `:829` (`should_defer`), and any sibling `should_skip`/`claim` calls in the loop
- Test: `tests/test_uw_poller.py` (or the existing poller test module)

- [ ] **Step 1: Write the failing test**

```python
async def test_is_market_open_runs_off_loop_and_caches(uw_poller, monkeypatch):
    calls = []
    def fake_open():
        calls.append(1)
        return True
    monkeypatch.setattr(uw_poller._calendar, "is_market_open", fake_open)
    a = await uw_poller._market_open_cached()
    b = await uw_poller._market_open_cached()  # within TTL → no second calendar call
    assert a is True and b is True
    assert len(calls) == 1
```

- [ ] **Step 2: Run it, verify it fails**

Run: `uv run pytest tests/test_uw_poller.py -k market_open_cached -q`
Expected: FAIL (`_market_open_cached` does not exist).

- [ ] **Step 3: Add a cached, off-loop market-open check**

In `gateway/core/uw_poller.py`, add a helper and TTL state (init `self._market_open_cache = (0.0, False)` in `__init__`):

```python
    _MARKET_OPEN_TTL_SECONDS = 30.0

    async def _market_open_cached(self) -> bool:
        now = time.monotonic()
        ts, value = self._market_open_cache
        if now - ts < self._MARKET_OPEN_TTL_SECONDS:
            return value
        value = await asyncio.to_thread(self._calendar.is_market_open)
        self._market_open_cache = (now, value)
        return value
```

Replace `self._calendar.is_market_open()` at line 193 with `await self._market_open_cached()`.

- [ ] **Step 4: Move EOD-state reads off the loop**

At line 829 and any sibling calls, wrap the synchronous `fcntl.flock` + `read_text` state calls:

```python
        if await asyncio.to_thread(self._eod_state.should_defer, today_str):
```

Apply the same `asyncio.to_thread(...)` wrap to `should_skip(...)` and `claim(...)` where they are awaited inside the poll loop. (These do blocking file I/O + `fcntl.flock` in `uw_eod_state.py`.)

- [ ] **Step 5: Run tests + lint, verify pass**

Run:
```bash
uv run pytest tests/test_uw_poller.py -q
ruff check gateway/core/uw_poller.py tests/test_uw_poller.py
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add gateway/core/uw_poller.py tests/test_uw_poller.py
git commit -m "perf(uw-poller): move is_market_open + EOD-state file I/O off the event loop"
```

### Task B2: Stop the `on_message_slow` hot-path log flood

**Files:** Modify `gateway/core/stream.py:56`. Test: `tests/test_stream.py`.

- [ ] **Step 1: Write the failing test**

```python
def test_on_message_slow_threshold_is_one_second():
    from gateway.core.stream import _ON_MESSAGE_SLOW_THRESHOLD_SECONDS
    assert _ON_MESSAGE_SLOW_THRESHOLD_SECONDS == 1.0
```

- [ ] **Step 2: Run it, verify it fails**

Run: `uv run pytest tests/test_stream.py -k on_message_slow_threshold -q`
Expected: FAIL (value is `0.1`).

- [ ] **Step 3: Raise the threshold**

In `gateway/core/stream.py:56`:

```python
_ON_MESSAGE_SLOW_THRESHOLD_SECONDS = 1.0  # 0.1s logged 2,646×/day of normal opening-bell volume
```

- [ ] **Step 4: Run test, verify pass; commit**

```bash
uv run pytest tests/test_stream.py -k on_message_slow_threshold -q
git add gateway/core/stream.py tests/test_stream.py
git commit -m "chore(stream): raise on_message_slow threshold to 1s to cut hot-path log noise"
```

### Task B3: Make the stall watchdog diagnostic

**Files:** Modify `gateway/core/loop_watchdog.py`. Test: `tests/test_loop_watchdog.py`.

The current `_default_on_stall` takes ONE `faulthandler` snapshot at first detection (~84% into long stalls — useless for attribution). Sample repeatedly through the stall (bounded) and include `gc.get_stats()`.

- [ ] **Step 1: Write the failing test**

```python
async def test_watchdog_samples_repeatedly_through_a_long_stall():
    samples = []
    wd = LoopStallWatchdog(
        stall_threshold_seconds=0.2, heartbeat_interval_seconds=0.05, check_interval_seconds=0.05,
        on_stall=lambda s: samples.append(s), on_recover=lambda t: None,
    )
    wd._max_stall_samples = 3
    await wd.start()
    time.sleep(0.7)          # long block → should yield >1 sample, capped at 3
    await asyncio.sleep(0.2)
    await wd.stop()
    assert 2 <= len(samples) <= 3
```

- [ ] **Step 2: Run it, verify it fails**

Run: `uv run pytest tests/test_loop_watchdog.py -k samples_repeatedly -q`
Expected: FAIL (only one `on_stall` per stall).

- [ ] **Step 3: Sample through the stall (bounded)**

In `_watch_loop`, keep firing `on_stall` while the stall persists, up to `self._max_stall_samples` (default 5), one per `check_interval`:

```python
    def _watch_loop(self) -> None:
        stall_active = False
        stall_started_beat: float | None = None
        samples = 0
        while self._running:
            time.sleep(self._check_interval)
            last_beat = self._last_beat
            if last_beat is None:
                continue
            gap = time.monotonic() - last_beat
            if gap >= self._stall_threshold:
                if not stall_active:
                    stall_active = True
                    stall_started_beat = last_beat
                    samples = 0
                if samples < self._max_stall_samples:
                    samples += 1
                    self._safe_call(self._on_stall, gap)
            elif stall_active:
                stall_active = False
                total = (time.monotonic() - stall_started_beat) if stall_started_beat is not None else gap
                self._safe_call(self._on_recover, total)
                stall_started_beat = None
```

Add `self._max_stall_samples = 5` in `__init__`. In `_default_on_stall`, add `gc_stats=gc.get_stats()` to the log call and rename the field for clarity so it reads as an ongoing sample (`elapsed_seconds=round(stalled_seconds, 2)` alongside the existing `stalled_seconds`).

- [ ] **Step 4: Run tests + the built-in self-check, verify pass**

Run:
```bash
uv run pytest tests/test_loop_watchdog.py -q
uv run python -m gateway.core.loop_watchdog   # ponytail self-check must still print OK
ruff check gateway/core/loop_watchdog.py tests/test_loop_watchdog.py
```
Expected: PASS + `OK: detected stall...`.

- [ ] **Step 5: Commit**

```bash
git add gateway/core/loop_watchdog.py tests/test_loop_watchdog.py
git commit -m "feat(watchdog): sample repeatedly through a stall + capture gc stats"
```

---

## Workstream C — Async logging (cross-repo, empire-core)

### Task C1: Move file/stream handlers behind a QueueListener

**Files:**
- Modify: `empire-core/empire_core/logger.py:335-357` (handler wiring), add `shutdown_logging()`
- Modify: `gateway/core/shutdown.py` (call `shutdown_logging()` last)
- Test: `empire-core/tests/test_logger.py`

Synchronous `TimedRotatingFileHandler.flush()`/rollover runs on whatever thread logs — including the event loop. A `QueueHandler` hands records to a `QueueListener` background thread that owns the real handlers, so `logger.info(...)` never touches disk on the loop.

- [ ] **Step 1: Write the failing test**

```python
def test_setup_logging_uses_queue_handler_and_drains_on_shutdown(tmp_path):
    from empire_core.logger import setup_logging, get_logger, shutdown_logging
    import logging
    setup_logging("t", log_dir=str(tmp_path), force=True)
    root = logging.getLogger()
    from logging.handlers import QueueHandler
    assert any(isinstance(h, QueueHandler) for h in root.handlers)
    get_logger("t").info("hello_async")
    shutdown_logging()                       # must stop listener + flush
    text = (tmp_path / "t.log").read_text()
    assert "hello_async" in text
```

- [ ] **Step 2: Run it, verify it fails**

Run: `cd ../empire-core && uv run pytest tests/test_logger.py -k queue_handler -q`
Expected: FAIL (root has raw file handlers, no `shutdown_logging`).

- [ ] **Step 3: Wrap handlers in a QueueListener**

In `empire_core/logger.py`, after building the `handlers` list and setting formatters (line ~350), replace the direct `logging.basicConfig(handlers=handlers, ...)` wiring with a queue in front. Add a module global `_log_listener: QueueListener | None = None`. Stop any prior listener (for `force=True` re-config) before starting a new one:

```python
    import atexit
    import queue as _queue
    from logging.handlers import QueueHandler, QueueListener

    global _log_listener
    if _log_listener is not None:
        _log_listener.stop()
        _log_listener = None

    log_queue: _queue.SimpleQueue = _queue.SimpleQueue()
    _log_listener = QueueListener(log_queue, *handlers, respect_handler_level=True)
    _log_listener.start()
    atexit.register(shutdown_logging)

    logging.basicConfig(
        format="%(message)s",
        handlers=[QueueHandler(log_queue)],
        level=getattr(logging, level, logging.INFO),
        force=force,
    )
```

Add the module-level function:

```python
def shutdown_logging() -> None:
    """Stop the logging QueueListener, flushing buffered records. Idempotent.

    Registered via atexit and called explicitly from service shutdown so a
    process that exits mid-burst does not lose queued log records."""
    global _log_listener
    if _log_listener is not None:
        _log_listener.stop()
        _log_listener = None
```

Export `shutdown_logging` from the package `__init__` / `__all__` alongside `setup_logging`.

- [ ] **Step 4: Run empire-core tests, verify pass**

Run: `cd ../empire-core && uv run pytest tests/test_logger.py -q && ruff check empire_core/logger.py tests/test_logger.py`
Expected: PASS.

- [ ] **Step 5: Commit empire-core**

```bash
cd ../empire-core
git add empire_core/logger.py tests/test_logger.py
git commit -m "perf(logger): move file handlers behind QueueListener; add shutdown_logging"
```

- [ ] **Step 6: Wire gateway shutdown to flush the queue**

Back in `Data-Gateway`, in `gateway/core/shutdown.py`, as the **final** shutdown step (after the sink is closed), call:

```python
    from empire_core.logger import shutdown_logging
    shutdown_logging()
```

- [ ] **Step 7: Re-sync empire-core into Data-Gateway + commit**

```bash
cd ../Data-Gateway
uv sync --extra local --extra dev
uv run pytest tests/test_shutdown.py -q
git add gateway/core/shutdown.py uv.lock
git commit -m "chore(shutdown): flush logging queue as final shutdown step"
```

---

## Workstream D — Integrate, verify, deploy

### Task D1: Full verification across both repos

- [ ] **Step 1: Run everything**

```bash
cd ../empire-core && uv run pytest -q
cd ../Data-Gateway && uv run pytest -q
ruff check . && mypy gateway/core/data_sink.py gateway/core/redis_sink.py gateway/core/uw_poller.py gateway/core/loop_watchdog.py
```
Expected: all PASS.

- [ ] **Step 2: Update CHANGELOG.md**

Add under `## [Unreleased]`:
- **Fixed** — Producer-timeout sink events now spill to the failover buffer instead of being dropped (silent data loss on saturation).
- **Fixed** — Event-loop stalls from synchronous UW-poller file I/O and calendar computation; these now run off the loop.
- **Changed** — File logging moved off the calling thread (QueueListener); `on_message_slow` threshold raised to 1s.
- **Added** — Stall watchdog now samples repeatedly through a freeze and records GC stats; separate true-loss sink metric + alert.

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog for stall + sink-spill recovery"
```

### Task D2: Deploy (single rebuild + recreate)

Because Workstream C changed `empire_core` (baked into the image), this is a **rebuild**, not a bare restart.

- [ ] **Step 1: Rebuild from monorepo root**

```bash
cd /Users/jacobmcmillan/Empire
docker build -f Data-Gateway/Dockerfile -t data-gateway .
```

- [ ] **Step 2: Recreate the container**

```bash
docker compose -f Data-Gateway/docker-compose.yml up -d --force-recreate gateway
```

- [ ] **Step 3: Verify no regression post-deploy**

After the next market session, confirm:
```bash
tail -500 logs/data-gateway_errors.log | rg 'data_sink_producer_timeout_drop|event_loop_stall' || echo "clean"
# producer-timeout drops should now carry spilled_to_buffer=true; check for true loss:
rg 'producer_timeout_loss|spilled_to_buffer.*false' logs/data-gateway.log || echo "no true loss"
```
Expected: any producer-timeout events show `spilled_to_buffer=true`; no `producer_timeout_loss`; stall count and `total_stalled_seconds` materially down vs the 2026-07-06 baseline (sum 607s / max 79.9s).

---

## Self-Review

- **Spec coverage:** A (spill) ✓ Task A1; B1 sync offenders ✓ (uw_poller + calendar), B2 log flood ✓, B3 watchdog observability ✓; C async logging ✓; single restart ✓ Task D2 (rebuild because empire_core changed).
- **Restart-safety ordering:** A is committed (Step A1.9) before any rebuild (D2) — the phantom-code landmine is defused before the container reloads from disk.
- **Cross-repo risk:** C keeps `setup_logging`'s public signature unchanged; other services pick it up on their own redeploys. `shutdown_logging` is additive.
- **Watch-outs for the implementer:** (1) `schedule_drain` must use `get_running_loop` (not `get_event_loop`) and no-op when disconnected. (2) The `producer_timeout_loss` counter is the *paging* signal now — don't leave the old alert firing on every spill. (3) QueueListener must be stopped before re-creating on `force=True` or tests leak threads.
