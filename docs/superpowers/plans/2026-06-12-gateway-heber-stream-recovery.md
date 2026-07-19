# Gateway-Heber Stream Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop false darkpool/flow alerts by restoring reliable Gateway -> Redis -> Heber flow, preventing heavy analytics traffic from starving live feeds, and making alerting/auth failures explicit.

**Architecture:** Keep `heber:events` for live/critical market data and prevent bulk analytics from overwhelming that stream. Make Gateway EOD polling restart-safe with persisted claim/complete state, gate and shed low-priority REST sink publishing under queue pressure, tighten Heber liveness checks around real feed windows and backlog state, and make Heber Watch fail clearly before it consumes work when Gateway auth is invalid. Recovery work is split across subagents with disjoint write scopes.

**Tech Stack:** Python 3.12/3.13, FastAPI, Redis Streams, Pydantic Settings, structlog, Docker Compose, pytest, ruff, mypy.

---

## Current Evidence

- Data Gateway is still publishing darkpool: `uw_poller_darkpool_published` continues with `fetched=200` and `published=200`.
- Heber darkpool recovered in `alert-check`: `darkpool: 1391 rows in last 60m`.
- Redis stream is the bottleneck: `XINFO GROUPS heber:events` showed `heber-writers lag=300000` and `pending=2000`.
- Heber dataflow-health already reported that unconsumed events are being evicted at the stream MAXLEN cap.
- Gateway had `data_sink_producer_timeout_drop` events: 134 yesterday, 3 today.
- Gateway EOD poller ran multiple full EOD dumps yesterday because `_last_eod_date` is in-memory.
- Data Gateway REST middleware publishes `/api/v1/uw/gex/<symbol>` list payloads to Heber as individual `greek_exposure` envelopes.
- Cerberus is making high-volume authenticated `uw_ticker_flow` and `uw_greek_exposure` REST calls.
- Heber Watch is failing Gateway auth/preflight and then crashing on repeated enrichment auth failures.
- Heber alert config currently uses `flow_alerts:351`, while recent flow reads were around 221-230 rows.
- Heber darkpool liveness window is `04:00-20:00 ET`, broader than observed useful availability.
- Native `heber alert-check` can run longer than its 300 second launch interval.

## Subagent Ownership

- **Lead integrator:** owns sequencing, conflict resolution, final live verification, Docker rebuild/recreate, changelog, and final commit.
- **Subagent A - Runtime Stabilization:** no source edits; captures current lag/health and performs only approved service operations.
- **Subagent B - Data Gateway Pressure Controls:** owns Data-Gateway EOD idempotency, REST heavy-feed sink gating, and sink-drop logging/metrics.
- **Subagent C - Heber Ingestion and Alerts:** owns Heber consumer/backlog behavior, darkpool/flow liveness rules, alert-check timeout, and dataflow-health alignment.
- **Subagent D - Heber Watch Auth:** owns Gateway client permissions/hash alignment and Heber Watch startup auth behavior.

Workers are not alone in the codebase. Do not revert edits made by others. Keep write scopes disjoint and coordinate through the lead integrator.

---

### Task 0: Runtime Stabilization Snapshot

**Owner:** Subagent A, with lead approval before service changes.

**Files:**
- No source edits.

- [ ] **Step 1: Capture current stream and service state**

Run:

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
date
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
docker exec data-gateway-redis redis-cli XLEN heber:events
docker exec data-gateway-redis redis-cli XINFO GROUPS heber:events
docker exec data-gateway-redis redis-cli XPENDING heber:events heber-writers
docker exec data-gateway-redis redis-cli XLEN heber:events:dlq
```

Expected:

- `data-gateway` and `data-gateway-redis` should be healthy.
- `heber-writers lag` should be recorded exactly.
- `heber:events:dlq` should be recorded exactly; previous observation was `0`.

- [ ] **Step 2: Capture current Heber writer/alert state**

Run:

```bash
docker logs --since 20m --tail 200 heber-consumer
docker logs --since 20m --tail 200 heber-dataflow-health
ps -axo pid,etime,command | rg 'heber alert-check|run_native_heber_service.sh alert-check' | rg -v rg || true
tail -80 /Users/jacobmcmillan/Empire/Heber/logs/native/alert-check.out.log
```

Expected:

- Confirm whether `heber-consumer` is processing current messages or historical/bulk partitions.
- Confirm whether an `alert-check` process is older than 300 seconds.

- [ ] **Step 3: Apply temporary operational mitigations only if approved**

If `heber-watch` is still auth-looping, ask lead approval before stopping it:

```bash
cd /Users/jacobmcmillan/Empire/Heber
docker compose stop watch
```

If `alert-check` is stuck longer than 2 intervals, ask lead approval before unloading the LaunchAgent temporarily:

```bash
launchctl bootout gui/$(id -u) /Users/jacobmcmillan/Library/LaunchAgents/com.empire.heber.alert-check.plist
```

Do not trim or delete Redis streams during this task. Stream trimming can make data loss permanent.

- [ ] **Step 4: Report the live numbers to the lead**

Report:

- `XLEN heber:events`
- `heber-writers lag`
- `heber-writers pending`
- `watch-consumer lag/pending`
- DLQ length
- Whether `alert-check` is stuck
- Whether `heber-postgres` is healthy or still unresponsive

---

### Task 1: Persist Data Gateway EOD Poller Run State

**Owner:** Subagent B.

**Files:**
- Create: `gateway/core/uw_eod_state.py`
- Modify: `gateway/config.py`
- Modify: `gateway/core/uw_poller.py`
- Modify: `docker-compose.yml`
- Test: `tests/test_uw_eod_state.py`
- Test: `tests/test_uw_poller.py`

- [ ] **Step 1: Write failing tests for restart-safe EOD idempotency**

Create `tests/test_uw_eod_state.py`:

```python
from datetime import UTC, datetime, timedelta

from gateway.core.uw_eod_state import UwEodRunState, UwEodStateStore


def test_eod_state_store_persists_completed_date_across_instances(tmp_path):
    path = tmp_path / "uw_eod_state.json"
    store = UwEodStateStore(path, stale_after_seconds=3600)

    assert store.claim("2026-06-12") is True
    store.mark_completed("2026-06-12", totals={"greek_exposure": {"published": 1, "errors": 0}})

    reloaded = UwEodStateStore(path, stale_after_seconds=3600)
    assert reloaded.should_skip("2026-06-12") is True


def test_eod_state_claim_blocks_same_day_after_restart(tmp_path):
    path = tmp_path / "uw_eod_state.json"
    store = UwEodStateStore(path, stale_after_seconds=3600)

    assert store.claim("2026-06-12") is True
    reloaded = UwEodStateStore(path, stale_after_seconds=3600)

    assert reloaded.claim("2026-06-12") is False


def test_eod_state_claim_allows_retry_after_stale_running_marker(tmp_path):
    path = tmp_path / "uw_eod_state.json"
    stale_started_at = datetime.now(UTC) - timedelta(hours=2)
    path.write_text(
        UwEodRunState(
            trading_date="2026-06-12",
            status="running",
            started_at=stale_started_at.isoformat(),
            completed_at=None,
            totals={},
        ).model_dump_json()
    )

    store = UwEodStateStore(path, stale_after_seconds=60)

    assert store.claim("2026-06-12") is True
```

Run:

```bash
uv run pytest tests/test_uw_eod_state.py -q
```

Expected: fail because `gateway.core.uw_eod_state` does not exist.

- [ ] **Step 2: Implement the state store**

Create `gateway/core/uw_eod_state.py`:

```python
"""Persistent run-state for the UW EOD poller."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Literal
from pathlib import Path

from pydantic import BaseModel, Field

from gateway.core.logger import logger


class UwEodRunState(BaseModel):
    trading_date: str
    status: Literal["running", "completed"]
    started_at: str
    completed_at: str | None = None
    totals: dict = Field(default_factory=dict)


class UwEodStateStore:
    """Atomic JSON state file for once-per-day EOD runs."""

    def __init__(self, path: Path, *, stale_after_seconds: int):
        self.path = path
        self.stale_after_seconds = stale_after_seconds

    def should_skip(self, trading_date: str) -> bool:
        state = self._read()
        return state is not None and state.trading_date == trading_date and state.status == "completed"

    def claim(self, trading_date: str) -> bool:
        state = self._read()
        if state is not None and state.trading_date == trading_date:
            if state.status == "completed":
                return False
            if not self._is_stale(state):
                return False
        self._write(
            UwEodRunState(
                trading_date=trading_date,
                status="running",
                started_at=datetime.now(UTC).isoformat(),
            )
        )
        return True

    def mark_completed(self, trading_date: str, *, totals: dict) -> None:
        state = self._read()
        started_at = state.started_at if state and state.trading_date == trading_date else datetime.now(UTC).isoformat()
        self._write(
            UwEodRunState(
                trading_date=trading_date,
                status="completed",
                started_at=started_at,
                completed_at=datetime.now(UTC).isoformat(),
                totals=totals,
            )
        )

    def _read(self) -> UwEodRunState | None:
        try:
            raw = self.path.read_text()
        except FileNotFoundError:
            return None
        except OSError as exc:
            logger.warning("uw_eod_state_read_failed", path=str(self.path), error=str(exc), exc_info=True)
            return None

        try:
            return UwEodRunState.model_validate_json(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("uw_eod_state_invalid_json", path=str(self.path), error=str(exc), exc_info=True)
            return None

    def _is_stale(self, state: UwEodRunState) -> bool:
        started_at = datetime.fromisoformat(state.started_at)
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=UTC)
        return (datetime.now(UTC) - started_at).total_seconds() >= self.stale_after_seconds

    def _write(self, state: UwEodRunState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        tmp_path.write_text(state.model_dump_json())
        os.replace(tmp_path, self.path)
```

- [ ] **Step 3: Add the Gateway setting**

In `gateway/config.py`, add near the UW EOD settings:

```python
uw_eod_state_path: Path = Field(
    default=Path("/app/logs/state/uw_eod_state.json"),
    description="Persistent JSON state file used to prevent duplicate UW EOD runs across restarts",
)
uw_eod_claim_stale_after_seconds: int = Field(
    default=7200,
    ge=300,
    description="Allow a UW EOD run to be retried when a running claim is older than this many seconds",
)
```

If `Path` is not already imported in `gateway/config.py`, add:

```python
from pathlib import Path
```

- [ ] **Step 4: Wire state into `UWPoller`**

In `gateway/core/uw_poller.py`, import:

```python
from gateway.core.uw_eod_state import UwEodStateStore
```

In `UWPoller.__init__`, replace the in-memory-only comment with:

```python
self._last_eod_date: str | None = None
self._eod_state = UwEodStateStore(
    settings.uw_eod_state_path,
    stale_after_seconds=settings.uw_eod_claim_stale_after_seconds,
)
```

In `_should_poll_eod`, after `today_str = now_et.strftime("%Y-%m-%d")`, add:

```python
if self._eod_state.should_skip(today_str):
    return False
```

At the start of `_poll_eod_snapshots`, claim the run:

```python
today_str = datetime.now(ET).strftime("%Y-%m-%d")
if not self._eod_state.claim(today_str):
    logger.info("uw_eod_skipped_persistent_state", trading_date=today_str)
    return
```

After all endpoint work succeeds and before the completed log:

```python
self._eod_state.mark_completed(today_str, totals=totals)
self._last_eod_date = today_str
```

Keep the existing in-memory guard as a cheap same-process fast path.

- [ ] **Step 5: Confirm Docker persistence**

`docker-compose.yml` already mounts `./logs:/app/logs`, so `/app/logs/state/uw_eod_state.json` persists without a new volume. Optionally add an explicit env line for clarity:

```yaml
      - GATEWAY_UW_EOD_STATE_PATH=/app/logs/state/uw_eod_state.json
```

Do not put secrets in this directory.

- [ ] **Step 6: Verify**

Run:

```bash
uv run pytest tests/test_uw_eod_state.py tests/test_uw_poller.py -q
ruff check gateway/core/uw_eod_state.py gateway/core/uw_poller.py gateway/config.py tests/test_uw_eod_state.py tests/test_uw_poller.py
```

Expected: tests pass, ruff pass.

- [ ] **Step 7: Commit**

```bash
git add gateway/core/uw_eod_state.py gateway/core/uw_poller.py gateway/config.py docker-compose.yml tests/test_uw_eod_state.py tests/test_uw_poller.py
git commit -m "fix: persist UW EOD run state"
```

---

### Task 2: Gate and Shed Low-Priority REST Sink Publishing

**Owner:** Subagent B.

**Files:**
- Modify: `gateway/config.py`
- Modify: `gateway/api/middleware/envelope.py`
- Modify: `gateway/core/data_sink.py`
- Modify: `gateway/core/metrics.py`
- Test: `tests/test_middleware_streaming.py` or create `tests/test_rest_sink_gating.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write failing tests for REST heavy-feed exclusion**

Create `tests/test_rest_sink_gating.py`:

```python
from gateway.api.middleware.envelope import EventEnvelopeMiddleware


def test_rest_sink_skips_gex_when_heavy_feed_disabled(monkeypatch):
    monkeypatch.setenv("GATEWAY_REST_SINK_EXCLUDED_FEEDS", "greek_exposure,iv_rank")
    from gateway.config import get_settings

    get_settings.cache_clear()
    middleware = EventEnvelopeMiddleware(app=lambda scope, receive, send: None)

    assert middleware._is_sink_publish_eligible(
        path="/api/v1/uw/gex/SPY",
        payload=[{"symbol": "SPY", "call_gamma": 1}],
        feed="greek_exposure",
    ) is False


def test_rest_sink_skips_darkpool_rest_when_poller_owns_live_feed(monkeypatch):
    monkeypatch.setenv("GATEWAY_REST_SINK_EXCLUDED_FEEDS", "darkpool,flow_alerts,greek_exposure")
    from gateway.config import get_settings

    get_settings.cache_clear()
    middleware = EventEnvelopeMiddleware(app=lambda scope, receive, send: None)

    assert middleware._is_sink_publish_eligible(
        path="/api/v1/uw/darkpool/SPY",
        payload=[{"symbol": "SPY", "price": 500, "size": 1000}],
        feed="darkpool",
    ) is False
```

Run:

```bash
uv run pytest tests/test_rest_sink_gating.py -q
```

Expected: fail because `rest_sink_excluded_feeds` does not exist.

- [ ] **Step 2: Add the setting**

In `gateway/config.py`, add near data sink settings:

```python
rest_sink_excluded_feeds: str = Field(
    default="greek_exposure,iv_rank,iv_term_structure,short_data,ftd,flow_alerts,darkpool",
    description="Comma-separated feed names that REST envelope middleware must not publish into live Heber stream",
)
rest_sink_low_priority_max_queue_utilization: float = Field(
    default=0.70,
    ge=0.0,
    le=1.0,
    description="Shed low-priority REST sink publishing when sink queue utilization is at or above this threshold",
)

@property
def rest_sink_excluded_feed_set(self) -> set[str]:
    return {item.strip() for item in self.rest_sink_excluded_feeds.split(",") if item.strip()}
```

- [ ] **Step 3: Add queue-pressure API to the sink registry**

In `gateway/core/data_sink.py`, add:

```python
def get_queue_utilization(self, sink_name: str) -> float:
    queue = self._sink_queues.get(sink_name)
    if queue is None:
        return 0.0
    maxsize = queue.maxsize or self._queue_size
    if maxsize <= 0:
        return 0.0
    return queue.qsize() / maxsize

def can_accept_low_priority(self, sink_name: str, *, max_utilization: float) -> bool:
    return self.get_queue_utilization(sink_name) < max_utilization
```

Add tests:

```python
def test_low_priority_publish_sheds_when_queue_pressure_exceeds_threshold():
    registry = DataSinkRegistry(queue_size=10)
    queue = asyncio.Queue(maxsize=10)
    for idx in range(7):
        queue.put_nowait(("heber:events", {"event_id": str(idx)}))
    registry._sink_queues["redis_streams"] = queue

    assert registry.can_accept_low_priority("redis_streams", max_utilization=0.70) is False


def test_high_priority_publish_still_enqueues_after_low_priority_shed():
    registry = DataSinkRegistry(queue_size=10)
    queue = asyncio.Queue(maxsize=10)
    for idx in range(7):
        queue.put_nowait(("heber:events", {"event_id": str(idx)}))
    registry._sink_queues["redis_streams"] = queue

    assert registry.can_accept_low_priority("redis_streams", max_utilization=0.90) is True
```

- [ ] **Step 4: Enforce exclusion and pressure shedding in middleware**

In `gateway/api/middleware/envelope.py`, import settings lazily inside `_is_sink_publish_eligible`:

```python
from gateway.config import get_settings

settings = get_settings()
if feed in settings.rest_sink_excluded_feed_set:
    logger.info("rest_envelope_sink_publish_skipped", path=path, feed=feed, reason="excluded_feed")
    return False
```

Place this check before route-specific allow rules.

Before creating background publish tasks, check queue pressure:

```python
if sink_registry and should_publish_to_sink:
    if not sink_registry.can_accept_low_priority(
        "redis_streams",
        max_utilization=settings.rest_sink_low_priority_max_queue_utilization,
    ):
        logger.warning(
            "rest_envelope_sink_publish_skipped",
            path=path,
            feed=feed,
            reason="queue_pressure",
        )
        should_publish_to_sink = False
```

Keep the HTTP response enveloped either way; only the background sink publish is shed.

- [ ] **Step 5: Confirm ownership boundaries**

REST sink publishing should default to skip:

- `/api/v1/uw/gex/<symbol>` -> `greek_exposure`
- `/api/v1/uw/flow/<symbol>` -> `flow_alerts`
- `/api/v1/uw/darkpool/<symbol>` -> `darkpool`

Pollers remain responsible for live UW `flow_alerts` and `darkpool`.

REST sink publishing may still allow:

- Alpaca stock bars/trades routes with non-empty payloads

- [ ] **Step 6: Verify**

```bash
uv run pytest tests/test_rest_sink_gating.py tests/test_data_sink.py tests/test_config.py -q
ruff check gateway/api/middleware/envelope.py gateway/core/data_sink.py gateway/core/metrics.py gateway/config.py tests/test_rest_sink_gating.py tests/test_data_sink.py tests/test_config.py
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add gateway/config.py gateway/api/middleware/envelope.py gateway/core/data_sink.py gateway/core/metrics.py tests/test_rest_sink_gating.py tests/test_data_sink.py tests/test_config.py
git commit -m "fix: protect live Heber stream from REST pressure"
```

---

### Task 3: Improve Data Sink Drop Diagnostics

**Owner:** Subagent B.

**Files:**
- Modify: `gateway/core/data_sink.py`
- Modify: `gateway/core/metrics.py`
- Modify: `gateway/api/health.py`
- Modify: `gateway/api/admin.py`
- Modify: `config/prometheus_alerts.yml`
- Test: `tests/test_data_sink.py`
- Test: `tests/test_metrics.py`
- Test: `tests/test_health.py`
- Test: `tests/test_admin_status.py`

- [ ] **Step 1: Write failing test for drop metadata extraction**

Add to `tests/test_data_sink.py`:

```python
from gateway.core.data_sink import _event_log_context


def test_event_log_context_extracts_feed_provider_and_event_id():
    data = {
        "event_id": "evt-1",
        "feed": "darkpool",
        "provider": "unusual_whales",
        "instrument_key": "equity:SPY",
    }

    assert _event_log_context(data) == {
        "event_id": "evt-1",
        "feed": "darkpool",
        "provider": "unusual_whales",
        "instrument_key": "equity:SPY",
    }


def test_event_log_context_handles_non_dict_payload():
    assert _event_log_context("raw") == {}
```

Run:

```bash
uv run pytest tests/test_data_sink.py::test_event_log_context_extracts_feed_provider_and_event_id tests/test_data_sink.py::test_event_log_context_handles_non_dict_payload -q
```

Expected: fail because `_event_log_context` does not exist.

- [ ] **Step 2: Add metadata helper**

In `gateway/core/data_sink.py`, add near the throttle constant:

```python
def _event_log_context(data: dict[str, Any] | str | bytes) -> dict[str, str]:
    if not isinstance(data, dict):
        return {}
    keys = ("event_id", "feed", "provider", "instrument_key")
    return {key: str(data[key]) for key in keys if data.get(key) is not None}
```

- [ ] **Step 3: Include metadata in critical drop log**

In `_enqueue_for_sink`, change:

```python
logger.critical(
    "data_sink_producer_timeout_drop",
    sink=sink.name,
    topic=topic,
    queue_size=self._queue_size,
    producer_block_timeout_seconds=self._producer_block_timeout_seconds,
    suppressed_since_last=suppressed,
)
```

to:

```python
logger.critical(
    "data_sink_producer_timeout_drop",
    sink=sink.name,
    topic=topic,
    queue_size=self._queue_size,
    producer_block_timeout_seconds=self._producer_block_timeout_seconds,
    suppressed_since_last=suppressed,
    **_event_log_context(data),
)
```

- [ ] **Step 4: Add partitioned backpressure stats**

In `gateway/core/data_sink.py`, extend publish stats from only global counters:

```python
self._publish_stats = {
    "scheduled": 0,
    "queued": 0,
    "dropped_producer_timeout": 0,
    "low_priority_shed": 0,
    "by_source_feed": {},
}
```

Add a helper:

```python
def _record_publish_stat(self, *, source: str, feed: str, status: str) -> None:
    key = f"{source}:{feed}"
    bucket = self._publish_stats["by_source_feed"].setdefault(
        key,
        {"queued": 0, "dropped_producer_timeout": 0, "low_priority_shed": 0},
    )
    bucket[status] = int(bucket.get(status, 0)) + 1
```

Use `source="poller"` for pollers and `source="rest"` for REST middleware. If a caller does not pass source, default to `source="unknown"`.

- [ ] **Step 5: Add health/admin backpressure snapshot**

Expose a compact snapshot from the registry:

```python
def get_backpressure_snapshot(self) -> dict[str, Any]:
    return {
        "queue_size": self._queue_size,
        "worker_count": self._worker_count,
        "producer_block_timeout_seconds": self._producer_block_timeout_seconds,
        "sinks": {
            sink.name: {
                "queue_depth": self.get_queue_depth(sink.name),
                "queue_utilization": round(self.get_queue_utilization(sink.name), 4),
            }
            for sink in self._sinks
        },
        "publish_stats": self.get_publish_stats(),
    }
```

Add the snapshot to `/health/status` and `/api/v1/status` without changing existing response fields.

- [ ] **Step 6: Add metrics and alerts**

In `gateway/core/metrics.py`, add counters/gauges for:

- Low-priority REST shed count by feed.
- Producer timeout drops by feed/source.
- Sink queue utilization by sink.

In `config/prometheus_alerts.yml`, add:

- warning for sustained queue utilization above 70%.
- warning for low-priority REST shedding.
- keep producer timeout drops as critical.

- [ ] **Step 7: Verify**

```bash
uv run pytest tests/test_data_sink.py tests/test_metrics.py tests/test_health.py tests/test_admin_status.py -q
ruff check gateway/core/data_sink.py gateway/core/metrics.py gateway/api/health.py gateway/api/admin.py tests/test_data_sink.py tests/test_metrics.py tests/test_health.py tests/test_admin_status.py
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add gateway/core/data_sink.py gateway/core/metrics.py gateway/api/health.py gateway/api/admin.py config/prometheus_alerts.yml tests/test_data_sink.py tests/test_metrics.py tests/test_health.py tests/test_admin_status.py
git commit -m "chore: add feed context to sink drop logs"
```

---

### Task 4: Fix Heber Watch Gateway Auth and Permissions

**Owner:** Subagent D.

**Files:**
- Modify: `config/clients.yaml`
- Modify: `gateway/core/auth.py` only if `old_key_hashes` should remain supported
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/watch/writer.py`
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/watch/features.py` only if startup behavior requires it
- Modify: `/Users/jacobmcmillan/Empire/Heber/.env.example`
- Modify: `/Users/jacobmcmillan/Empire/Heber/docs/configuration-guide.md`
- Test: `tests/test_auth.py`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/test_watch_service_auth_preflight.py`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/test_watch_gateway_key_contract.py`

- [ ] **Step 1: Write failing Gateway config test**

In `tests/test_auth.py`, add:

```python
from pathlib import Path

import yaml


def test_heber_watch_client_has_watch_route_permissions():
    config = yaml.safe_load(Path("config/clients.yaml").read_text())
    client = next(item for item in config["clients"] if item["id"] == "heber-watch")
    permissions = client["permissions"]

    assert "alpaca" in permissions["providers"]
    assert "uw" in permissions["providers"] or "unusual_whales" in permissions["providers"]
    assert "bars" in permissions["feeds"]
    assert "quotes" in permissions["feeds"]
    assert "trades" in permissions["feeds"]
    assert "options" in permissions["feeds"]
    assert "option_quotes" in permissions["feeds"]
    assert "flow" in permissions["feeds"]
    assert "flow_alerts" in permissions["feeds"]
    assert "greek_exposure" in permissions["feeds"]
    assert "iv_rank" in permissions["feeds"]
    assert "market_tide" in permissions["feeds"]
    assert "max_pain" in permissions["feeds"]
    assert permissions.get("trading", False) is False
```

Run:

```bash
uv run pytest tests/test_auth.py::test_heber_watch_client_has_watch_route_permissions -q
```

Expected: fail because `heber-watch` lacks UW and enrichment feeds.

- [ ] **Step 2: Update Gateway client permissions without exposing plaintext keys**

In `config/clients.yaml`, update only the `heber-watch` permissions block:

```yaml
    feeds:
    - bars
    - quotes
    - trades
    - options
    - option_quotes
    - flow
    - flow_alerts
    - greek_exposure
    - iv_rank
    - market_tide
    - max_pain
    providers:
    - alpaca
    - yfinance
    - uw
```

Do not add plaintext keys. If the existing hash does not match the local Heber key, generate a new hash locally without printing the key:

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
set -a
source /Users/jacobmcmillan/Empire/Heber/.env
set +a
uv run python - <<'PY'
import os
from gateway.core.auth import ClientAuthenticator

print(ClientAuthenticator.hash_key(os.environ["HEBER_WATCH_GATEWAY_API_KEY"]))
PY
```

Paste only the resulting `sha256:...` value into `config/clients.yaml`.

- [ ] **Step 3: Decide whether to support key rotation hashes**

If `config/clients.yaml` uses `old_key_hashes` for `heber-watch`, add a failing test:

```python
def test_authenticator_accepts_old_key_hashes_for_rotation_window(tmp_path):
    key = "gw_old_test_key"
    config_path = tmp_path / "clients.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "clients": [
                    {
                        "id": "heber-watch",
                        "enabled": True,
                        "key_hash": "sha256:" + ("0" * 64),
                        "old_key_hashes": [ClientAuthenticator.hash_key(key)],
                        "permissions": {"providers": ["alpaca"], "feeds": ["bars"]},
                        "role": "trader",
                    }
                ]
            }
        )
    )

    assert ClientAuthenticator(config_path).authenticate(key).id == "heber-watch"
```

If the test fails, modify `gateway/core/auth.py` to load every hash in `old_key_hashes` into `_hashed_keys`.

- [ ] **Step 4: Make Watch fail before consuming messages when auth preflight fails**

In `/Users/jacobmcmillan/Empire/Heber/heber/watch/writer.py`, locate the startup preflight path. Change behavior so a Gateway `401`/`403` during required preflight raises before starting the consumer loop.

Expected shape:

```python
class GatewayAuthPreflightError(RuntimeError):
    """Gateway auth preflight failed before Watch started consuming alerts."""


if not preflight.ok:
    logger.critical(
        "gateway_auth_preflight_failed",
        status_code=preflight.status_code,
        gateway_url=self.gateway_url,
    )
    raise GatewayAuthPreflightError("Gateway auth preflight failed; refusing to start watch consumer")
```

Do not log the API key.

- [ ] **Step 5: Add or update Heber Watch tests**

In `/Users/jacobmcmillan/Empire/Heber/tests/test_watch_service_auth_preflight.py`, ensure there is a test equivalent to:

```python
async def test_watch_service_refuses_to_start_when_gateway_auth_preflight_returns_401(...):
    service = make_watch_service_with_preflight_status(401)

    with pytest.raises(GatewayAuthPreflightError, match="Gateway auth preflight failed"):
        await service.run()


async def test_watch_service_refuses_to_start_when_gateway_auth_preflight_returns_403(...):
    service = make_watch_service_with_preflight_status(403)

    with pytest.raises(GatewayAuthPreflightError, match="Gateway auth preflight failed"):
        await service.run()
```

If existing fixtures use different names, keep the local style and assert the same behavior.

- [ ] **Step 6: Update docs**

In `/Users/jacobmcmillan/Empire/Heber/.env.example`, use a placeholder:

```bash
HEBER_WATCH_GATEWAY_API_KEY=gw_replace_with_data_gateway_heber_watch_key
```

In `/Users/jacobmcmillan/Empire/Heber/docs/configuration-guide.md`, document:

- Watch needs a Gateway API key.
- The key must match the `heber-watch` client hash in Data Gateway.
- The client must have read-only `alpaca` and `uw` access.

- [ ] **Step 7: Verify**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest tests/test_auth.py -q
ruff check gateway/core/auth.py tests/test_auth.py

cd /Users/jacobmcmillan/Empire/Heber
uv run pytest tests/test_watch_service_auth_preflight.py tests/test_watch_gateway_key_contract.py tests/test_watch_gateway_auth_headers.py tests/test_watch_feature_enrichment_resilience.py tests/test_watch_gateway_auth_diagnostics.py -q
ruff check heber/watch/writer.py tests/test_watch_service_auth_preflight.py tests/test_watch_gateway_key_contract.py tests/test_watch_gateway_auth_headers.py tests/test_watch_feature_enrichment_resilience.py tests/test_watch_gateway_auth_diagnostics.py
```

Expected: pass.

- [ ] **Step 8: Commit**

Commit Data Gateway and Heber changes separately because they are separate repos:

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
git add config/clients.yaml gateway/core/auth.py tests/test_auth.py
git commit -m "fix: allow heber-watch enrichment routes"

cd /Users/jacobmcmillan/Empire/Heber
git add heber/watch/writer.py .env.example docs/configuration-guide.md tests/test_watch_service_auth_preflight.py tests/test_watch_gateway_key_contract.py tests/test_watch_gateway_auth_headers.py tests/test_watch_feature_enrichment_resilience.py tests/test_watch_gateway_auth_diagnostics.py
git commit -m "fix: fail heber-watch before consuming on gateway auth failure"
```

---

### Task 5: Tighten Heber Liveness Rules and Alert Runtime

**Owner:** Subagent C.

**Files:**
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/health_monitor/feed_registry.py`
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/health_monitor/checks/liveness.py`
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/config.py`
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/cli.py`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/health_monitor/test_feed_registry.py`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/health_monitor/test_liveness.py`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/test_cli_alert.py`

- [ ] **Step 1: Write failing darkpool window test**

In `/Users/jacobmcmillan/Empire/Heber/tests/health_monitor/test_feed_registry.py`, add:

```python
from heber.health_monitor.feed_registry import DEFAULT_REGISTRY


def test_darkpool_liveness_uses_regular_open_start():
    darkpool = next(rule for rule in DEFAULT_REGISTRY if rule.feed == "darkpool")

    assert darkpool.window_start_et == "09:30"
    assert darkpool.window_end_et == "20:00"
```

Run:

```bash
cd /Users/jacobmcmillan/Empire/Heber
uv run pytest tests/health_monitor/test_feed_registry.py::test_darkpool_liveness_uses_regular_open_start -q
```

Expected: fail because the current start is `04:00`.

- [ ] **Step 2: Update darkpool rule**

In `/Users/jacobmcmillan/Empire/Heber/heber/health_monitor/feed_registry.py`, change:

```python
FeedRule("darkpool", "continuous", "04:00", "20:00", 60, 1),
```

to:

```python
FeedRule("darkpool", "continuous", "09:30", "20:00", 60, 1),
```

- [ ] **Step 3: Add alert-check timeout setting**

In `/Users/jacobmcmillan/Empire/Heber/heber/config.py`, add near alert settings:

```python
alert_check_lock_path: Path = Field(
    default=Path("/tmp/heber-alert-check.lock"),
    description="Lock file preventing overlapping native alert-check runs",
)
alert_check_timeout_seconds: int = Field(
    default=240,
    ge=30,
    le=600,
    description="Wall-clock timeout for one-shot alert-check runs so launchd intervals cannot overlap indefinitely",
)
```

- [ ] **Step 4: Write failing CLI tests for timeout and lock behavior**

In `/Users/jacobmcmillan/Empire/Heber/tests/test_cli_alert.py`, add tests equivalent to:

```python
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from heber.cli import _cmd_alert_check


def test_alert_check_returns_nonzero_when_existing_lock_is_active(tmp_path):
    lock_path = tmp_path / "alert-check.lock"
    lock_path.write_text("999999")

    class _Settings:
        alert_check_lock_path = lock_path
        alert_check_timeout_seconds = 240

    with patch("heber.config.get_settings", return_value=_Settings()):
        rc = _cmd_alert_check(SimpleNamespace())

    assert rc == 2


def test_alert_check_times_out_before_dispatching(tmp_path):
    class _Settings:
        alert_check_lock_path = tmp_path / "alert-check.lock"
        alert_check_timeout_seconds = 1

    async def _slow_checks(*_args, **_kwargs):
        import asyncio

        await asyncio.sleep(10)

    with (
        patch("heber.config.get_settings", return_value=_Settings()),
        patch("heber.health_monitor.checks.liveness.run_liveness_checks", new=AsyncMock(side_effect=_slow_checks)),
        patch("heber.cli.DiscordNotifier") as notifier_cls,
    ):
        rc = _cmd_alert_check(SimpleNamespace())

    assert rc == 2
    notifier_cls.return_value.dispatch.assert_not_called()
```

If `tests/test_cli_alert.py` already has fixtures for settings or liveness mocks, reuse those fixtures and keep the expected behavior: overlapping runs return `2`, timed-out runs return `2`, and timed-out runs do not dispatch stale pages.

Run:

```bash
cd /Users/jacobmcmillan/Empire/Heber
uv run pytest tests/test_cli_alert.py -q
```

Expected: fail because alert-check has no lock and no timeout.

- [ ] **Step 5: Enforce lock and timeout in CLI**

In `/Users/jacobmcmillan/Empire/Heber/heber/cli.py`, change:

```python
results = asyncio.run(run_liveness_checks(ctx, now=now))
```

to:

```python
import os


def _try_claim_alert_check_lock(lock_path: Path) -> bool:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return False
    try:
        os.write(fd, str(os.getpid()).encode())
    finally:
        os.close(fd)
    return True


async def _run_with_timeout():
    return await asyncio.wait_for(
        run_liveness_checks(ctx, now=now),
        timeout=settings.alert_check_timeout_seconds,
    )

lock_acquired = _try_claim_alert_check_lock(settings.alert_check_lock_path)
if not lock_acquired:
    print("alert-check: previous run still active", file=sys.stderr)
    return 2

try:
    results = asyncio.run(_run_with_timeout())
except TimeoutError:
    print(
        f"alert-check: timed out after {settings.alert_check_timeout_seconds}s",
        file=sys.stderr,
    )
    return 2
finally:
    if lock_acquired:
        try:
            settings.alert_check_lock_path.unlink(missing_ok=True)
        except OSError:
            pass
```

When implementing, call `_try_claim_alert_check_lock(...)` immediately after `settings = get_settings()` and before creating `CheckContext`, `HeberReader`, or `DiscordNotifier`. Keep helper functions small and module-local. Import `Path`, `os`, and `sys` at the top of `heber/cli.py` if they are not already imported.

- [ ] **Step 6: Recalibrate flow_alerts floor as an operational change**

Do not commit local webhook values. Run:

```bash
cd /Users/jacobmcmillan/Empire/Heber
uv run heber alert-calibrate --days-back 2 --ratio 0.25
```

Then update only the local `HEBER_ALERT_FLOOR_OVERRIDES` value in `/Users/jacobmcmillan/Empire/Heber/.env`. Keep `bars` and `trades` disabled if that is still desired. Do not commit `.env`.

Expected temporary safe value if calibration is unavailable:

```bash
HEBER_ALERT_FLOOR_OVERRIDES='{"bars":0,"trades":0,"flow_alerts":25}'
```

- [ ] **Step 7: Verify**

```bash
cd /Users/jacobmcmillan/Empire/Heber
uv run pytest tests/health_monitor/test_feed_registry.py tests/health_monitor/test_liveness.py tests/test_cli_alert.py -q
ruff check heber/health_monitor/feed_registry.py heber/health_monitor/checks/liveness.py heber/config.py heber/cli.py tests/health_monitor/test_feed_registry.py tests/health_monitor/test_liveness.py tests/test_cli_alert.py
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
cd /Users/jacobmcmillan/Empire/Heber
git add heber/health_monitor/feed_registry.py heber/config.py heber/cli.py tests/health_monitor/test_feed_registry.py tests/health_monitor/test_liveness.py tests/test_cli_alert.py
git commit -m "fix: reduce false critical feed alerts"
```

---

### Task 6: Make Alerting Backlog-Aware

**Owner:** Subagent C.

**Files:**
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/config.py`
- Create: `/Users/jacobmcmillan/Empire/Heber/heber/health_monitor/checks/backlog.py`
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/health_monitor/checks/stream_health.py`
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/ops/dataflow_health.py`
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/cli.py`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/health_monitor/test_backlog.py`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/health_monitor/test_stream_health.py`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/test_dataflow_health.py`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/test_cli_alert.py`

- [ ] **Step 1: Write failing backlog check tests**

Create `/Users/jacobmcmillan/Empire/Heber/tests/health_monitor/test_backlog.py`:

```python
from heber.health_monitor.checks.backlog import classify_stream_backlog, should_suppress_feed_liveness


def test_should_suppress_feed_liveness_when_lag_at_stream_cap():
    assert should_suppress_feed_liveness(lag=300000, stream_len=300003, min_lag=5000, ratio=0.8) is True


def test_should_not_suppress_feed_liveness_when_lag_is_small():
    assert should_suppress_feed_liveness(lag=100, stream_len=300003, min_lag=5000, ratio=0.8) is False


def test_classify_stream_backlog_marks_near_cap_as_critical():
    status, lag_ratio = classify_stream_backlog(
        lag=295000,
        stream_len=300003,
        min_lag=5000,
        warn_ratio=0.5,
        critical_ratio=0.8,
    )

    assert status == "critical"
    assert lag_ratio == 0.983


def test_classify_stream_backlog_ignores_high_ratio_below_floor():
    status, lag_ratio = classify_stream_backlog(
        lag=200,
        stream_len=210,
        min_lag=5000,
        warn_ratio=0.5,
        critical_ratio=0.8,
    )

    assert status == "ok"
    assert lag_ratio == 0.952
```

Run:

```bash
cd /Users/jacobmcmillan/Empire/Heber
uv run pytest tests/health_monitor/test_backlog.py -q
```

Expected: fail because module does not exist.

- [ ] **Step 2: Implement pure backlog decision helper**

Create `/Users/jacobmcmillan/Empire/Heber/heber/health_monitor/checks/backlog.py`:

```python
"""Backlog guards for feed liveness alerting."""

from __future__ import annotations

from typing import Literal


BacklogStatus = Literal["ok", "warn", "critical", "unknown"]


def classify_stream_backlog(
    *,
    lag: int | None,
    stream_len: int | None,
    min_lag: int,
    warn_ratio: float,
    critical_ratio: float,
) -> tuple[BacklogStatus, float | None]:
    if lag is None or stream_len is None or stream_len <= 0:
        return "unknown", None
    ratio = round(lag / stream_len, 3)
    if lag < min_lag:
        return "ok", ratio
    if ratio >= critical_ratio:
        return "critical", ratio
    if ratio >= warn_ratio:
        return "warn", ratio
    return "ok", ratio


def should_suppress_feed_liveness(
    *,
    lag: int | None,
    stream_len: int | None,
    min_lag: int,
    ratio: float,
) -> bool:
    if lag is None or stream_len is None or stream_len <= 0:
        return False
    if lag < min_lag:
        return False
    return (lag / stream_len) >= ratio
```

- [ ] **Step 3: Add settings**

In `/Users/jacobmcmillan/Empire/Heber/heber/config.py`, add:

```python
alert_suppress_feed_liveness_on_backlog: bool = Field(
    default=True,
    description="Suppress per-feed liveness pages when Redis writer lag shows storage is catching up",
)
alert_backlog_min_lag: int = Field(default=5000, ge=0)
alert_backlog_warn_ratio: float = Field(default=0.5, ge=0.0, le=1.0)
alert_backlog_critical_ratio: float = Field(default=0.8, ge=0.0, le=1.0)
```

- [ ] **Step 4: Add stream-health coverage for Redis lag ratio**

In `/Users/jacobmcmillan/Empire/Heber/tests/health_monitor/test_stream_health.py`, add:

```python
@pytest.mark.unit
@patch("heber.health_monitor.checks.stream_health._now_et", return_value=MARKET_OPEN_DT)
async def test_consumer_lag_critical_when_redis_lag_near_stream_cap(_mock_now: MagicMock) -> None:
    redis = _healthy_redis()
    redis.xinfo_stream = AsyncMock(return_value={"length": 300003})
    redis.xinfo_groups = AsyncMock(
        return_value=[{"name": "heber-writers", "pending": 2000, "lag": 300000, "consumers": 1}]
    )
    ctx = _make_ctx(redis)

    results = await run_stream_health_checks(ctx)

    lag = next(r for r in results if r.check_name == "consumer_lag")
    assert lag.status == Status.FAIL
    assert lag.severity == Severity.P0_CRITICAL
    assert lag.details["lag"] == 300000
    assert lag.details["stream_len"] == 300003
    assert lag.details["lag_ratio"] == 1.0
```

Run:

```bash
cd /Users/jacobmcmillan/Empire/Heber
uv run pytest tests/health_monitor/test_stream_health.py::test_consumer_lag_critical_when_redis_lag_near_stream_cap -q
```

Expected: fail because `stream_health.py` only evaluates `pending`, not Redis group `lag`.

- [ ] **Step 5: Use the shared backlog helper in stream-health and dataflow-health**

In `/Users/jacobmcmillan/Empire/Heber/heber/health_monitor/checks/stream_health.py`, import:

```python
from heber.health_monitor.checks.backlog import classify_stream_backlog
```

When building `consumer_lag`, read both `pending` and `lag` from `group_info`, and read `stream_len` from `stream_info["length"]`:

```python
pending = int(group_info.get("pending") or 0)
lag = group_info.get("lag")
lag = int(lag) if lag is not None else None
stream_len = int(stream_info.get("length") or 0)
backlog_status, lag_ratio = classify_stream_backlog(
    lag=lag,
    stream_len=stream_len,
    min_lag=ctx.settings.alert_backlog_min_lag,
    warn_ratio=ctx.settings.alert_backlog_warn_ratio,
    critical_ratio=ctx.settings.alert_backlog_critical_ratio,
)
```

Escalate `consumer_lag` to P0/FAIL when `backlog_status == "critical"`, P1/WARN when `backlog_status == "warn"` or `pending >= PENDING_WARN`, and PASS only when both pending and lag are healthy. Include:

```python
details={"group": target_group, "pending": pending, "lag": lag, "stream_len": stream_len, "lag_ratio": lag_ratio}
```

In `/Users/jacobmcmillan/Empire/Heber/heber/ops/dataflow_health.py`, replace the duplicated ratio logic in `_build_consumer_lag_check` with `classify_stream_backlog(...)`, preserving the existing public report shape.

- [ ] **Step 6: Wire backlog guard into `alert-check`**

In `/Users/jacobmcmillan/Empire/Heber/heber/cli.py`, before running liveness checks:

```python
def _current_writer_lag(settings) -> tuple[int | None, int | None]:
    import redis

    client = redis.from_url(settings.redis_url)
    try:
        info = client.xinfo_groups(settings.redis_stream_name)
        stream_len = client.xlen(settings.redis_stream_name)
    finally:
        client.close()

    for group in info:
        name = group.get("name")
        if isinstance(name, bytes):
            name = name.decode()
        if name == settings.redis_consumer_group:
            return int(group.get("lag") or 0), int(stream_len)
    return None, int(stream_len)
```

If `should_suppress_feed_liveness(...)` returns `True`, print one storage-backlog warning and return nonzero without dispatching per-feed darkpool/flow pages:

```python
print(
    f"alert-check: storage backlog suppressing feed liveness (lag={lag}, stream_len={stream_len})",
    file=sys.stderr,
)
return 2
```

This prevents `darkpool is down` pages when the real problem is `Heber is behind`.

- [ ] **Step 7: Verify**

```bash
cd /Users/jacobmcmillan/Empire/Heber
uv run pytest tests/health_monitor/test_backlog.py tests/health_monitor/test_stream_health.py tests/test_dataflow_health.py tests/test_cli_alert.py -q
ruff check heber/health_monitor/checks/backlog.py heber/health_monitor/checks/stream_health.py heber/ops/dataflow_health.py heber/cli.py heber/config.py tests/health_monitor/test_backlog.py tests/health_monitor/test_stream_health.py tests/test_dataflow_health.py tests/test_cli_alert.py
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
cd /Users/jacobmcmillan/Empire/Heber
git add heber/health_monitor/checks/backlog.py heber/health_monitor/checks/stream_health.py heber/ops/dataflow_health.py heber/cli.py heber/config.py tests/health_monitor/test_backlog.py tests/health_monitor/test_stream_health.py tests/test_dataflow_health.py tests/test_cli_alert.py
git commit -m "fix: make feed alerts backlog aware"
```

---

### Task 7: Drain Heber Pending Backlog Before Reading New Messages

**Owner:** Subagent C.

**Files:**
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/writer/consumer.py`
- Modify: `/Users/jacobmcmillan/Empire/Heber/heber/config.py`
- Modify only if env tuning is needed: `/Users/jacobmcmillan/Empire/Heber/docker-compose.yml`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/test_consumer_concurrency.py`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/test_writer_consumer_reliability.py`
- Test: `/Users/jacobmcmillan/Empire/Heber/tests/test_dataflow_health.py`

- [ ] **Step 1: Characterize current bottleneck**

Run:

```bash
cd /Users/jacobmcmillan/Empire/Heber
docker logs --since 30m heber-consumer | rg 'batch_processed|Flushed Bronze partition|Flushed Silver partition|Skipping ACK|Failed to process event'
docker exec data-gateway-redis redis-cli XINFO CONSUMERS heber:events heber-writers
```

Record:

- Current `messages_per_second`
- Whether ACKs stop because `_flush_layers()` fails
- Whether processing is dominated by one feed
- Whether pending messages are idle for more than `HEBER_REDIS_CLAIM_IDLE_MS`

- [ ] **Step 2: Write failing tests for repeated pending recovery and feed interleaving**

In `/Users/jacobmcmillan/Empire/Heber/tests/test_writer_consumer_reliability.py`, add:

```python
def test_interleave_stream_messages_prioritizes_live_feeds():
    consumer = EventConsumer()
    messages = [
        ("1-0", {"data": '{"feed":"greek_exposure"}'}),
        ("2-0", {"data": '{"feed":"darkpool"}'}),
        ("3-0", {"data": '{"feed":"flow_alerts"}'}),
        ("4-0", {"data": '{"feed":"bars"}'}),
    ]

    ordered = consumer._interleave_stream_messages(messages)

    assert [message_id for message_id, _ in ordered] == ["3-0", "2-0", "4-0", "1-0"]
```

Add an async test:

```python
@pytest.mark.asyncio
async def test_consume_iteration_recovers_pending_before_new_reads(monkeypatch):
    consumer = EventConsumer()
    redis = AsyncMock()
    redis.xreadgroup = AsyncMock(return_value=[])
    consumer.redis = redis
    consumer._recover_pending_cycle = AsyncMock(return_value=3)
    consumer._flush_layers = MagicMock(return_value=True)

    await consumer._consume_iteration()

    consumer._recover_pending_cycle.assert_awaited_once()
    redis.xreadgroup.assert_awaited_once()
```

Run:

```bash
cd /Users/jacobmcmillan/Empire/Heber
uv run pytest tests/test_writer_consumer_reliability.py::test_interleave_stream_messages_prioritizes_live_feeds tests/test_writer_consumer_reliability.py::test_consume_iteration_recovers_pending_before_new_reads -q
```

Expected: fail because `_interleave_stream_messages` and `_recover_pending_cycle` do not exist.

- [ ] **Step 3: Add bounded pending recovery settings**

In `/Users/jacobmcmillan/Empire/Heber/heber/config.py`, add near Redis consumer settings:

```python
redis_pending_recovery_batches_per_iteration: int = Field(
    default=5,
    ge=0,
    le=100,
    description="Maximum pending batches to claim and process before each live read iteration",
)
redis_live_feed_priority_order: list[str] = Field(
    default=["flow_alerts", "darkpool", "bars", "trades"],
    description="Feed ordering used inside one mixed Redis batch so live feeds are processed first",
)
```

- [ ] **Step 4: Implement bounded pending recovery and feed interleaving**

In `/Users/jacobmcmillan/Empire/Heber/heber/writer/consumer.py`, add:

```python
async def _recover_pending_cycle(self) -> int:
    recovered_total = 0
    max_batches = settings.redis_pending_recovery_batches_per_iteration
    for _ in range(max_batches):
        recovered = await self._recover_pending_messages()
        recovered_total += recovered
        if recovered == 0:
            break
    return recovered_total
```

Add a helper in `heber/writer/consumer.py`:

```python
def _interleave_stream_messages(self, stream_messages: list[tuple[Any, dict]]) -> list[tuple[Any, dict]]:
    priority = {feed: index for index, feed in enumerate(settings.redis_live_feed_priority_order)}
    fallback = len(priority)
    return sorted(
        stream_messages,
        key=lambda item: (priority.get(self._extract_feed_from_message(item[1]), fallback), self._decode_string(item[0])),
    )


def _count_feeds_in_batch(self, stream_messages: list[tuple[Any, dict]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for _, message_data in stream_messages:
        feed = self._extract_feed_from_message(message_data)
        counts[feed] = counts.get(feed, 0) + 1
    return counts
```

Include `feeds=self._count_feeds_in_batch(stream_messages)` in the `batch_processed` log.

Add a test in `tests/test_writer_consumer_reliability.py`:

```python
def test_count_feeds_in_batch_reports_feed_mix():
    consumer = EventConsumer()
    messages = [
        ("1-0", {"data": '{"feed":"darkpool"}'}),
        ("2-0", {"data": '{"feed":"greek_exposure"}'}),
        ("3-0", {"data": '{"feed":"greek_exposure"}'}),
    ]

    assert consumer._count_feeds_in_batch(messages) == {"darkpool": 1, "greek_exposure": 2}
```

At the start of `_consume_iteration`, before `xreadgroup(">")`, run:

```python
recovered = await self._recover_pending_cycle()
if recovered:
    logger.info("pending_recovery_cycle_completed", recovered=recovered)
```

Before calling `_process_stream_messages`, order the current stream batch:

```python
ordered_messages = self._interleave_stream_messages(stream_messages)
record_batch_processed(feed="mixed", batch_size=len(ordered_messages))
ack_ids, stream_failed_ids = await self._process_stream_messages(ordered_messages)
```

This improves catch-up behavior without changing ACK safety: messages are still ACKed only after flush succeeds.

- [ ] **Step 5: Tune env only after the code path is verified**

If CPU/memory are low and lag is still falling too slowly, test these settings in `/Users/jacobmcmillan/Empire/Heber/docker-compose.yml`:

```yaml
- HEBER_REDIS_READ_BATCH_SIZE=1000
- HEBER_REDIS_PROCESS_CONCURRENCY=20
- HEBER_REDIS_PENDING_RECOVERY_BATCHES_PER_ITERATION=10
```

Run the focused tests before committing Docker config:

```bash
uv run pytest tests/test_consumer_concurrency.py tests/test_writer_consumer_reliability.py -q
```

- [ ] **Step 6: Verify**

```bash
cd /Users/jacobmcmillan/Empire/Heber
uv run pytest tests/test_consumer_concurrency.py tests/test_writer_consumer_reliability.py tests/test_dataflow_health.py -q
ruff check heber/writer/consumer.py heber/config.py tests/test_writer_consumer_reliability.py
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/jacobmcmillan/Empire/Heber
git add heber/writer/consumer.py heber/config.py docker-compose.yml tests/test_writer_consumer_reliability.py
git commit -m "fix: drain pending writer backlog before live reads"
```

---

### Task 8: Postgres Health Follow-Up

**Owner:** Subagent A first; code owner only if a reproducible config bug is found.

**Files:**
- Read first:
  - `/Users/jacobmcmillan/Empire/Heber/docker-compose.yml`
  - `/Users/jacobmcmillan/Empire/Heber/heber/config.py`
- Modify only if confirmed:
  - `/Users/jacobmcmillan/Empire/Heber/docker-compose.yml`
  - `/Users/jacobmcmillan/Empire/Heber/tests/test_dataflow_health_compose_contract.py`

- [ ] **Step 1: Recheck Postgres after stream pressure is reduced**

Run:

```bash
docker inspect heber-postgres --format '{{json .State.Health}}'
docker exec heber-postgres pg_isready -h 127.0.0.1 -U heber -d heber_catalog
docker logs --since 20m --tail 120 heber-postgres
```

Expected:

- If Postgres responds after stream pressure drops, treat previous unhealthy state as load/recovery related.
- If Postgres still does not respond, investigate DB/container separately before declaring Heber healthy.

- [ ] **Step 2: If healthcheck differs from real readiness, add a compose contract test**

In `/Users/jacobmcmillan/Empire/Heber/tests/test_dataflow_health_compose_contract.py`, add a test that asserts the Postgres healthcheck targets `heber_catalog` and the correct user.

- [ ] **Step 3: Do not change Postgres data files**

Do not delete, move, or reinitialize `/Volumes/heber/postgres/data`.

---

### Task 9: Rebuild, Recreate, and Live Verification

**Owner:** Lead integrator.

**Files:**
- Modify: `CHANGELOG.md` in each repo touched.

- [ ] **Step 1: Update changelogs**

In Data Gateway `CHANGELOG.md`, add an entry covering:

- Persistent UW EOD state
- Heavy REST sink gating
- Sink drop diagnostics
- Heber Watch client permission update, if done in Data Gateway

In Heber `CHANGELOG.md`, add an entry covering:

- Watch auth preflight behavior
- Darkpool alert window
- Alert-check timeout
- Backlog-aware alert suppression
- Consumer diagnostics/tuning if changed

- [ ] **Step 2: Run focused tests**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest tests/test_uw_poller.py tests/test_rest_sink_gating.py tests/test_data_sink.py tests/test_auth.py -q
ruff check gateway/core/uw_eod_state.py gateway/core/uw_poller.py gateway/api/middleware/envelope.py gateway/core/data_sink.py gateway/config.py tests/test_uw_poller.py tests/test_rest_sink_gating.py tests/test_data_sink.py tests/test_auth.py

cd /Users/jacobmcmillan/Empire/Heber
uv run pytest tests/health_monitor/test_feed_registry.py tests/health_monitor/test_liveness.py tests/health_monitor/test_backlog.py tests/test_watch_service_auth_preflight.py tests/test_watch_gateway_key_contract.py tests/test_consumer_concurrency.py tests/test_writer_consumer_reliability.py tests/test_dataflow_health.py -q
ruff check heber/health_monitor heber/watch heber/writer heber/config.py heber/cli.py tests/health_monitor tests/test_watch_service_auth_preflight.py tests/test_watch_gateway_key_contract.py tests/test_writer_consumer_reliability.py
```

- [ ] **Step 3: Rebuild affected Docker containers**

```bash
cd /Users/jacobmcmillan/Empire
docker build -f Data-Gateway/Dockerfile -t data-gateway .

cd /Users/jacobmcmillan/Empire/Data-Gateway
docker compose up -d --build gateway

cd /Users/jacobmcmillan/Empire/Heber
docker compose up -d --build consumer watch dataflow-health health-monitor
```

- [ ] **Step 4: Verify live behavior for 15 minutes**

Run immediately and again after 15 minutes:

```bash
docker exec data-gateway-redis redis-cli XINFO GROUPS heber:events
docker exec data-gateway-redis redis-cli XLEN heber:events:dlq
tail -120 /Users/jacobmcmillan/Empire/Data-Gateway/logs/data-gateway_errors.log | rg 'data_sink_producer_timeout_drop|redis_sink|auth_failed_invalid_key' || true
tail -120 /Users/jacobmcmillan/Empire/Heber/logs/native/alert-check.out.log
docker logs --since 15m heber-watch | rg 'gateway_auth_preflight|Feature enrichment request failed|Watch service stopped' || true
docker logs --since 15m heber-dataflow-health | tail -20
```

Expected:

- `heber-writers lag` decreases or stays far below the stream cap.
- `data_sink_producer_timeout_drop` does not recur.
- `heber-watch` does not emit Gateway auth failures.
- Darkpool liveness passes during the configured active window.
- Flow-alert alerts match calibrated floor, not stale `351`.
- `alert-check` exits before 300 seconds.

- [ ] **Step 5: Final commits**

Commit any remaining changelog/doc updates:

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
git add CHANGELOG.md docs/superpowers/plans/2026-06-12-gateway-heber-stream-recovery.md
git commit -m "docs: plan Gateway Heber stream recovery"

cd /Users/jacobmcmillan/Empire/Heber
git add CHANGELOG.md
git commit -m "docs: record Heber stream recovery changes"
```

---

## Execution Order

1. Task 0 first. Do not edit code until current stream lag and stuck services are captured.
2. Task 4 can run in parallel with Task 1 and Task 2 because auth/config changes are independent.
3. Task 1 and Task 2 should land before retesting runtime, because they reduce new stream pressure.
4. Task 5 and Task 6 can run in parallel after Task 0 because they only touch Heber alerting.
5. Task 7 should wait until Task 2 is in place unless current lag does not improve.
6. Task 8 should wait until after stream pressure drops, unless Postgres blocks Heber startup.
7. Task 9 is integration only. No worker should rebuild shared containers independently without lead coordination.

## Self-Review

- Spec coverage: covers Gateway drops, EOD duplicate runs, REST heavy-feed flood, Heber lag, alert noise, Watch auth failures, Postgres follow-up, Docker rebuild, tests, and changelogs.
- Placeholder scan: no `TBD`, no unowned TODOs, and no secret values included.
- Type consistency: new Gateway state class is `UwEodStateStore`; new Heber backlog helpers are `classify_stream_backlog` and `should_suppress_feed_liveness`.
