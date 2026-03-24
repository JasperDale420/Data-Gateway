# Redis Resilience Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Prevent silent data loss when `data-gateway-redis` restarts by making Redis ephemeral, buffering events through circuit breaker OPEN state, and surfacing sink failures in logs and health endpoints.

**Architecture:** Three independent changes to Data-Gateway: (1) docker-compose config for ephemeral Redis, (2) circuit-breaker-aware buffering in DataSinkRegistry, (3) WARNING-level alerting in poller and sink. All changes are additive — no breaking changes to existing APIs.

**Tech Stack:** Python 3.12, asyncio, FastAPI, Redis 7, Docker Compose, pytest

---

### Task 1: Make Redis Ephemeral

**Files:**
- Modify: `docker-compose.yml:47-65`

**Step 1: Update docker-compose.yml**

Replace the current Redis service definition:

```yaml
  # Redis for Heber data sink
  redis:
    image: redis:7-alpine
    container_name: data-gateway-redis
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes
    restart: unless-stopped
    healthcheck:
      test: [ "CMD-SHELL", "redis-cli ping | grep -q PONG" ]
      interval: 10s
      timeout: 5s
      retries: 30
      start_period: 300s

volumes:
  redis_data:
```

With:

```yaml
  # Redis for Heber data sink (ephemeral — no persistence)
  redis:
    image: redis:7-alpine
    container_name: data-gateway-redis
    ports:
      - "6379:6379"
    command: redis-server --save "" --appendonly no --maxmemory 512mb --maxmemory-policy allkeys-lru
    restart: unless-stopped
    healthcheck:
      test: [ "CMD-SHELL", "redis-cli ping | grep -q PONG" ]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 10s
```

Key changes:
- `--save ""` disables RDB snapshots
- `--appendonly no` disables AOF
- `--maxmemory 512mb --maxmemory-policy allkeys-lru` caps memory and evicts LRU keys
- Removed `redis_data` volume (no persistence needed)
- Reduced `retries: 30` → `5` and `start_period: 300s` → `10s` (ephemeral Redis starts in <1s)

**Step 2: Verify locally**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
docker compose down redis && docker compose up -d redis
docker logs data-gateway-redis 2>&1 | tail -5
# Expected: "Ready to accept connections" within 1-2 seconds, no "Loading RDB" or "Loading AOF"
docker exec data-gateway-redis redis-cli CONFIG GET save
# Expected: save ""
docker exec data-gateway-redis redis-cli CONFIG GET maxmemory
# Expected: maxmemory 536870912
```

**Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "fix: make data-gateway-redis ephemeral to eliminate 15s restart penalty"
```

---

### Task 2: Buffer Events Through Circuit Breaker OPEN State

**Files:**
- Modify: `gateway/core/data_sink.py:168-183` (publish_all circuit check)
- Modify: `gateway/core/data_sink.py:225-234` (publish_all_batch circuit check)
- Modify: `gateway/core/data_sink.py:19` (DataSink ABC — add buffer_event method)
- Test: `tests/test_data_sink.py`

**Step 1: Write the failing test**

Add to `tests/test_data_sink.py`:

```python
class _BufferingSink(_TrackingSink):
    """Sink that supports buffering (like RedisStreamsSink)."""

    def __init__(self, sink_name: str = "buffering") -> None:
        super().__init__(sink_name)
        self.buffered: list[tuple[str, Any]] = []

    def buffer_event(self, topic: str, data: dict[str, Any] | str | bytes) -> None:
        self.buffered.append((topic, data))


class TestCircuitOpenBuffering:
    """Tests that events are buffered (not dropped) when circuit is OPEN."""

    @pytest.mark.asyncio
    async def test_publish_all_buffers_when_circuit_open(self) -> None:
        """When circuit is OPEN and sink supports buffering, events go to buffer."""
        cb_registry = CircuitBreakerRegistry()
        breaker = await cb_registry.get("data_sink:buffering")
        breaker.state = CircuitState.OPEN
        breaker.last_failure_time = 9999999999.0

        sink = _BufferingSink(sink_name="buffering")
        registry = DataSinkRegistry()
        registry.register(sink)

        with patch("gateway.core.data_sink.get_circuit_breaker", new=cb_registry.get):
            await registry.publish_all("heber:events", {"event_id": "test1", "symbol": "AAPL"})

        await asyncio.sleep(0.05)

        # Event should be buffered, not published and not dropped
        assert len(sink.published) == 0
        assert len(sink.buffered) == 1
        assert sink.buffered[0] == ("heber:events", {"event_id": "test1", "symbol": "AAPL"})

    @pytest.mark.asyncio
    async def test_publish_all_drops_when_circuit_open_no_buffer(self) -> None:
        """When circuit is OPEN and sink has no buffer_event, events are dropped (existing behavior)."""
        cb_registry = CircuitBreakerRegistry()
        breaker = await cb_registry.get("data_sink:tracking")
        breaker.state = CircuitState.OPEN
        breaker.last_failure_time = 9999999999.0

        sink = _TrackingSink(sink_name="tracking")
        registry = DataSinkRegistry()
        registry.register(sink)

        with patch("gateway.core.data_sink.get_circuit_breaker", new=cb_registry.get):
            await registry.publish_all("heber:events", {"event_id": "test1"})

        await asyncio.sleep(0.05)
        assert len(sink.published) == 0

    @pytest.mark.asyncio
    async def test_publish_all_batch_buffers_when_circuit_open(self) -> None:
        """Batch publish buffers events when circuit is OPEN."""
        cb_registry = CircuitBreakerRegistry()
        breaker = await cb_registry.get("data_sink:buffering")
        breaker.state = CircuitState.OPEN
        breaker.last_failure_time = 9999999999.0

        sink = _BufferingSink(sink_name="buffering")
        registry = DataSinkRegistry()
        registry.register(sink)

        messages = [
            ("heber:events", {"event_id": "e1"}),
            ("heber:events", {"event_id": "e2"}),
        ]

        with patch("gateway.core.data_sink.get_circuit_breaker", new=cb_registry.get):
            result = await registry.publish_all_batch(messages)

        assert result == 0
        assert len(sink.buffered) == 2
```

**Step 2: Run tests to verify they fail**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest tests/test_data_sink.py::TestCircuitOpenBuffering -v
```

Expected: FAIL — `_BufferingSink` and `TestCircuitOpenBuffering` don't exist yet (add the test class), then `buffer_event` not called because `publish_all` drops on OPEN.

**Step 3: Implement the buffering**

In `gateway/core/data_sink.py`, modify the `publish_all()` method. Replace the circuit OPEN block (around lines 168-183):

```python
            breaker = await get_circuit_breaker(f"data_sink:{sink.name}")
            if breaker.state == CircuitState.OPEN:
                logger.debug(
                    "data_sink_circuit_open_skip",
                    sink=sink.name,
                    topic=topic,
                )
                if not sink.record_publish_metrics:
                    record_sink_publish(sink=sink.name, topic=topic, success=False)
                continue
```

With:

```python
            breaker = await get_circuit_breaker(f"data_sink:{sink.name}")
            if breaker.state == CircuitState.OPEN:
                if hasattr(sink, "buffer_event"):
                    sink.buffer_event(topic, data)
                    logger.debug(
                        "data_sink_circuit_open_buffered",
                        sink=sink.name,
                        topic=topic,
                    )
                else:
                    logger.debug(
                        "data_sink_circuit_open_skip",
                        sink=sink.name,
                        topic=topic,
                    )
                if not sink.record_publish_metrics:
                    record_sink_publish(sink=sink.name, topic=topic, success=False)
                continue
```

In the same file, modify `publish_all_batch()`. Replace the circuit OPEN block (around lines 225-234):

```python
                breaker = await get_circuit_breaker(f"data_sink:{sink.name}")
                if breaker.state == CircuitState.OPEN:
                    logger.warning(
                        "data_sink_batch_circuit_open",
                        sink=sink.name,
                        count=len(messages),
                    )
                    continue
```

With:

```python
                breaker = await get_circuit_breaker(f"data_sink:{sink.name}")
                if breaker.state == CircuitState.OPEN:
                    if hasattr(sink, "buffer_event"):
                        for msg_topic, msg_data in messages:
                            sink.buffer_event(msg_topic, msg_data)
                        logger.warning(
                            "data_sink_batch_circuit_open_buffered",
                            sink=sink.name,
                            count=len(messages),
                        )
                    else:
                        logger.warning(
                            "data_sink_batch_circuit_open",
                            sink=sink.name,
                            count=len(messages),
                        )
                    continue
```

Now add `buffer_event` to `RedisStreamsSink` in `gateway/core/redis_sink.py`. Add this method after `_buffer_failed_event` (after line 237):

```python
    def buffer_event(self, topic: str, data: dict[str, Any] | str | bytes) -> None:
        """Buffer an event from the registry when circuit breaker is OPEN.

        Serializes the data and delegates to _buffer_failed_event so it
        participates in the same drain logic on reconnect.
        """
        if isinstance(data, str):
            payload = data.encode()
        elif isinstance(data, bytes):
            payload = data
        else:
            payload = orjson.dumps(data, default=str)
        self._buffer_failed_event(topic, payload)
```

**Step 4: Run tests to verify they pass**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest tests/test_data_sink.py -v
```

Expected: All tests pass, including the new `TestCircuitOpenBuffering` tests.

**Step 5: Run full test suite to check for regressions**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest tests/test_data_sink.py tests/test_redis_sink.py tests/test_uw_poller.py -v
```

Expected: All pass.

**Step 6: Commit**

```bash
git add gateway/core/data_sink.py gateway/core/redis_sink.py tests/test_data_sink.py
git commit -m "fix: buffer events through circuit breaker OPEN state instead of dropping"
```

---

### Task 3: Promote Poller No-Sink Log to WARNING

**Files:**
- Modify: `gateway/core/uw_poller.py:444`
- Modify: `gateway/core/uw_poller.py:349-376` (get_runtime_snapshot)
- Test: `tests/test_uw_poller.py`

**Step 1: Write the failing test**

Add to `tests/test_uw_poller.py`:

```python
@pytest.mark.asyncio
async def test_runtime_snapshot_includes_sink_available() -> None:
    """get_runtime_snapshot should include sink_available field."""
    poller = UWPoller()
    snapshot = poller.get_runtime_snapshot()
    assert "sink_available" in snapshot
    assert snapshot["sink_available"] is False  # No sink configured by default
```

**Step 2: Run test to verify it fails**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest tests/test_uw_poller.py::test_runtime_snapshot_includes_sink_available -v
```

Expected: FAIL — `sink_available` not in snapshot.

**Step 3: Implement the changes**

In `gateway/core/uw_poller.py`, change line 444 from:

```python
            logger.debug("uw_poller_no_sink")
```

To:

```python
            logger.warning("uw_poller_no_sink")
```

In the same file, update `get_runtime_snapshot()` (around line 349). Add `sink_available` to the returned dict. Add after the `"running"` key:

```python
    def get_runtime_snapshot(self) -> dict[str, Any]:
        """Return lightweight runtime/tuning telemetry for admin surfaces."""
        from gateway.core.globals import get_sink_registry

        sink_registry = get_sink_registry()
        return {
            "running": self._running,
            "sink_available": sink_registry is not None,
            "enabled": True,
            ...  # rest unchanged
```

**Step 4: Run tests to verify they pass**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest tests/test_uw_poller.py -v
```

Expected: All pass.

**Step 5: Commit**

```bash
git add gateway/core/uw_poller.py tests/test_uw_poller.py
git commit -m "fix: promote uw_poller_no_sink to WARNING, add sink_available to snapshot"
```

---

### Task 4: Add Circuit Breaker State Change Logging in Redis Sink

**Files:**
- Modify: `gateway/core/redis_sink.py` (publish method, around line 400)
- Test: `tests/test_redis_sink.py`

**Step 1: Write the failing test**

Add to `tests/test_redis_sink.py` (adapt to existing test patterns in that file):

```python
@pytest.mark.asyncio
async def test_buffer_event_delegates_to_failed_buffer() -> None:
    """buffer_event() should serialize and delegate to _buffer_failed_event."""
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    sink.buffer_event("heber:events", {"event_id": "test1", "symbol": "AAPL"})

    assert len(sink._failed_buffer) == 1
    topic, payload = sink._failed_buffer[0]
    assert topic == "heber:events"
    assert b"test1" in payload
    assert b"AAPL" in payload
    assert sink._buffer_stats["buffered"] == 1
```

**Step 2: Run test to verify it fails**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest tests/test_redis_sink.py::test_buffer_event_delegates_to_failed_buffer -v
```

Expected: FAIL — `buffer_event` method doesn't exist yet (if Task 2 not yet done) or PASS if Task 2 is complete.

**Step 3: Verify and commit**

If Task 2 is already complete, this test should pass immediately. Run:

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest tests/test_redis_sink.py -v
```

Expected: All pass.

```bash
git add tests/test_redis_sink.py
git commit -m "test: add coverage for RedisStreamsSink.buffer_event"
```

---

### Task 5: Add Sink Status to Health Endpoint

The `/health/ready` endpoint already checks sink health and reports `"degraded"` when the circuit breaker is OPEN (lines 87-100 of `gateway/api/health.py`). The `/health/status` detailed endpoint does NOT include sink status.

**Files:**
- Modify: `gateway/api/health.py:111-131` (detailed_status endpoint)
- Test: `tests/test_health.py`

**Step 1: Write the failing test**

Add to `tests/test_health.py`:

```python
@pytest.mark.asyncio
async def test_detailed_status_includes_data_sink(client) -> None:
    """GET /health/status should include data_sink component status."""
    response = await client.get("/health/status")
    assert response.status_code == 200
    body = response.json()
    assert "data_sink" in body["components"]
```

**Step 2: Run test to verify it fails**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest tests/test_health.py::test_detailed_status_includes_data_sink -v
```

Expected: FAIL — `data_sink` not in components.

**Step 3: Implement the change**

In `gateway/api/health.py`, update the `detailed_status` function. Add the sink status to the components dict. Replace the function body (lines 111-131):

```python
@router.get("/status")
async def detailed_status(
    cache: InMemoryCache = Depends(get_cache),
    connections: ConnectionManager = Depends(get_connection_manager),
) -> dict[str, Any]:
    """Detailed status with component health and stats."""
    components: dict[str, Any] = {
        "cache": {
            "status": "ok",
            "stats": cache.get_stats_dict(),
        },
        "connections": {
            "status": "ok",
            "stats": connections.get_stats(),
        },
    }

    # Include data sink health if configured
    sink_registry = get_sink_registry()
    if sink_registry:
        try:
            sink_results = await sink_registry.health_check_all()
            all_healthy = all(sink_results.values())
            components["data_sink"] = {
                "status": "ok" if all_healthy else "degraded",
                "sinks": {name: "ok" if healthy else "degraded" for name, healthy in sink_results.items()},
            }
        except Exception:
            components["data_sink"] = {"status": "degraded"}

    return {
        "status": "ok",
        "version": __version__,
        "timestamp": datetime.now(UTC).isoformat(),
        "components": components,
    }
```

**Step 4: Run tests to verify they pass**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest tests/test_health.py -v
```

Expected: All pass.

**Step 5: Commit**

```bash
git add gateway/api/health.py tests/test_health.py
git commit -m "feat: add data_sink status to /health/status endpoint"
```

---

### Task 6: Verify End-to-End and Update Changelog

**Files:**
- Modify: `CHANGELOG.md`

**Step 1: Run full test suite**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
uv run pytest -v
```

Expected: All tests pass.

**Step 2: Lint and format**

```bash
cd /Users/jacobmcmillan/Empire/Data-Gateway
ruff check . && ruff format --check .
```

Expected: No violations.

**Step 3: Update CHANGELOG.md**

Add under `## [Unreleased]`:

```markdown
### Fixed
- Made `data-gateway-redis` ephemeral (no AOF/RDB) to eliminate 15-second restart penalty that caused flow alert data loss
- Events are now buffered through circuit breaker OPEN state instead of being silently dropped
- Promoted `uw_poller_no_sink` log from DEBUG to WARNING for visibility when Redis sink is unavailable

### Added
- `sink_available` field in UW poller runtime snapshot
- `data_sink` component status in `/health/status` endpoint for monitoring integration
```

**Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: update changelog for Redis resilience improvements"
```
