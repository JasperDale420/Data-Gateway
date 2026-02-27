from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from gateway.core.redis_sink import (
    RedisStreamsSink,
)

# ── Mock Redis Clients ────────────────────────────────────────────────


class _LoadingRedisClient:
    async def xadd(self, *args: Any, **kwargs: Any) -> bytes:
        raise RuntimeError("Redis is loading the dataset in memory")

    async def ping(self) -> bool:
        raise RuntimeError("Redis is loading the dataset in memory")

    async def close(self) -> None:
        return None


class _HealthyRedisClient:
    def __init__(self) -> None:
        self.messages: list[tuple[Any, Any]] = []

    async def xadd(self, *args: Any, **kwargs: Any) -> bytes:
        self.messages.append((args, kwargs))
        return b"1-0"

    def pipeline(self, transaction: bool = False) -> _HealthyPipeline:
        return _HealthyPipeline()

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        return None


class _HealthyPipeline:
    def __init__(self) -> None:
        self._commands: list[tuple[Any, ...]] = []

    async def __aenter__(self) -> _HealthyPipeline:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    def xadd(self, *args: Any, **kwargs: Any) -> _HealthyPipeline:
        self._commands.append((args, kwargs))
        return self

    async def execute(self) -> list[bytes]:
        return [b"1-0"] * len(self._commands)


class _SlowRedisClient:
    async def xadd(self, *args: Any, **kwargs: Any) -> bytes:
        await asyncio.sleep(0.08)
        return b"1-0"

    async def close(self) -> None:
        return None


class _FailThenSucceedPipeline:
    """Pipeline that fails on first execute, succeeds on second."""

    def __init__(self) -> None:
        self._commands: list[tuple[Any, ...]] = []
        self._call_count = 0

    async def __aenter__(self) -> _FailThenSucceedPipeline:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    def xadd(self, *args: Any, **kwargs: Any) -> _FailThenSucceedPipeline:
        self._commands.append((args, kwargs))
        return self

    async def execute(self) -> list[bytes]:
        self._call_count += 1
        if self._call_count == 1:
            raise TimeoutError("timed out")
        return [b"1-0"] * len(self._commands)


class _FailingPipeline:
    """Pipeline that always fails."""

    def __init__(self) -> None:
        self._commands: list[tuple[Any, ...]] = []

    async def __aenter__(self) -> _FailingPipeline:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    def xadd(self, *args: Any, **kwargs: Any) -> _FailingPipeline:
        self._commands.append((args, kwargs))
        return self

    async def execute(self) -> list[bytes]:
        raise TimeoutError("timed out")


class _TimingPipeline:
    """Pipeline that records execution timing."""

    def __init__(self, delay: float = 0.05) -> None:
        self._commands: list[tuple[Any, ...]] = []
        self._delay = delay
        self.executed_at: float | None = None

    async def __aenter__(self) -> _TimingPipeline:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    def xadd(self, *args: Any, **kwargs: Any) -> _TimingPipeline:
        self._commands.append((args, kwargs))
        return self

    async def execute(self) -> list[bytes]:
        self.executed_at = time.perf_counter()
        await asyncio.sleep(self._delay)
        return [b"1-0"] * len(self._commands)


# ── Connection Reset and Reconnect Tests ──────────────────────────────


@pytest.mark.asyncio
async def test_publish_resets_connection_after_loading_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    sink._redis = _LoadingRedisClient()
    sink._connected = True

    published = await sink.publish("gateway.stream.bars", {"event_id": "evt-1"})

    assert published is False
    assert sink._redis is None

    healthy_client = _HealthyRedisClient()

    async def _fake_ensure_connected() -> None:
        sink._redis = healthy_client
        sink._connected = True

    monkeypatch.setattr(sink, "_ensure_connected", _fake_ensure_connected)

    published_retry = await sink.publish("gateway.stream.bars", {"event_id": "evt-2"})

    assert published_retry is True
    assert len(healthy_client.messages) == 1


@pytest.mark.asyncio
async def test_publish_uses_bounded_operation_timeout() -> None:
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    sink._redis = _SlowRedisClient()
    sink._connected = True
    sink._operation_timeout_seconds = 0.02

    start = time.perf_counter()
    published = await sink.publish("gateway.stream.bars", {"event_id": "evt-1"})
    elapsed = time.perf_counter() - start

    assert published is False
    assert elapsed < 0.06
    assert sink._redis is None


@pytest.mark.asyncio
async def test_health_check_resets_connection_after_loading_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    sink._redis = _LoadingRedisClient()
    sink._connected = True

    healthy = await sink.health_check()

    assert healthy is False
    assert sink._redis is None

    healthy_client = _HealthyRedisClient()

    async def _fake_ensure_connected() -> None:
        sink._redis = healthy_client
        sink._connected = True

    monkeypatch.setattr(sink, "_ensure_connected", _fake_ensure_connected)

    healthy_retry = await sink.health_check()

    assert healthy_retry is True


# ── Connection Pool Tests ─────────────────────────────────────────────


def test_default_pool_size() -> None:
    """Default pool size should be 8."""
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    assert sink._pool_size == 8


def test_custom_pool_size() -> None:
    """Custom pool size is accepted and clamped within bounds."""
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0", pool_size=16)
    assert sink._pool_size == 16

    sink_max = RedisStreamsSink(redis_url="redis://localhost:6379/0", pool_size=100)
    assert sink_max._pool_size == 32  # Clamped to max

    sink_min = RedisStreamsSink(redis_url="redis://localhost:6379/0", pool_size=0)
    assert sink_min._pool_size == 1  # Clamped to min


def test_default_timeout() -> None:
    """Default timeout should be 5.0 seconds."""
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    assert sink._operation_timeout_seconds == 5.0


def test_custom_timeout_clamped() -> None:
    """Timeout should be clamped to minimum 0.5s."""
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0", operation_timeout_seconds=0.1)
    assert sink._operation_timeout_seconds == 0.5


# ── Batch Publishing Tests ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_batch_empty_returns_zero() -> None:
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    sink._redis = _HealthyRedisClient()
    sink._connected = True

    result = await sink.publish_batch([])
    assert result == 0


@pytest.mark.asyncio
async def test_publish_batch_success(monkeypatch: pytest.MonkeyPatch) -> None:
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    client = _HealthyRedisClient()
    sink._redis = client
    sink._connected = True

    messages = [("gateway.stream.bars", {"symbol": f"SYM{i}"}) for i in range(100)]
    result = await sink.publish_batch(messages)
    assert result == 100


@pytest.mark.asyncio
async def test_publish_batch_concurrent_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Multiple batch chunks should execute concurrently, not sequentially."""
    import gateway.core.redis_sink

    monkeypatch.setattr(gateway.core.redis_sink, "BATCH_CHUNK_SIZE", 10)

    pipelines: list[_TimingPipeline] = []
    pipeline_index = 0

    class _ConcurrentClient:
        def pipeline(self, transaction: bool = False) -> _TimingPipeline:
            nonlocal pipeline_index
            p = _TimingPipeline(delay=0.05)
            pipelines.append(p)
            pipeline_index += 1
            return p

        async def close(self) -> None:
            pass

    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    sink._redis = _ConcurrentClient()
    sink._connected = True

    # Create enough messages for 3 chunks
    chunk_count = 3
    messages = [("gateway.stream.bars", {"i": i}) for i in range(10 * chunk_count)]

    start = time.perf_counter()
    result = await sink.publish_batch(messages)
    elapsed = time.perf_counter() - start

    assert result == 10 * chunk_count
    assert len(pipelines) == chunk_count

    # If sequential, would take chunk_count * 0.05 = 0.15s
    # If concurrent, should take ~0.05s (all run in parallel)
    assert elapsed < 0.12, f"Chunks appear sequential, took {elapsed:.3f}s"


@pytest.mark.asyncio
async def test_publish_batch_retries_failed_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed chunk should be retried once."""
    attempt_count = 0

    class _RetryClient:
        def pipeline(self, transaction: bool = False) -> Any:
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                return _FailingPipeline()
            return _HealthyPipeline()

        async def close(self) -> None:
            pass

    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    sink._redis = _RetryClient()
    sink._connected = True

    async def _fake_ensure() -> None:
        sink._redis = _RetryClient()
        sink._connected = True

    monkeypatch.setattr(sink, "_ensure_connected", _fake_ensure)

    messages = [("gateway.stream.bars", {"i": i}) for i in range(10)]
    result = await sink.publish_batch(messages)

    # After retry, should succeed
    assert result == 10


# ── Binary Mode Tests ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_sends_bytes_payload() -> None:
    """Payloads should be sent as bytes, not decoded strings."""
    client = _HealthyRedisClient()
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    sink._redis = client
    sink._connected = True

    await sink.publish("gateway.stream.bars", {"symbol": "AAPL", "close": 150.0})

    assert len(client.messages) == 1
    args, kwargs = client.messages[0]
    # The payload dict should have bytes key
    payload = args[1]  # Second arg to xadd is the payload dict
    assert b"data" in payload
    assert isinstance(payload[b"data"], bytes)


@pytest.mark.asyncio
async def test_publish_string_payload() -> None:
    """String payloads should be encoded to bytes."""
    client = _HealthyRedisClient()
    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    sink._redis = client
    sink._connected = True

    await sink.publish("gateway.stream.bars", '{"symbol": "AAPL"}')

    assert len(client.messages) == 1
    args, _ = client.messages[0]
    payload = args[1]
    assert b"data" in payload
    assert isinstance(payload[b"data"], bytes)


# ── Async Reset and Backoff Tests ─────────────────────────────────────


@pytest.mark.asyncio
async def test_reset_connection_awaits_close() -> None:
    """_reset_connection should await close on the old client, not fire-and-forget."""
    closed = False

    class _TrackingClient:
        async def close(self) -> None:
            nonlocal closed
            closed = True

    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    tracking_client = _TrackingClient()
    sink._redis = tracking_client
    sink._connected = True

    await sink._reset_connection(
        operation="test",
        error=RuntimeError("test error"),
        failed_client=tracking_client,
    )

    assert closed, "_reset_connection should have awaited close on the old client"
    assert sink._redis is None
    assert sink._connected is False


@pytest.mark.asyncio
async def test_reconnect_backoff_prevents_tight_loop() -> None:
    """After a reset, _ensure_connected should respect the backoff cooldown."""
    from gateway.core.redis_sink import INITIAL_BACKOFF_SECONDS

    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    sink._redis = None
    sink._connected = False

    created_count = 0

    def _counting_create() -> Any:
        nonlocal created_count
        created_count += 1
        return _HealthyRedisClient()

    sink._create_client = _counting_create

    # Trigger a reset to set cooldown
    await sink._reset_connection(operation="test", error=RuntimeError("boom"))

    assert sink._reconnect_cooldown_until > 0
    assert sink._backoff_seconds == INITIAL_BACKOFF_SECONDS * 2

    # _ensure_connected should sleep until cooldown expires, then reconnect
    start = time.perf_counter()
    await sink._ensure_connected()
    elapsed = time.perf_counter() - start

    assert created_count == 1
    assert sink._redis is not None
    # Should have waited at least close to INITIAL_BACKOFF_SECONDS
    assert elapsed >= INITIAL_BACKOFF_SECONDS * 0.8, f"Expected backoff ~{INITIAL_BACKOFF_SECONDS}s, got {elapsed:.3f}s"


@pytest.mark.asyncio
async def test_idempotent_reset_skips_when_client_already_replaced() -> None:
    """_reset_connection should no-op if self._redis was already swapped out."""
    closed = False

    class _TrackClose:
        async def close(self) -> None:
            nonlocal closed
            closed = True

    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    old_client = _TrackClose()
    new_client = _HealthyRedisClient()
    sink._redis = new_client
    sink._connected = True

    # Reset referencing the OLD client — should be a no-op
    await sink._reset_connection(
        operation="test",
        error=RuntimeError("stale"),
        failed_client=old_client,
    )

    assert sink._redis is new_client, "Should NOT have reset; client was already replaced"
    assert sink._connected is True
    assert not closed, "Should NOT have closed — the old client was already gone"


@pytest.mark.asyncio
async def test_concurrent_chunk_reset_does_not_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When one chunk fails mid-flight, other concurrent chunks must not crash.

    Simulates the race: chunk A's pipeline.execute() raises, triggering
    _reset_connection (which nulls self._redis).  Chunk B is already past
    _ensure_connected and holds a local client ref, so it should not see
    a NoneType error.
    """
    import gateway.core.redis_sink

    monkeypatch.setattr(gateway.core.redis_sink, "BATCH_CHUNK_SIZE", 5)

    call_count = 0

    class _RacyPipeline:
        def __init__(self) -> None:
            self._cmds: list = []

        async def __aenter__(self) -> _RacyPipeline:
            return self

        async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

        def xadd(self, *a: Any, **kw: Any) -> _RacyPipeline:
            self._cmds.append(1)
            return self

        async def execute(self) -> list[bytes]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First chunk fails
                raise ConnectionError("Too many connections")
            await asyncio.sleep(0.01)
            return [b"1-0"] * len(self._cmds)

    class _RacyClient:
        def pipeline(self, transaction: bool = False) -> _RacyPipeline:
            return _RacyPipeline()

        async def close(self) -> None:
            pass

    sink = RedisStreamsSink(redis_url="redis://localhost:6379/0")
    sink._redis = _RacyClient()
    sink._connected = True

    async def _noop_ensure() -> None:
        if sink._redis is None:
            sink._redis = _RacyClient()
            sink._connected = True

    monkeypatch.setattr(sink, "_ensure_connected", _noop_ensure)

    # 3 chunks of 5 = 15 messages; chunk A fails, chunks B & C should succeed
    messages = [("stream.test", {"i": i}) for i in range(15)]
    result = await sink.publish_batch(messages)

    # At least two chunks should have succeeded (chunk A may recover on retry)
    assert result >= 10, f"Expected at least 10 published, got {result}"
