from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from gateway.core.redis_sink import (
    BATCH_CHUNK_SIZE,
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
async def test_publish_batch_concurrent_chunks() -> None:
    """Multiple batch chunks should execute concurrently, not sequentially."""
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

    # Create enough messages for 3 chunks (3 * BATCH_CHUNK_SIZE)
    chunk_count = 3
    messages = [("gateway.stream.bars", {"i": i}) for i in range(BATCH_CHUNK_SIZE * chunk_count)]

    start = time.perf_counter()
    result = await sink.publish_batch(messages)
    elapsed = time.perf_counter() - start

    assert result == BATCH_CHUNK_SIZE * chunk_count
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
