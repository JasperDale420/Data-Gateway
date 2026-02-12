from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from gateway.core.redis_sink import RedisStreamsSink


class _LoadingRedisClient:
    async def xadd(self, *args: Any, **kwargs: Any) -> str:
        raise RuntimeError("Redis is loading the dataset in memory")

    async def ping(self) -> bool:
        raise RuntimeError("Redis is loading the dataset in memory")

    async def close(self) -> None:
        return None


class _HealthyRedisClient:
    def __init__(self) -> None:
        self.messages: list[tuple[Any, Any]] = []

    async def xadd(self, *args: Any, **kwargs: Any) -> str:
        self.messages.append((args, kwargs))
        return "1-0"

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        return None


class _SlowRedisClient:
    async def xadd(self, *args: Any, **kwargs: Any) -> str:
        await asyncio.sleep(0.08)
        return "1-0"

    async def close(self) -> None:
        return None


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
