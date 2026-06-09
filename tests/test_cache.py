"""Tests for in-memory cache."""

from unittest.mock import patch

import pytest

import gateway.core.cache as cache_module
from gateway.core.cache import InMemoryCache, RedisCache
from gateway.core.log_throttle import LogThrottle


@pytest.mark.asyncio
async def test_cache_set_and_get(test_cache):
    """Set and get value from cache."""
    await test_cache.set("key1", "value1")
    assert await test_cache.get("key1") == "value1"


@pytest.mark.asyncio
async def test_cache_miss_returns_none(test_cache):
    """Missing key returns None."""
    assert await test_cache.get("nonexistent") is None


@pytest.mark.asyncio
async def test_cache_delete(test_cache):
    """Delete removes key from cache."""
    await test_cache.set("key1", "value1")
    assert test_cache.delete("key1") is True
    assert await test_cache.get("key1") is None


def test_cache_delete_nonexistent(test_cache):
    """Delete nonexistent key returns False."""
    assert test_cache.delete("nonexistent") is False


@pytest.mark.asyncio
async def test_cache_exists(test_cache):
    """Exists checks key presence."""
    await test_cache.set("key1", "value1")
    assert test_cache.exists("key1") is True
    assert test_cache.exists("nonexistent") is False


@pytest.mark.asyncio
async def test_cache_clear(test_cache):
    """Clear removes all entries."""
    await test_cache.set("key1", "value1")
    await test_cache.set("key2", "value2")
    test_cache.clear()
    assert await test_cache.get("key1") is None
    assert await test_cache.get("key2") is None


@pytest.mark.asyncio
async def test_cache_stats_hits_misses(test_cache):
    """Stats track hits and misses."""
    await test_cache.set("key1", "value1")

    await test_cache.get("key1")  # hit
    await test_cache.get("key1")  # hit
    await test_cache.get("missing")  # miss

    stats = test_cache.stats
    assert stats.hits == 2
    assert stats.misses == 1


@pytest.mark.asyncio
async def test_cache_stats_hit_rate(test_cache):
    """Hit rate calculation."""
    await test_cache.set("key1", "value1")

    await test_cache.get("key1")  # hit
    await test_cache.get("key1")  # hit
    await test_cache.get("missing")  # miss
    await test_cache.get("missing2")  # miss

    stats = test_cache.stats
    assert stats.hit_rate == pytest.approx(0.5)  # 2 hits / 4 total


@pytest.mark.asyncio
async def test_cache_stats_dict(test_cache):
    """Stats dict format for API."""
    await test_cache.set("key1", "value1")
    await test_cache.get("key1")

    stats = test_cache.get_stats_dict()

    assert "hits" in stats
    assert "misses" in stats
    assert "size" in stats
    assert "hit_rate" in stats
    assert isinstance(stats["hit_rate"], float)


@pytest.mark.asyncio
async def test_custom_ttl_prune_runs_on_interval_not_every_set() -> None:
    cache = InMemoryCache(max_size=100, default_ttl=300)
    cache.CUSTOM_PRUNE_SET_INTERVAL = 3

    prune_calls = {"count": 0}
    original_prune = cache._prune_custom_expired

    def _counted_prune() -> None:
        prune_calls["count"] += 1
        original_prune()

    cache._prune_custom_expired = _counted_prune  # type: ignore[method-assign]

    for idx in range(7):
        await cache.set(f"k{idx}", idx, ttl=10)

    # Custom prune should trigger at set counts 3 and 6 only.
    assert prune_calls["count"] == 2


@pytest.mark.asyncio
async def test_enforce_max_size_evicts_exact_overflow_count() -> None:
    cache = InMemoryCache(max_size=3, default_ttl=300)

    await cache.set("d1", "v1")
    await cache.set("d2", "v2")
    await cache.set("c1", "v3", ttl=30)

    # Two entries above max_size should evict two oldest custom/default entries.
    await cache.set("c2", "v4", ttl=30)
    await cache.set("c3", "v5", ttl=30)

    stats = cache.stats
    assert stats.size == 3
    assert stats.evictions == 2


class _FakeRedis:
    """Minimal async stand-in for redis.asyncio.Redis used by RedisCache."""

    def __init__(self, *, result=None, error: Exception | None = None) -> None:
        self._result = result
        self._error = error
        self.calls: list[tuple] = []

    async def set(self, key, value, nx=False, ex=None):  # noqa: A002 - mirror redis API
        self.calls.append((key, value, nx, ex))
        if self._error is not None:
            raise self._error
        return self._result


class TestRedisCacheSetNx:
    """set_nx must distinguish 'set' / 'already exists' / 'backend unavailable'.

    A backend error must NOT be reported as 'already exists' — the dedup caller
    treats that as a duplicate and silently drops the event.
    """

    @pytest.mark.asyncio
    async def test_uses_bounded_blocking_pool_when_max_connections_configured(self, monkeypatch) -> None:
        """High-volume dedup caches should wait on a bounded pool instead of fanning out connections."""
        import redis.asyncio as aioredis

        created_pool = object()
        calls: dict[str, object] = {}

        class _FakeBlockingPool:
            @staticmethod
            def from_url(redis_url: str, **kwargs):
                calls["redis_url"] = redis_url
                calls["pool_kwargs"] = kwargs
                return created_pool

        class _FakeRedisClient:
            def __init__(self, *, connection_pool) -> None:
                calls["connection_pool"] = connection_pool

        monkeypatch.setattr(aioredis, "BlockingConnectionPool", _FakeBlockingPool)
        monkeypatch.setattr(aioredis, "Redis", _FakeRedisClient)

        cache = RedisCache(
            redis_url="redis://test",
            default_ttl=60,
            max_connections=32,
            operation_timeout_seconds=2.5,
        )

        await cache._ensure_connected()

        assert calls["redis_url"] == "redis://test"
        assert calls["connection_pool"] is created_pool
        assert calls["pool_kwargs"] == {
            "max_connections": 32,
            "timeout": 2.5,
            "decode_responses": True,
            "socket_connect_timeout": 2.5,
            "socket_timeout": 2.5,
        }

    @pytest.mark.asyncio
    async def test_returns_true_when_newly_set(self) -> None:
        cache = RedisCache(redis_url="redis://test", default_ttl=60)
        cache._redis = _FakeRedis(result="OK")  # SET NX returns OK when it set the key
        assert await cache.set_nx("k", "1") is True

    @pytest.mark.asyncio
    async def test_returns_false_when_key_exists(self) -> None:
        cache = RedisCache(redis_url="redis://test", default_ttl=60)
        cache._redis = _FakeRedis(result=None)  # SET NX returns None when key exists
        assert await cache.set_nx("k", "1") is False

    @pytest.mark.asyncio
    async def test_returns_none_on_backend_error(self) -> None:
        cache = RedisCache(redis_url="redis://test", default_ttl=60)
        cache._redis = _FakeRedis(error=ConnectionError("Too many connections"))
        assert await cache.set_nx("k", "1") is None

    @pytest.mark.asyncio
    async def test_returns_none_when_closed(self) -> None:
        cache = RedisCache(redis_url="redis://test", default_ttl=60)
        cache._closed = True
        assert await cache.set_nx("k", "1") is None

    @pytest.mark.asyncio
    async def test_repeated_backend_errors_log_once_per_interval(self, caplog) -> None:
        cache = RedisCache(redis_url="redis://test", default_ttl=60)
        cache._redis = _FakeRedis(error=ConnectionError("Too many connections"))

        with (
            patch.object(cache_module, "_SET_NX_ERROR_LOG_THROTTLE", LogThrottle(60.0)),
            caplog.at_level("WARNING"),
        ):
            for _ in range(50):
                assert await cache.set_nx("k", "1") is None

        nx_errors = [r for r in caplog.records if "redis_cache_set_nx_error" in r.getMessage()]
        assert len(nx_errors) == 1, f"expected throttled to 1 log, got {len(nx_errors)}"
