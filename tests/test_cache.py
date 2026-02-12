"""Tests for in-memory cache."""

import pytest

from gateway.core.cache import InMemoryCache


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
