"""Tests for in-memory cache."""

import pytest


def test_cache_set_and_get(test_cache):
    """Set and get value from cache."""
    test_cache.set("key1", "value1")
    assert test_cache.get("key1") == "value1"


def test_cache_miss_returns_none(test_cache):
    """Missing key returns None."""
    assert test_cache.get("nonexistent") is None


def test_cache_delete(test_cache):
    """Delete removes key from cache."""
    test_cache.set("key1", "value1")
    assert test_cache.delete("key1") is True
    assert test_cache.get("key1") is None


def test_cache_delete_nonexistent(test_cache):
    """Delete nonexistent key returns False."""
    assert test_cache.delete("nonexistent") is False


def test_cache_exists(test_cache):
    """Exists checks key presence."""
    test_cache.set("key1", "value1")
    assert test_cache.exists("key1") is True
    assert test_cache.exists("nonexistent") is False


def test_cache_clear(test_cache):
    """Clear removes all entries."""
    test_cache.set("key1", "value1")
    test_cache.set("key2", "value2")
    test_cache.clear()
    assert test_cache.get("key1") is None
    assert test_cache.get("key2") is None


def test_cache_stats_hits_misses(test_cache):
    """Stats track hits and misses."""
    test_cache.set("key1", "value1")

    test_cache.get("key1")  # hit
    test_cache.get("key1")  # hit
    test_cache.get("missing")  # miss

    stats = test_cache.stats
    assert stats.hits == 2
    assert stats.misses == 1


def test_cache_stats_hit_rate(test_cache):
    """Hit rate calculation."""
    test_cache.set("key1", "value1")

    test_cache.get("key1")  # hit
    test_cache.get("key1")  # hit
    test_cache.get("missing")  # miss
    test_cache.get("missing2")  # miss

    stats = test_cache.stats
    assert stats.hit_rate == pytest.approx(0.5)  # 2 hits / 4 total


def test_cache_stats_dict(test_cache):
    """Stats dict format for API."""
    test_cache.set("key1", "value1")
    test_cache.get("key1")

    stats = test_cache.get_stats_dict()

    assert "hits" in stats
    assert "misses" in stats
    assert "size" in stats
    assert "hit_rate" in stats
    assert isinstance(stats["hit_rate"], float)
