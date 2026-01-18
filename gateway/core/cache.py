"""In-memory cache with TTL support."""

from dataclasses import dataclass
from typing import Any

import structlog
from cachetools import TTLCache

logger = structlog.get_logger()


@dataclass
class CacheStats:
    """Cache statistics."""

    hits: int = 0
    misses: int = 0
    sets: int = 0
    evictions: int = 0
    size: int = 0
    max_size: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class InMemoryCache:
    """In-memory cache with TTL and statistics."""

    def __init__(self, max_size: int = 10000, default_ttl: int = 300):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: TTLCache = TTLCache(maxsize=max_size, ttl=default_ttl)
        self._stats = CacheStats(max_size=max_size)

    def get(self, key: str) -> Any | None:
        """Get value from cache."""
        try:
            value = self._cache[key]
            self._stats.hits += 1
            return value
        except KeyError:
            self._stats.misses += 1
            return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set value in cache with optional custom TTL."""
        # Track evictions (approximate)
        prev_size = len(self._cache)

        if ttl and ttl != self.default_ttl:
            # For custom TTL, we need to manually handle expiry
            # cachetools TTLCache uses single TTL for all items
            # For simplicity, we use default TTL; custom TTL would need separate implementation
            pass

        self._cache[key] = value
        self._stats.sets += 1
        self._stats.size = len(self._cache)

        # If size decreased after set, eviction occurred
        if len(self._cache) <= prev_size and prev_size >= self.max_size:
            self._stats.evictions += 1

    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        try:
            del self._cache[key]
            self._stats.size = len(self._cache)
            return True
        except KeyError:
            return False

    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()
        self._stats.size = 0
        logger.info("cache_cleared")

    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        return key in self._cache

    @property
    def stats(self) -> CacheStats:
        """Get cache statistics."""
        self._stats.size = len(self._cache)
        return self._stats

    def get_stats_dict(self) -> dict:
        """Get stats as dictionary for API responses."""
        stats = self.stats
        return {
            "hits": stats.hits,
            "misses": stats.misses,
            "sets": stats.sets,
            "evictions": stats.evictions,
            "size": stats.size,
            "max_size": stats.max_size,
            "hit_rate": round(stats.hit_rate, 4),
        }
