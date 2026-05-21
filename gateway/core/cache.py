"""In-memory cache with TTL support."""

import asyncio
import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from cachetools import TTLCache

from gateway.core.logger import logger


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


@dataclass
class _CustomCacheEntry:
    value: Any
    expires_at: float


class InMemoryCache:
    """In-memory cache with TTL and statistics."""

    CUSTOM_PRUNE_SET_INTERVAL = 64

    def __init__(self, max_size: int = 10000, default_ttl: int = 300):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: TTLCache = TTLCache(maxsize=max_size, ttl=default_ttl)
        self._custom_cache: OrderedDict[str, _CustomCacheEntry] = OrderedDict()
        self._custom_sets_since_prune = 0
        self._stats = CacheStats(max_size=max_size)

    async def get(self, key: str) -> Any | None:
        """Get value from cache."""
        try:
            value = self._cache[key]
            self._stats.hits += 1
            return value
        except KeyError:
            entry = self._custom_cache.get(key)
            if entry:
                if entry.expires_at <= time.time():
                    del self._custom_cache[key]
                    self._stats.misses += 1
                    return None
                # LRU: mark as recently used
                self._custom_cache.move_to_end(key)
                self._stats.hits += 1
                return entry.value

            self._stats.misses += 1
            return None

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set value in cache with optional custom TTL."""
        if ttl is not None and ttl != self.default_ttl:
            expires_at = time.time() + ttl
            self._custom_cache[key] = _CustomCacheEntry(value=value, expires_at=expires_at)
            self._custom_cache.move_to_end(key)
            # Remove from default cache if present to avoid stale duplicates
            if key in self._cache:
                del self._cache[key]

            self._stats.sets += 1
            self._custom_sets_since_prune += 1
            if (
                self._custom_sets_since_prune >= self.CUSTOM_PRUNE_SET_INTERVAL
                or len(self._custom_cache) > self.max_size
            ):
                self._prune_custom_expired()
                self._custom_sets_since_prune = 0
            self._enforce_max_size()
            self._stats.size = len(self._cache) + len(self._custom_cache)
            return

        # Track evictions (approximate)
        prev_size = len(self._cache)

        # Remove from custom cache if present so default TTL wins
        if key in self._custom_cache:
            del self._custom_cache[key]

        self._cache[key] = value
        self._stats.sets += 1
        self._enforce_max_size()
        self._stats.size = len(self._cache) + len(self._custom_cache)

        # If size decreased after set, eviction occurred in default cache
        if len(self._cache) <= prev_size and prev_size >= self.max_size:
            self._stats.evictions += 1

    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        deleted = False
        if key in self._cache:
            del self._cache[key]
            deleted = True
        if key in self._custom_cache:
            del self._custom_cache[key]
            deleted = True
        if deleted:
            self._stats.size = len(self._cache) + len(self._custom_cache)
        return deleted

    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()
        self._custom_cache.clear()
        self._custom_sets_since_prune = 0
        self._stats.size = 0
        logger.info("cache_cleared")

    def invalidate_matching(self, key_predicate) -> int:
        """Delete every cache entry whose key matches a callable predicate.

        Used by CacheMiddleware to invalidate per-client read caches on
        mutating writes (POST/PATCH/DELETE on /orders, /positions, etc.).
        Without this method a cached order list stayed stale for the full
        TTL (default 300s) after a new order was placed — flagged as a
        BLOCKER in the 2026-05-21 audit.

        Returns the number of entries removed. O(n) over cache size; safe
        on a TTLCache because we materialize the key list first (no
        mutation during iteration).
        """
        removed = 0
        for key in list(self._cache.keys()):
            if key_predicate(key):
                del self._cache[key]
                removed += 1
        for key in list(self._custom_cache.keys()):
            if key_predicate(key):
                del self._custom_cache[key]
                removed += 1
        if removed:
            self._stats.size = len(self._cache) + len(self._custom_cache)
        return removed

    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        if key in self._cache:
            return True
        entry = self._custom_cache.get(key)
        if not entry:
            return False
        if entry.expires_at <= time.time():
            del self._custom_cache[key]
            return False
        return True

    @property
    def stats(self) -> CacheStats:
        """Get cache statistics."""
        self._stats.size = len(self._cache) + len(self._custom_cache)
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

    def _prune_custom_expired(self) -> None:
        """Remove expired custom TTL entries."""
        now = time.time()
        expired_keys = [k for k, v in self._custom_cache.items() if v.expires_at <= now]
        for key in expired_keys:
            del self._custom_cache[key]

    def _enforce_max_size(self) -> None:
        """Enforce total max size across default and custom caches."""
        overflow = len(self._cache) + len(self._custom_cache) - self.max_size
        if overflow <= 0:
            return

        custom_evictions = min(overflow, len(self._custom_cache))
        for _ in range(custom_evictions):
            self._custom_cache.popitem(last=False)
        self._stats.evictions += custom_evictions
        overflow -= custom_evictions

        if overflow <= 0:
            return

        default_evictions = 0
        for _ in range(overflow):
            try:
                self._cache.popitem()
                default_evictions += 1
            except KeyError:
                break
        self._stats.evictions += default_evictions


class RedisCache:
    """Async Redis cache backend (L2 layer).

    Provides durable cache storage that persists across restarts.
    All keys are prefixed with 'cache:' to avoid collisions.
    """

    KEY_PREFIX = "cache:"

    def __init__(self, redis_url: str, default_ttl: int = 300) -> None:
        """Initialize Redis cache.

        Args:
            redis_url: Redis connection URL
            default_ttl: Default TTL in seconds
        """
        self._redis_url = redis_url
        self.default_ttl = default_ttl
        self._redis: Any | None = None
        self._connected = False
        self._closed = False
        self._stats = CacheStats()

    def _loop_is_closed(self) -> bool:
        """Return True if the running event loop is closed or absent."""
        try:
            loop = asyncio.get_event_loop()
            return loop.is_closed()
        except RuntimeError:
            return True

    async def _ensure_connected(self) -> None:
        """Lazy connection to Redis.

        Refuses to (re)connect after ``close()`` has been called or when the
        event loop is closed to prevent "Event loop is closed" errors during
        shutdown or test teardown.
        """
        if self._closed or self._loop_is_closed():
            return
        if self._redis is None:
            try:
                import redis.asyncio as aioredis

                self._redis = aioredis.from_url(
                    self._redis_url,
                    decode_responses=True,
                )
                self._connected = True
                logger.info("redis_cache_connected")
            except ImportError:
                logger.error("redis_cache_import_error", msg="redis package not installed")
                raise

    async def get(self, key: str) -> Any | None:
        """Get value from Redis cache."""
        if self._closed or self._loop_is_closed():
            self._stats.misses += 1
            return None
        try:
            await self._ensure_connected()
            if self._redis is None:
                raise RuntimeError("Redis connection not established")
            value = await self._redis.get(f"{self.KEY_PREFIX}{key}")
            if value is not None:
                self._stats.hits += 1
                return json.loads(value)
            self._stats.misses += 1
            return None
        except Exception as e:
            logger.warning("redis_cache_get_error", key=key, error=str(e))
            self._stats.misses += 1
            return None

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set value in Redis cache with TTL."""
        if self._closed or self._loop_is_closed():
            return
        try:
            await self._ensure_connected()
            if self._redis is None:
                raise RuntimeError("Redis connection not established")

            serialized = json.dumps(value, default=str)
            await self._redis.setex(
                f"{self.KEY_PREFIX}{key}",
                ttl or self.default_ttl,
                serialized,
            )
            self._stats.sets += 1
        except Exception as e:
            logger.warning("redis_cache_set_error", key=key, error=str(e))

    async def mget(self, keys: list[str]) -> dict[str, Any]:
        """Fetch multiple keys in a single MGET round trip.

        Returns:
            Mapping of key -> parsed value for keys that exist.
        """
        if not keys or self._closed or self._loop_is_closed():
            return {}
        try:
            await self._ensure_connected()
            if self._redis is None:
                raise RuntimeError("Redis connection not established")

            prefixed = [f"{self.KEY_PREFIX}{k}" for k in keys]
            values = await self._redis.mget(prefixed)
            result: dict[str, Any] = {}
            for key, raw in zip(keys, values, strict=False):
                if raw is not None:
                    self._stats.hits += 1
                    result[key] = json.loads(raw)
                else:
                    self._stats.misses += 1
            return result
        except Exception as e:
            logger.warning("redis_cache_mget_error", count=len(keys), error=str(e))
            self._stats.misses += len(keys)
            return {}

    async def set_many(
        self,
        items: list[tuple[str, Any]],
        ttl: int | None = None,
    ) -> int:
        """Set multiple keys in a single Redis pipeline.

        Args:
            items: List of (key, value) tuples to set.
            ttl: TTL in seconds (uses default_ttl if None).

        Returns:
            Number of successfully set keys.
        """
        if not items or self._closed or self._loop_is_closed():
            return 0
        try:
            await self._ensure_connected()
            if self._redis is None:
                raise RuntimeError("Redis connection not established")

            effective_ttl = ttl or self.default_ttl
            pipe = self._redis.pipeline(transaction=False)
            for key, value in items:
                serialized = json.dumps(value, default=str)
                pipe.setex(f"{self.KEY_PREFIX}{key}", effective_ttl, serialized)
            results = await pipe.execute()
            success = sum(1 for r in results if not isinstance(r, Exception))
            self._stats.sets += success
            return success
        except Exception as e:
            logger.warning("redis_cache_set_many_error", count=len(items), error=str(e))
            return 0

    async def set_nx(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Atomically set key only if it does not exist (SET NX EX).

        Returns True if the key was set (first caller wins), False if it already
        exists. Uses a single Redis SET command with NX and EX flags so there is
        no TOCTOU window between checking and setting.
        """
        if self._closed or self._loop_is_closed():
            return False
        try:
            await self._ensure_connected()
            if self._redis is None:
                raise RuntimeError("Redis connection not established")

            serialized = json.dumps(value, default=str)
            result = await self._redis.set(
                f"{self.KEY_PREFIX}{key}",
                serialized,
                nx=True,
                ex=ttl or self.default_ttl,
            )
            if result:
                self._stats.sets += 1
                return True
            return False
        except Exception as e:
            logger.warning("redis_cache_set_nx_error", key=key, error=str(e))
            return False

    async def delete(self, key: str) -> bool:
        """Delete key from Redis cache."""
        if self._closed or self._loop_is_closed():
            return False
        try:
            await self._ensure_connected()
            if self._redis is None:
                raise RuntimeError("Redis connection not established")
            result = await self._redis.delete(f"{self.KEY_PREFIX}{key}")
            return result > 0
        except Exception as e:
            logger.warning("redis_cache_delete_error", key=key, error=str(e))
            return False

    async def exists(self, key: str) -> bool:
        """Check if key exists in Redis cache."""
        if self._closed or self._loop_is_closed():
            return False
        try:
            await self._ensure_connected()
            if self._redis is None:
                raise RuntimeError("Redis connection not established")
            return await self._redis.exists(f"{self.KEY_PREFIX}{key}") > 0
        except Exception:
            return False

    @property
    def stats(self) -> CacheStats:
        """Get Redis cache statistics."""
        return self._stats

    async def close(self) -> None:
        """Close Redis connection.

        Sets ``_closed`` so that no further operations attempt to reconnect,
        which would fail with "Event loop is closed" during shutdown.
        """
        self._closed = True
        if self._redis:
            try:
                if hasattr(self._redis, "aclose"):
                    await self._redis.aclose()
                else:
                    await self._redis.close()
            except Exception:
                pass
            self._redis = None
            self._connected = False


class HybridCache:
    """Two-tier cache: L1 (memory) + L2 (Redis).

    Provides fast in-memory caching with Redis fallback for durability.
    On L1 miss, checks L2 and populates L1 on hit.

    Usage:
        cache = HybridCache("redis://localhost:6379")
        await cache.set("key", {"data": "value"})
        result = await cache.get("key")  # Returns from L1 or L2
    """

    def __init__(
        self,
        redis_url: str,
        max_size: int = 10000,
        default_ttl: int = 300,
    ) -> None:
        """Initialize hybrid cache.

        Args:
            redis_url: Redis connection URL for L2
            max_size: Max entries in L1 memory cache
            default_ttl: Default TTL in seconds
        """
        self._l1 = InMemoryCache(max_size=max_size, default_ttl=default_ttl)
        self._l2 = RedisCache(redis_url=redis_url, default_ttl=default_ttl)
        self.default_ttl = default_ttl

    async def get(self, key: str) -> Any | None:
        """Get value from cache (L1 first, then L2).

        On L2 hit, populates L1 for future fast access.
        """
        # Check L1 (fast memory cache)
        value = await self._l1.get(key)
        if value is not None:
            return value

        # Check L2 (async, durable)
        value = await self._l2.get(key)
        if value is not None:
            # Populate L1 from L2
            await self._l1.set(key, value)
            return value

        return None

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set value in both L1 and L2."""
        await self._l1.set(key, value, ttl)
        await self._l2.set(key, value, ttl)

    async def delete(self, key: str) -> bool:
        """Delete from both L1 and L2."""
        l1_deleted = self._l1.delete(key)
        l2_deleted = await self._l2.delete(key)
        return l1_deleted or l2_deleted

    async def exists(self, key: str) -> bool:
        """Check if key exists in L1 or L2."""
        if self._l1.exists(key):
            return True
        return await self._l2.exists(key)

    def clear(self) -> None:
        """Clear L1 cache only (L2 uses TTL for cleanup)."""
        self._l1.clear()

    async def invalidate_matching(self, key_predicate) -> int:
        """Invalidate entries in BOTH layers that match the predicate.

        L1: O(n) iterate-and-delete. L2 (Redis): not currently implemented;
        a future improvement is to use SCAN MATCH with a pattern derived
        from the predicate signature. For now, callers that depend on L2
        invalidation must call `clear()` or wait for TTL.
        """
        removed = self._l1.invalidate_matching(key_predicate)
        # L2 (Redis) invalidate_matching not yet implemented — see note above.
        # Returning L1-only count is honest about what was invalidated.
        return removed

    @property
    def stats(self) -> dict:
        """Get combined stats from both layers."""
        l1_stats = self._l1.stats
        l2_stats = self._l2.stats
        return {
            "l1": {
                "hits": l1_stats.hits,
                "misses": l1_stats.misses,
                "size": l1_stats.size,
                "hit_rate": round(l1_stats.hit_rate, 4),
            },
            "l2": {
                "hits": l2_stats.hits,
                "misses": l2_stats.misses,
                "hit_rate": round(l2_stats.hit_rate, 4),
            },
        }

    def get_stats_dict(self) -> dict:
        """Get stats as dictionary for API responses."""
        return self.stats

    async def close(self) -> None:
        """Close L2 Redis connection."""
        await self._l2.close()
