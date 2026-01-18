"""Redis cache backend with fallback support."""

import json
import os
from typing import Any, Protocol

import structlog

logger = structlog.get_logger()


class CacheBackend(Protocol):
    """Protocol for cache backends."""

    def get(self, key: str) -> Any | None: ...
    def set(self, key: str, value: Any, ttl: int | None = None) -> None: ...
    def delete(self, key: str) -> bool: ...
    def clear(self) -> None: ...
    def exists(self, key: str) -> bool: ...
    def get_stats_dict(self) -> dict: ...


class RedisCache:
    """Redis cache backend with automatic reconnection.

    Falls back to in-memory cache if Redis is unavailable.
    """

    def __init__(
        self,
        url: str | None = None,
        default_ttl: int = 300,
        key_prefix: str = "gw:",
    ):
        self.url = url or os.environ.get("REDIS_URL", "redis://localhost:6379")
        self.default_ttl = default_ttl
        self.key_prefix = key_prefix
        self._client: Any | None = None
        self._connected = False
        self._stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "errors": 0,
        }

    async def connect(self) -> bool:
        """Connect to Redis server."""
        try:
            import redis.asyncio as redis

            self._client = redis.from_url(
                self.url,
                encoding="utf-8",
                decode_responses=True,
            )
            # Test connection
            await self._client.ping()
            self._connected = True
            logger.info("redis_cache_connected", url=self._sanitize_url())
            return True

        except ImportError:
            logger.error("redis_package_not_installed", code="GW-E5002")
            return False
        except Exception as e:
            logger.warning(
                "redis_connection_failed",
                code="GW-W5002",
                error=str(e),
                url=self._sanitize_url(),
            )
            self._connected = False
            return False

    async def disconnect(self) -> None:
        """Disconnect from Redis server."""
        if self._client:
            await self._client.close()
            self._client = None
            self._connected = False
            logger.info("redis_cache_disconnected")

    def _sanitize_url(self) -> str:
        """Remove password from URL for logging."""
        if "@" in self.url:
            # Hide password: redis://:password@host:port -> redis://***@host:port
            parts = self.url.split("@")
            return f"redis://***@{parts[-1]}"
        return self.url

    def _make_key(self, key: str) -> str:
        """Add prefix to key."""
        return f"{self.key_prefix}{key}"

    async def get(self, key: str) -> Any | None:
        """Get value from Redis cache."""
        if not self._connected or not self._client:
            return None

        try:
            value = await self._client.get(self._make_key(key))
            if value is not None:
                self._stats["hits"] += 1
                return json.loads(value)
            self._stats["misses"] += 1
            return None
        except Exception as e:
            self._stats["errors"] += 1
            logger.warning("redis_get_failed", key=key[:50], error=str(e))
            return None

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set value in Redis cache."""
        if not self._connected or not self._client:
            return

        try:
            await self._client.setex(
                self._make_key(key),
                ttl or self.default_ttl,
                json.dumps(value),
            )
            self._stats["sets"] += 1
        except Exception as e:
            self._stats["errors"] += 1
            logger.warning("redis_set_failed", key=key[:50], error=str(e))

    async def delete(self, key: str) -> bool:
        """Delete key from Redis cache."""
        if not self._connected or not self._client:
            return False

        try:
            result = await self._client.delete(self._make_key(key))
            return result > 0
        except Exception as e:
            self._stats["errors"] += 1
            logger.warning("redis_delete_failed", key=key[:50], error=str(e))
            return False

    async def clear(self) -> None:
        """Clear all keys with our prefix."""
        if not self._connected or not self._client:
            return

        try:
            # Use SCAN to find and delete keys with our prefix
            cursor = 0
            pattern = f"{self.key_prefix}*"
            while True:
                cursor, keys = await self._client.scan(cursor, match=pattern, count=100)
                if keys:
                    await self._client.delete(*keys)
                if cursor == 0:
                    break
            logger.info("redis_cache_cleared", pattern=pattern)
        except Exception as e:
            self._stats["errors"] += 1
            logger.warning("redis_clear_failed", error=str(e))

    async def exists(self, key: str) -> bool:
        """Check if key exists in Redis."""
        if not self._connected or not self._client:
            return False

        try:
            return await self._client.exists(self._make_key(key)) > 0
        except Exception:
            self._stats["errors"] += 1
            return False

    def get_stats_dict(self) -> dict:
        """Get cache statistics."""
        total = self._stats["hits"] + self._stats["misses"]
        hit_rate = self._stats["hits"] / total if total > 0 else 0.0
        return {
            **self._stats,
            "hit_rate": round(hit_rate, 4),
            "connected": self._connected,
            "backend": "redis",
            "url": self._sanitize_url(),
        }

    @property
    def is_connected(self) -> bool:
        """Check if connected to Redis."""
        return self._connected


class HybridCache:
    """Cache that uses Redis as primary and in-memory as fallback.

    Automatically falls back to in-memory cache when Redis is unavailable.
    """

    def __init__(
        self,
        redis_url: str | None = None,
        max_memory_size: int = 10000,
        default_ttl: int = 300,
    ):
        from gateway.core.cache import InMemoryCache

        self.redis = RedisCache(url=redis_url, default_ttl=default_ttl)
        self.memory = InMemoryCache(max_size=max_memory_size, default_ttl=default_ttl)
        self._use_redis = False

    async def initialize(self) -> None:
        """Initialize cache backends."""
        redis_url = os.environ.get("REDIS_URL")
        if redis_url:
            self._use_redis = await self.redis.connect()
            if not self._use_redis:
                logger.warning(
                    "redis_fallback_to_memory",
                    code="GW-W5002",
                    message="Using in-memory cache as fallback",
                )
        else:
            logger.info("redis_not_configured", message="Using in-memory cache")

    async def shutdown(self) -> None:
        """Shutdown cache backends."""
        if self._use_redis:
            await self.redis.disconnect()

    def get(self, key: str) -> Any | None:
        """Get from cache (sync wrapper for compatibility)."""
        # For sync access, use memory cache
        return self.memory.get(key)

    async def get_async(self, key: str) -> Any | None:
        """Get from cache (async)."""
        if self._use_redis:
            value = await self.redis.get(key)
            if value is not None:
                return value
        return self.memory.get(key)

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set in cache (sync wrapper - stores in memory only)."""
        self.memory.set(key, value, ttl)

    async def set_async(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set in cache (async - stores in both if Redis available)."""
        self.memory.set(key, value, ttl)
        if self._use_redis:
            await self.redis.set(key, value, ttl)

    def delete(self, key: str) -> bool:
        """Delete from cache (sync)."""
        return self.memory.delete(key)

    async def delete_async(self, key: str) -> bool:
        """Delete from cache (async)."""
        result = self.memory.delete(key)
        if self._use_redis:
            await self.redis.delete(key)
        return result

    def clear(self) -> None:
        """Clear memory cache (sync)."""
        self.memory.clear()

    async def clear_async(self) -> None:
        """Clear both caches (async)."""
        self.memory.clear()
        if self._use_redis:
            await self.redis.clear()

    def exists(self, key: str) -> bool:
        """Check if exists (sync - memory only)."""
        return self.memory.exists(key)

    def get_stats_dict(self) -> dict:
        """Get combined stats."""
        stats = self.memory.get_stats_dict()
        stats["backend"] = "redis" if self._use_redis else "memory"
        if self._use_redis:
            stats["redis"] = self.redis.get_stats_dict()
        return stats
