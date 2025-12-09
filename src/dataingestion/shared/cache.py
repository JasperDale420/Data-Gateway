import json
import logging
from typing import Any, Optional
from functools import wraps
import hashlib
from redis.asyncio import Redis
from dataingestion.shared.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class CacheService:
    def __init__(self):
        self.redis: Optional[Redis] = None

    async def connect(self):
        self.redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        try:
            await self.redis.ping()
            logger.info("Connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis = None

    async def close(self):
        if self.redis:
            await self.redis.close()

    async def get(self, key: str) -> Optional[Any]:
        if not self.redis:
            return None
        try:
            data = await self.redis.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None

    async def set(self, key: str, value: Any, ttl: int = 3600):
        if not self.redis:
            return
        try:
            await self.redis.set(key, json.dumps(value), ex=ttl)
        except Exception as e:
            logger.error(f"Cache set error: {e}")


cache_service = CacheService()


def cached(ttl: int = 3600, prefix: str = ""):
    """Decorator to cache async function results in Redis."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            key_parts = [prefix or func.__name__]
            # Add args and kwargs to key
            for arg in args:
                if hasattr(arg, "dict"):  # Pydantic models
                    key_parts.append(str(arg.dict()))
                else:
                    key_parts.append(str(arg))
            key_parts.append(str(kwargs))

            key_str = ":".join(key_parts)
            key_hash = hashlib.md5(key_str.encode()).hexdigest()
            cache_key = f"cache:{prefix}:{key_hash}"

            # Try get from cache
            cached_val = await cache_service.get(cache_key)
            if cached_val:
                logger.info(f"Cache HIT: {cache_key}")
                return cached_val

            # Execute function
            result = await func(*args, **kwargs)

            # Set cache
            await cache_service.set(cache_key, result, ttl)
            logger.info(f"Cache MISS: {cache_key}")
            return result

        return wrapper

    return decorator
