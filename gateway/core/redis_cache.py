"""Legacy cache module.

Deprecated: use gateway.core.cache instead.
This module remains as a thin compatibility layer.
"""

from gateway.core.cache import CacheStats, HybridCache, InMemoryCache, RedisCache

__all__ = [
    "CacheStats",
    "HybridCache",
    "InMemoryCache",
    "RedisCache",
]
