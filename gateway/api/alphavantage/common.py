"""Alpha Vantage API shared constants and dependencies."""

import structlog
from fastapi import Depends

from gateway.api.deps import (
    get_cache,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.registry import ProviderRegistry

logger = structlog.get_logger()

PROVIDER_NOT_AVAILABLE = "Alpha Vantage provider not available"
CACHE_TTL_QUOTE = 60  # 1 minute for quotes
CACHE_TTL_BARS = 300  # 5 minutes for bars
CACHE_TTL_FUNDAMENTALS = 3600  # 1 hour for fundamentals
CACHE_TTL_INDICATOR = 3600  # 1 hour for indicators

# Re-export dependencies for sub-routers
__all__ = [
    "logger",
    "get_cache",
    "get_registry",
    "require_api_key",
    "require_provider_rate_limit",
    "Client",
    "InMemoryCache",
    "ProviderRegistry",
    "PROVIDER_NOT_AVAILABLE",
    "CACHE_TTL_QUOTE",
    "CACHE_TTL_BARS",
    "CACHE_TTL_FUNDAMENTALS",
    "CACHE_TTL_INDICATOR",
    "cache_key",
    "Depends",
]


def cache_key(prefix: str, *args) -> str:
    """Generate cache key."""
    parts = [prefix] + [str(a) for a in args if a]
    return ":".join(parts)
