"""Finnhub API shared constants and dependencies."""

from datetime import datetime

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

PROVIDER_NOT_AVAILABLE = "Finnhub provider not available"
CACHE_TTL = 60  # 1 minute for real-time data

# Re-export dependencies for sub-routers
__all__ = [
    "logger",
    "datetime",
    "get_cache",
    "get_registry",
    "require_api_key",
    "require_provider_rate_limit",
    "Client",
    "InMemoryCache",
    "ProviderRegistry",
    "PROVIDER_NOT_AVAILABLE",
    "CACHE_TTL",
    "cache_key",
    "Depends",
]


def cache_key(prefix: str, *args) -> str:
    """Generate cache key."""
    parts = [prefix] + [str(a) for a in args if a]
    return ":".join(parts)
