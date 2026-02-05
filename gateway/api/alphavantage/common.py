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


def _normalize_cache_arg(value) -> str:
    if value is None:
        return "<none>"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value == "":
        return "<empty>"
    return str(value)


def cache_key(prefix: str, *args) -> str:
    """Generate cache key."""
    parts = [prefix] + [_normalize_cache_arg(a) for a in args]
    return ":".join(parts)
