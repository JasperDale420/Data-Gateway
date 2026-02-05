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
