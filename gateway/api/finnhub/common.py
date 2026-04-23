"""Finnhub API shared constants and dependencies."""

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

from fastapi import Depends

from gateway.api.deps import (
    execute_provider_cached,
    get_cache,
    get_registry,
    make_cache_key,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.logger import logger
from gateway.core.registry import ProviderRegistry

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
    "execute_finnhub_cached",
]


# Delegate to shared utility (backward-compatible alias)
cache_key = make_cache_key


async def execute_finnhub_cached(
    *,
    cache: InMemoryCache,
    cache_key_value: str,
    registry: ProviderRegistry,
    ttl: int,
    fetcher: Callable[[Any], Awaitable[Any]],
    cache_transform: Callable[[Any], Any] = lambda x: x,
    miss_meta: dict[str, Any] | None = None,
    miss_meta_builder: Callable[[Any, Any], dict[str, Any] | None] | None = None,
    cache_enabled: bool = True,
) -> dict[str, Any]:
    """Execute shared Finnhub cache -> rate-limit -> provider flow."""
    return await execute_provider_cached(
        provider_name="finnhub",
        registry=registry,
        cache=cache,
        cache_key=cache_key_value,
        ttl=ttl,
        fetcher=fetcher,
        cache_transform=cache_transform,
        miss_meta=miss_meta,
        miss_meta_builder=miss_meta_builder,
        cache_enabled=cache_enabled,
        not_available_msg=PROVIDER_NOT_AVAILABLE,
    )
