"""Finnhub API shared constants and dependencies."""

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

import httpx
import structlog
from fastapi import Depends, HTTPException

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
    "execute_finnhub_cached",
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
    if cache_enabled:
        cached = await cache.get(cache_key_value)
        if cached:
            # Note: We rely on the caller to record cache hits/misses if needed
            return {
                "success": True,
                "data": cached,
                "meta": {"cached": True, "provider": "finnhub"},
            }

    provider = registry.get("finnhub")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)

    await require_provider_rate_limit("finnhub")
    try:
        result = await fetcher(provider)
    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        logger.error("provider_request_failed", exc_info=True, status_code=status_code)
        raise HTTPException(status_code=status_code, detail=f"Upstream provider error: {status_code}")
    except Exception:
        logger.error("provider_request_failed", exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")

    cached_value = cache_transform(result)
    if cache_enabled:
        await cache.set(cache_key_value, cached_value, ttl=ttl)

    extra_meta = miss_meta or {}
    if miss_meta_builder:
        built_meta = miss_meta_builder(result, cached_value)
        if built_meta:
            extra_meta = {**extra_meta, **built_meta}

    return {
        "success": True,
        "data": cached_value,
        "meta": {"cached": False, "provider": "finnhub", **extra_meta},
    }
