"""Alpha Vantage API shared constants, dependencies, and route helpers."""

import json
import re
from collections.abc import Awaitable, Callable
from typing import Any

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
from gateway.core.metrics import (
    record_alphavantage_payload_bytes,
    record_alphavantage_route_cache,
)
from gateway.core.registry import ProviderRegistry

logger = structlog.get_logger()

PROVIDER_NOT_AVAILABLE = "Alpha Vantage provider not available"
CACHE_TTL_QUOTE = 60  # 1 minute for quotes
CACHE_TTL_BARS = 300  # 5 minutes for bars
CACHE_TTL_FUNDAMENTALS = 3600  # 1 hour for fundamentals
CACHE_TTL_INDICATOR = 3600  # 1 hour for indicators
MAX_SEARCH_KEY_CHARS = 64

# Re-export dependencies for sub-routers
__all__ = [
    "logger",
    "execute_av_cached",
    "get_cache",
    "get_registry",
    "get_alphavantage_provider",
    "require_api_key",
    "require_provider_rate_limit",
    "Client",
    "make_response",
    "InMemoryCache",
    "ProviderRegistry",
    "PROVIDER_NOT_AVAILABLE",
    "CACHE_TTL_QUOTE",
    "CACHE_TTL_BARS",
    "CACHE_TTL_FUNDAMENTALS",
    "CACHE_TTL_INDICATOR",
    "cache_key",
    "normalize_search_query",
    "Depends",
    "HTTPException",
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


def normalize_search_query(query: str, max_chars: int = MAX_SEARCH_KEY_CHARS) -> str:
    """Normalize free-form search query to constrain cache-key cardinality."""
    normalized = re.sub(r"\s+", " ", query.strip().lower())
    if len(normalized) > max_chars:
        return normalized[:max_chars]
    return normalized


def get_alphavantage_provider(registry: ProviderRegistry):
    """Get Alpha Vantage provider or raise 503 when unavailable."""
    provider = registry.get("alphavantage")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)
    return provider


def make_response(
    data: Any, cached: bool, extra_meta: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Build consistent Alpha Vantage success response payload."""
    meta = {"cached": cached, "provider": "alphavantage"}
    if extra_meta:
        for key, value in extra_meta.items():
            if value is not None:
                meta[key] = value
    return {"success": True, "data": data, "meta": meta}


async def execute_av_cached(
    *,
    cache: InMemoryCache,
    cache_key_value: str,
    registry: ProviderRegistry,
    ttl: int,
    fetcher: Callable[[Any], Awaitable[Any]],
    cache_transform: Callable[[Any], Any],
    miss_meta: dict[str, Any] | None = None,
    miss_meta_builder: Callable[[Any, Any], dict[str, Any] | None] | None = None,
    cache_enabled: bool = True,
    endpoint: str | None = None,
    cache_mode: str = "default",
) -> dict[str, Any]:
    """Execute shared Alpha Vantage cache -> rate-limit -> provider flow."""
    if cache_enabled:
        cached = await cache.get(cache_key_value)
        if cached:
            if endpoint:
                record_alphavantage_route_cache(
                    endpoint=endpoint, status="hit", cache_mode=cache_mode
                )
            return make_response(cached, cached=True)

    provider = get_alphavantage_provider(registry)
    await require_provider_rate_limit("alphavantage")
    result = await fetcher(provider)
    cached_value = cache_transform(result)
    if cache_enabled:
        await cache.set(cache_key_value, cached_value, ttl=ttl)
    if endpoint:
        record_alphavantage_route_cache(endpoint=endpoint, status="miss", cache_mode=cache_mode)
        payload_bytes = len(json.dumps(cached_value, default=str, separators=(",", ":")).encode())
        record_alphavantage_payload_bytes(
            endpoint=endpoint,
            cache_mode=cache_mode,
            payload_bytes=payload_bytes,
        )
    extra_meta = miss_meta
    if miss_meta_builder:
        built_meta = miss_meta_builder(result, cached_value)
        if built_meta:
            extra_meta = {**(extra_meta or {}), **built_meta}
    return make_response(cached_value, cached=False, extra_meta=extra_meta)
