"""Alpha Vantage API shared constants, dependencies, and route helpers."""

import json
import re
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import Depends, HTTPException

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
from gateway.core.metrics import (
    record_alphavantage_payload_bytes,
    record_alphavantage_route_cache,
)
from gateway.core.registry import ProviderRegistry

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


# Delegate to shared utility (backward-compatible alias)
cache_key = make_cache_key


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


def make_response(data: Any, cached: bool, extra_meta: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build consistent Alpha Vantage success response payload."""
    meta = {"cached": cached, "provider": "alphavantage"}
    if extra_meta:
        for key, value in extra_meta.items():
            if value is not None:
                meta[key] = value
    return {"success": True, "data": data, "meta": meta}


def _translate_provider_error(error: Exception) -> None:
    """Map known provider runtime errors to explicit HTTP status codes."""
    message = str(error).strip()
    normalized = message.lower()
    if "rate limit exceeded" in normalized:
        raise HTTPException(
            status_code=429,
            detail="Provider rate limit exceeded: alphavantage",
        )


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
    # Capture raw result in closure so miss_meta_builder gets both raw + transformed
    raw_result_holder: list[Any] = []

    async def _capturing_fetcher(provider: Any) -> Any:
        result = await fetcher(provider)
        raw_result_holder.append(result)
        return result

    def _av_response_builder(data: Any, cached_flag: bool) -> dict[str, Any]:
        """Build AV response, recording metrics on miss."""
        if cached_flag:
            if endpoint:
                record_alphavantage_route_cache(endpoint=endpoint, status="hit", cache_mode=cache_mode)
            return make_response(data, cached=True)
        # Miss path — record metrics
        if endpoint:
            record_alphavantage_route_cache(endpoint=endpoint, status="miss", cache_mode=cache_mode)
            payload_bytes = len(json.dumps(data, default=str, separators=(",", ":")).encode())
            record_alphavantage_payload_bytes(endpoint=endpoint, cache_mode=cache_mode, payload_bytes=payload_bytes)
        extra = miss_meta
        if miss_meta_builder:
            raw = raw_result_holder[0] if raw_result_holder else data
            built = miss_meta_builder(raw, data)
            if built:
                extra = {**(extra or {}), **built}
        return make_response(data, cached=False, extra_meta=extra)

    def _handle_runtime_error(exc: Exception) -> None:
        if isinstance(exc, RuntimeError):
            _translate_provider_error(exc)

    return await execute_provider_cached(
        provider_name="alphavantage",
        registry=registry,
        cache=cache,
        cache_key=cache_key_value,
        ttl=ttl,
        fetcher=_capturing_fetcher,
        cache_transform=cache_transform,
        build_response=_av_response_builder,
        cache_enabled=cache_enabled,
        error_handlers=[_handle_runtime_error],
        not_available_msg=PROVIDER_NOT_AVAILABLE,
    )
