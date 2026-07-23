"""Common dependencies and constants for Alpaca API endpoints."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from typing import Any

# Query description constants
from fastapi import Depends, HTTPException

from gateway.api.deps import (
    get_cache,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.core.auth import Client
from gateway.core.cache import HybridCache, InMemoryCache
from gateway.core.logger import logger
from gateway.core.metrics import record_route_cache
from gateway.core.registry import ProviderRegistry

# Single source of truth for the Alpaca sub-router's URL prefix. The parent
# router (``gateway/api/alpaca/__init__.py``) mounts the trading endpoints at
# this prefix; the trading route docstrings + 5xx ``retry_hint`` strings
# reference it so callers know exactly where to GET/POST for retries.
#
# Keeping this constant here (instead of in ``alpaca/__init__.py``) avoids a
# circular import: ``__init__`` imports ``trading``, so ``trading`` cannot
# import from ``__init__``. Both modules import from ``common`` already, so
# this is the canonical location.
ALPACA_ROUTER_PREFIX = "/api/v1/alpaca"

DESC_BAR_TIMEFRAME = "Bar timeframe"
DESC_START_TIME = "Start time (ISO 8601)"
DESC_END_TIME = "End time (ISO 8601)"
DESC_MAX_BARS = "Max bars to return"
DESC_COMMA_SYMBOLS = "Comma-separated symbols"

# Error message constants
ERR_PROVIDER_NOT_AVAILABLE = "Alpaca provider not available"
CacheBackend = InMemoryCache | HybridCache
_ALPACA_CACHE_INFLIGHT: dict[str, asyncio.Task[Any]] = {}
_ALPACA_CACHE_LOCK = asyncio.Lock()


def parse_comma_values(
    raw: str,
    *,
    uppercase: bool = False,
    drop_empty: bool = False,
) -> list[str]:
    """Parse comma-separated values with whitespace trimming."""
    values = [item.strip() for item in raw.split(",")]
    if drop_empty:
        values = [item for item in values if item]
    parsed = [item.upper() if uppercase else item for item in values]
    return parsed


async def get_alpaca_provider(
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get the Alpaca provider or raise 503 if unavailable."""
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)
    return provider


import httpx
from alpaca.common.exceptions import APIError

from gateway.core.rate_limiter import get_rate_limiter


def _handle_alpaca_error(exc: Exception, endpoint: str | None = None) -> None:
    """Map Alpaca-specific exceptions (APIError) to HTTP errors."""
    if isinstance(exc, APIError):
        status_code = getattr(exc, "status_code", 400)
        if status_code < 500:
            logger.warning(
                "provider_request_failed",
                endpoint=endpoint,
                error=str(exc),
                status_code=status_code,
            )
        else:
            logger.error(
                "provider_request_failed",
                endpoint=endpoint,
                exc_info=True,
                status_code=status_code,
            )
        raise HTTPException(status_code=status_code, detail=f"Alpaca API Error: {str(exc)}")


async def execute_alpaca_provider_call[T](
    *,
    registry: ProviderRegistry,
    provider_call: Callable[[Any], Awaitable[T]],
    block: bool = True,
    log_context: dict[str, Any] | None = None,
) -> T:
    """Run Alpaca provider call with shared provider lookup, rate-limit, and error handling.

    Acquires an upstream concurrency semaphore (default 25) so that at most N
    requests are in-flight to Alpaca simultaneously, preventing upstream 502s
    when multiple clients burst requests at market open.

    ``log_context`` is merged into provider failure logs only. Routes use it
    for RCA fields such as client ID, symbol, and order shape; response bodies
    are unchanged.
    """
    provider = registry.get("alpaca")
    if not provider:
        raise HTTPException(status_code=503, detail=ERR_PROVIDER_NOT_AVAILABLE)

    endpoint = getattr(provider_call, "__qualname__", None) or getattr(provider_call, "__name__", "<unknown>")
    log_fields = {**(log_context or {}), "endpoint": endpoint}
    # nosemgrep: empire-no-bare-exception -- route boundary: unclassified provider failures map to 502; logged with exc_info
    try:
        await require_provider_rate_limit("alpaca", block=block)
        sem = get_rate_limiter().upstream_semaphore("alpaca") or nullcontext()
        async with sem:
            return await provider_call(provider)
    except HTTPException:
        raise
    except APIError as e:
        status_code = getattr(e, "status_code", 400)
        if status_code < 500:
            logger.warning(
                "provider_request_failed",
                **log_fields,
                error=str(e),
                status_code=status_code,
            )
        else:
            logger.error(
                "provider_request_failed",
                **log_fields,
                exc_info=True,
                status_code=status_code,
            )
        raise HTTPException(status_code=status_code, detail=f"Alpaca API Error: {str(e)}")
    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        if status_code < 500:
            logger.warning(
                "provider_request_failed",
                **log_fields,
                error=str(e),
                status_code=status_code,
            )
        else:
            logger.error(
                "provider_request_failed",
                **log_fields,
                exc_info=True,
                status_code=status_code,
            )
        raise HTTPException(status_code=status_code, detail=f"Upstream provider error: {status_code}")
    except Exception:
        logger.error("provider_request_failed", **log_fields, exc_info=True)
        raise HTTPException(status_code=502, detail="Upstream provider error")


async def execute_alpaca_cached_call[T](
    *,
    registry: ProviderRegistry,
    cache: CacheBackend,
    cache_key: str,
    ttl: int,
    provider_call: Callable[[Any], Awaitable[T]],
    route_label: str,
    cache_mode: str = "alpaca",
    block: bool = True,
) -> T:
    """Run Alpaca provider call with shared cache + in-flight de-dupe.

    Alpaca uses task-level in-flight dedup (``_ALPACA_CACHE_INFLIGHT``) on top
    of the standard cache pipeline. This is kept as a provider-specific wrapper
    because the asyncio.Task-based dedup pattern is unique to Alpaca's upstream
    concurrency semaphore model.
    """
    cached = await cache.get(cache_key)
    if cached is not None:
        record_route_cache(route_label, "hit", cache_mode)
        return cached

    record_route_cache(route_label, "miss", cache_mode)

    owner = False
    async with _ALPACA_CACHE_LOCK:
        future = _ALPACA_CACHE_INFLIGHT.get(cache_key)
        if future is None:
            future = asyncio.create_task(
                execute_alpaca_provider_call(
                    registry=registry,
                    provider_call=provider_call,
                    block=block,
                )
            )
            _ALPACA_CACHE_INFLIGHT[cache_key] = future
            owner = True

    try:
        result = await future
        if owner:
            await cache.set(cache_key, result, ttl=ttl)
        return result
    finally:
        if owner:
            async with _ALPACA_CACHE_LOCK:
                if _ALPACA_CACHE_INFLIGHT.get(cache_key) is future:
                    _ALPACA_CACHE_INFLIGHT.pop(cache_key, None)


__all__ = [
    "ALPACA_ROUTER_PREFIX",
    "DESC_BAR_TIMEFRAME",
    "DESC_START_TIME",
    "DESC_END_TIME",
    "DESC_MAX_BARS",
    "DESC_COMMA_SYMBOLS",
    "ERR_PROVIDER_NOT_AVAILABLE",
    "parse_comma_values",
    "get_alpaca_provider",
    "execute_alpaca_cached_call",
    "execute_alpaca_provider_call",
    "get_cache",
    "require_api_key",
    "require_provider_rate_limit",
    "Client",
    "ProviderRegistry",
    "get_registry",
]
