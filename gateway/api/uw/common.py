"""Shared utilities for UW sub-routers."""

import base64
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import (
    get_cache,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

logger = structlog.get_logger()

# Query description constants
DESC_DATE = "Date (YYYY-MM-DD)"
DESC_EXPIRY = "Expiration (YYYY-MM-DD)"
DESC_LIMIT = "Maximum number of results"

# Error message constants
PROVIDER_NOT_AVAILABLE = "Unusual Whales provider not available"

# Guardrail to avoid pathologically deep offset pagination over-fetch.
MAX_UW_CURSOR_OFFSET = 5_000


def _serialize_value(value: Any) -> Any:
    """Convert response values to JSON-compatible payloads when needed."""
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    if isinstance(value, tuple):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    return value


def serialize_list(values: list[Any]) -> list[Any]:
    """Serialize list values without changing list shape."""
    return [_serialize_value(value) for value in values]


def cursor_fetch_limit(
    limit: int,
    cursor: str | None,
    max_offset: int = MAX_UW_CURSOR_OFFSET,
) -> tuple[int, int]:
    """Return decoded offset and bounded fetch size for cursor-style requests."""
    offset = decode_cursor(cursor, max_offset=max_offset)
    return offset, limit + offset + 1


def paginate_response(
    data: list[Any],
    limit: int,
    cursor: str | None = None,
    max_offset: int = MAX_UW_CURSOR_OFFSET,
) -> dict:
    """Build paginated response per PRD spec."""
    offset = decode_cursor(cursor, max_offset=max_offset)

    total_count = len(data)
    if offset < 0:
        offset = 0
    if offset > total_count:
        offset = total_count

    paginated_data = serialize_list(data[offset : offset + limit])
    has_more = total_count > offset + limit

    next_cursor = None
    if has_more:
        next_cursor = base64.b64encode(str(offset + limit).encode()).decode()

    return {
        "success": True,
        "data": paginated_data,
        "pagination": {
            "next_cursor": next_cursor,
            "has_more": has_more,
            "total_count": total_count,
        },
    }


def decode_cursor(cursor: str | None, max_offset: int | None = None) -> int:
    """Decode cursor to integer offset."""
    if not cursor:
        return 0
    try:
        offset = max(int(base64.b64decode(cursor).decode()), 0)
        if max_offset is not None and offset > max_offset:
            logger.warning(
                "uw_cursor_offset_clamped",
                requested_offset=offset,
                max_offset=max_offset,
            )
            return max_offset
        return offset
    except Exception:
        return 0


def get_uw_provider(registry: ProviderRegistry):
    """Get the UW provider or raise 503 if not available."""
    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)
    return provider


def make_response(data, symbol: str | None = None, count: int | None = None) -> dict:
    """Build standard success response with metadata."""
    meta: dict[str, str | int] = {"provider": "unusual_whales"}
    if symbol:
        meta["symbol"] = symbol
    if count is not None:
        meta["count"] = count

    return {
        "success": True,
        "data": _serialize_value(data),
        "meta": meta,
    }


def make_list_response(data_list: list[Any]) -> dict:
    """Build success response for list data without pagination."""
    return {
        "success": True,
        "data": serialize_list(data_list),
        "pagination": {
            "next_cursor": None,
            "has_more": False,
            "total_count": len(data_list),
        },
    }


async def execute_uw_cached(
    *,
    cache: InMemoryCache,
    cache_key: str,
    registry: ProviderRegistry,
    ttl: int,
    fetcher: Callable[[Any], Awaitable[Any]],
    build_response: Callable[[Any], dict],
) -> dict:
    """Execute a UW route fetch with shared cache and rate-limit flow."""
    cached = await cache.get(cache_key)
    if cached:
        return cached

    provider = get_uw_provider(registry)
    await require_provider_rate_limit("unusual_whales")
    result = await fetcher(provider)
    response = build_response(result)
    await cache.set(cache_key, response, ttl=ttl)
    return response


# Re-export all common dependencies for sub-routers
__all__ = [
    "APIRouter",
    "Client",
    "Depends",
    "DESC_DATE",
    "DESC_EXPIRY",
    "DESC_LIMIT",
    "MAX_UW_CURSOR_OFFSET",
    "cursor_fetch_limit",
    "execute_uw_cached",
    "get_cache",
    "get_registry",
    "get_uw_provider",
    "HTTPException",
    "InMemoryCache",
    "logger",
    "make_list_response",
    "make_response",
    "decode_cursor",
    "paginate_response",
    "serialize_list",
    "PROVIDER_NOT_AVAILABLE",
    "ProviderRegistry",
    "Query",
    "require_api_key",
    "require_provider_rate_limit",
    "SuccessResponse",
]
