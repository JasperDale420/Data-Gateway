"""Shared utilities for UW sub-routers."""

import base64
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from gateway.api.deps import (
    execute_provider_cached,
    get_cache,
    get_registry,
    require_api_key,
    require_provider_rate_limit,
)
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.logger import logger
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

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


def cursor_page_limit(
    limit: int,
    cursor: str | None,
    max_offset: int = MAX_UW_CURSOR_OFFSET,
) -> tuple[int, int]:
    """Return decoded offset and page fetch size for native-offset requests."""
    offset = decode_cursor(cursor, max_offset=max_offset)
    return offset, limit + 1


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


def paginate_offset_response(
    data: list[Any],
    limit: int,
    offset: int,
) -> dict:
    """Build paginated response for data fetched with native offset pagination."""
    safe_offset = max(offset, 0)
    paginated_data = serialize_list(data[:limit])
    has_more = len(data) > limit
    next_cursor = None
    if has_more:
        next_cursor = base64.b64encode(str(safe_offset + limit).encode()).decode()

    return {
        "success": True,
        "data": paginated_data,
        "pagination": {
            "next_cursor": next_cursor,
            "has_more": has_more,
            "total_count": safe_offset + len(data),
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
    except ValueError:
        # Covers binascii.Error (bad base64), UnicodeDecodeError, and int()
        # failures — all ValueError subclasses. An unparseable client-supplied
        # cursor restarts pagination from the top.
        logger.debug("uw_cursor_invalid", cursor=cursor)
        return 0


def get_uw_provider(registry: ProviderRegistry):
    """Get the UW provider or raise 503 if not available."""
    provider = registry.get("unusual_whales")
    if not provider:
        raise HTTPException(status_code=503, detail=PROVIDER_NOT_AVAILABLE)
    return provider


def make_response(
    data: Any,
    symbol: str | None = None,
    count: int | None = None,
    extra_meta: dict[str, Any] | None = None,
) -> dict:
    """Build standard success response with metadata."""
    meta: dict[str, Any] = {"provider": "unusual_whales"}
    if extra_meta:
        for key, value in extra_meta.items():
            if value is not None:
                meta[key] = value
    if symbol:
        meta["symbol"] = symbol
    if count is not None:
        meta["count"] = count

    return {
        "success": True,
        "data": _serialize_value(data),
        "meta": meta,
    }


def count_of(data: Any) -> int | None:
    """Return element count for list/tuple payloads, else None (dict/scalar responses)."""
    return len(data) if isinstance(data, list | tuple) else None


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
    """Execute a UW route fetch with shared cache and rate-limit flow.

    UW caches the fully-built response dict (not raw data), so we use
    ``cache_transform`` to produce the response and ``build_response``
    to return it as-is from the unified pipeline.
    """
    return await execute_provider_cached(
        provider_name="unusual_whales",
        registry=registry,
        cache=cache,
        cache_key=cache_key,
        ttl=ttl,
        fetcher=fetcher,
        cache_transform=build_response,
        build_response=lambda data, _cached: data,
        not_available_msg=PROVIDER_NOT_AVAILABLE,
    )


# Re-export all common dependencies for sub-routers
__all__ = [
    "APIRouter",
    "Client",
    "Depends",
    "DESC_DATE",
    "DESC_EXPIRY",
    "DESC_LIMIT",
    "MAX_UW_CURSOR_OFFSET",
    "count_of",
    "cursor_fetch_limit",
    "cursor_page_limit",
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
    "paginate_offset_response",
    "paginate_response",
    "serialize_list",
    "PROVIDER_NOT_AVAILABLE",
    "ProviderRegistry",
    "Query",
    "require_api_key",
    "require_provider_rate_limit",
    "SuccessResponse",
]
