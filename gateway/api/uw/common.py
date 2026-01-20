"""Shared utilities for UW sub-routers.

Common imports, pagination logic, and provider access patterns.
All sub-routers import from this module to reduce duplication.
"""

import base64

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


def paginate_response(
    data: list,
    limit: int,
    cursor: str | None = None,
) -> dict:
    """Build paginated response per PRD spec."""
    offset = 0
    if cursor:
        try:
            offset = int(base64.b64decode(cursor).decode())
        except Exception:
            offset = 0

    total_count = len(data)
    has_more = total_count > limit
    paginated_data = data[:limit]

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
        "data": data,
        "meta": meta,
    }


def make_list_response(data_list: list) -> dict:
    """Build success response for list data without pagination."""
    return {
        "success": True,
        "data": [d.model_dump(mode="json") if hasattr(d, "model_dump") else d for d in data_list],
        "pagination": {
            "next_cursor": None,
            "has_more": False,
            "total_count": len(data_list),
        },
    }


# Re-export all common dependencies for sub-routers
__all__ = [
    "APIRouter",
    "Client",
    "Depends",
    "DESC_DATE",
    "DESC_EXPIRY",
    "DESC_LIMIT",
    "get_cache",
    "get_registry",
    "get_uw_provider",
    "HTTPException",
    "InMemoryCache",
    "logger",
    "make_list_response",
    "make_response",
    "paginate_response",
    "PROVIDER_NOT_AVAILABLE",
    "ProviderRegistry",
    "Query",
    "require_api_key",
    "require_provider_rate_limit",
    "SuccessResponse",
]
