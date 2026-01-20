"""Admin and status endpoints."""

from collections import deque
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query

from gateway.api.deps import (
    get_cache,
    get_connection_manager,
    get_provider_rate_limiter,
    get_registry,
    require_api_key,
)
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.connections import ConnectionManager
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter(tags=["admin"])

# In-memory error log buffer
_error_buffer: deque[dict] = deque(maxlen=1000)
_error_counts: dict[str, int] = {}
_error_counts_reset: datetime = datetime.now(UTC)


def log_error(error_code: str, message: str, component: str = "gateway") -> None:
    """Log an error to the in-memory buffer."""
    global _error_counts, _error_counts_reset

    now = datetime.now(UTC)

    # Reset counts every hour
    if now - _error_counts_reset > timedelta(hours=1):
        _error_counts = {}
        _error_counts_reset = now

    _error_buffer.append(
        {
            "timestamp": now.isoformat(),
            "error_code": error_code,
            "message": message,
            "component": component,
        }
    )

    _error_counts[error_code] = _error_counts.get(error_code, 0) + 1


@router.get("/api/v1/status", response_model=SuccessResponse)
async def get_status(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
    connections: ConnectionManager = Depends(get_connection_manager),
):
    """Get full system status including clients, providers, and subscriptions."""
    # Provider health
    provider_status = await registry.health_check_all()

    # Cache stats
    cache_stats = cache.get_stats_dict()

    # Connection stats
    connection_stats = connections.get_stats()

    return {
        "success": True,
        "data": {
            "providers": {
                name: {
                    "healthy": status.healthy,
                    "error": status.error,
                    "latency_ms": status.latency_ms,
                }
                for name, status in provider_status.items()
            },
            "cache": cache_stats,
            "connections": connection_stats,
            "registry": registry.get_stats(),
        },
        "meta": {
            "timestamp": datetime.now(UTC).isoformat(),
        },
    }


@router.get("/api/v1/admin/logs/recent", response_model=SuccessResponse)
async def get_recent_logs(
    level: str = Query(default="ERROR", description="Log level filter"),
    limit: int = Query(default=100, le=1000, description="Max entries"),
    client: Client = Depends(require_api_key),
):
    """Get recent error logs from in-memory buffer."""
    # Filter by level (currently only ERROR supported)
    logs = list(_error_buffer)[-limit:]

    return {
        "success": True,
        "data": {
            "logs": logs,
            "count": len(logs),
            "buffer_size": len(_error_buffer),
            "max_buffer": _error_buffer.maxlen,
        },
        "meta": {
            "level": level,
            "limit": limit,
        },
    }


@router.get("/api/v1/admin/errors/summary", response_model=SuccessResponse)
async def get_error_summary(
    client: Client = Depends(require_api_key),
):
    """Get error code counts for the last hour."""
    return {
        "success": True,
        "data": {
            "period": "last_hour",
            "total_errors": sum(_error_counts.values()),
            "by_code": dict(_error_counts),
            "period_start": _error_counts_reset.isoformat(),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Provider Rate Limit Status
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/api/v1/admin/rate-limits", response_model=SuccessResponse)
async def get_rate_limit_status(
    provider: str | None = Query(default=None, description="Filter by provider"),
    client: Client = Depends(require_api_key),
):
    """Get rate limit status for all or specific provider.

    Shows:
    - Requests remaining in current window
    - Time until reset
    - Total requests/throttled counts
    """
    limiter = get_provider_rate_limiter()
    status = limiter.get_status(provider)

    return {
        "success": True,
        "data": status,
        "meta": {
            "timestamp": datetime.now(UTC).isoformat(),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Provider Management (PRD 3.7.4-5)
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/api/v1/admin/providers", response_model=SuccessResponse)
async def list_providers(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """List all registered providers with status.

    Returns:
    - Provider name and enabled state
    - Capabilities supported
    - Current health status
    """
    providers = []
    health_status = await registry.health_check_all()

    for name in registry.list_providers():
        config = registry.get_provider_config(name)
        health = health_status.get(name)

        providers.append(
            {
                "name": name,
                "enabled": config.enabled if config else True,
                "priority": config.priority if config else 50,
                "capabilities": list(registry.get_capabilities(name)),
                "health": {
                    "healthy": health.healthy if health else False,
                    "latency_ms": health.latency_ms if health else None,
                    "error": health.error if health else "unknown",
                },
            }
        )

    return {
        "success": True,
        "data": {
            "providers": providers,
            "count": len(providers),
        },
        "meta": {
            "timestamp": datetime.now(UTC).isoformat(),
        },
    }


@router.post("/api/v1/admin/providers/{name}/enable", response_model=SuccessResponse)
async def enable_provider(
    name: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Enable a provider for routing."""
    if not registry.has_provider(name):
        return {
            "success": False,
            "error": {"code": "GW-E4004", "message": f"Provider '{name}' not found"},
        }

    registry.set_provider_enabled(name, True)

    return {
        "success": True,
        "data": {"provider": name, "enabled": True},
        "meta": {"timestamp": datetime.now(UTC).isoformat()},
    }


@router.post("/api/v1/admin/providers/{name}/disable", response_model=SuccessResponse)
async def disable_provider(
    name: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Disable a provider from routing."""
    if not registry.has_provider(name):
        return {
            "success": False,
            "error": {"code": "GW-E4004", "message": f"Provider '{name}' not found"},
        }

    registry.set_provider_enabled(name, False)

    return {
        "success": True,
        "data": {"provider": name, "enabled": False},
        "meta": {"timestamp": datetime.now(UTC).isoformat()},
    }
