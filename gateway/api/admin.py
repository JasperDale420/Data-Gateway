"""Admin and status endpoints."""

import logging
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

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
from gateway.core.metrics import get_stream_fanout_snapshot, get_stream_sink_dispatch_snapshot
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter(tags=["admin"])

# In-memory error log buffer
_error_buffer: deque[dict] = deque(maxlen=1000)
_error_counts: dict[str, int] = {}
_error_counts_reset: datetime = datetime.now(UTC)
_PROVIDER_HEALTH_CACHE_TTL_SECONDS = 5
_provider_health_cache: dict | None = None
_provider_health_cache_at: datetime | None = None
_provider_health_payload_cache_detailed: dict[str, dict[str, object]] | None = None
_provider_health_payload_cache_minimal: dict[str, dict[str, object]] | None = None
_status_section_stats_cache: dict[str, dict[str, Any]] | None = None
_status_section_stats_cache_at: datetime | None = None


def _utcnow() -> datetime:
    """Clock helper for status-related caching and testability."""
    return datetime.now(UTC)


def _provider_health_cache_is_fresh(now: datetime, *, ttl_seconds: int) -> bool:
    """Whether provider-health cache is valid for reuse."""
    if _provider_health_cache is None or _provider_health_cache_at is None:
        return False
    return (now - _provider_health_cache_at).total_seconds() <= ttl_seconds


async def _load_provider_health_status(
    *,
    registry: ProviderRegistry,
    include_provider_health: bool,
    include_provider_details: bool,
    force_provider_health_refresh: bool,
    provider_health_cache_ttl_seconds: int | None,
) -> tuple[dict, dict]:
    """Load provider health status with optional short-lived cache reuse."""
    global _provider_health_cache, _provider_health_cache_at
    global _provider_health_payload_cache_detailed, _provider_health_payload_cache_minimal
    ttl_seconds = (
        _PROVIDER_HEALTH_CACHE_TTL_SECONDS
        if provider_health_cache_ttl_seconds is None
        else max(0, provider_health_cache_ttl_seconds)
    )
    payload_shape = "detailed" if include_provider_details else "minimal"
    if not include_provider_health:
        return {}, {
            "source": "skipped",
            "ttl_seconds": ttl_seconds,
            "age_seconds": None,
            "payload_shape": payload_shape,
        }

    def _build_payload_caches(
        health_status: dict,
    ) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, object]]]:
        detailed: dict[str, dict[str, object]] = {}
        minimal: dict[str, dict[str, object]] = {}
        for name, status in health_status.items():
            healthy = bool(getattr(status, "healthy", False))
            minimal[name] = {"healthy": healthy}
            detailed[name] = {
                "healthy": healthy,
                "error": getattr(status, "error", None),
                "latency_ms": getattr(status, "latency_ms", None),
            }
        return detailed, minimal

    now = _utcnow()
    if not force_provider_health_refresh and _provider_health_cache_is_fresh(
        now,
        ttl_seconds=ttl_seconds,
    ):
        if (
            _provider_health_payload_cache_detailed is None
            or _provider_health_payload_cache_minimal is None
        ):
            detailed, minimal = _build_payload_caches(_provider_health_cache or {})
            _provider_health_payload_cache_detailed = detailed
            _provider_health_payload_cache_minimal = minimal
        age_seconds = (
            (now - _provider_health_cache_at).total_seconds()
            if _provider_health_cache_at is not None
            else None
        )
        payload = (
            _provider_health_payload_cache_detailed
            if include_provider_details
            else _provider_health_payload_cache_minimal
        ) or {}
        return payload, {
            "source": "cache",
            "ttl_seconds": ttl_seconds,
            "age_seconds": age_seconds,
            "payload_shape": payload_shape,
        }

    health = await registry.health_check_all()
    _provider_health_cache = health
    _provider_health_cache_at = now
    detailed, minimal = _build_payload_caches(health)
    _provider_health_payload_cache_detailed = detailed
    _provider_health_payload_cache_minimal = minimal
    payload = detailed if include_provider_details else minimal
    return payload, {
        "source": "live",
        "ttl_seconds": ttl_seconds,
        "age_seconds": 0.0,
        "payload_shape": payload_shape,
    }


def _status_section_cache_is_fresh(now: datetime, *, ttl_seconds: int) -> bool:
    """Whether optional status section stats cache is valid for reuse."""
    if _status_section_stats_cache is None or _status_section_stats_cache_at is None:
        return False
    return (now - _status_section_stats_cache_at).total_seconds() <= ttl_seconds


def _load_optional_status_sections(
    *,
    registry: ProviderRegistry,
    cache: InMemoryCache,
    connections: ConnectionManager,
    include_cache_stats: bool,
    include_connection_stats: bool,
    include_registry_stats: bool,
    status_section_cache_ttl_seconds: int,
    force_status_section_refresh: bool,
) -> tuple[dict, dict, dict, str, float | None]:
    """Load optional status sections with optional short-lived cache reuse."""
    global _status_section_stats_cache, _status_section_stats_cache_at

    if not (include_cache_stats or include_connection_stats or include_registry_stats):
        return {}, {}, {}, "skipped", None

    now = _utcnow()
    if (
        status_section_cache_ttl_seconds > 0
        and not force_status_section_refresh
        and _status_section_cache_is_fresh(now, ttl_seconds=status_section_cache_ttl_seconds)
    ):
        age_seconds = (
            (now - _status_section_stats_cache_at).total_seconds()
            if _status_section_stats_cache_at is not None
            else None
        )
        cached = _status_section_stats_cache or {}
        return (
            cached.get("cache", {}) if include_cache_stats else {},
            cached.get("connections", {}) if include_connection_stats else {},
            cached.get("registry", {}) if include_registry_stats else {},
            "cache",
            age_seconds,
        )

    latest = {
        "cache": cache.get_stats_dict(),
        "connections": connections.get_stats(),
        "registry": registry.get_stats(),
    }
    if status_section_cache_ttl_seconds > 0:
        _status_section_stats_cache = latest
        _status_section_stats_cache_at = now

    return (
        latest["cache"] if include_cache_stats else {},
        latest["connections"] if include_connection_stats else {},
        latest["registry"] if include_registry_stats else {},
        "live",
        0.0 if status_section_cache_ttl_seconds > 0 else None,
    )


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


class ErrorBufferHandler(logging.Handler):
    """Logging handler that records ERROR logs in the admin error buffer."""

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno < logging.ERROR:
            return

        event = record.__dict__.get("event")
        error_code = record.__dict__.get("error_code") or record.__dict__.get("code")
        if not error_code and isinstance(event, str) and event:
            error_code = event

        message = record.getMessage()
        if not message and isinstance(record.msg, str):
            message = record.msg

        log_error(
            error_code or "log_error",
            message or "unknown error",
            component=record.name,
        )


def attach_error_buffer_handler() -> None:
    """Attach the error buffer handler to the root logger once."""
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, ErrorBufferHandler):
            return

    handler = ErrorBufferHandler()
    handler.setLevel(logging.ERROR)
    root_logger.addHandler(handler)


@router.get("/api/v1/status", response_model=SuccessResponse)
async def get_status(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
    cache: InMemoryCache = Depends(get_cache),
    connections: ConnectionManager = Depends(get_connection_manager),
    include_provider_health: Annotated[
        bool,
        Query(description="Whether to run live provider health checks (may add latency)"),
    ] = True,
    include_provider_details: Annotated[
        bool,
        Query(description="Whether to include provider error and latency fields"),
    ] = True,
    force_provider_health_refresh: Annotated[
        bool,
        Query(description="Bypass short-lived provider health cache for this request"),
    ] = False,
    provider_health_cache_ttl_seconds: Annotated[
        int | None,
        Query(ge=0, description="Optional override for provider-health cache TTL seconds"),
    ] = None,
    include_cache_stats: Annotated[
        bool,
        Query(description="Whether to include cache stats in status response"),
    ] = True,
    include_connection_stats: Annotated[
        bool,
        Query(description="Whether to include connection stats in status response"),
    ] = True,
    include_registry_stats: Annotated[
        bool,
        Query(description="Whether to include registry stats in status response"),
    ] = True,
    include_stream_sink_dispatch: Annotated[
        bool,
        Query(description="Whether to include stream sink dispatch telemetry"),
    ] = True,
    include_stream_fanout: Annotated[
        bool,
        Query(description="Whether to include stream fanout telemetry"),
    ] = True,
    include_provider_health_cache_metadata: Annotated[
        bool,
        Query(description="Whether to include provider health cache metadata block"),
    ] = True,
    include_status_sections: Annotated[
        bool,
        Query(description="Whether to include status section inclusion metadata"),
    ] = True,
    status_section_cache_ttl_seconds: Annotated[
        int,
        Query(ge=0, description="TTL seconds for optional status section stats cache"),
    ] = 0,
    force_status_section_refresh: Annotated[
        bool,
        Query(description="Bypass optional status section stats cache for this request"),
    ] = False,
):
    """Get full system status including clients, providers, and subscriptions."""
    provider_status, provider_health_cache = await _load_provider_health_status(
        registry=registry,
        include_provider_health=include_provider_health,
        include_provider_details=include_provider_details,
        force_provider_health_refresh=force_provider_health_refresh,
        provider_health_cache_ttl_seconds=provider_health_cache_ttl_seconds,
    )

    (
        cache_stats,
        connection_stats,
        registry_stats,
        optional_stats_source,
        optional_stats_age_seconds,
    ) = _load_optional_status_sections(
        registry=registry,
        cache=cache,
        connections=connections,
        include_cache_stats=include_cache_stats,
        include_connection_stats=include_connection_stats,
        include_registry_stats=include_registry_stats,
        status_section_cache_ttl_seconds=status_section_cache_ttl_seconds,
        force_status_section_refresh=force_status_section_refresh,
    )
    stream_sink_dispatch = (
        get_stream_sink_dispatch_snapshot() if include_stream_sink_dispatch else {}
    )
    stream_fanout = get_stream_fanout_snapshot() if include_stream_fanout else {}

    data = {
        "providers": provider_status,
        "cache": cache_stats,
        "connections": connection_stats,
        "registry": registry_stats,
        "stream_sink_dispatch": stream_sink_dispatch,
        "stream_fanout": stream_fanout,
    }
    if include_provider_health_cache_metadata:
        data["provider_health_cache"] = provider_health_cache
    if include_status_sections:
        data["status_sections"] = {
            "cache": include_cache_stats,
            "connections": include_connection_stats,
            "registry": include_registry_stats,
            "stream_sink_dispatch": include_stream_sink_dispatch,
            "stream_fanout": include_stream_fanout,
            "optional_stats_source": optional_stats_source,
            "optional_stats_ttl_seconds": status_section_cache_ttl_seconds,
            "optional_stats_age_seconds": optional_stats_age_seconds,
        }

    return {
        "success": True,
        "data": data,
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
