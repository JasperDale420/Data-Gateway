"""Admin and status endpoints."""

import logging
from collections import deque
from datetime import UTC, datetime, timedelta
from heapq import nsmallest
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
from gateway.core.metrics import (
    get_provider_health_check_snapshot,
    get_provider_quote_batch_snapshot,
    get_stream_fanout_snapshot,
    get_stream_sink_dispatch_snapshot,
)
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
_status_section_stats_cache: (
    dict[tuple[bool, bool, bool, bool, bool], dict[str, dict[str, Any]]] | None
) = None
_status_section_stats_cache_at: dict[tuple[bool, bool, bool, bool, bool], datetime] | None = None
_stream_section_stats_cache: dict[tuple[bool, bool], dict[str, dict[str, Any]]] | None = None
_stream_section_stats_cache_at: dict[tuple[bool, bool], datetime] | None = None
_STATUS_SECTION_CACHE_MAX_ENTRIES = 32
_STREAM_SECTION_CACHE_MAX_ENTRIES = 16


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


def _status_section_cache_is_fresh(
    now: datetime,
    *,
    cache_key: tuple[bool, bool, bool, bool, bool],
    ttl_seconds: int,
) -> bool:
    """Whether optional status section stats cache is valid for reuse."""
    if (
        _status_section_stats_cache is None
        or _status_section_stats_cache_at is None
        or cache_key not in _status_section_stats_cache
        or cache_key not in _status_section_stats_cache_at
    ):
        return False
    return (now - _status_section_stats_cache_at[cache_key]).total_seconds() <= ttl_seconds


def _prune_stale_status_section_cache(*, now: datetime, ttl_seconds: int) -> None:
    """Prune stale optional-section cache entries to bound cache growth."""
    global _status_section_stats_cache, _status_section_stats_cache_at
    if (
        ttl_seconds <= 0
        or _status_section_stats_cache is None
        or _status_section_stats_cache_at is None
    ):
        return
    stale_keys = [
        key
        for key, cached_at in _status_section_stats_cache_at.items()
        if (now - cached_at).total_seconds() > ttl_seconds
    ]
    for key in stale_keys:
        _status_section_stats_cache.pop(key, None)
        _status_section_stats_cache_at.pop(key, None)


def _enforce_status_section_cache_limit(*, max_entries: int) -> None:
    """Evict oldest optional-section cache entries when entry limit is exceeded."""
    global _status_section_stats_cache, _status_section_stats_cache_at
    if _status_section_stats_cache is None or _status_section_stats_cache_at is None:
        return
    overflow = len(_status_section_stats_cache_at) - max(1, max_entries)
    if overflow <= 0:
        return
    oldest_keys = nsmallest(
        overflow, _status_section_stats_cache_at, key=_status_section_stats_cache_at.get
    )
    for key in oldest_keys:
        _status_section_stats_cache.pop(key, None)
        _status_section_stats_cache_at.pop(key, None)


def _load_optional_status_sections(
    *,
    registry: ProviderRegistry,
    cache: InMemoryCache,
    connections: ConnectionManager,
    include_cache_stats: bool,
    include_connection_stats: bool,
    include_registry_stats: bool,
    include_provider_health_checks: bool,
    include_provider_quote_batches: bool,
    status_section_cache_ttl_seconds: int,
    status_section_cache_max_entries: int,
    force_status_section_refresh: bool,
) -> tuple[dict, dict, dict, dict, dict, str, float | None]:
    """Load optional status sections with optional short-lived cache reuse."""
    global _status_section_stats_cache, _status_section_stats_cache_at

    if not (
        include_cache_stats
        or include_connection_stats
        or include_registry_stats
        or include_provider_health_checks
        or include_provider_quote_batches
    ):
        return {}, {}, {}, {}, {}, "skipped", None

    cache_key = (
        include_cache_stats,
        include_connection_stats,
        include_registry_stats,
        include_provider_health_checks,
        include_provider_quote_batches,
    )
    now = _utcnow()
    _prune_stale_status_section_cache(now=now, ttl_seconds=status_section_cache_ttl_seconds)
    if (
        status_section_cache_ttl_seconds > 0
        and not force_status_section_refresh
        and _status_section_cache_is_fresh(
            now,
            cache_key=cache_key,
            ttl_seconds=status_section_cache_ttl_seconds,
        )
    ):
        age_seconds = (
            (now - _status_section_stats_cache_at[cache_key]).total_seconds()
            if _status_section_stats_cache_at is not None
            and cache_key in _status_section_stats_cache_at
            else None
        )
        cached = (_status_section_stats_cache or {}).get(cache_key, {})
        return (
            cached.get("cache", {}) if include_cache_stats else {},
            cached.get("connections", {}) if include_connection_stats else {},
            cached.get("registry", {}) if include_registry_stats else {},
            (cached.get("provider_health_checks", {}) if include_provider_health_checks else {}),
            (cached.get("provider_quote_batches", {}) if include_provider_quote_batches else {}),
            "cache",
            age_seconds,
        )

    latest = {
        "cache": cache.get_stats_dict() if include_cache_stats else {},
        "connections": connections.get_stats() if include_connection_stats else {},
        "registry": registry.get_stats() if include_registry_stats else {},
        "provider_health_checks": (
            get_provider_health_check_snapshot() if include_provider_health_checks else {}
        ),
        "provider_quote_batches": (
            get_provider_quote_batch_snapshot() if include_provider_quote_batches else {}
        ),
    }
    if status_section_cache_ttl_seconds > 0:
        if _status_section_stats_cache is None:
            _status_section_stats_cache = {}
        if _status_section_stats_cache_at is None:
            _status_section_stats_cache_at = {}
        _status_section_stats_cache[cache_key] = latest
        _status_section_stats_cache_at[cache_key] = now
        _enforce_status_section_cache_limit(max_entries=status_section_cache_max_entries)

    return (
        latest["cache"] if include_cache_stats else {},
        latest["connections"] if include_connection_stats else {},
        latest["registry"] if include_registry_stats else {},
        latest["provider_health_checks"] if include_provider_health_checks else {},
        latest["provider_quote_batches"] if include_provider_quote_batches else {},
        "live",
        0.0 if status_section_cache_ttl_seconds > 0 else None,
    )


def _stream_section_cache_is_fresh(
    now: datetime,
    *,
    cache_key: tuple[bool, bool],
    ttl_seconds: int,
) -> bool:
    """Whether stream telemetry section cache is valid for reuse."""
    if (
        _stream_section_stats_cache is None
        or _stream_section_stats_cache_at is None
        or cache_key not in _stream_section_stats_cache
        or cache_key not in _stream_section_stats_cache_at
    ):
        return False
    return (now - _stream_section_stats_cache_at[cache_key]).total_seconds() <= ttl_seconds


def _prune_stale_stream_section_cache(*, now: datetime, ttl_seconds: int) -> None:
    """Prune stale stream-section cache entries to bound cache growth."""
    global _stream_section_stats_cache, _stream_section_stats_cache_at
    if (
        ttl_seconds <= 0
        or _stream_section_stats_cache is None
        or _stream_section_stats_cache_at is None
    ):
        return
    stale_keys = [
        key
        for key, cached_at in _stream_section_stats_cache_at.items()
        if (now - cached_at).total_seconds() > ttl_seconds
    ]
    for key in stale_keys:
        _stream_section_stats_cache.pop(key, None)
        _stream_section_stats_cache_at.pop(key, None)


def _enforce_stream_section_cache_limit(*, max_entries: int) -> None:
    """Evict oldest stream-section cache entries when entry limit is exceeded."""
    global _stream_section_stats_cache, _stream_section_stats_cache_at
    if _stream_section_stats_cache is None or _stream_section_stats_cache_at is None:
        return
    overflow = len(_stream_section_stats_cache_at) - max(1, max_entries)
    if overflow <= 0:
        return
    oldest_keys = nsmallest(
        overflow, _stream_section_stats_cache_at, key=_stream_section_stats_cache_at.get
    )
    for key in oldest_keys:
        _stream_section_stats_cache.pop(key, None)
        _stream_section_stats_cache_at.pop(key, None)


def _load_stream_status_sections(
    *,
    include_stream_sink_dispatch: bool,
    include_stream_fanout: bool,
    stream_section_cache_ttl_seconds: int,
    stream_section_cache_max_entries: int,
    force_stream_section_refresh: bool,
) -> tuple[dict, dict, str, float | None]:
    """Load optional stream telemetry sections with optional short-lived cache reuse."""
    global _stream_section_stats_cache, _stream_section_stats_cache_at

    if not (include_stream_sink_dispatch or include_stream_fanout):
        return {}, {}, "skipped", None

    cache_key = (include_stream_sink_dispatch, include_stream_fanout)
    now = _utcnow()
    _prune_stale_stream_section_cache(now=now, ttl_seconds=stream_section_cache_ttl_seconds)
    if (
        stream_section_cache_ttl_seconds > 0
        and not force_stream_section_refresh
        and _stream_section_cache_is_fresh(
            now,
            cache_key=cache_key,
            ttl_seconds=stream_section_cache_ttl_seconds,
        )
    ):
        age_seconds = (
            (now - _stream_section_stats_cache_at[cache_key]).total_seconds()
            if _stream_section_stats_cache_at is not None
            and cache_key in _stream_section_stats_cache_at
            else None
        )
        cached = (_stream_section_stats_cache or {}).get(cache_key, {})
        return (
            cached.get("stream_sink_dispatch", {}) if include_stream_sink_dispatch else {},
            cached.get("stream_fanout", {}) if include_stream_fanout else {},
            "cache",
            age_seconds,
        )

    latest = {
        "stream_sink_dispatch": (
            get_stream_sink_dispatch_snapshot() if include_stream_sink_dispatch else {}
        ),
        "stream_fanout": get_stream_fanout_snapshot() if include_stream_fanout else {},
    }
    if stream_section_cache_ttl_seconds > 0:
        if _stream_section_stats_cache is None:
            _stream_section_stats_cache = {}
        if _stream_section_stats_cache_at is None:
            _stream_section_stats_cache_at = {}
        _stream_section_stats_cache[cache_key] = latest
        _stream_section_stats_cache_at[cache_key] = now
        _enforce_stream_section_cache_limit(max_entries=stream_section_cache_max_entries)

    return (
        latest["stream_sink_dispatch"],
        latest["stream_fanout"],
        "live",
        0.0 if stream_section_cache_ttl_seconds > 0 else None,
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
    include_provider_health_checks: Annotated[
        bool,
        Query(description="Whether to include provider health-check telemetry summary"),
    ] = True,
    include_provider_quote_batches: Annotated[
        bool,
        Query(description="Whether to include provider multi-quote batch telemetry summary"),
    ] = True,
    include_stream_sink_dispatch: Annotated[
        bool,
        Query(description="Whether to include stream sink dispatch telemetry"),
    ] = True,
    include_stream_fanout: Annotated[
        bool,
        Query(description="Whether to include stream fanout telemetry"),
    ] = True,
    stream_section_cache_ttl_seconds: Annotated[
        int,
        Query(ge=0, description="TTL seconds for optional stream telemetry section cache"),
    ] = 0,
    stream_section_cache_max_entries: Annotated[
        int | None,
        Query(ge=1, description="Optional max entries for stream telemetry section cache"),
    ] = None,
    force_stream_section_refresh: Annotated[
        bool,
        Query(description="Bypass optional stream telemetry section cache for this request"),
    ] = False,
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
    status_section_cache_max_entries: Annotated[
        int | None,
        Query(ge=1, description="Optional max entries for optional status section stats cache"),
    ] = None,
    force_status_section_refresh: Annotated[
        bool,
        Query(description="Bypass optional status section stats cache for this request"),
    ] = False,
):
    """Get full system status including clients, providers, and subscriptions."""
    effective_status_cache_max_entries = (
        _STATUS_SECTION_CACHE_MAX_ENTRIES
        if status_section_cache_max_entries is None
        else max(1, status_section_cache_max_entries)
    )
    effective_stream_cache_max_entries = (
        _STREAM_SECTION_CACHE_MAX_ENTRIES
        if stream_section_cache_max_entries is None
        else max(1, stream_section_cache_max_entries)
    )
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
        provider_health_checks,
        provider_quote_batches,
        optional_stats_source,
        optional_stats_age_seconds,
    ) = _load_optional_status_sections(
        registry=registry,
        cache=cache,
        connections=connections,
        include_cache_stats=include_cache_stats,
        include_connection_stats=include_connection_stats,
        include_registry_stats=include_registry_stats,
        include_provider_health_checks=include_provider_health_checks,
        include_provider_quote_batches=include_provider_quote_batches,
        status_section_cache_ttl_seconds=status_section_cache_ttl_seconds,
        status_section_cache_max_entries=effective_status_cache_max_entries,
        force_status_section_refresh=force_status_section_refresh,
    )
    (
        stream_sink_dispatch,
        stream_fanout,
        stream_stats_source,
        stream_stats_age_seconds,
    ) = _load_stream_status_sections(
        include_stream_sink_dispatch=include_stream_sink_dispatch,
        include_stream_fanout=include_stream_fanout,
        stream_section_cache_ttl_seconds=stream_section_cache_ttl_seconds,
        stream_section_cache_max_entries=effective_stream_cache_max_entries,
        force_stream_section_refresh=force_stream_section_refresh,
    )

    data = {
        "providers": provider_status,
        "cache": cache_stats,
        "connections": connection_stats,
        "registry": registry_stats,
        "provider_health_checks": provider_health_checks,
        "provider_quote_batches": provider_quote_batches,
        "stream_sink_dispatch": stream_sink_dispatch,
        "stream_fanout": stream_fanout,
    }
    if include_provider_health_cache_metadata:
        data["provider_health_cache"] = provider_health_cache
    if include_status_sections:
        optional_cache_entries = (
            len(_status_section_stats_cache) if isinstance(_status_section_stats_cache, dict) else 0
        )
        stream_cache_entries = (
            len(_stream_section_stats_cache) if isinstance(_stream_section_stats_cache, dict) else 0
        )
        data["status_sections"] = {
            "cache": include_cache_stats,
            "connections": include_connection_stats,
            "registry": include_registry_stats,
            "provider_health_checks": include_provider_health_checks,
            "provider_quote_batches": include_provider_quote_batches,
            "stream_sink_dispatch": include_stream_sink_dispatch,
            "stream_fanout": include_stream_fanout,
            "optional_stats_source": optional_stats_source,
            "optional_stats_ttl_seconds": status_section_cache_ttl_seconds,
            "optional_stats_age_seconds": optional_stats_age_seconds,
            "optional_cache_entries": optional_cache_entries,
            "optional_cache_max_entries": effective_status_cache_max_entries,
            "stream_stats_source": stream_stats_source,
            "stream_stats_ttl_seconds": stream_section_cache_ttl_seconds,
            "stream_stats_age_seconds": stream_stats_age_seconds,
            "stream_cache_entries": stream_cache_entries,
            "stream_cache_max_entries": effective_stream_cache_max_entries,
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
