"""Health check endpoints."""

import time
from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import APIRouter, Depends

from gateway import __version__
from gateway.api.deps import get_cache, get_connection_manager, get_sink_registry
from gateway.core.cache import InMemoryCache
from gateway.core.connections import ConnectionManager
from gateway.schemas import HealthResponse

logger = structlog.get_logger()

router = APIRouter(prefix="/health", tags=["health"])

_LAST_CACHE_ERROR_LOG: float = 0.0
_LAST_SINK_ERROR_LOG: float = 0.0


def _should_log(last_log: float, interval_seconds: float = 60.0) -> bool:
    return (time.time() - last_log) >= interval_seconds


@router.get("", response_model=HealthResponse)
async def liveness() -> dict[str, str]:
    """Liveness probe - always returns ok if server is running."""
    return {"status": "ok"}


@router.get("/ready")
async def readiness(
    cache: InMemoryCache = Depends(get_cache),
    connections: ConnectionManager = Depends(get_connection_manager),
) -> dict[str, Any]:
    """Readiness probe - checks if dependencies are ready."""
    checks = {
        "cache": "ok",
        "connections": "ok",
    }

    # Verify cache is operational
    try:
        await cache.set("__health_check__", True)
        if not await cache.get("__health_check__"):
            checks["cache"] = "error"
        cache.delete("__health_check__")
    except Exception:
        checks["cache"] = "error"
        global _LAST_CACHE_ERROR_LOG
        if _should_log(_LAST_CACHE_ERROR_LOG):
            _LAST_CACHE_ERROR_LOG = time.time()
            logger.exception("readiness_cache_check_failed")

    # Verify sink health (including circuit breaker state)
    sink_registry = get_sink_registry()
    if sink_registry:
        checks["sinks"] = "ok"
        try:
            sink_results = await sink_registry.health_check_all()
            if not all(sink_results.values()):
                checks["sinks"] = "error"
        except Exception:
            checks["sinks"] = "error"
            global _LAST_SINK_ERROR_LOG
            if _should_log(_LAST_SINK_ERROR_LOG):
                _LAST_SINK_ERROR_LOG = time.time()
                logger.exception("readiness_sink_check_failed")

    # All checks passed?
    all_ok = all(v == "ok" for v in checks.values())

    return {
        "status": "ready" if all_ok else "not_ready",
        "checks": checks,
    }


@router.get("/status")
async def detailed_status(
    cache: InMemoryCache = Depends(get_cache),
    connections: ConnectionManager = Depends(get_connection_manager),
) -> dict[str, Any]:
    """Detailed status with component health and stats."""
    return {
        "status": "ok",
        "version": __version__,
        "timestamp": datetime.now(UTC).isoformat(),
        "components": {
            "cache": {
                "status": "ok",
                "stats": cache.get_stats_dict(),
            },
            "connections": {
                "status": "ok",
                "stats": connections.get_stats(),
            },
        },
    }
