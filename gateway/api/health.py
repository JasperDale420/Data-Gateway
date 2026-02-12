"""Health check endpoints."""

import inspect
import time
from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from gateway import __version__
from gateway.api.deps import get_cache, get_connection_manager, get_sink_registry
from gateway.core.cache import InMemoryCache
from gateway.core.connections import ConnectionManager
from gateway.core.shutdown import ShutdownCoordinator

logger = structlog.get_logger()

router = APIRouter(prefix="/health", tags=["health"])

_LAST_CACHE_ERROR_LOG: float = 0.0
_LAST_SINK_ERROR_LOG: float = 0.0


def _should_log(last_log: float, interval_seconds: float = 60.0) -> bool:
    return (time.time() - last_log) >= interval_seconds


@router.get("")
async def liveness():
    """Liveness probe - returns 503 during graceful shutdown (PRD §Graceful Shutdown)."""
    coord = ShutdownCoordinator.get_instance()
    if coord.is_shutting_down:
        return JSONResponse(
            status_code=503,
            content={
                "status": "shutting_down",
                "drain_remaining_seconds": round(coord.drain_remaining(), 1),
            },
        )
    return {"status": "ok"}


@router.get("/ready", response_model=None)
async def readiness(
    cache: InMemoryCache = Depends(get_cache),
    connections: ConnectionManager = Depends(get_connection_manager),
):
    """Readiness probe - returns 503 during shutdown, otherwise checks dependencies."""
    coord = ShutdownCoordinator.get_instance()
    if coord.is_shutting_down:
        return JSONResponse(
            status_code=503,
            content={
                "status": "shutting_down",
                "drain_remaining_seconds": round(coord.drain_remaining(), 1),
            },
        )
    checks = {
        "cache": "ok",
        "connections": "ok",
    }

    # Verify cache is operational
    try:
        await cache.set("__health_check__", True)
        if not await cache.get("__health_check__"):
            checks["cache"] = "error"
        delete_result = cache.delete("__health_check__")
        if inspect.isawaitable(delete_result):
            await delete_result
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
