"""Health check endpoints."""

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends

from gateway import __version__
from gateway.api.deps import get_cache, get_connection_manager
from gateway.schemas import HealthResponse
from gateway.core.cache import InMemoryCache
from gateway.core.connections import ConnectionManager

router = APIRouter(prefix="/health", tags=["health"])


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
        cache.set("__health_check__", True)
        if not cache.get("__health_check__"):
            checks["cache"] = "error"
        cache.delete("__health_check__")
    except Exception:
        checks["cache"] = "error"

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
