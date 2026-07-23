"""Health check endpoints."""

import inspect
import time
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from gateway import __version__
from gateway.api.deps import get_cache, get_connection_manager, get_sink_registry
from gateway.config import get_settings
from gateway.core.cache import InMemoryCache
from gateway.core.connections import ConnectionManager
from gateway.core.globals import get_multiplexer
from gateway.core.logger import logger
from gateway.core.shutdown import ShutdownCoordinator
from gateway.core.stream import AlpacaStreamType

router = APIRouter(prefix="/health", tags=["health"])

_LAST_CACHE_ERROR_LOG: float = 0.0
_LAST_SINK_ERROR_LOG: float = 0.0


def _should_log(last_log: float, interval_seconds: float = 60.0) -> bool:
    return (time.time() - last_log) >= interval_seconds


def _is_redis_loading_error(error: Exception) -> bool:
    return "loading the dataset in memory" in str(error).lower()


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
    checks: dict[str, Any] = {
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
    except Exception as e:
        checks["cache"] = "warming_up" if _is_redis_loading_error(e) else "error"
        global _LAST_CACHE_ERROR_LOG
        if _should_log(_LAST_CACHE_ERROR_LOG):
            _LAST_CACHE_ERROR_LOG = time.time()
            if _is_redis_loading_error(e):
                logger.warning("readiness_cache_warming_up", error=str(e))
            else:
                logger.exception("readiness_cache_check_failed")

    # Verify sink health (including circuit breaker state)
    sink_registry = get_sink_registry()
    if sink_registry:
        checks["sinks"] = "ok"
        try:
            sink_results = await sink_registry.health_check_all()
            if not all(sink_results.values()):
                checks["sinks"] = "degraded"
        except Exception:
            checks["sinks"] = "degraded"
            global _LAST_SINK_ERROR_LOG
            if _should_log(_LAST_SINK_ERROR_LOG):
                _LAST_SINK_ERROR_LOG = time.time()
                logger.exception("readiness_sink_check_failed")

    # Verify each eagerly-configured upstream stream is connected + authenticated.
    # Trading bots poll /health/ready before opening their own WS at market open;
    # we must NOT report "ready" until the upstream Alpaca connection is hot, or
    # the first 9:30 ET subscribe will pay the cold-start cost we're trying to
    # eliminate. Lazy-only streams are not part of the readiness contract.
    settings = get_settings()
    eager_types = [t.strip().lower() for t in (settings.stream_eager_connect_types or "").split(",") if t.strip()]
    streams_status: dict[str, str] = {}
    if eager_types:
        try:
            mux = get_multiplexer()
        except RuntimeError:
            mux = None  # multiplexer_skipped (no Alpaca creds) — not blocking
        if mux is not None:
            stocks_type = AlpacaStreamType.STOCKS_IEX if settings.stream_use_iex else AlpacaStreamType.STOCKS_SIP
            type_map = {
                "stocks": stocks_type,
                "stocks_sip": AlpacaStreamType.STOCKS_SIP,
                "stocks_iex": AlpacaStreamType.STOCKS_IEX,
                "options": AlpacaStreamType.OPTIONS,
                "crypto": AlpacaStreamType.CRYPTO,
                "news": AlpacaStreamType.NEWS,
            }
            for name in eager_types:
                stream_type = type_map.get(name)
                if stream_type is None:
                    streams_status[name] = "unknown"
                    continue
                streams_status[name] = "ok" if mux.is_stream_ready(stream_type) else "not_ready"
        if streams_status:
            checks["streams"] = streams_status

    streams_ok = all(s == "ok" for s in streams_status.values()) if streams_status else True

    # Cache + connection + eager-stream readiness gate request serving; sink failures are degraded.
    all_ok = checks["cache"] == "ok" and checks["connections"] == "ok" and streams_ok

    payload = {
        "status": "ready" if all_ok else "not_ready",
        "checks": checks,
    }
    # Container orchestrators (k8s, ECS, docker compose healthchecks, load
    # balancers) read the HTTP status code — not the body — to decide
    # whether an instance is ready. A not-ready instance returning 200
    # would be treated as ready and have traffic routed to it. Return 503
    # on any failed check so the orchestrator routes around the unhealthy
    # instance until readiness is restored. Body stays the same shape on
    # both status codes so clients that DO read the body see the per-check
    # breakdown consistently.
    if not all_ok:
        return JSONResponse(status_code=503, content=payload)
    return payload


@router.get("/status")
async def detailed_status(
    cache: InMemoryCache = Depends(get_cache),
    connections: ConnectionManager = Depends(get_connection_manager),
) -> dict[str, Any]:
    """Detailed status with component health and stats."""
    components: dict[str, Any] = {
        "cache": {
            "status": "ok",
            "stats": cache.get_stats_dict(),
        },
        "connections": {
            "status": "ok",
            "stats": connections.get_stats(),
        },
    }

    # Include data sink health if configured
    sink_registry = get_sink_registry()
    if sink_registry:
        sink_backpressure: dict[str, Any] = {}
        get_backpressure_snapshot = getattr(sink_registry, "get_backpressure_snapshot", None)
        if callable(get_backpressure_snapshot):
            try:
                sink_backpressure = get_backpressure_snapshot()
            except Exception:
                logger.exception("health_data_sink_backpressure_snapshot_failed")
        try:
            sink_results = await sink_registry.health_check_all()
            all_healthy = all(sink_results.values())
            components["data_sink"] = {
                "status": "ok" if all_healthy else "degraded",
                "sinks": {name: "ok" if healthy else "degraded" for name, healthy in sink_results.items()},
                "backpressure": sink_backpressure,
            }
        except Exception:
            components["data_sink"] = {
                "status": "degraded",
                "backpressure": sink_backpressure,
            }

    return {
        "status": "ok",
        "version": __version__,
        "timestamp": datetime.now(UTC).isoformat(),
        "components": components,
    }
