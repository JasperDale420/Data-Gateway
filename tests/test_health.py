"""Tests for health endpoints."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.api import health as health_module


def test_liveness_returns_ok(client):
    """GET /health returns status ok."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readiness_returns_ready(client):
    """GET /health/ready returns ready status."""
    response = client.get("/health/ready")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ready"
    assert "checks" in data
    assert data["checks"]["cache"] == "ok"


def test_status_returns_detailed_info(client):
    """GET /health/status returns detailed status."""
    response = client.get("/health/status")
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "ok"
    assert "version" in data
    assert "timestamp" in data
    assert "components" in data

    # Check cache stats
    assert "cache" in data["components"]
    assert data["components"]["cache"]["status"] == "ok"

    # Check connection stats
    assert "connections" in data["components"]
    assert data["components"]["connections"]["status"] == "ok"


def test_root_endpoint(client):
    """GET / returns gateway info."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()

    assert data["name"] == "Data Gateway"
    assert "version" in data
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_readiness_awaits_async_cache_delete(monkeypatch: pytest.MonkeyPatch):
    """Readiness should await async cache delete implementations (HybridCache)."""
    cache = MagicMock()
    cache.set = AsyncMock(return_value=None)
    cache.get = AsyncMock(return_value=True)
    cache.delete = AsyncMock(return_value=True)
    connections = MagicMock()

    monkeypatch.setattr(health_module, "get_sink_registry", lambda: None)

    response = await health_module.readiness(cache=cache, connections=connections)

    assert response["status"] == "ready"
    cache.delete.assert_awaited_once_with("__health_check__")


@pytest.mark.asyncio
async def test_readiness_treats_sink_failures_as_degraded_not_not_ready(
    monkeypatch: pytest.MonkeyPatch,
):
    cache = MagicMock()
    cache.set = AsyncMock(return_value=None)
    cache.get = AsyncMock(return_value=True)
    cache.delete = AsyncMock(return_value=True)
    connections = MagicMock()

    class _UnhealthySinkRegistry:
        async def health_check_all(self) -> dict[str, bool]:
            return {"redis_streams": False}

    monkeypatch.setattr(health_module, "get_sink_registry", lambda: _UnhealthySinkRegistry())

    response = await health_module.readiness(cache=cache, connections=connections)

    assert response["status"] == "ready"
    assert response["checks"]["sinks"] == "degraded"


@pytest.mark.asyncio
async def test_status_includes_data_sink_when_registry_configured(
    monkeypatch: pytest.MonkeyPatch,
):
    """GET /health/status should include data_sink in components when sink registry is configured."""
    cache = MagicMock()
    cache.get_stats_dict = MagicMock(return_value={"hits": 0, "misses": 0})
    connections = MagicMock()
    connections.get_stats = MagicMock(return_value={})

    class _HealthySinkRegistry:
        async def health_check_all(self) -> dict[str, bool]:
            return {"redis_streams": True, "heber": True}

        def get_backpressure_snapshot(self) -> dict:
            return {
                "queue_size": 10,
                "worker_count": 2,
                "producer_block_timeout_seconds": 0.1,
                "sinks": {"redis_streams": {"queue_depth": 3, "queue_utilization": 0.25}},
                "publish_stats": {"queued": 4, "dropped_producer_timeout": 0, "by_source_feed": {}},
            }

    monkeypatch.setattr(health_module, "get_sink_registry", lambda: _HealthySinkRegistry())

    response = await health_module.detailed_status(cache=cache, connections=connections)

    assert "data_sink" in response["components"]
    assert response["components"]["data_sink"]["status"] == "ok"
    assert response["components"]["data_sink"]["sinks"] == {"redis_streams": "ok", "heber": "ok"}
    assert response["components"]["data_sink"]["backpressure"]["queue_size"] == 10
    assert response["components"]["data_sink"]["backpressure"]["sinks"]["redis_streams"]["queue_depth"] == 3


@pytest.mark.asyncio
async def test_status_data_sink_shows_degraded_when_unhealthy(
    monkeypatch: pytest.MonkeyPatch,
):
    """data_sink should show degraded status when a sink is unhealthy."""
    cache = MagicMock()
    cache.get_stats_dict = MagicMock(return_value={})
    connections = MagicMock()
    connections.get_stats = MagicMock(return_value={})

    class _UnhealthySinkRegistry:
        async def health_check_all(self) -> dict[str, bool]:
            return {"redis_streams": False}

    monkeypatch.setattr(health_module, "get_sink_registry", lambda: _UnhealthySinkRegistry())

    response = await health_module.detailed_status(cache=cache, connections=connections)

    assert response["components"]["data_sink"]["status"] == "degraded"
    assert response["components"]["data_sink"]["sinks"]["redis_streams"] == "degraded"


@pytest.mark.asyncio
async def test_readiness_marks_cache_as_warming_up_when_redis_is_loading(
    monkeypatch: pytest.MonkeyPatch,
):
    """Cache-warming readiness must return HTTP 503 (orchestrators read the
    status code, not the body). The body still describes the failure shape."""
    import json as _json

    from fastapi.responses import JSONResponse as _JSONResponse

    cache = MagicMock()
    cache.set = AsyncMock(side_effect=RuntimeError("Redis is loading the dataset in memory"))
    cache.get = AsyncMock(return_value=None)
    cache.delete = AsyncMock(return_value=False)
    connections = MagicMock()

    monkeypatch.setattr(health_module, "get_sink_registry", lambda: None)

    response = await health_module.readiness(cache=cache, connections=connections)

    # Not-ready must be a 503 JSONResponse so k8s/ALB/docker healthchecks
    # route traffic away from the unhealthy instance.
    assert isinstance(response, _JSONResponse), (
        "readiness handler must return JSONResponse with status 503 when "
        "not_ready so orchestrators see the right status code"
    )
    assert response.status_code == 503
    body = _json.loads(response.body)
    assert body["status"] == "not_ready"
    assert body["checks"]["cache"] == "warming_up"
