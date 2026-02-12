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
async def test_readiness_marks_cache_as_warming_up_when_redis_is_loading(
    monkeypatch: pytest.MonkeyPatch,
):
    cache = MagicMock()
    cache.set = AsyncMock(side_effect=RuntimeError("Redis is loading the dataset in memory"))
    cache.get = AsyncMock(return_value=None)
    cache.delete = AsyncMock(return_value=False)
    connections = MagicMock()

    monkeypatch.setattr(health_module, "get_sink_registry", lambda: None)

    response = await health_module.readiness(cache=cache, connections=connections)

    assert response["status"] == "not_ready"
    assert response["checks"]["cache"] == "warming_up"
