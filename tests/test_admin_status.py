from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api import admin
from gateway.core.auth import Client
from gateway.core.cache import InMemoryCache
from gateway.core.connections import ConnectionManager
from gateway.core.provider import HealthStatus
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self) -> None:
        self.health_check_calls = 0

    async def health_check_all(self) -> dict[str, HealthStatus]:
        self.health_check_calls += 1
        return {
            "finnhub": HealthStatus(
                healthy=True,
                error=None,
                latency_ms=12.0,
                last_check=datetime.now(UTC),
            )
        }

    def get_stats(self) -> dict[str, Any]:
        return {"total_providers": 1, "providers": ["finnhub"], "routes": []}


class _FakeCache:
    def get_stats_dict(self) -> dict[str, Any]:
        return {"backend": "memory", "size": 1}


class _FakeConnections:
    def get_stats(self) -> dict[str, Any]:
        return {"active_connections": 0}


@pytest.mark.asyncio
async def test_get_status_includes_stream_sink_dispatch_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = {
        "limits": {"max_inflight_publish": 32, "max_pending_tasks": 512},
        "pending_tasks": 3,
        "events": {"scheduled": 12, "completed": 9, "dropped_backpressure": 1},
    }
    fanout_snapshot = {
        "limits": {"max_inflight": 100, "batch_size": 32},
        "events": {"delivered": 1200, "error": 4},
        "batches": {"count": 100, "total_clients": 2300, "max_batch_size": 32},
    }
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: fanout_snapshot)

    registry = _FakeRegistry()

    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
    )

    assert response["success"] is True
    assert response["data"]["stream_sink_dispatch"] == snapshot
    assert response["data"]["stream_fanout"] == fanout_snapshot
    assert registry.health_check_calls == 1


@pytest.mark.asyncio
async def test_get_status_can_skip_provider_health_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = {
        "limits": {"max_inflight_publish": 32, "max_pending_tasks": 512},
        "pending_tasks": 1,
        "events": {"scheduled": 4, "completed": 4},
        "derived": {"pending_utilization": 0.0, "completion_rate": 1.0, "drop_rate": 0.0},
    }
    fanout_snapshot = {
        "limits": {"max_inflight": 100, "batch_size": 32},
        "events": {"delivered": 100, "error": 0},
        "batches": {"count": 4, "total_clients": 64, "max_batch_size": 16},
        "derived": {"avg_batch_size": 16.0, "batch_fill_ratio": 0.5, "error_rate": 0.0},
    }
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: fanout_snapshot)

    registry = _FakeRegistry()
    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
    )

    assert response["success"] is True
    assert response["data"]["providers"] == {}
    assert response["data"]["stream_sink_dispatch"]["derived"]["completion_rate"] == 1.0
    assert response["data"]["stream_fanout"]["derived"]["batch_fill_ratio"] == 0.5
    assert registry.health_check_calls == 0


@pytest.mark.asyncio
async def test_get_status_reuses_provider_health_within_cache_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = {
        "limits": {"max_inflight_publish": 32, "max_pending_tasks": 512},
        "pending_tasks": 1,
        "events": {"scheduled": 4, "completed": 4},
        "derived": {"pending_utilization": 0.0, "completion_rate": 1.0, "drop_rate": 0.0},
    }
    fanout_snapshot = {
        "limits": {"max_inflight": 100, "batch_size": 32},
        "events": {"delivered": 100, "error": 0},
        "batches": {"count": 4, "total_clients": 64, "max_batch_size": 16},
        "derived": {"avg_batch_size": 16.0, "batch_fill_ratio": 0.5, "error_rate": 0.0},
    }
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: fanout_snapshot)
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)

    now = datetime(2026, 2, 9, 10, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    registry = _FakeRegistry()
    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
    )

    assert registry.health_check_calls == 1


@pytest.mark.asyncio
async def test_get_status_force_refresh_bypasses_provider_health_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = {
        "limits": {"max_inflight_publish": 32, "max_pending_tasks": 512},
        "pending_tasks": 1,
        "events": {"scheduled": 4, "completed": 4},
        "derived": {"pending_utilization": 0.0, "completion_rate": 1.0, "drop_rate": 0.0},
    }
    fanout_snapshot = {
        "limits": {"max_inflight": 100, "batch_size": 32},
        "events": {"delivered": 100, "error": 0},
        "batches": {"count": 4, "total_clients": 64, "max_batch_size": 16},
        "derived": {"avg_batch_size": 16.0, "batch_fill_ratio": 0.5, "error_rate": 0.0},
    }
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: fanout_snapshot)
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)

    now = datetime(2026, 2, 9, 11, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    registry = _FakeRegistry()
    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
        force_provider_health_refresh=True,
    )

    assert registry.health_check_calls == 2
