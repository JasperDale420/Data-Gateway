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
        self.get_stats_calls = 0

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
        self.get_stats_calls += 1
        return {"total_providers": 1, "providers": ["finnhub"], "routes": []}


class _FakeCache:
    def __init__(self) -> None:
        self.get_stats_calls = 0

    def get_stats_dict(self) -> dict[str, Any]:
        self.get_stats_calls += 1
        return {"backend": "memory", "size": 1}


class _FakeConnections:
    def __init__(self) -> None:
        self.get_stats_calls = 0

    def get_stats(self) -> dict[str, Any]:
        self.get_stats_calls += 1
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
    sink_calls = {"count": 0}
    fanout_calls = {"count": 0}

    def _get_sink_snapshot() -> dict[str, Any]:
        sink_calls["count"] += 1
        return snapshot

    def _get_fanout_snapshot() -> dict[str, Any]:
        fanout_calls["count"] += 1
        return fanout_snapshot

    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", _get_sink_snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", _get_fanout_snapshot)

    registry = _FakeRegistry()
    cache = _FakeCache()
    connections = _FakeConnections()

    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, cache),
        connections=cast(ConnectionManager, connections),
    )

    assert response["success"] is True
    assert response["data"]["stream_sink_dispatch"] == snapshot
    assert response["data"]["stream_fanout"] == fanout_snapshot
    assert response["data"]["status_sections"]["cache"] is True
    assert response["data"]["status_sections"]["connections"] is True
    assert response["data"]["status_sections"]["registry"] is True
    assert response["data"]["status_sections"]["stream_sink_dispatch"] is True
    assert response["data"]["status_sections"]["stream_fanout"] is True
    assert registry.health_check_calls == 1
    assert registry.get_stats_calls == 1
    assert cache.get_stats_calls == 1
    assert connections.get_stats_calls == 1
    assert sink_calls["count"] == 1
    assert fanout_calls["count"] == 1


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


@pytest.mark.asyncio
async def test_get_status_reports_provider_health_cache_metadata(
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

    now = datetime(2026, 2, 9, 12, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    registry = _FakeRegistry()
    first = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
    )

    assert first["data"]["provider_health_cache"]["source"] == "live"
    assert first["data"]["provider_health_cache"]["ttl_seconds"] == 5
    assert first["data"]["provider_health_cache"]["age_seconds"] == 0.0
    assert second["data"]["provider_health_cache"]["source"] == "cache"
    assert second["data"]["provider_health_cache"]["ttl_seconds"] == 5
    assert second["data"]["provider_health_cache"]["age_seconds"] == 1.0
    assert registry.health_check_calls == 1


@pytest.mark.asyncio
async def test_get_status_provider_health_ttl_override_controls_cache_reuse(
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

    now = datetime(2026, 2, 9, 13, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    registry = _FakeRegistry()
    first = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
        provider_health_cache_ttl_seconds=0,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
        provider_health_cache_ttl_seconds=0,
    )

    assert first["data"]["provider_health_cache"]["source"] == "live"
    assert second["data"]["provider_health_cache"]["source"] == "live"
    assert first["data"]["provider_health_cache"]["ttl_seconds"] == 0
    assert second["data"]["provider_health_cache"]["ttl_seconds"] == 0
    assert registry.health_check_calls == 2


@pytest.mark.asyncio
async def test_get_status_can_skip_cache_connection_and_registry_sections(
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

    registry = _FakeRegistry()
    cache = _FakeCache()
    connections = _FakeConnections()
    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, cache),
        connections=cast(ConnectionManager, connections),
        include_provider_health=False,
        include_cache_stats=False,
        include_connection_stats=False,
        include_registry_stats=False,
    )

    assert response["success"] is True
    assert response["data"]["providers"] == {}
    assert response["data"]["cache"] == {}
    assert response["data"]["connections"] == {}
    assert response["data"]["registry"] == {}
    assert response["data"]["status_sections"]["cache"] is False
    assert response["data"]["status_sections"]["connections"] is False
    assert response["data"]["status_sections"]["registry"] is False
    assert registry.health_check_calls == 0
    assert registry.get_stats_calls == 0
    assert cache.get_stats_calls == 0
    assert connections.get_stats_calls == 0


@pytest.mark.asyncio
async def test_get_status_can_skip_stream_telemetry_sections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink_calls = {"count": 0}
    fanout_calls = {"count": 0}

    def _get_sink_snapshot() -> dict[str, Any]:
        sink_calls["count"] += 1
        return {"limits": {}, "events": {}, "pending_tasks": 0, "derived": {}}

    def _get_fanout_snapshot() -> dict[str, Any]:
        fanout_calls["count"] += 1
        return {"limits": {}, "events": {}, "batches": {}, "derived": {}}

    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", _get_sink_snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", _get_fanout_snapshot)
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)

    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_stream_sink_dispatch=False,
        include_stream_fanout=False,
    )

    assert response["success"] is True
    assert response["data"]["stream_sink_dispatch"] == {}
    assert response["data"]["stream_fanout"] == {}
    assert response["data"]["status_sections"]["stream_sink_dispatch"] is False
    assert response["data"]["status_sections"]["stream_fanout"] is False
    assert sink_calls["count"] == 0
    assert fanout_calls["count"] == 0
