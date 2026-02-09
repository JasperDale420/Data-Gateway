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

    provider_health_checks_snapshot = {
        "finnhub": {
            "count": 3,
            "success_count": 3,
            "error_count": 0,
            "derived": {"error_rate": 0.0},
        }
    }
    provider_quote_batches_snapshot = {
        "alphavantage": {
            "count": 2,
            "total_symbols": 30,
            "max_batch_size": 20,
            "derived": {"avg_batch_size": 15.0},
        }
    }
    provider_health_checks_calls = {"count": 0}
    provider_quote_batches_calls = {"count": 0}

    def _get_provider_health_checks_snapshot() -> dict[str, Any]:
        provider_health_checks_calls["count"] += 1
        return provider_health_checks_snapshot

    def _get_provider_quote_batches_snapshot() -> dict[str, Any]:
        provider_quote_batches_calls["count"] += 1
        return provider_quote_batches_snapshot

    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", _get_sink_snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", _get_fanout_snapshot)
    monkeypatch.setattr(
        admin, "get_provider_health_check_snapshot", _get_provider_health_checks_snapshot
    )
    monkeypatch.setattr(
        admin, "get_provider_quote_batch_snapshot", _get_provider_quote_batches_snapshot
    )

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
    assert response["data"]["provider_health_checks"] == provider_health_checks_snapshot
    assert response["data"]["provider_quote_batches"] == provider_quote_batches_snapshot
    assert response["data"]["status_sections"]["cache"] is True
    assert response["data"]["status_sections"]["connections"] is True
    assert response["data"]["status_sections"]["registry"] is True
    assert response["data"]["status_sections"]["stream_sink_dispatch"] is True
    assert response["data"]["status_sections"]["stream_fanout"] is True
    assert response["data"]["status_sections"]["provider_health_checks"] is True
    assert response["data"]["status_sections"]["provider_quote_batches"] is True
    assert response["data"]["provider_health_cache"]["source"] == "live"
    assert registry.health_check_calls == 1
    assert registry.get_stats_calls == 1
    assert cache.get_stats_calls == 1
    assert connections.get_stats_calls == 1
    assert sink_calls["count"] == 1
    assert fanout_calls["count"] == 1
    assert provider_health_checks_calls["count"] == 1
    assert provider_quote_batches_calls["count"] == 1


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


@pytest.mark.asyncio
async def test_get_status_can_skip_provider_health_checks_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_health_checks_calls = {"count": 0}

    def _get_provider_health_checks_snapshot() -> dict[str, Any]:
        provider_health_checks_calls["count"] += 1
        return {"finnhub": {"count": 1}}

    monkeypatch.setattr(
        admin, "get_provider_health_check_snapshot", _get_provider_health_checks_snapshot
    )
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache_at", None)

    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_provider_health_checks=False,
    )

    assert response["success"] is True
    assert response["data"]["provider_health_checks"] == {}
    assert response["data"]["status_sections"]["provider_health_checks"] is False
    assert provider_health_checks_calls["count"] == 0


@pytest.mark.asyncio
async def test_get_status_can_skip_provider_quote_batches_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_quote_batches_calls = {"count": 0}

    def _get_provider_quote_batches_snapshot() -> dict[str, Any]:
        provider_quote_batches_calls["count"] += 1
        return {"alphavantage": {"count": 1}}

    monkeypatch.setattr(
        admin, "get_provider_quote_batch_snapshot", _get_provider_quote_batches_snapshot
    )
    monkeypatch.setattr(admin, "get_provider_health_check_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache_at", None)

    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_provider_quote_batches=False,
    )

    assert response["success"] is True
    assert response["data"]["provider_quote_batches"] == {}
    assert response["data"]["status_sections"]["provider_quote_batches"] is False
    assert provider_quote_batches_calls["count"] == 0


@pytest.mark.asyncio
async def test_get_status_can_return_minimal_provider_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink_snapshot = {"limits": {}, "events": {}, "pending_tasks": 0, "derived": {}}
    fanout_snapshot = {"limits": {}, "events": {}, "batches": {}, "derived": {}}
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: sink_snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: fanout_snapshot)
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)

    now = datetime(2026, 2, 9, 14, 0, tzinfo=UTC)
    monkeypatch.setattr(admin, "_utcnow", lambda: now)

    registry = _FakeRegistry()
    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
        include_provider_details=False,
    )

    assert response["success"] is True
    assert response["data"]["providers"] == {"finnhub": {"healthy": True}}
    assert response["data"]["provider_health_cache"]["payload_shape"] == "minimal"


@pytest.mark.asyncio
async def test_get_status_provider_payload_shape_tracks_cache_and_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink_snapshot = {"limits": {}, "events": {}, "pending_tasks": 0, "derived": {}}
    fanout_snapshot = {"limits": {}, "events": {}, "batches": {}, "derived": {}}
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: sink_snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: fanout_snapshot)
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)

    now = datetime(2026, 2, 9, 15, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    registry = _FakeRegistry()
    first = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
        include_provider_details=True,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
        include_provider_details=True,
    )

    assert first["data"]["provider_health_cache"]["source"] == "live"
    assert second["data"]["provider_health_cache"]["source"] == "cache"
    assert first["data"]["provider_health_cache"]["payload_shape"] == "detailed"
    assert second["data"]["provider_health_cache"]["payload_shape"] == "detailed"
    assert registry.health_check_calls == 1


@pytest.mark.asyncio
async def test_get_status_can_omit_status_metadata_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink_snapshot = {"limits": {}, "events": {}, "pending_tasks": 0, "derived": {}}
    fanout_snapshot = {"limits": {}, "events": {}, "batches": {}, "derived": {}}
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: sink_snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: fanout_snapshot)
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)

    now = datetime(2026, 2, 9, 16, 0, tzinfo=UTC)
    monkeypatch.setattr(admin, "_utcnow", lambda: now)

    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=True,
        include_status_sections=False,
        include_provider_health_cache_metadata=False,
    )

    assert response["success"] is True
    assert "status_sections" not in response["data"]
    assert "provider_health_cache" not in response["data"]


@pytest.mark.asyncio
async def test_get_status_reuses_optional_section_stats_within_cache_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink_snapshot = {"limits": {}, "events": {}, "pending_tasks": 0, "derived": {}}
    fanout_snapshot = {"limits": {}, "events": {}, "batches": {}, "derived": {}}
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: sink_snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: fanout_snapshot)
    provider_health_checks_calls = {"count": 0}

    def _get_provider_health_checks_snapshot() -> dict[str, Any]:
        provider_health_checks_calls["count"] += 1
        return {"finnhub": {"count": provider_health_checks_calls["count"]}}

    monkeypatch.setattr(
        admin, "get_provider_health_check_snapshot", _get_provider_health_checks_snapshot
    )
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache_at", None)

    now = datetime(2026, 2, 9, 17, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    registry = _FakeRegistry()
    cache = _FakeCache()
    connections = _FakeConnections()
    first = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, cache),
        connections=cast(ConnectionManager, connections),
        status_section_cache_ttl_seconds=5,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, cache),
        connections=cast(ConnectionManager, connections),
        status_section_cache_ttl_seconds=5,
    )

    assert first["data"]["status_sections"]["optional_stats_source"] == "live"
    assert second["data"]["status_sections"]["optional_stats_source"] == "cache"
    assert first["data"]["status_sections"]["optional_stats_ttl_seconds"] == 5
    assert second["data"]["status_sections"]["optional_stats_ttl_seconds"] == 5
    assert second["data"]["status_sections"]["optional_stats_age_seconds"] == 1.0
    assert registry.get_stats_calls == 1
    assert cache.get_stats_calls == 1
    assert connections.get_stats_calls == 1
    assert provider_health_checks_calls["count"] == 1


@pytest.mark.asyncio
async def test_get_status_force_refresh_bypasses_optional_section_stats_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink_snapshot = {"limits": {}, "events": {}, "pending_tasks": 0, "derived": {}}
    fanout_snapshot = {"limits": {}, "events": {}, "batches": {}, "derived": {}}
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: sink_snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: fanout_snapshot)
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache_at", None)

    now = datetime(2026, 2, 9, 18, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    registry = _FakeRegistry()
    cache = _FakeCache()
    connections = _FakeConnections()
    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, cache),
        connections=cast(ConnectionManager, connections),
        status_section_cache_ttl_seconds=5,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
        cache=cast(InMemoryCache, cache),
        connections=cast(ConnectionManager, connections),
        status_section_cache_ttl_seconds=5,
        force_status_section_refresh=True,
    )

    assert second["data"]["status_sections"]["optional_stats_source"] == "live"
    assert registry.get_stats_calls == 2
    assert cache.get_stats_calls == 2
    assert connections.get_stats_calls == 2


@pytest.mark.asyncio
async def test_get_status_reuses_provider_quote_batches_within_optional_cache_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache_at", None)

    quote_batch_calls = {"count": 0}

    def _get_provider_quote_batches_snapshot() -> dict[str, Any]:
        quote_batch_calls["count"] += 1
        return {"alphavantage": {"count": quote_batch_calls["count"]}}

    monkeypatch.setattr(
        admin, "get_provider_quote_batch_snapshot", _get_provider_quote_batches_snapshot
    )

    now = datetime(2026, 2, 9, 21, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    first = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=True,
        status_section_cache_ttl_seconds=5,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=True,
        status_section_cache_ttl_seconds=5,
    )

    assert first["data"]["status_sections"]["optional_stats_source"] == "live"
    assert second["data"]["status_sections"]["optional_stats_source"] == "cache"
    assert quote_batch_calls["count"] == 1


@pytest.mark.asyncio
async def test_get_status_force_refresh_bypasses_provider_quote_batches_optional_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache_at", None)

    quote_batch_calls = {"count": 0}

    def _get_provider_quote_batches_snapshot() -> dict[str, Any]:
        quote_batch_calls["count"] += 1
        return {"alphavantage": {"count": quote_batch_calls["count"]}}

    monkeypatch.setattr(
        admin, "get_provider_quote_batch_snapshot", _get_provider_quote_batches_snapshot
    )

    now = datetime(2026, 2, 9, 22, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=True,
        status_section_cache_ttl_seconds=5,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=True,
        status_section_cache_ttl_seconds=5,
        force_status_section_refresh=True,
    )

    assert second["data"]["status_sections"]["optional_stats_source"] == "live"
    assert quote_batch_calls["count"] == 2


@pytest.mark.asyncio
async def test_get_status_optional_cache_is_shape_aware_for_provider_quote_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache_at", None)

    quote_batch_calls = {"count": 0}

    def _get_provider_quote_batches_snapshot() -> dict[str, Any]:
        quote_batch_calls["count"] += 1
        return {"alphavantage": {"count": quote_batch_calls["count"]}}

    monkeypatch.setattr(
        admin, "get_provider_quote_batch_snapshot", _get_provider_quote_batches_snapshot
    )

    now = datetime(2026, 2, 9, 22, 30, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    first = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=False,
        status_section_cache_ttl_seconds=5,
    )
    assert first["data"]["status_sections"]["optional_stats_source"] == "live"
    assert quote_batch_calls["count"] == 0

    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=True,
        status_section_cache_ttl_seconds=5,
    )
    assert second["data"]["status_sections"]["optional_stats_source"] == "live"
    assert quote_batch_calls["count"] == 1

    time_ref["now"] = now + timedelta(seconds=2)
    third = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=True,
        status_section_cache_ttl_seconds=5,
    )
    assert third["data"]["status_sections"]["optional_stats_source"] == "cache"
    assert quote_batch_calls["count"] == 1


@pytest.mark.asyncio
async def test_get_status_reuses_stream_section_stats_within_cache_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink_calls = {"count": 0}
    fanout_calls = {"count": 0}

    def _get_sink_snapshot() -> dict[str, Any]:
        sink_calls["count"] += 1
        return {"limits": {}, "events": {}, "pending_tasks": sink_calls["count"], "derived": {}}

    def _get_fanout_snapshot() -> dict[str, Any]:
        fanout_calls["count"] += 1
        return {
            "limits": {},
            "events": {},
            "batches": {"count": fanout_calls["count"]},
            "derived": {},
        }

    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", _get_sink_snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", _get_fanout_snapshot)
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_stream_section_stats_cache", None)
    monkeypatch.setattr(admin, "_stream_section_stats_cache_at", None)

    now = datetime(2026, 2, 9, 19, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    first = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        stream_section_cache_ttl_seconds=5,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        stream_section_cache_ttl_seconds=5,
    )

    assert first["data"]["status_sections"]["stream_stats_source"] == "live"
    assert second["data"]["status_sections"]["stream_stats_source"] == "cache"
    assert second["data"]["status_sections"]["stream_stats_age_seconds"] == 1.0
    assert sink_calls["count"] == 1
    assert fanout_calls["count"] == 1


@pytest.mark.asyncio
async def test_get_status_force_refresh_bypasses_stream_section_stats_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink_calls = {"count": 0}
    fanout_calls = {"count": 0}

    def _get_sink_snapshot() -> dict[str, Any]:
        sink_calls["count"] += 1
        return {"limits": {}, "events": {}, "pending_tasks": sink_calls["count"], "derived": {}}

    def _get_fanout_snapshot() -> dict[str, Any]:
        fanout_calls["count"] += 1
        return {
            "limits": {},
            "events": {},
            "batches": {"count": fanout_calls["count"]},
            "derived": {},
        }

    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", _get_sink_snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", _get_fanout_snapshot)
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_stream_section_stats_cache", None)
    monkeypatch.setattr(admin, "_stream_section_stats_cache_at", None)

    now = datetime(2026, 2, 9, 20, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        stream_section_cache_ttl_seconds=5,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        stream_section_cache_ttl_seconds=5,
        force_stream_section_refresh=True,
    )

    assert second["data"]["status_sections"]["stream_stats_source"] == "live"
    assert sink_calls["count"] == 2
    assert fanout_calls["count"] == 2


@pytest.mark.asyncio
async def test_get_status_stream_cache_is_shape_aware_for_sink_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink_calls = {"count": 0}
    fanout_calls = {"count": 0}

    def _get_sink_snapshot() -> dict[str, Any]:
        sink_calls["count"] += 1
        return {"pending_tasks": sink_calls["count"], "events": {}, "limits": {}, "derived": {}}

    def _get_fanout_snapshot() -> dict[str, Any]:
        fanout_calls["count"] += 1
        return {
            "batches": {"count": fanout_calls["count"]},
            "events": {},
            "limits": {},
            "derived": {},
        }

    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", _get_sink_snapshot)
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", _get_fanout_snapshot)
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_stream_section_stats_cache", None)
    monkeypatch.setattr(admin, "_stream_section_stats_cache_at", None)

    now = datetime(2026, 2, 9, 23, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    first = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_stream_sink_dispatch=False,
        include_stream_fanout=True,
        stream_section_cache_ttl_seconds=5,
    )
    assert first["data"]["status_sections"]["stream_stats_source"] == "live"
    assert sink_calls["count"] == 0
    assert fanout_calls["count"] == 1

    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_stream_sink_dispatch=True,
        include_stream_fanout=True,
        stream_section_cache_ttl_seconds=5,
    )
    assert second["data"]["status_sections"]["stream_stats_source"] == "live"
    assert sink_calls["count"] == 1
    assert fanout_calls["count"] == 2

    time_ref["now"] = now + timedelta(seconds=2)
    third = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_stream_sink_dispatch=True,
        include_stream_fanout=True,
        stream_section_cache_ttl_seconds=5,
    )
    assert third["data"]["status_sections"]["stream_stats_source"] == "cache"
    assert sink_calls["count"] == 1
    assert fanout_calls["count"] == 2


@pytest.mark.asyncio
async def test_get_status_prunes_stale_optional_cache_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    status_cache: dict[tuple[bool, bool, bool, bool, bool], dict[str, dict[str, Any]]] = {}
    status_cache_at: dict[tuple[bool, bool, bool, bool, bool], datetime] = {}
    monkeypatch.setattr(admin, "_status_section_stats_cache", status_cache)
    monkeypatch.setattr(admin, "_status_section_stats_cache_at", status_cache_at)

    stale_key = (True, False, False, False, False)
    fresh_key = (False, True, False, False, False)
    now = datetime(2026, 2, 10, 0, 0, tzinfo=UTC)
    status_cache[stale_key] = {"cache": {"backend": "memory"}}
    status_cache[fresh_key] = {"connections": {"active_connections": 0}}
    status_cache_at[stale_key] = now - timedelta(seconds=10)
    status_cache_at[fresh_key] = now - timedelta(seconds=1)

    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_cache_stats=False,
        include_connection_stats=True,
        include_registry_stats=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=False,
        status_section_cache_ttl_seconds=5,
    )

    assert response["data"]["status_sections"]["optional_stats_source"] == "cache"
    assert stale_key not in (admin._status_section_stats_cache or {})
    assert stale_key not in (admin._status_section_stats_cache_at or {})
    assert fresh_key in (admin._status_section_stats_cache or {})


@pytest.mark.asyncio
async def test_get_status_reports_status_cache_entry_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(
        admin, "_status_section_stats_cache", {(True, True, True, True, True): {"cache": {}}}
    )
    monkeypatch.setattr(
        admin,
        "_status_section_stats_cache_at",
        {(True, True, True, True, True): datetime(2026, 2, 10, 0, 0, tzinfo=UTC)},
    )
    monkeypatch.setattr(
        admin,
        "_stream_section_stats_cache",
        {(True, True): {"stream_sink_dispatch": {}, "stream_fanout": {}}},
    )
    monkeypatch.setattr(
        admin,
        "_stream_section_stats_cache_at",
        {(True, True): datetime(2026, 2, 10, 0, 0, tzinfo=UTC)},
    )
    monkeypatch.setattr(admin, "_utcnow", lambda: datetime(2026, 2, 10, 0, 0, tzinfo=UTC))

    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
    )

    assert response["data"]["status_sections"]["optional_cache_entries"] == 1
    assert response["data"]["status_sections"]["stream_cache_entries"] == 1


@pytest.mark.asyncio
async def test_get_status_enforces_optional_cache_entry_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache_at", None)
    monkeypatch.setattr(admin, "_STATUS_SECTION_CACHE_MAX_ENTRIES", 2)

    now = datetime(2026, 2, 10, 0, 10, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_cache_stats=True,
        include_connection_stats=False,
        include_registry_stats=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=False,
        status_section_cache_ttl_seconds=60,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_cache_stats=False,
        include_connection_stats=True,
        include_registry_stats=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=False,
        status_section_cache_ttl_seconds=60,
    )
    time_ref["now"] = now + timedelta(seconds=2)
    third = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_cache_stats=False,
        include_connection_stats=False,
        include_registry_stats=True,
        include_provider_health_checks=False,
        include_provider_quote_batches=False,
        status_section_cache_ttl_seconds=60,
    )

    assert third["data"]["status_sections"]["optional_cache_entries"] == 2
    assert len(admin._status_section_stats_cache or {}) == 2
    assert (True, False, False, False, False) not in (admin._status_section_stats_cache or {})


@pytest.mark.asyncio
async def test_get_status_enforces_stream_cache_entry_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_stream_section_stats_cache", None)
    monkeypatch.setattr(admin, "_stream_section_stats_cache_at", None)
    monkeypatch.setattr(admin, "_STREAM_SECTION_CACHE_MAX_ENTRIES", 2)

    now = datetime(2026, 2, 10, 0, 20, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_stream_sink_dispatch=True,
        include_stream_fanout=False,
        stream_section_cache_ttl_seconds=60,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_stream_sink_dispatch=False,
        include_stream_fanout=True,
        stream_section_cache_ttl_seconds=60,
    )
    time_ref["now"] = now + timedelta(seconds=2)
    third = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_stream_sink_dispatch=True,
        include_stream_fanout=True,
        stream_section_cache_ttl_seconds=60,
    )

    assert third["data"]["status_sections"]["stream_cache_entries"] == 2
    assert len(admin._stream_section_stats_cache or {}) == 2
    assert (True, False) not in (admin._stream_section_stats_cache or {})


@pytest.mark.asyncio
async def test_get_status_reports_effective_cache_entry_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_STATUS_SECTION_CACHE_MAX_ENTRIES", 11)
    monkeypatch.setattr(admin, "_STREAM_SECTION_CACHE_MAX_ENTRIES", 7)
    monkeypatch.setattr(admin, "_utcnow", lambda: datetime(2026, 2, 10, 0, 30, tzinfo=UTC))

    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
    )

    assert response["data"]["status_sections"]["optional_cache_max_entries"] == 11
    assert response["data"]["status_sections"]["stream_cache_max_entries"] == 7


@pytest.mark.asyncio
async def test_get_status_can_override_cache_entry_limits_per_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache_at", None)

    now = datetime(2026, 2, 10, 0, 40, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_cache_stats=True,
        include_connection_stats=False,
        include_registry_stats=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=False,
        status_section_cache_ttl_seconds=60,
        status_section_cache_max_entries=1,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    second = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_cache_stats=False,
        include_connection_stats=True,
        include_registry_stats=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=False,
        status_section_cache_ttl_seconds=60,
        status_section_cache_max_entries=1,
    )

    assert second["data"]["status_sections"]["optional_cache_entries"] == 1
    assert second["data"]["status_sections"]["optional_cache_max_entries"] == 1
    assert len(admin._status_section_stats_cache or {}) == 1
    assert (True, False, False, False, False) not in (admin._status_section_stats_cache or {})


@pytest.mark.asyncio
async def test_get_status_optional_cache_evicts_oldest_shapes_when_overflow_is_multiple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_status_section_stats_cache", {})
    monkeypatch.setattr(admin, "_status_section_stats_cache_at", {})
    monkeypatch.setattr(admin, "_STATUS_SECTION_CACHE_MAX_ENTRIES", 2)

    now = datetime(2026, 2, 10, 0, 50, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_cache_stats=True,
        include_connection_stats=False,
        include_registry_stats=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=False,
        status_section_cache_ttl_seconds=60,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_cache_stats=False,
        include_connection_stats=True,
        include_registry_stats=False,
        include_provider_health_checks=False,
        include_provider_quote_batches=False,
        status_section_cache_ttl_seconds=60,
    )
    time_ref["now"] = now + timedelta(seconds=2)
    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_cache_stats=False,
        include_connection_stats=False,
        include_registry_stats=True,
        include_provider_health_checks=False,
        include_provider_quote_batches=False,
        status_section_cache_ttl_seconds=60,
    )
    time_ref["now"] = now + timedelta(seconds=3)
    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_cache_stats=False,
        include_connection_stats=False,
        include_registry_stats=False,
        include_provider_health_checks=True,
        include_provider_quote_batches=False,
        status_section_cache_ttl_seconds=60,
    )

    assert response["data"]["status_sections"]["optional_cache_entries"] == 2
    assert len(admin._status_section_stats_cache or {}) == 2
    assert (False, False, True, False, False) in (admin._status_section_stats_cache or {})
    assert (False, False, False, True, False) in (admin._status_section_stats_cache or {})


@pytest.mark.asyncio
async def test_get_status_stream_cache_evicts_oldest_shapes_when_overflow_is_multiple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: {})
    monkeypatch.setattr(admin, "get_stream_fanout_snapshot", lambda: {})
    monkeypatch.setattr(admin, "_provider_health_cache", None)
    monkeypatch.setattr(admin, "_provider_health_cache_at", None)
    monkeypatch.setattr(admin, "_stream_section_stats_cache", {})
    monkeypatch.setattr(admin, "_stream_section_stats_cache_at", {})
    monkeypatch.setattr(admin, "_STREAM_SECTION_CACHE_MAX_ENTRIES", 2)

    now = datetime(2026, 2, 10, 1, 0, tzinfo=UTC)
    time_ref = {"now": now}
    monkeypatch.setattr(admin, "_utcnow", lambda: time_ref["now"])

    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_stream_sink_dispatch=True,
        include_stream_fanout=False,
        stream_section_cache_ttl_seconds=60,
    )
    time_ref["now"] = now + timedelta(seconds=1)
    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_stream_sink_dispatch=False,
        include_stream_fanout=True,
        stream_section_cache_ttl_seconds=60,
    )
    time_ref["now"] = now + timedelta(seconds=2)
    await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_stream_sink_dispatch=True,
        include_stream_fanout=True,
        stream_section_cache_ttl_seconds=60,
    )
    time_ref["now"] = now + timedelta(seconds=3)
    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
        include_provider_health=False,
        include_stream_sink_dispatch=False,
        include_stream_fanout=False,
        stream_section_cache_ttl_seconds=60,
    )

    assert response["data"]["status_sections"]["stream_cache_entries"] == 2
    assert len(admin._stream_section_stats_cache or {}) == 2
    assert (False, True) in (admin._stream_section_stats_cache or {})
    assert (True, True) in (admin._stream_section_stats_cache or {})
