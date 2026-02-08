from __future__ import annotations

from datetime import UTC, datetime
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
    async def health_check_all(self) -> dict[str, HealthStatus]:
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
    monkeypatch.setattr(admin, "get_stream_sink_dispatch_snapshot", lambda: snapshot)

    response = await admin.get_status(
        client=cast(Client, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry()),
        cache=cast(InMemoryCache, _FakeCache()),
        connections=cast(ConnectionManager, _FakeConnections()),
    )

    assert response["success"] is True
    assert response["data"]["stream_sink_dispatch"] == snapshot
