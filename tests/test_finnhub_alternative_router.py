from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.finnhub import alternative
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


class _FakeCache:
    def __init__(self, initial: dict[str, Any] | None = None) -> None:
        self._store: dict[str, Any] = initial or {}
        self.set_calls: list[tuple[str, Any, int]] = []

    async def get(self, key: str) -> Any | None:
        return self._store.get(key)

    async def set(self, key: str, value: Any, ttl: int) -> None:
        self._store[key] = value
        self.set_calls.append((key, value, ttl))


class _FakeProvider:
    def __init__(self) -> None:
        self.fda_calls = 0
        self.congress_calls: list[dict[str, Any]] = []

    async def get_fda_calendar(self) -> list[dict[str, Any]]:
        self.fda_calls += 1
        return [{"event": "approval"}]

    async def get_congress_trading(
        self,
        *,
        symbol: str | None,
        start: datetime | None,
        end: datetime | None,
    ) -> list[dict[str, Any]]:
        self.congress_calls.append({"symbol": symbol, "start": start, "end": end})
        return [{"symbol": symbol or "ALL"}]


@pytest.mark.asyncio
async def test_fda_calendar_emits_cache_hit_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = alternative.cache_key("finnhub:fda-calendar")
    cache = _FakeCache(initial={key: [{"event": "approval"}]})
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"finnhub": provider})
    cache_events: list[tuple[str, str, str]] = []

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(alternative, "record_route_cache", _record_route_cache)

    response = await alternative.get_fda_calendar(
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is True
    assert provider.fda_calls == 0
    assert cache_events == [("finnhub_fda_calendar", "hit", "finnhub")]


@pytest.mark.asyncio
async def test_congress_trading_emits_cache_miss_telemetry_and_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = _FakeCache()
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"finnhub": provider})
    cache_events: list[tuple[str, str, str]] = []

    async def _rate_limit(_provider_name: str, block: bool = False) -> None:
        return None

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(alternative, "require_provider_rate_limit", _rate_limit)
    monkeypatch.setattr(alternative, "record_route_cache", _record_route_cache)

    response = await alternative.get_congress_trading(
        symbol="AAPL",
        start=None,
        end=None,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is False
    assert provider.congress_calls == [{"symbol": "AAPL", "start": None, "end": None}]
    assert cache.set_calls
    assert cache_events == [("finnhub_congress_trading", "miss", "finnhub")]
