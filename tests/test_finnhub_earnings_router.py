from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.finnhub import earnings
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
        self.earnings_calls: list[dict[str, Any]] = []
        self.price_target_calls: list[str] = []

    async def get_earnings_calendar(
        self,
        *,
        start: datetime | None,
        end: datetime | None,
    ) -> list[dict[str, Any]]:
        self.earnings_calls.append({"start": start, "end": end})
        return [{"symbol": "AAPL"}]

    async def get_price_target(self, symbol: str) -> dict[str, Any]:
        self.price_target_calls.append(symbol)
        return {"symbol": symbol.upper(), "targetHigh": 250}


@pytest.mark.asyncio
async def test_earnings_calendar_emits_cache_hit_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = earnings.cache_key("finnhub:earnings", None, None)
    cache = _FakeCache(initial={key: [{"symbol": "AAPL"}]})
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"finnhub": provider})
    cache_events: list[tuple[str, str, str]] = []

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(earnings, "record_route_cache", _record_route_cache)

    response = await earnings.get_earnings_calendar(
        start=None,
        end=None,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is True
    assert provider.earnings_calls == []
    assert cache_events == [("finnhub_earnings_calendar", "hit", "finnhub")]


@pytest.mark.asyncio
async def test_price_target_emits_cache_miss_telemetry_and_caches(
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

    monkeypatch.setattr(earnings, "require_provider_rate_limit", _rate_limit)
    monkeypatch.setattr(earnings, "record_route_cache", _record_route_cache)

    response = await earnings.get_price_target(
        symbol="aapl",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is False
    assert provider.price_target_calls == ["aapl"]
    assert cache.set_calls
    assert cache_events == [("finnhub_price_target", "miss", "finnhub")]
