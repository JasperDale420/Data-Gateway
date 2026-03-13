from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.finnhub import analysis
from gateway.api.finnhub import common as finnhub_common
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
        self.insider_calls: list[str] = []
        self.pattern_calls: list[tuple[str, str]] = []

    async def get_insider_sentiment(self, symbol: str) -> dict[str, Any]:
        self.insider_calls.append(symbol)
        return {"symbol": symbol.upper(), "sentiment": []}

    async def get_pattern_recognition(
        self,
        symbol: str,
        *,
        resolution: str,
    ) -> dict[str, Any]:
        self.pattern_calls.append((symbol, resolution))
        return {"symbol": symbol.upper(), "patterns": []}


@pytest.mark.asyncio
async def test_insider_sentiment_emits_cache_hit_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = analysis.cache_key("finnhub:insider-sentiment", "AAPL")
    cache = _FakeCache(initial={key: {"symbol": "AAPL", "sentiment": []}})
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"finnhub": provider})
    cache_events: list[tuple[str, str, str]] = []

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(analysis, "record_route_cache", _record_route_cache)

    response = await analysis.get_insider_sentiment(
        symbol="aapl",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is True
    assert provider.insider_calls == []
    assert cache_events == [("finnhub_insider_sentiment", "hit", "finnhub")]


@pytest.mark.asyncio
async def test_pattern_recognition_emits_cache_miss_telemetry_and_caches(
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

    monkeypatch.setattr(finnhub_common, "require_provider_rate_limit", _rate_limit)
    monkeypatch.setattr(analysis, "record_route_cache", _record_route_cache)

    response = await analysis.get_pattern_recognition(
        symbol="aapl",
        resolution="D",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is False
    assert provider.pattern_calls == [("aapl", "D")]
    assert cache.set_calls
    assert cache_events == [("finnhub_pattern_recognition", "miss", "finnhub")]
