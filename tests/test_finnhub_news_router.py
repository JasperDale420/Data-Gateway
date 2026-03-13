from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.finnhub import common as finnhub_common
from gateway.api.finnhub import news
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
        self.company_calls: list[dict[str, Any]] = []
        self.market_calls: list[dict[str, Any]] = []

    async def get_news(self, symbol: str, *, start: Any, end: Any) -> list[dict[str, Any]]:
        self.company_calls.append({"symbol": symbol, "start": start, "end": end})
        return [{"id": "n1"}]

    async def get_market_news(self, *, category: str) -> list[dict[str, Any]]:
        self.market_calls.append({"category": category})
        return [{"id": "m1"}]


@pytest.mark.asyncio
async def test_company_news_emits_cache_hit_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = news.cache_key("finnhub:news", "AAPL", None, None)
    cache = _FakeCache(initial={key: {"symbol": "AAPL", "articles": []}})
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"finnhub": provider})
    cache_events: list[tuple[str, str, str]] = []

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(news, "record_route_cache", _record_route_cache)

    response = await news.get_company_news(
        symbol="aapl",
        start=None,
        end=None,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is True
    assert provider.company_calls == []
    assert cache_events == [("finnhub_company_news", "hit", "finnhub")]


@pytest.mark.asyncio
async def test_market_news_emits_cache_miss_telemetry_and_caches(
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
    monkeypatch.setattr(news, "record_route_cache", _record_route_cache)

    response = await news.get_market_news(
        category="general",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is False
    assert provider.market_calls == [{"category": "general"}]
    assert cache.set_calls
    assert cache_events == [("finnhub_market_news", "miss", "finnhub")]
