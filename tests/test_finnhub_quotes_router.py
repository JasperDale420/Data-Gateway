from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.finnhub import quotes
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


class _FakeQuote:
    def model_dump(self, mode: str = "python") -> dict[str, Any]:
        return {"symbol": "AAPL", "price": 100.0}


class _FakeBar:
    def model_dump(self, mode: str = "python") -> dict[str, Any]:
        return {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5}


class _FakeProvider:
    def __init__(self) -> None:
        self.quote_calls: list[str] = []
        self.bars_calls: list[dict[str, Any]] = []

    async def get_quote(self, symbol: str) -> _FakeQuote:
        self.quote_calls.append(symbol)
        return _FakeQuote()

    async def get_bars(
        self,
        symbol: str,
        *,
        resolution: str,
        start: Any,
        end: Any,
    ) -> list[_FakeBar]:
        self.bars_calls.append(
            {"symbol": symbol, "resolution": resolution, "start": start, "end": end}
        )
        return [_FakeBar()]


@pytest.mark.asyncio
async def test_quote_emits_cache_hit_telemetry(monkeypatch: pytest.MonkeyPatch) -> None:
    key = quotes.cache_key("finnhub:quote", "AAPL")
    cache = _FakeCache(initial={key: {"symbol": "AAPL", "price": 100.0}})
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"finnhub": provider})
    cache_events: list[tuple[str, str, str]] = []

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(quotes, "record_route_cache", _record_route_cache)

    response = await quotes.get_quote(
        symbol="aapl",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is True
    assert provider.quote_calls == []
    assert cache_events == [("finnhub_quote", "hit", "finnhub")]


@pytest.mark.asyncio
async def test_bars_emits_cache_miss_telemetry_and_caches(
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

    monkeypatch.setattr(quotes, "require_provider_rate_limit", _rate_limit)
    monkeypatch.setattr(quotes, "record_route_cache", _record_route_cache)

    response = await quotes.get_bars(
        symbol="aapl",
        resolution="D",
        start=None,
        end=None,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is False
    assert provider.bars_calls == [
        {"symbol": "aapl", "resolution": "D", "start": None, "end": None}
    ]
    assert cache.set_calls
    assert cache_events == [("finnhub_bars", "miss", "finnhub")]
