from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.finnhub import common as finnhub_common
from gateway.api.finnhub import fundamentals
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
        self.profile_calls: list[str] = []
        self.insider_tx_calls: list[dict[str, Any]] = []

    async def get_company_profile(self, symbol: str) -> dict[str, Any]:
        self.profile_calls.append(symbol)
        return {"ticker": symbol.upper(), "name": "Apple Inc."}

    async def get_insider_transactions(
        self,
        symbol: str,
        *,
        start: datetime | None,
        end: datetime | None,
    ) -> list[dict[str, Any]]:
        self.insider_tx_calls.append({"symbol": symbol, "start": start, "end": end})
        return [{"name": "Insider"}]


@pytest.mark.asyncio
async def test_company_profile_emits_cache_hit_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = fundamentals.cache_key("finnhub:profile", "AAPL")
    cache = _FakeCache(initial={key: {"ticker": "AAPL", "name": "Apple Inc."}})
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"finnhub": provider})
    cache_events: list[tuple[str, str, str]] = []

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(fundamentals, "record_route_cache", _record_route_cache)

    response = await fundamentals.get_company_profile(
        symbol="aapl",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is True
    assert provider.profile_calls == []
    assert cache_events == [("finnhub_company_profile", "hit", "finnhub")]


@pytest.mark.asyncio
async def test_insider_transactions_emits_cache_miss_telemetry_and_caches(
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
    monkeypatch.setattr(fundamentals, "record_route_cache", _record_route_cache)

    response = await fundamentals.get_insider_transactions(
        symbol="aapl",
        start=None,
        end=None,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is False
    assert provider.insider_tx_calls == [{"symbol": "aapl", "start": None, "end": None}]
    assert cache.set_calls
    assert cache_events == [("finnhub_insider_transactions", "miss", "finnhub")]
