from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.finnhub import common as finnhub_common
from gateway.api.finnhub import funds
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
        self.holdings_calls: list[str] = []
        self.sector_calls: list[str] = []

    async def get_mutual_fund_profile(self, symbol: str) -> dict[str, Any]:
        self.profile_calls.append(symbol)
        return {"symbol": symbol.upper(), "name": "Fund"}

    async def get_mutual_fund_holdings(self, symbol: str) -> dict[str, Any]:
        self.holdings_calls.append(symbol)
        return {"symbol": symbol.upper(), "holdings": []}

    async def get_mutual_fund_sector(self, symbol: str) -> dict[str, Any]:
        self.sector_calls.append(symbol)
        return {"symbol": symbol.upper(), "sectors": []}


@pytest.mark.asyncio
async def test_mutual_fund_profile_emits_cache_hit_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = funds.cache_key("finnhub:mf-profile", "VTSAX")
    cache = _FakeCache(initial={key: {"symbol": "VTSAX", "name": "Fund"}})
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"finnhub": provider})
    cache_events: list[tuple[str, str, str]] = []

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(funds, "record_route_cache", _record_route_cache)

    response = await funds.get_mutual_fund_profile(
        symbol="vtsax",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is True
    assert provider.profile_calls == []
    assert cache_events == [("finnhub_mutual_fund_profile", "hit", "finnhub")]


@pytest.mark.asyncio
async def test_mutual_fund_holdings_emits_cache_miss_telemetry_and_caches(
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
    monkeypatch.setattr(funds, "record_route_cache", _record_route_cache)

    response = await funds.get_mutual_fund_holdings(
        symbol="vtsax",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is False
    assert provider.holdings_calls == ["vtsax"]
    assert cache.set_calls
    assert cache_events == [("finnhub_mutual_fund_holdings", "miss", "finnhub")]


@pytest.mark.asyncio
async def test_mutual_fund_sector_emits_cache_miss_telemetry_and_caches(
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
    monkeypatch.setattr(funds, "record_route_cache", _record_route_cache)

    response = await funds.get_mutual_fund_sector(
        symbol="vtsax",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is False
    assert provider.sector_calls == ["vtsax"]
    assert cache.set_calls
    assert cache_events == [("finnhub_mutual_fund_sector", "miss", "finnhub")]
