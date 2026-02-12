from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.finnhub import crypto
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
        self.exchanges_calls: int = 0
        self.candles_calls: list[dict[str, Any]] = []

    async def get_crypto_exchanges(self) -> list[str]:
        self.exchanges_calls += 1
        return ["BINANCE", "COINBASE"]

    async def get_crypto_candles(
        self,
        symbol: str,
        *,
        resolution: str,
        start: datetime | None,
        end: datetime | None,
    ) -> dict[str, Any]:
        self.candles_calls.append(
            {"symbol": symbol, "resolution": resolution, "start": start, "end": end}
        )
        return {"symbol": symbol, "candles": []}


@pytest.mark.asyncio
async def test_crypto_exchanges_emits_cache_hit_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = crypto.cache_key("finnhub:crypto-exchanges")
    cache = _FakeCache(initial={key: ["BINANCE", "COINBASE"]})
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"finnhub": provider})
    cache_events: list[tuple[str, str, str]] = []

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(crypto, "record_route_cache", _record_route_cache)

    response = await crypto.get_crypto_exchanges(
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is True
    assert provider.exchanges_calls == 0
    assert cache_events == [("finnhub_crypto_exchanges", "hit", "finnhub")]


@pytest.mark.asyncio
async def test_crypto_candles_emits_cache_miss_telemetry_and_caches(
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

    monkeypatch.setattr(crypto, "require_provider_rate_limit", _rate_limit)
    monkeypatch.setattr(crypto, "record_route_cache", _record_route_cache)

    response = await crypto.get_crypto_candles(
        symbol="BINANCE:BTCUSDT",
        resolution="D",
        start=None,
        end=None,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
        cache=cast(Any, cache),
    )

    assert response["meta"]["cached"] is False
    assert provider.candles_calls == [
        {"symbol": "BINANCE:BTCUSDT", "resolution": "D", "start": None, "end": None}
    ]
    assert cache.set_calls
    assert cache_events == [("finnhub_crypto_candles", "miss", "finnhub")]
