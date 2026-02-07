from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import HTTPException

from gateway.api.alpaca import screener
from gateway.core.cache import InMemoryCache
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


class _ModelLike:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def model_dump(self, mode: str = "json") -> dict[str, Any]:
        assert mode == "json"
        return self._payload


class _FakeProvider:
    def __init__(self) -> None:
        self.most_actives_calls: list[dict[str, Any]] = []
        self.movers_calls: list[dict[str, Any]] = []

    async def get_most_actives(self, **kwargs: Any) -> list[_ModelLike]:
        self.most_actives_calls.append(kwargs)
        return [_ModelLike({"symbol": "AAPL"})]

    async def get_movers(self, **kwargs: Any) -> dict[str, list[_ModelLike]]:
        self.movers_calls.append(kwargs)
        return {
            "gainers": [_ModelLike({"symbol": "AAPL"})],
            "losers": [_ModelLike({"symbol": "MSFT"})],
        }


@pytest.mark.asyncio
async def test_get_most_actives_rejects_invalid_by_value() -> None:
    cache = InMemoryCache(max_size=32, default_ttl=60)
    with pytest.raises(HTTPException) as exc:
        await screener.get_most_actives(
            by="price",
            top=10,
            client=cast(Any, SimpleNamespace(id="test-client")),
            cache=cache,
            registry=cast(ProviderRegistry, _FakeRegistry({"alpaca": object()})),
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_get_most_actives_uses_shared_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    cache = InMemoryCache(max_size=32, default_ttl=60)
    observed: dict[str, Any] = {}

    async def _execute_alpaca_cached_call(
        **kwargs: Any,
    ):
        observed["key"] = kwargs["cache_key"]
        observed["route_label"] = kwargs["route_label"]
        provider_obj = kwargs["registry"].get("alpaca")
        return await kwargs["provider_call"](provider_obj)

    monkeypatch.setattr(screener, "execute_alpaca_cached_call", _execute_alpaca_cached_call)

    response = await screener.get_most_actives(
        by="volume",
        top=15,
        client=cast(Any, SimpleNamespace(id="test-client")),
        cache=cache,
        registry=cast(ProviderRegistry, route_registry),
    )

    assert observed["key"] == "alpaca:screener:most_actives:volume:15"
    assert observed["route_label"] == "alpaca_screener_most_actives"
    assert provider.most_actives_calls == [{"by": "volume", "top": 15}]
    assert response["meta"]["count"] == 1
    assert response["meta"]["by"] == "volume"


@pytest.mark.asyncio
async def test_get_movers_uses_shared_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    cache = InMemoryCache(max_size=32, default_ttl=60)
    observed: dict[str, Any] = {}

    async def _execute_alpaca_cached_call(
        **kwargs: Any,
    ):
        observed["key"] = kwargs["cache_key"]
        observed["route_label"] = kwargs["route_label"]
        provider_obj = kwargs["registry"].get("alpaca")
        return await kwargs["provider_call"](provider_obj)

    monkeypatch.setattr(screener, "execute_alpaca_cached_call", _execute_alpaca_cached_call)

    response = await screener.get_movers(
        market_type="stocks",
        top=12,
        client=cast(Any, SimpleNamespace(id="test-client")),
        cache=cache,
        registry=cast(ProviderRegistry, route_registry),
    )

    assert observed["key"] == "alpaca:screener:movers:stocks:12"
    assert observed["route_label"] == "alpaca_screener_movers"
    assert provider.movers_calls == [{"market_type": "stocks", "top": 12}]
    assert response["meta"]["gainers_count"] == 1
    assert response["meta"]["losers_count"] == 1
