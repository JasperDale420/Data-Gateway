from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.alpaca import forex
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


class _FakeProvider:
    def __init__(self) -> None:
        self.rates_calls: list[dict[str, Any]] = []
        self.historical_calls: list[dict[str, Any]] = []

    async def get_forex_rates(self, *, pairs: list[str]) -> dict[str, dict[str, float]]:
        self.rates_calls.append({"pairs": pairs})
        return {"rates": {"EUR/USD": 1.1, "GBP/USD": 1.2}}

    async def get_forex_rates_historical(self, **kwargs: Any) -> dict[str, Any]:
        self.historical_calls.append(kwargs)
        return {"series": [{"pair": "EUR/USD", "close": 1.1}]}


@pytest.mark.asyncio
async def test_get_forex_rates_threads_pairs_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(
        *, registry: ProviderRegistry, provider_call: Any, block: bool = False
    ):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is False
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(forex, "execute_alpaca_provider_call", _execute_alpaca_call)

    response = await forex.get_forex_rates(
        pairs="eur/usd, gbp/usd",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert len(provider.rates_calls) == 1
    assert provider.rates_calls[0]["pairs"] == ["EUR/USD", "GBP/USD"]
    assert response["meta"]["count"] == 2
    assert response["data"]["rates"]["EUR/USD"] == 1.1


@pytest.mark.asyncio
async def test_get_forex_rates_historical_threads_query_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 2, tzinfo=UTC)

    async def _execute_alpaca_call(
        *, registry: ProviderRegistry, provider_call: Any, block: bool = False
    ):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is False
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(forex, "execute_alpaca_provider_call", _execute_alpaca_call)

    response = await forex.get_forex_rates_historical(
        pairs="eur/usd, gbp/usd",
        timeframe="1Day",
        start=start,
        end=end,
        limit=250,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert len(provider.historical_calls) == 1
    assert provider.historical_calls[0]["pairs"] == ["EUR/USD", "GBP/USD"]
    assert provider.historical_calls[0]["timeframe"] == "1Day"
    assert provider.historical_calls[0]["start"] == start
    assert provider.historical_calls[0]["end"] == end
    assert provider.historical_calls[0]["limit"] == 250
    assert response["meta"]["provider"] == "alpaca"
