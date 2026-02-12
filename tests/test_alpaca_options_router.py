from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.alpaca import options
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


class _ModelLike:
    def __init__(self, idx: int) -> None:
        self.idx = idx

    def model_dump(self, mode: str = "json") -> dict[str, int]:
        assert mode == "json"
        return {"idx": self.idx}


class _QuoteModelLike:
    def __init__(self, symbol: str, idx: int) -> None:
        self.symbol = symbol
        self.idx = idx

    def model_dump(self, mode: str = "json") -> dict[str, int]:
        assert mode == "json"
        return {"idx": self.idx}


class _FakeProvider:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def get_option_chain(self, **kwargs: Any) -> list[_ModelLike]:
        self.calls.append(kwargs)
        return [_ModelLike(i) for i in range(120)]

    async def get_option_quotes(self, contracts: list[str]) -> list[_ModelLike]:
        self.calls.append({"contracts": contracts})
        return [_QuoteModelLike(symbol=symbol, idx=i) for i, symbol in enumerate(contracts)]


@pytest.mark.asyncio
async def test_option_chain_snapshot_threads_limit_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is True
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(options, "execute_alpaca_provider_call", _execute_alpaca_call)

    response = await options.get_option_chain_snapshot(
        underlying="aapl",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert len(provider.calls) == 1
    assert provider.calls[0]["underlying"] == "AAPL"
    assert provider.calls[0]["limit"] == 100
    assert response["meta"]["count"] == 120
    assert len(response["data"]["contracts"]) == 120


@pytest.mark.asyncio
async def test_option_quotes_batch_contract_supports_symbols_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is True
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(options, "execute_alpaca_provider_call", _execute_alpaca_call)

    response = await options.get_option_quotes_batch(
        symbols="AAPL250117C00200000,MSFT250117P00400000",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.calls[-1]["contracts"] == ["AAPL250117C00200000", "MSFT250117P00400000"]
    assert response["success"] is True
    assert response["meta"]["count"] == 2
    assert set(response["data"]["quotes"].keys()) == {"AAPL250117C00200000", "MSFT250117P00400000"}
