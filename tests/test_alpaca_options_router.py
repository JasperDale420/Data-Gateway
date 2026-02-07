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


class _FakeProvider:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def get_option_chain(self, **kwargs: Any) -> list[_ModelLike]:
        self.calls.append(kwargs)
        return [_ModelLike(i) for i in range(120)]


@pytest.mark.asyncio
async def test_option_chain_snapshot_threads_limit_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    registry = _FakeRegistry({"alpaca": provider})

    async def _fake_rate_limit(provider_name: str) -> None:
        assert provider_name == "alpaca"

    monkeypatch.setattr(options, "require_provider_rate_limit", _fake_rate_limit)

    response = await options.get_option_chain_snapshot(
        underlying="aapl",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
    )

    assert len(provider.calls) == 1
    assert provider.calls[0]["underlying"] == "AAPL"
    assert provider.calls[0]["limit"] == 100
    assert response["meta"]["count"] == 120
    assert len(response["data"]["contracts"]) == 120
