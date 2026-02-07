from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.alpaca import stock
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

    async def get_trades(self, **kwargs: Any) -> list[_ModelLike]:
        self.calls.append(kwargs)
        return [_ModelLike(i) for i in range(12)]


@pytest.mark.asyncio
async def test_get_stock_trades_threads_limit_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    registry = _FakeRegistry({"alpaca": provider})
    start = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 13, 0, tzinfo=UTC)

    async def _fake_rate_limit(provider_name: str, block: bool = True) -> None:
        assert provider_name == "alpaca"
        assert block is True

    monkeypatch.setattr(stock, "require_provider_rate_limit", _fake_rate_limit)

    response = await stock.get_stock_trades(
        symbol="aapl",
        start=start,
        end=end,
        limit=5,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, registry),
    )

    assert len(provider.calls) == 1
    assert provider.calls[0]["symbols"] == ["AAPL"]
    assert provider.calls[0]["limit"] == 5
    assert response["meta"]["count"] == 12
    assert len(response["data"]["trades"]) == 12
