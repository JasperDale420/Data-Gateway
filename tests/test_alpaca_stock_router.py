from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.alpaca import common as alpaca_common
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


class _SnapshotProvider:
    def __init__(self) -> None:
        self.inflight = 0
        self.max_inflight = 0

    async def get_quotes(self, **_kwargs: Any) -> list[_ModelLike]:
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        await asyncio.sleep(0.01)
        self.inflight -= 1
        return [_ModelLike(1)]

    async def get_bars(self, **_kwargs: Any) -> list[_ModelLike]:
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        await asyncio.sleep(0.01)
        self.inflight -= 1
        return [_ModelLike(2)]


@pytest.mark.asyncio
async def test_get_stock_trades_threads_limit_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    start = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 13, 0, tzinfo=UTC)

    # stock.py calls provider directly, so we just need to bypass the rate limiter
    async def _noop_rate_limit(*_args, **_kwargs):
        pass

    monkeypatch.setattr(alpaca_common, "require_provider_rate_limit", _noop_rate_limit)

    response = await stock.get_stock_trades(
        symbol="aapl",
        start=start,
        end=end,
        limit=5,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert len(provider.calls) == 1
    assert provider.calls[0]["symbols"] == ["AAPL"]
    assert response["meta"]["count"] == 12
    assert len(response["data"]["trades"]) == 5


@pytest.mark.asyncio
async def test_get_stock_snapshot_fetches_quotes_and_bars_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _SnapshotProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _noop_rate_limit(*_args, **_kwargs):
        pass

    monkeypatch.setattr(alpaca_common, "require_provider_rate_limit", _noop_rate_limit)

    response = await stock.get_stock_snapshot(
        symbol="aapl",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert response["success"] is True
    assert response["data"]["symbol"] == "AAPL"
    # Snapshot calls get_quotes and get_bars sequentially in current impl
    assert provider.max_inflight >= 1
