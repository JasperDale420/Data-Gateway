from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.alpaca import watchlists
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


class _FakeProvider:
    def __init__(self) -> None:
        self.create_calls: list[tuple[str, list[str] | None]] = []
        self.update_calls: list[tuple[str, str | None, list[str] | None]] = []
        self.delete_calls: list[str] = []
        self.add_asset_calls: list[tuple[str, str]] = []
        self.remove_asset_calls: list[tuple[str, str]] = []

    def get_watchlists(self) -> list[dict[str, str]]:
        return [{"id": "wl-1"}, {"id": "wl-2"}]

    def create_watchlist(self, name: str, symbols: list[str] | None = None) -> dict[str, Any]:
        self.create_calls.append((name, symbols))
        return {"id": "wl-3", "name": name, "symbols": symbols or []}

    def get_watchlist(self, watchlist_id: str) -> dict[str, str]:
        return {"id": watchlist_id}

    def update_watchlist(
        self, watchlist_id: str, name: str | None, symbols: list[str] | None
    ) -> dict[str, Any]:
        self.update_calls.append((watchlist_id, name, symbols))
        return {"id": watchlist_id, "name": name, "symbols": symbols or []}

    def delete_watchlist(self, watchlist_id: str) -> bool:
        self.delete_calls.append(watchlist_id)
        return True

    def add_asset_to_watchlist(self, watchlist_id: str, symbol: str) -> dict[str, str]:
        self.add_asset_calls.append((watchlist_id, symbol))
        return {"id": watchlist_id, "symbol": symbol}

    def remove_asset_from_watchlist(self, watchlist_id: str, symbol: str) -> dict[str, str]:
        self.remove_asset_calls.append((watchlist_id, symbol))
        return {"id": watchlist_id, "symbol": symbol}


def _helper_monkeypatch(
    monkeypatch: pytest.MonkeyPatch,
    *,
    route_registry: _FakeRegistry,
) -> None:
    async def _execute_alpaca_call(
        *, registry: ProviderRegistry, provider_call: Any, block: bool = False
    ):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is False
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(watchlists, "execute_alpaca_provider_call", _execute_alpaca_call)


@pytest.mark.asyncio
async def test_get_watchlists_uses_shared_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await watchlists.get_watchlists(
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert response["meta"]["count"] == 2
    assert response["data"][0]["id"] == "wl-1"


@pytest.mark.asyncio
async def test_create_watchlist_splits_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await watchlists.create_watchlist(
        name="Tech",
        symbols="AAPL,MSFT",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.create_calls == [("Tech", ["AAPL", "MSFT"])]
    assert response["data"]["id"] == "wl-3"


@pytest.mark.asyncio
async def test_update_watchlist_splits_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await watchlists.update_watchlist(
        watchlist_id="wl-1",
        name="Updated",
        symbols="NVDA,TSLA",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.update_calls == [("wl-1", "Updated", ["NVDA", "TSLA"])]
    assert response["data"]["id"] == "wl-1"


@pytest.mark.asyncio
async def test_delete_watchlist_returns_success_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await watchlists.delete_watchlist(
        watchlist_id="wl-1",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.delete_calls == ["wl-1"]
    assert response["success"] is True
    assert response["data"]["deleted"] is True


@pytest.mark.asyncio
async def test_add_and_remove_asset_use_shared_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    add_response = await watchlists.add_asset_to_watchlist(
        watchlist_id="wl-1",
        symbol="AAPL",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )
    remove_response = await watchlists.remove_asset_from_watchlist(
        watchlist_id="wl-1",
        symbol="AAPL",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.add_asset_calls == [("wl-1", "AAPL")]
    assert provider.remove_asset_calls == [("wl-1", "AAPL")]
    assert add_response["success"] is True
    assert remove_response["success"] is True
