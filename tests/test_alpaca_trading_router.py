from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import HTTPException

from gateway.api.alpaca import trading
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


class _FakeProvider:
    def __init__(self) -> None:
        self.orders_calls: list[dict[str, Any]] = []
        self.cancel_calls: list[str] = []
        self.calendar_calls: list[tuple[date | None, date | None]] = []

    def get_account(self) -> dict[str, Any]:
        return {"status": "ACTIVE"}

    def create_order(self, **kwargs: Any) -> dict[str, Any]:
        return kwargs

    def get_orders(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.orders_calls.append(kwargs)
        return [{"id": "o-1"}, {"id": "o-2"}]

    def cancel_order(self, order_id: str) -> bool:
        self.cancel_calls.append(order_id)
        return True

    def get_calendar(self, start: date | None, end: date | None) -> list[dict[str, Any]]:
        self.calendar_calls.append((start, end))
        return [{"date": "2026-01-02"}]


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

    monkeypatch.setattr(trading, "execute_alpaca_provider_call", _execute_alpaca_call)


@pytest.mark.asyncio
async def test_get_account_uses_shared_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.get_account(
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert response["success"] is True
    assert response["data"]["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_create_order_maps_value_error_to_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ValueErrorProvider(_FakeProvider):
        def create_order(self, **kwargs: Any) -> dict[str, Any]:
            raise ValueError("invalid order")

    provider = _ValueErrorProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    with pytest.raises(HTTPException) as exc:
        await trading.create_order(
            symbol="AAPL",
            side="buy",
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, route_registry),
        )

    assert exc.value.status_code == 400
    assert exc.value.detail == "invalid order"


@pytest.mark.asyncio
async def test_get_orders_splits_symbols_and_sets_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.get_orders(
        status="open",
        limit=50,
        direction="desc",
        symbols="AAPL,MSFT",
        nested=True,
        side=None,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.orders_calls[0]["symbols"] == ["AAPL", "MSFT"]
    assert response["meta"]["count"] == 2


@pytest.mark.asyncio
async def test_cancel_order_returns_success_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)

    response = await trading.cancel_order(
        order_id="ord-1",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.cancel_calls == ["ord-1"]
    assert response["success"] is True
    assert response["data"]["cancelled"] is True


@pytest.mark.asyncio
async def test_get_calendar_threads_dates_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    _helper_monkeypatch(monkeypatch, route_registry=route_registry)
    start = date(2026, 1, 1)
    end = date(2026, 1, 31)

    response = await trading.get_calendar(
        start=start,
        end=end,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.calendar_calls == [(start, end)]
    assert response["meta"]["count"] == 1
