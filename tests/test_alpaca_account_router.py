from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.alpaca import account
from gateway.core.cache import InMemoryCache
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


class _FakeProvider:
    def __init__(self) -> None:
        self.config_set_calls: list[dict[str, Any]] = []
        self.activity_calls: list[list[str] | None] = []

    def get_account_configurations(self) -> dict[str, Any]:
        return {"dtbp_check": "both", "suspend_trade": False}

    def set_account_configurations(self, **kwargs: Any) -> dict[str, Any]:
        self.config_set_calls.append(kwargs)
        return kwargs

    def get_account_activities(self, activity_types: list[str] | None) -> list[dict[str, str]]:
        self.activity_calls.append(activity_types)
        return [
            {"id": "1", "activity_type": "FILL"},
            {"id": "2", "activity_type": "DIV"},
        ]


@pytest.mark.asyncio
async def test_get_account_configurations_uses_shared_helper(
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

    monkeypatch.setattr(account, "execute_alpaca_cached_call", _execute_alpaca_cached_call)

    response = await account.get_account_configurations(
        client=cast(Any, SimpleNamespace(id="test-client")),
        cache=cache,
        registry=cast(ProviderRegistry, route_registry),
    )

    assert observed["key"] == "alpaca:account:configurations"
    assert observed["route_label"] == "alpaca_account_configurations"
    assert response["success"] is True
    assert response["data"]["dtbp_check"] == "both"
    assert response["meta"]["provider"] == "alpaca"


@pytest.mark.asyncio
async def test_set_account_configurations_threads_all_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is False
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(account, "execute_alpaca_provider_call", _execute_alpaca_call)

    response = await account.set_account_configurations(
        dtbp_check="both",
        trade_confirm_email="all",
        suspend_trade=False,
        no_shorting=False,
        fractional_trading=True,
        max_margin_multiplier="2",
        pdt_check="entry",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert response["success"] is True
    assert len(provider.config_set_calls) == 1
    assert provider.config_set_calls[0]["dtbp_check"] == "both"
    assert provider.config_set_calls[0]["fractional_trading"] is True
    assert provider.config_set_calls[0]["max_margin_multiplier"] == "2"


@pytest.mark.asyncio
async def test_get_account_activities_splits_activity_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is False
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(account, "execute_alpaca_provider_call", _execute_alpaca_call)

    response = await account.get_account_activities(
        activity_types="FILL,DIV",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.activity_calls == [["FILL", "DIV"]]
    assert response["success"] is True
    assert response["meta"]["count"] == 2
