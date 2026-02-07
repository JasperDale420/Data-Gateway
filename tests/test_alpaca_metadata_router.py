from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import HTTPException

from gateway.api.alpaca import common, metadata
from gateway.core.cache import InMemoryCache
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


class _MetadataProvider:
    def __init__(self) -> None:
        self.condition_calls: list[str] = []
        self.exchange_calls: list[str] = []

    async def get_condition_codes(self, *, asset_class: str) -> dict[str, str]:
        self.condition_calls.append(asset_class)
        return {"A": "Average Price Trade"}

    async def get_exchange_codes(self, *, asset_class: str) -> dict[str, str]:
        self.exchange_calls.append(asset_class)
        return {"Q": "NASDAQ"}


@pytest.mark.asyncio
async def test_condition_codes_uses_cache_and_avoids_second_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = InMemoryCache(max_size=32, default_ttl=60)
    provider = _MetadataProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    cache_events: list[tuple[str, str, str]] = []

    async def _fake_rate_limit(_provider_name: str, block: bool = False) -> None:
        return None

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(common, "require_provider_rate_limit", _fake_rate_limit)
    monkeypatch.setattr(common, "record_route_cache", _record_route_cache)

    first = await metadata.get_condition_codes(
        asset_class="stocks",
        client=cast(Any, SimpleNamespace(id="test-client")),
        cache=cache,
        registry=cast(ProviderRegistry, route_registry),
    )
    second = await metadata.get_condition_codes(
        asset_class="stocks",
        client=cast(Any, SimpleNamespace(id="test-client")),
        cache=cache,
        registry=cast(ProviderRegistry, route_registry),
    )

    assert first["success"] is True
    assert second["success"] is True
    assert provider.condition_calls == ["stocks"]
    assert first["data"] == {"A": "Average Price Trade"}
    assert cache_events == [
        ("alpaca_meta_conditions", "miss", "alpaca"),
        ("alpaca_meta_conditions", "hit", "alpaca"),
    ]


@pytest.mark.asyncio
async def test_exchange_codes_cache_key_normalizes_asset_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = InMemoryCache(max_size=32, default_ttl=60)
    provider = _MetadataProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _fake_rate_limit(_provider_name: str, block: bool = False) -> None:
        return None

    monkeypatch.setattr(common, "require_provider_rate_limit", _fake_rate_limit)

    first = await metadata.get_exchange_codes(
        asset_class=" options ",
        client=cast(Any, SimpleNamespace(id="test-client")),
        cache=cache,
        registry=cast(ProviderRegistry, route_registry),
    )
    second = await metadata.get_exchange_codes(
        asset_class="OPTIONS",
        client=cast(Any, SimpleNamespace(id="test-client")),
        cache=cache,
        registry=cast(ProviderRegistry, route_registry),
    )

    assert first["success"] is True
    assert second["success"] is True
    assert provider.exchange_calls == [" options "]
    assert first["data"] == {"Q": "NASDAQ"}
    assert second["data"] == {"Q": "NASDAQ"}


@pytest.mark.asyncio
async def test_logo_endpoint_returns_bytes_and_404_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_execute(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        return b"PNG_BYTES"

    monkeypatch.setattr(metadata, "execute_alpaca_provider_call", _fake_execute)

    ok_response = await metadata.get_logo(
        symbol="aapl",
        placeholder=True,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry({"alpaca": object()})),
    )
    assert ok_response.body == b"PNG_BYTES"
    assert ok_response.media_type == "image/png"

    async def _fake_execute_none(
        *, registry: ProviderRegistry, provider_call: Any, block: bool = False
    ):
        return None

    monkeypatch.setattr(metadata, "execute_alpaca_provider_call", _fake_execute_none)

    with pytest.raises(HTTPException) as exc:
        await metadata.get_logo(
            symbol="aapl",
            placeholder=True,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, _FakeRegistry({"alpaca": object()})),
        )
    assert exc.value.status_code == 404
    assert exc.value.detail == "Logo not found"


@pytest.mark.asyncio
async def test_fixed_income_prices_uses_shared_parser_drop_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class _FixedIncomeProvider:
        async def get_fixed_income_prices(self, *, isins: list[str]) -> list[dict[str, Any]]:
            captured["isins"] = isins
            return [{"isin": isin, "price": 100.0} for isin in isins]

    async def _fake_execute(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        return await provider_call(_FixedIncomeProvider())

    monkeypatch.setattr(metadata, "execute_alpaca_provider_call", _fake_execute)

    response = await metadata.get_fixed_income_prices(
        isins="US123,, US456 ",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, _FakeRegistry({"alpaca": object()})),
    )

    assert captured["isins"] == ["US123", "US456"]
    assert response["meta"]["isin_count"] == 2
    assert response["data"] == [
        {"isin": "US123", "price": 100.0},
        {"isin": "US456", "price": 100.0},
    ]
