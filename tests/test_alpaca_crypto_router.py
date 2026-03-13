from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import HTTPException

from gateway.api.alpaca import crypto
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
        self.bars_calls: list[dict[str, Any]] = []
        self.latest_bars_calls: list[list[str]] = []

    async def get_crypto_bars(self, **kwargs: Any) -> list[_ModelLike]:
        self.bars_calls.append(kwargs)
        return [_ModelLike({"close": 1.0})]

    async def get_crypto_latest_bars(self, pairs: list[str]) -> list[dict[str, Any]]:
        self.latest_bars_calls.append(pairs)
        return [{"pair": pair, "close": 1.0} for pair in pairs]


@pytest.mark.asyncio
async def test_get_crypto_bars_threads_query_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 2, tzinfo=UTC)

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is False
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(crypto, "execute_alpaca_provider_call", _execute_alpaca_call)

    response = await crypto.get_crypto_bars(
        pair="btc/usd",
        timeframe="1Hour",
        start=start,
        end=end,
        limit=500,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert len(provider.bars_calls) == 1
    assert provider.bars_calls[0]["pair"] == "BTC/USD"
    assert provider.bars_calls[0]["timeframe"] == "1Hour"
    assert provider.bars_calls[0]["start"] == start
    assert provider.bars_calls[0]["end"] == end
    assert provider.bars_calls[0]["limit"] == 500
    assert response["meta"]["count"] == 1
    assert response["data"]["pair"] == "BTC/USD"


@pytest.mark.asyncio
async def test_get_crypto_latest_quotes_threads_pairs_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _fake_execute(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        return {"BTC/USD": _ModelLike({"bid_price": 1.0})}

    monkeypatch.setattr(crypto, "execute_alpaca_provider_call", _fake_execute)

    response = await crypto.get_crypto_latest_quotes(
        pairs="btc/usd",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert response["data"]["BTC/USD"].model_dump() == {"bid_price": 1.0}
    assert response["meta"]["count"] == 1


@pytest.mark.asyncio
async def test_get_crypto_latest_bars_threads_pairs_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})

    async def _execute_alpaca_call(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is False
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(crypto, "execute_alpaca_provider_call", _execute_alpaca_call)

    response = await crypto.get_crypto_latest_bars(
        pairs="btc/usd,eth/usd",
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert provider.latest_bars_calls == [["BTC/USD", "ETH/USD"]]
    assert response["meta"]["count"] == 2


@pytest.mark.asyncio
async def test_get_crypto_bars_rejects_invalid_pair_before_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _should_not_execute(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("provider call should not execute for invalid input")

    monkeypatch.setattr(crypto, "execute_alpaca_provider_call", _should_not_execute)

    with pytest.raises(HTTPException) as exc:
        await crypto.get_crypto_bars(
            pair="AAPL",
            timeframe="1Hour",
            start=None,
            end=None,
            limit=100,
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, _FakeRegistry({"alpaca": object()})),
        )

    assert exc.value.status_code == 400
    assert "Invalid crypto pair format" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_get_crypto_latest_bars_rejects_invalid_pairs_before_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _should_not_execute(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("provider call should not execute for invalid input")

    monkeypatch.setattr(crypto, "execute_alpaca_provider_call", _should_not_execute)

    with pytest.raises(HTTPException) as exc:
        await crypto.get_crypto_latest_bars(
            pairs="BTC/USD,AAPL",
            client=cast(Any, SimpleNamespace(id="test-client")),
            registry=cast(ProviderRegistry, _FakeRegistry({"alpaca": object()})),
        )

    assert exc.value.status_code == 400
    assert "Invalid crypto pair format" in str(exc.value.detail)
