from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.alpaca import corporate
from gateway.core.cache import InMemoryCache
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
        self.calls: list[dict[str, Any]] = []

    async def get_corporate_actions(self, **kwargs: Any) -> list[_ModelLike]:
        self.calls.append(kwargs)
        return [_ModelLike({"type": "dividend"}), _ModelLike({"type": "split"})]


@pytest.mark.asyncio
async def test_get_corporate_actions_threads_params_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    cache = InMemoryCache(max_size=32, default_ttl=60)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 31, tzinfo=UTC)
    observed: dict[str, Any] = {}

    async def _execute_alpaca_cached_call(
        **kwargs: Any,
    ):
        observed["key"] = kwargs["cache_key"]
        observed["route_label"] = kwargs["route_label"]
        provider_obj = kwargs["registry"].get("alpaca")
        return await kwargs["provider_call"](provider_obj)

    monkeypatch.setattr(corporate, "execute_alpaca_cached_call", _execute_alpaca_cached_call)

    response = await corporate.get_corporate_actions(
        symbol="aapl",
        types="dividend, split",
        start=start,
        end=end,
        client=cast(Any, SimpleNamespace(id="test-client")),
        cache=cache,
        registry=cast(ProviderRegistry, route_registry),
    )

    assert (
        observed["key"]
        == "alpaca:corporate:actions:AAPL:dividend,split:2026-01-01T00:00:00+00:00:2026-01-31T00:00:00+00:00"
    )
    assert observed["route_label"] == "alpaca_corporate_actions"
    assert len(provider.calls) == 1
    assert provider.calls[0]["symbols"] == ["AAPL"]
    assert provider.calls[0]["types"] == ["dividend", "split"]
    assert provider.calls[0]["start"] == start
    assert provider.calls[0]["end"] == end
    assert response["data"]["symbol"] == "AAPL"
    assert response["meta"]["count"] == 2
