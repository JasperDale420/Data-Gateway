from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api.alpaca import news
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

    async def get_news(self, **kwargs: Any) -> list[_ModelLike]:
        self.calls.append(kwargs)
        return [_ModelLike(1), _ModelLike(2)]


@pytest.mark.asyncio
async def test_get_news_threads_filters_to_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    route_registry = _FakeRegistry({"alpaca": provider})
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 2, tzinfo=UTC)

    async def _execute_alpaca_call(
        *, registry: ProviderRegistry, provider_call: Any, block: bool = False
    ):
        assert registry is cast(ProviderRegistry, route_registry)
        assert block is False
        provider_obj = registry.get("alpaca")
        return await provider_call(provider_obj)

    monkeypatch.setattr(news, "execute_alpaca_provider_call", _execute_alpaca_call)

    response = await news.get_news(
        symbols="aapl,msft",
        start=start,
        end=end,
        limit=5,
        include_content=True,
        client=cast(Any, SimpleNamespace(id="test-client")),
        registry=cast(ProviderRegistry, route_registry),
    )

    assert len(provider.calls) == 1
    assert provider.calls[0]["symbols"] == ["AAPL", "MSFT"]
    assert provider.calls[0]["start"] == start
    assert provider.calls[0]["end"] == end
    assert provider.calls[0]["limit"] == 5
    assert provider.calls[0]["include_content"] is True
    assert response["meta"]["count"] == 2
    assert response["meta"]["symbols_filter"] == ["AAPL", "MSFT"]
