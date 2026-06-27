from __future__ import annotations

from typing import Any, cast

import pytest

from gateway.api.uw import misc


class _FakeNewsProvider:
    def __init__(self, captured: dict[str, Any]) -> None:
        self._captured = captured

    async def get_news_headlines(self, **kwargs: Any) -> list[dict[str, str]]:
        self._captured["provider_kwargs"] = kwargs
        return [{"headline": "Tesla headline"}]


@pytest.mark.asyncio
async def test_uw_news_headlines_forwards_ticker_as_search_term(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    async def _fake_execute_uw_cached(**kwargs: Any) -> dict[str, Any]:
        captured["cache_key"] = kwargs["cache_key"]
        data = await kwargs["fetcher"](_FakeNewsProvider(captured))
        return kwargs["build_response"](data)

    monkeypatch.setattr(misc, "execute_uw_cached", _fake_execute_uw_cached)

    response = await misc.get_news_headlines(
        ticker="tsla",
        sources=None,
        search_term=None,
        major_only=None,
        limit=3,
        page=None,
        client=cast(misc.Client, object()),
        registry=cast(misc.ProviderRegistry, object()),
        cache=cast(misc.InMemoryCache, object()),
    )

    assert captured["provider_kwargs"]["search_term"] == "TSLA"
    assert "TSLA" in captured["cache_key"]
    assert response["meta"]["ticker"] == "TSLA"
    assert response["meta"]["search_term"] == "TSLA"
    assert response["meta"]["count"] == 1


@pytest.mark.asyncio
async def test_uw_news_headlines_keeps_explicit_search_term_when_ticker_is_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def _fake_execute_uw_cached(**kwargs: Any) -> dict[str, Any]:
        captured["cache_key"] = kwargs["cache_key"]
        data = await kwargs["fetcher"](_FakeNewsProvider(captured))
        return kwargs["build_response"](data)

    monkeypatch.setattr(misc, "execute_uw_cached", _fake_execute_uw_cached)

    response = await misc.get_news_headlines(
        ticker="tsla",
        sources=None,
        search_term="earnings",
        major_only=True,
        limit=5,
        page=2,
        client=cast(misc.Client, object()),
        registry=cast(misc.ProviderRegistry, object()),
        cache=cast(misc.InMemoryCache, object()),
    )

    assert captured["provider_kwargs"]["search_term"] == "earnings"
    assert "earnings" in captured["cache_key"]
    assert response["meta"]["ticker"] == "TSLA"
    assert response["meta"]["search_term"] == "earnings"
