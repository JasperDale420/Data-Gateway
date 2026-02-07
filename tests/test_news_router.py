from __future__ import annotations

from datetime import datetime
from typing import Any, cast

import pytest

from gateway.api import news
from gateway.core.auth import Client, ClientPermissions
from gateway.core.cache import InMemoryCache


class _FakeCache:
    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    async def get(self, key: str) -> Any:
        return self._store.get(key)

    async def set(self, key: str, value: Any, ttl: int) -> None:
        assert ttl > 0
        self._store[key] = value


class _FakeProvider:
    def __init__(self) -> None:
        self._client = object()
        self.calls: list[dict[str, Any]] = []
        self.response: dict[str, Any] = {"items": [{"article_id": "1"}], "meta": {"total": 1}}

    async def initialize(self, _config: dict[str, Any]) -> None:
        self._client = object()

    async def get_articles(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return self.response


class _FakeDeduper:
    def __init__(self) -> None:
        self.keys: list[str] = []

    async def dedupe(self, key: str, fetcher) -> Any:
        self.keys.append(key)
        return await fetcher()


def _make_client() -> Client:
    return Client(id="test-client", permissions=ClientPermissions())


@pytest.mark.asyncio
async def test_get_articles_cache_hit_skips_datetime_parse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = _FakeCache()
    provider = _FakeProvider()
    cache_key = "news:articles:AAPL:earnings:not-a-date:still-not-a-date:50:None:desc"
    cache._store[cache_key] = {"items": [{"article_id": "cached"}]}

    async def _fail_rate_limit(_provider_name: str) -> None:
        raise AssertionError("rate limit should not run on cache hit")

    monkeypatch.setattr(news, "get_news_provider", lambda: provider)
    monkeypatch.setattr(news, "require_provider_rate_limit", _fail_rate_limit)

    response = await news.get_articles(
        symbols="AAPL",
        keywords="earnings",
        start="not-a-date",
        end="still-not-a-date",
        limit=50,
        cursor=None,
        sort="desc",
        client=_make_client(),
        cache=cast(InMemoryCache, cache),
    )

    assert response == {
        "success": True,
        "data": {"items": [{"article_id": "cached"}]},
        "cached": True,
    }
    assert provider.calls == []


@pytest.mark.asyncio
async def test_get_articles_cache_miss_parses_dates_and_fetches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = _FakeCache()
    provider = _FakeProvider()
    deduper = _FakeDeduper()
    calls = {"rate_limit": 0}

    async def _fake_rate_limit(provider_name: str) -> None:
        assert provider_name == "news"
        calls["rate_limit"] += 1

    monkeypatch.setattr(news, "get_news_provider", lambda: provider)
    monkeypatch.setattr(news, "get_deduplicator", lambda: deduper)
    monkeypatch.setattr(news, "require_provider_rate_limit", _fake_rate_limit)

    response = await news.get_articles(
        symbols="AAPL, msft",
        keywords="earnings",
        start="2026-01-01T00:00:00Z",
        end="2026-01-02T00:00:00Z",
        limit=10,
        cursor="2",
        sort="asc",
        client=_make_client(),
        cache=cast(InMemoryCache, cache),
    )

    assert calls["rate_limit"] == 1
    assert deduper.keys == [
        "news:articles:AAPL, msft:earnings:2026-01-01T00:00:00Z:2026-01-02T00:00:00Z:10:2:asc"
    ]
    assert len(provider.calls) == 1
    assert provider.calls[0]["symbols"] == ["AAPL", "MSFT"]
    assert provider.calls[0]["keywords"] == "earnings"
    assert provider.calls[0]["limit"] == 10
    assert provider.calls[0]["cursor"] == "2"
    assert provider.calls[0]["sort"] == "asc"
    assert isinstance(provider.calls[0]["start"], datetime)
    assert isinstance(provider.calls[0]["end"], datetime)
    assert response == {"success": True, "data": provider.response, "cached": False}
