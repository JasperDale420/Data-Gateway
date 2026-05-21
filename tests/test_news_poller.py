"""Tests for the Alpaca news REST poller."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from gateway.core.news_poller import (
    AlpacaNewsPoller,
    get_news_poller,
    stop_news_poller,
)


def _make_article(article_id: str, headline: str = "AAPL up") -> Any:
    """Build a NormalizedNewsArticle-shaped object."""
    article = AsyncMock()
    article.model_dump = lambda mode=None: {
        "article_id": article_id,
        "headline": headline,
        "summary": "summary",
        "url": f"https://news.example/{article_id}",
        "source": "Reuters",
        "author": "Jane Doe",
        "published_at": "2026-04-29T15:30:00Z",
        "symbols": ["AAPL"],
    }
    return article


@pytest.fixture
def mock_provider():
    provider = AsyncMock()
    provider.get_news = AsyncMock(return_value=[_make_article("a1"), _make_article("a2")])
    return provider


@pytest.fixture
def mock_sink_registry():
    sink = AsyncMock()
    sink.publish_all = AsyncMock()
    sink.publish_all_batch = AsyncMock(side_effect=lambda msgs: len(msgs))
    sink.publish_all_batch_results = AsyncMock(side_effect=lambda msgs: [True] * len(msgs))
    return sink


@pytest.fixture
def poller():
    p = AlpacaNewsPoller(symbols=None, poll_interval_seconds=60, fetch_limit=50)
    p._redis_dedupe = None
    return p


def test_construction_clamps_minimum_poll_interval():
    p = AlpacaNewsPoller(symbols=None, poll_interval_seconds=1, fetch_limit=50)
    assert p._poll_interval >= 5


def test_construction_clamps_fetch_limit_to_alpaca_max_per_page():
    p = AlpacaNewsPoller(symbols=None, poll_interval_seconds=60, fetch_limit=99999)
    assert p._fetch_limit <= 50


@pytest.mark.asyncio
async def test_poll_publishes_envelopes_with_news_feed(mock_provider, mock_sink_registry, poller):
    poller._provider = mock_provider

    await poller._poll_news(mock_sink_registry)

    # Each article -> one envelope
    assert mock_sink_registry.publish_all_batch_results.called
    msgs = mock_sink_registry.publish_all_batch_results.call_args[0][0]
    assert len(msgs) == 2
    for _topic, env in msgs:
        assert env["feed"] == "news"
        assert env["provider"] == "alpaca"
        assert env["source"] == "rest"


@pytest.mark.asyncio
async def test_poll_skips_when_no_news_returned(mock_sink_registry, poller):
    provider = AsyncMock()
    provider.get_news = AsyncMock(return_value=[])
    poller._provider = provider

    await poller._poll_news(mock_sink_registry)
    assert not mock_sink_registry.publish_all_batch_results.called


@pytest.mark.asyncio
async def test_poll_logs_and_swallows_provider_errors(mock_sink_registry, poller):
    provider = AsyncMock()
    provider.get_news = AsyncMock(side_effect=RuntimeError("alpaca news 502"))
    poller._provider = provider

    # Must NOT raise.
    await poller._poll_news(mock_sink_registry)
    assert not mock_sink_registry.publish_all_batch_results.called


@pytest.mark.asyncio
async def test_poll_dedupes_by_article_id_across_polls(mock_provider, mock_sink_registry, poller):
    poller._provider = mock_provider

    await poller._poll_news(mock_sink_registry)
    first_count = mock_sink_registry.publish_all_batch_results.call_count

    await poller._poll_news(mock_sink_registry)
    # Same articles → deduped → no second publish call.
    assert mock_sink_registry.publish_all_batch_results.call_count == first_count


@pytest.mark.asyncio
async def test_start_and_stop_singleton_lifecycle():
    await stop_news_poller()
    assert get_news_poller() is None
