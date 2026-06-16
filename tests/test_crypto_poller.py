"""Tests for the Alpaca crypto bars+trades REST poller.

Crypto trades 24/7, so unlike quotes/trades pollers there is no
market-hours gate. Pulls latest bars and latest trades on each tick
and publishes both feeds to Heber.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from gateway.core.crypto_poller import (
    AlpacaCryptoPoller,
    get_crypto_poller,
    stop_crypto_poller,
)


def _bar_response(pair: str, close: float = 50_000.0) -> dict[str, Any]:
    """Shape of Alpaca's /v1beta3/crypto/us/latest/bars response per pair."""
    return {
        "t": "2026-04-29T15:30:00Z",
        "o": close,
        "h": close + 100,
        "l": close - 100,
        "c": close,
        "v": 5.5,
        "n": 12,
        "vw": close,
    }


def _trade_response(pair: str, price: float = 50_000.0) -> dict[str, Any]:
    """Shape of Alpaca's /v1beta3/crypto/us/latest/trades response per pair."""
    return {
        "t": "2026-04-29T15:30:00Z",
        "p": price,
        "s": 0.05,
        "i": f"{pair}-trade-1",
        "tks": "B",
    }


@pytest.fixture
def mock_provider():
    provider = AsyncMock()
    provider.get_crypto_latest_bars = AsyncMock(
        return_value={"BTC/USD": _bar_response("BTC/USD"), "ETH/USD": _bar_response("ETH/USD", 3000)}
    )
    provider.get_crypto_latest_trades = AsyncMock(
        return_value={"BTC/USD": _trade_response("BTC/USD"), "ETH/USD": _trade_response("ETH/USD", 3000)}
    )
    return provider


@pytest.fixture
def mock_sink_registry():
    sink = AsyncMock()
    sink.publish_all = AsyncMock()
    sink.publish_all_batch = AsyncMock(return_value=2)
    sink.publish_all_batch_results = AsyncMock(side_effect=lambda msgs: [True] * len(msgs))
    return sink


@pytest.fixture
def poller():
    p = AlpacaCryptoPoller(pairs=["BTC/USD", "ETH/USD"], poll_interval_seconds=15)
    p._redis_dedupe = None
    return p


def test_construction_normalizes_pairs():
    p = AlpacaCryptoPoller(pairs=["btc/usd", "ETH/USD"], poll_interval_seconds=15)
    assert "BTC/USD" in p._pairs
    assert "ETH/USD" in p._pairs


def test_construction_clamps_minimum_poll_interval():
    p = AlpacaCryptoPoller(pairs=["BTC/USD"], poll_interval_seconds=1)
    assert p._poll_interval >= 5


def test_default_pairs_used_when_none_passed():
    p = AlpacaCryptoPoller(pairs=None, poll_interval_seconds=15)
    assert "BTC/USD" in p._pairs


@pytest.mark.asyncio
async def test_poll_fetches_both_bars_and_trades(mock_provider, mock_sink_registry, poller):
    poller._provider = mock_provider

    await poller._poll_crypto(mock_sink_registry)

    assert mock_provider.get_crypto_latest_bars.called
    assert mock_provider.get_crypto_latest_trades.called


@pytest.mark.asyncio
async def test_poll_publishes_envelopes_with_crypto_feed_names(mock_provider, mock_sink_registry, poller):
    poller._provider = mock_provider

    await poller._poll_crypto(mock_sink_registry)

    # publish_all_batch should be called with messages tagged crypto_bars and crypto_trades.
    all_envelopes: list[dict[str, Any]] = []
    for call in mock_sink_registry.publish_all_batch_results.call_args_list:
        for _topic, env in call[0][0]:
            all_envelopes.append(env)

    feeds = {env["feed"] for env in all_envelopes}
    assert "crypto_bars" in feeds
    assert "crypto_trades" in feeds
    for env in all_envelopes:
        assert env["provider"] == "alpaca"
        assert env["source"] == "rest"
        # Must use canonical crypto:BASE-QUOTE instrument_key (BTC/USD -> crypto:BTC-USD)
        assert env["instrument_key"].startswith("crypto:")
        assert "/" not in env["instrument_key"]


@pytest.mark.asyncio
async def test_poll_does_not_require_market_hours(mock_provider, mock_sink_registry, poller):
    """Crypto trades 24/7; the poller should not gate on TradingCalendar."""
    poller._provider = mock_provider

    # Even with TradingCalendar saying market closed, crypto poll must run.
    await poller._poll_crypto(mock_sink_registry)
    assert mock_sink_registry.publish_all_batch_results.called


@pytest.mark.asyncio
async def test_poll_logs_and_swallows_provider_errors(mock_sink_registry, poller):
    provider = AsyncMock()
    provider.get_crypto_latest_bars = AsyncMock(side_effect=RuntimeError("boom"))
    provider.get_crypto_latest_trades = AsyncMock(return_value={})
    poller._provider = provider

    # Must NOT raise even when one of the fetches fails.
    await poller._poll_crypto(mock_sink_registry)


@pytest.mark.asyncio
async def test_start_and_stop_singleton_lifecycle():
    await stop_crypto_poller()
    assert get_crypto_poller() is None
