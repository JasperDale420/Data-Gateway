"""Tests for the Alpaca trades REST-fallback poller.

Mirrors test_treasury_poller.py / quotes_poller behavior:
- Trades fetched via provider.get_latest_trades and published with feed="trades".
- Deduplication via in-memory + optional Redis cache.
- Errors logged and swallowed (poll loop must keep running).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.core.trades_poller import (
    AlpacaTradesPoller,
    get_trades_poller,
    stop_trades_poller,
)


def _make_trade(symbol: str, price: float = 100.0, size: int = 10) -> Any:
    """Build a fake NormalizedTrade-like object."""
    trade = MagicMock()
    trade.model_dump = MagicMock(
        return_value={
            "symbol": symbol,
            "price": price,
            "size": size,
            "trade_id": f"{symbol}-trade-id",
            "timestamp": "2026-04-29T15:30:00Z",
        }
    )
    return trade


@pytest.fixture
def mock_provider():
    provider = AsyncMock()
    provider.get_latest_trades = AsyncMock(return_value=[_make_trade("AAPL"), _make_trade("MSFT")])
    return provider


@pytest.fixture
def mock_sink_registry():
    sink = AsyncMock()
    sink.publish_all = AsyncMock()
    sink.publish_all_batch = AsyncMock(return_value=2)
    return sink


@pytest.fixture
def poller():
    p = AlpacaTradesPoller(symbols=["AAPL", "MSFT"], poll_interval_seconds=15)
    # Disable optional Redis dedupe so tests are hermetic regardless of any
    # leftover keys from prior runs in the shared dev Redis.
    p._redis_dedupe = None
    return p


def test_construction_normalizes_symbols_to_uppercase():
    p = AlpacaTradesPoller(symbols=["aapl", "Msft"], poll_interval_seconds=15)
    assert p._symbols == ["AAPL", "MSFT"]


def test_construction_clamps_minimum_poll_interval():
    p = AlpacaTradesPoller(symbols=["AAPL"], poll_interval_seconds=1)
    assert p._poll_interval >= 5


def test_default_symbols_used_when_none_passed():
    p = AlpacaTradesPoller(symbols=None, poll_interval_seconds=15)
    assert len(p._symbols) > 0
    assert "SPY" in p._symbols


@pytest.mark.asyncio
async def test_poll_trades_publishes_envelopes_with_feed_trades(mock_provider, mock_sink_registry, poller):
    poller._provider = mock_provider

    await poller._poll_trades(mock_sink_registry)

    assert mock_sink_registry.publish_all_batch.called
    args, _kwargs = mock_sink_registry.publish_all_batch.call_args
    messages = args[0]
    assert len(messages) == 2
    for topic, envelope in messages:
        assert topic == "heber:events"
        assert envelope["feed"] == "trades"
        assert envelope["provider"] == "alpaca"
        assert envelope["source"] == "rest"


@pytest.mark.asyncio
async def test_poll_trades_skips_when_no_trades_returned(mock_sink_registry, poller):
    provider = AsyncMock()
    provider.get_latest_trades = AsyncMock(return_value=[])
    poller._provider = provider

    await poller._poll_trades(mock_sink_registry)
    assert not mock_sink_registry.publish_all_batch.called


@pytest.mark.asyncio
async def test_poll_trades_logs_and_swallows_provider_errors(mock_sink_registry, poller, caplog):
    provider = AsyncMock()
    provider.get_latest_trades = AsyncMock(side_effect=RuntimeError("boom"))
    poller._provider = provider

    # Must NOT raise; the poll loop is required to keep running across transient errors.
    await poller._poll_trades(mock_sink_registry)
    assert not mock_sink_registry.publish_all_batch.called


@pytest.mark.asyncio
async def test_poll_trades_dedupe_drops_repeated_event_ids(mock_provider, mock_sink_registry, poller):
    """In-memory dedupe must drop repeated event_ids on subsequent polls."""
    poller._provider = mock_provider

    # First call: nothing seen, both go through.
    await poller._poll_trades(mock_sink_registry)
    first_call_msgs = mock_sink_registry.publish_all_batch.call_args_list[0][0][0]
    assert len(first_call_msgs) == 2

    # Second call with the SAME trades: in-memory dedupe should drop both as duplicates,
    # so publish_all_batch is not called a second time.
    await poller._poll_trades(mock_sink_registry)
    assert mock_sink_registry.publish_all_batch.call_count == 1


@pytest.mark.asyncio
async def test_start_and_stop_trades_poller_singleton_lifecycle():
    # Ensure clean state
    await stop_trades_poller()
    assert get_trades_poller() is None
