"""WebSocket subscribe-surface aliases for the stock streaming feeds.

The canonical wire envelope labels (bars/quotes/trades) are accepted on the
subscribe surface as equivalents of the stock_* feed names. Exercises
gateway.api.websocket._handle_message directly (same pattern as
test_websocket_flow_routing.py) with a recording fake multiplexer to verify:

  - alias and stock_* spellings produce identical multiplexer routing and
    identical local subscription tracking,
  - permission feeds listed under either spelling authorize both,
  - unknown feed names are still rejected,
  - stock_* keeps working unchanged (back-compat).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import gateway.core.globals as globals_module
from gateway.api import websocket as ws
from gateway.core.stream import AlpacaStreamType


class _RecordingMultiplexer:
    """Records client_subscribe/client_unsubscribe calls and acks ok."""

    def __init__(self) -> None:
        self.subscribe_calls: list[dict[str, Any]] = []
        self.unsubscribe_calls: list[dict[str, Any]] = []

    async def client_subscribe(
        self,
        client_id: str,
        stream_type: AlpacaStreamType,
        bars: list[str] | None = None,
        quotes: list[str] | None = None,
        trades: list[str] | None = None,
        news: list[str] | None = None,
    ) -> dict[str, Any]:
        self.subscribe_calls.append(
            {"stream_type": stream_type, "bars": bars, "quotes": quotes, "trades": trades, "news": news}
        )
        subscribed = list(bars or []) + list(quotes or []) + list(trades or []) + list(news or [])
        return {"type": "subscription_ack", "status": "ok", "subscribed": subscribed, "failed": []}

    async def client_unsubscribe(
        self,
        client_id: str,
        stream_type: AlpacaStreamType,
        bars: list[str] | None = None,
        quotes: list[str] | None = None,
        trades: list[str] | None = None,
        news: list[str] | None = None,
    ) -> dict[str, Any]:
        self.unsubscribe_calls.append(
            {"stream_type": stream_type, "bars": bars, "quotes": quotes, "trades": trades, "news": news}
        )
        unsubscribed = list(bars or []) + list(quotes or []) + list(trades or []) + list(news or [])
        return {"type": "unsubscription_ack", "status": "ok", "unsubscribed": unsubscribed}


class _FakeConnections:
    def __init__(self, feeds: list[str]) -> None:
        self._conn = SimpleNamespace(
            client=SimpleNamespace(
                permissions=SimpleNamespace(
                    providers=["alpaca"],
                    feeds=feeds,
                    max_symbols=100,
                    ws_subscriptions_max=1000,
                )
            ),
            subscriptions=set(),
            client_id="cerberus",
            authenticated=True,
        )

    def get(self, _connection_id):
        return self._conn


@pytest.fixture
def wired_multiplexer():
    mux = _RecordingMultiplexer()
    original = globals_module._multiplexer
    globals_module.set_multiplexer(mux)  # type: ignore[arg-type]
    try:
        yield mux
    finally:
        globals_module._multiplexer = original


async def _subscribe(conns: _FakeConnections, feeds: list[str], symbols: list[str]) -> dict[str, Any]:
    return await ws._handle_message(
        {"action": "subscribe", "provider": "alpaca", "feeds": feeds, "symbols": symbols},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )


@pytest.mark.parametrize(
    ("alias", "canonical", "channel"),
    [
        ("bars", "stock_bars", "bars"),
        ("quotes", "stock_quotes", "quotes"),
        ("trades", "stock_trades", "trades"),
    ],
)
async def test_alias_and_stock_name_route_identically(wired_multiplexer, alias, canonical, channel):
    """Subscribing with the alias yields the same routing and tracking as stock_*."""
    conns_alias = _FakeConnections(feeds=["bars", "quotes", "trades"])
    result_alias = await _subscribe(conns_alias, [alias], ["AAPL"])
    assert result_alias["status"] == "ok"
    assert result_alias["feeds"] == [canonical]
    assert result_alias["subscribed"] == ["AAPL"]
    assert conns_alias._conn.subscriptions == {f"{canonical}:AAPL"}

    conns_stock = _FakeConnections(feeds=["bars", "quotes", "trades"])
    result_stock = await _subscribe(conns_stock, [canonical], ["AAPL"])
    assert result_stock == result_alias
    assert conns_stock._conn.subscriptions == conns_alias._conn.subscriptions

    # Both spellings hit the multiplexer with identical arguments.
    assert len(wired_multiplexer.subscribe_calls) == 2
    call_alias, call_stock = wired_multiplexer.subscribe_calls
    assert call_alias == call_stock
    assert call_alias["stream_type"] == AlpacaStreamType.STOCKS_SIP
    assert call_alias[channel] == ["AAPL"]


async def test_mixed_alias_and_stock_name_deduplicates(wired_multiplexer):
    """['bars', 'stock_bars'] collapses to a single stock_bars subscription."""
    conns = _FakeConnections(feeds=["bars"])
    result = await _subscribe(conns, ["bars", "stock_bars"], ["AAPL"])
    assert result["status"] == "ok"
    assert result["feeds"] == ["stock_bars"]
    assert len(wired_multiplexer.subscribe_calls) == 1
    assert conns._conn.subscriptions == {"stock_bars:AAPL"}


async def test_permission_stock_name_authorizes_alias(wired_multiplexer):
    """A client whose permission list uses stock_* spellings can subscribe via aliases."""
    conns = _FakeConnections(feeds=["stock_bars", "stock_quotes", "stock_trades"])
    result = await _subscribe(conns, ["bars", "quotes", "trades"], ["AAPL"])
    assert result["status"] == "ok"
    assert result["feeds"] == ["stock_bars", "stock_quotes", "stock_trades"]


async def test_permission_alias_name_authorizes_stock_name(wired_multiplexer):
    """A client whose permission list uses the short spellings can subscribe via stock_*."""
    conns = _FakeConnections(feeds=["bars", "quotes", "trades"])
    result = await _subscribe(conns, ["stock_bars", "stock_quotes", "stock_trades"], ["AAPL"])
    assert result["status"] == "ok"


async def test_unpermissioned_feed_denied_under_either_spelling(wired_multiplexer):
    """Aliasing must not widen access: a bars-only client still can't get quotes."""
    conns = _FakeConnections(feeds=["stock_bars"])
    for spelling in ("quotes", "stock_quotes"):
        result = await _subscribe(conns, [spelling], ["AAPL"])
        assert result["type"] == "error"
        assert result["error_code"] == "GW-E2007"


async def test_unknown_feed_still_rejected(wired_multiplexer):
    """Unknown feed names don't silently normalize into anything subscribable."""
    conns = _FakeConnections(feeds=["bars", "quotes", "trades"])
    result = await _subscribe(conns, ["garbage_feed"], ["AAPL"])
    assert result["type"] == "error"
    assert result["error_code"] == "GW-E2007"
    assert wired_multiplexer.subscribe_calls == []


async def test_unsubscribe_accepts_either_spelling(wired_multiplexer):
    """Subscribe with the alias, unsubscribe with stock_* — tracking fully clears."""
    conns = _FakeConnections(feeds=["bars"])
    result = await _subscribe(conns, ["bars"], ["AAPL"])
    assert result["status"] == "ok"
    assert conns._conn.subscriptions == {"stock_bars:AAPL"}

    result = await ws._handle_message(
        {"action": "unsubscribe", "provider": "alpaca", "feeds": ["stock_bars"], "symbols": ["AAPL"]},
        connection_id="conn-1",
        connections=conns,  # type: ignore[arg-type]
    )
    assert result["type"] == "unsubscription_ack"
    assert result["status"] == "ok"
    assert conns._conn.subscriptions == set()
    assert len(wired_multiplexer.unsubscribe_calls) == 1
    assert wired_multiplexer.unsubscribe_calls[0]["bars"] == ["AAPL"]
