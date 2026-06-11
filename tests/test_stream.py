"""Behavioural tests for gateway.core.stream.

Complements the existing stream suites (test_multiplexer.py,
test_sequence_tracker.py, test_stream_slow_handler.py,
test_option_stream_options.py) by covering:

- AlpacaStreamType feed/endpoint/instrument-type routing
- subscription-request sanitisation and OCC symbol-shape filtering
  (the 2026-05-01 RCA: wrong-shape symbols silently kill the whole upstream
  subscribe), and the options-stream "no bars" rule
- fallback fanout client isolation: a raising client must not stop delivery
  to the other subscribed clients
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import pytest

import gateway.core.stream as stream_module
from gateway.core.stream import (
    AlpacaStreamType,
    StreamMultiplexer,
    UpstreamConnection,
)

# A valid OCC contract symbol (matches SYMBOL_PATTERNS["option_occ"]).
_OCC_SYMBOL = "AAPL260116C00200000"


def _make_upstream(stream_type: AlpacaStreamType) -> UpstreamConnection:
    async def _noop(_msg: dict) -> None:
        return

    return UpstreamConnection(
        stream_type=stream_type,
        api_key="k",  # pragma: allowlist secret
        api_secret="s",  # pragma: allowlist secret
        on_message=_noop,
    )


# ---------------------------------------------------------------------------
# AlpacaStreamType routing
# ---------------------------------------------------------------------------


class TestAlpacaStreamTypeRouting:
    def test_from_feed_maps_known_feeds(self) -> None:
        assert AlpacaStreamType.from_feed("stock_quotes") == AlpacaStreamType.STOCKS_SIP
        assert AlpacaStreamType.from_feed("option_trades") == AlpacaStreamType.OPTIONS
        assert AlpacaStreamType.from_feed("crypto_bars") == AlpacaStreamType.CRYPTO
        assert AlpacaStreamType.from_feed("news") == AlpacaStreamType.NEWS

    def test_from_feed_unknown_defaults_to_stocks_sip(self) -> None:
        assert AlpacaStreamType.from_feed("totally_unknown") == AlpacaStreamType.STOCKS_SIP

    def test_endpoint_per_stream_type(self) -> None:
        assert AlpacaStreamType.OPTIONS.endpoint.endswith("/opra")
        assert "iex" in AlpacaStreamType.STOCKS_IEX.endpoint
        assert "sip" in AlpacaStreamType.STOCKS_SIP.endpoint
        assert "crypto" in AlpacaStreamType.CRYPTO.endpoint

    def test_instrument_type_mapping(self) -> None:
        assert AlpacaStreamType.STOCKS_SIP.instrument_type == "equity"
        assert AlpacaStreamType.STOCKS_IEX.instrument_type == "equity"
        assert AlpacaStreamType.NEWS.instrument_type == "equity"
        assert AlpacaStreamType.OPTIONS.instrument_type == "option"
        assert AlpacaStreamType.CRYPTO.instrument_type == "crypto"


# ---------------------------------------------------------------------------
# Subscription-request sanitisation / OCC shape filtering (2026-05-01 RCA)
# ---------------------------------------------------------------------------


class TestSubscriptionSanitisation:
    def test_options_stream_rejects_bars_with_warning(self) -> None:
        conn = _make_upstream(AlpacaStreamType.OPTIONS)
        bars, quotes, trades, news, warnings = conn._sanitize_subscription_request(
            bars={_OCC_SYMBOL}, quotes=None, trades=None, news=None
        )
        assert bars is None
        assert any("does not support bars" in w for w in warnings)

    def test_options_stream_drops_equity_shaped_symbols(self) -> None:
        """Equity tickers on the OPRA stream are filtered out — leaving them in
        makes Alpaca silently drop the entire subscribe payload."""
        conn = _make_upstream(AlpacaStreamType.OPTIONS)
        _bars, quotes, _trades, _news, warnings = conn._sanitize_subscription_request(
            bars=None, quotes={"AAPL", _OCC_SYMBOL}, trades=None, news=None
        )
        assert quotes == {_OCC_SYMBOL}
        assert any("AAPL" in w for w in warnings)

    def test_stocks_stream_drops_occ_shaped_symbols(self) -> None:
        """OCC option contracts on the SIP/IEX stock stream are filtered out."""
        conn = _make_upstream(AlpacaStreamType.STOCKS_SIP)
        bars, _quotes, _trades, _news, warnings = conn._sanitize_subscription_request(
            bars={"AAPL", _OCC_SYMBOL}, quotes=None, trades=None, news=None
        )
        assert bars == {"AAPL"}
        assert any(_OCC_SYMBOL in w for w in warnings)

    def test_stocks_stream_passes_clean_equity_symbols_unchanged(self) -> None:
        conn = _make_upstream(AlpacaStreamType.STOCKS_SIP)
        bars, quotes, trades, _news, warnings = conn._sanitize_subscription_request(
            bars={"AAPL", "MSFT"}, quotes={"TSLA"}, trades=None, news=None
        )
        assert bars == {"AAPL", "MSFT"}
        assert quotes == {"TSLA"}
        assert trades is None
        assert warnings == []

    def test_all_mismatched_symbols_collapse_feed_to_none(self) -> None:
        """If every symbol in a feed is the wrong shape the feed becomes None,
        so we never send an empty list that confuses Alpaca."""
        conn = _make_upstream(AlpacaStreamType.STOCKS_SIP)
        bars, _quotes, _trades, _news, warnings = conn._sanitize_subscription_request(
            bars={_OCC_SYMBOL}, quotes=None, trades=None, news=None
        )
        assert bars is None
        assert warnings  # a warning was emitted about the dropped symbol

    def test_crypto_stream_passes_symbols_through_unfiltered(self) -> None:
        """Non-stocks/options streams are out of scope for OCC filtering."""
        conn = _make_upstream(AlpacaStreamType.CRYPTO)
        bars, quotes, _trades, _news, warnings = conn._sanitize_subscription_request(
            bars={"BTC/USD"}, quotes={"ETH/USD"}, trades=None, news=None
        )
        assert bars == {"BTC/USD"}
        assert quotes == {"ETH/USD"}
        assert warnings == []


# ---------------------------------------------------------------------------
# Fallback fanout: a raising client must not block other clients
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_raising_client_does_not_block_other_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In the fallback fanout path, one client's on_data raising must be
    isolated — every other subscribed client still receives the message, and
    the failure is recorded as an error dispatch (not a delivery)."""

    class _Validator:
        def validate_bar(self, _message: dict[str, str]) -> SimpleNamespace:
            return SimpleNamespace(valid=True, error_codes=[])

    monkeypatch.setattr(stream_module, "get_validator", lambda: _Validator())

    dispatch_events: list[str] = []
    monkeypatch.setattr(
        stream_module,
        "record_stream_fanout_dispatch_event",
        dispatch_events.append,
    )

    delivered: list[str] = []

    async def _on_data(client_id: str, _data_type: str, _envelope: dict) -> None:
        if client_id == "bad":
            raise RuntimeError("client send failed")
        delivered.append(client_id)

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
        on_broadcast=None,  # force the fallback per-client fanout path
        fanout_batch_size=10,  # all clients dispatched together in one gather
    )

    subscribers = ["good-1", "bad", "good-2", "good-3"]

    class _Subscriptions:
        def get_clients_for_symbol(self, symbol: str, _data_type: str) -> list[str]:
            return list(subscribers) if symbol == "AAPL" else []

        def get_clients_for_symbol_view(self, symbol: str, _data_type: str) -> list[str]:
            return self.get_clients_for_symbol(symbol, _data_type)

    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(Any, SimpleNamespace(subscriptions=_Subscriptions()))

    await multiplexer._handle_message(
        AlpacaStreamType.STOCKS_SIP,
        {"T": "b", "S": "AAPL", "t": "2026-06-10T13:30:00Z", "o": 10, "h": 10, "l": 10, "c": 10, "v": 1},
    )

    # Every healthy client got the message despite "bad" raising.
    assert sorted(delivered) == ["good-1", "good-2", "good-3"]
    # One error dispatch (the raising client) and three deliveries.
    assert dispatch_events.count("error") == 1
    assert dispatch_events.count("delivered") == 3


@pytest.mark.asyncio
async def test_slow_client_times_out_without_blocking_others(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A client whose on_data hangs is bounded by the per-client fanout
    timeout and recorded as a timeout — the other clients still get delivered.
    The 5s production timeout is patched down so the test stays fast."""

    class _Validator:
        def validate_bar(self, _message: dict[str, str]) -> SimpleNamespace:
            return SimpleNamespace(valid=True, error_codes=[])

    monkeypatch.setattr(stream_module, "get_validator", lambda: _Validator())

    dispatch_events: list[str] = []
    monkeypatch.setattr(
        stream_module,
        "record_stream_fanout_dispatch_event",
        dispatch_events.append,
    )

    # Shrink the per-client fanout timeout so a hung client resolves quickly.
    real_wait_for = asyncio.wait_for

    async def _fast_wait_for(awaitable: Any, timeout: float) -> Any:
        return await real_wait_for(awaitable, timeout=0.05)

    monkeypatch.setattr(stream_module.asyncio, "wait_for", _fast_wait_for)

    delivered: list[str] = []

    async def _on_data(client_id: str, _data_type: str, _envelope: dict) -> None:
        if client_id == "slow":
            await asyncio.sleep(10)  # never completes within the (patched) timeout
        delivered.append(client_id)

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
        on_broadcast=None,
        fanout_batch_size=10,
    )

    subscribers = ["fast-1", "slow", "fast-2"]

    class _Subscriptions:
        def get_clients_for_symbol(self, symbol: str, _data_type: str) -> list[str]:
            return list(subscribers) if symbol == "AAPL" else []

        def get_clients_for_symbol_view(self, symbol: str, _data_type: str) -> list[str]:
            return self.get_clients_for_symbol(symbol, _data_type)

    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(Any, SimpleNamespace(subscriptions=_Subscriptions()))

    await multiplexer._handle_message(
        AlpacaStreamType.STOCKS_SIP,
        {"T": "b", "S": "AAPL", "t": "2026-06-10T13:30:00Z", "o": 10, "h": 10, "l": 10, "c": 10, "v": 1},
    )

    assert sorted(delivered) == ["fast-1", "fast-2"]
    assert dispatch_events.count("timeout") == 1
    assert dispatch_events.count("delivered") == 2
