"""Ancillary Alpaca stock channels -> Heber contract.

``MESSAGE_TYPE_TO_DATA_TYPE`` mapped only b/q/t/n, so every LULD (``l``),
trading status (``s``), trade correction (``c``) and auction imbalance (``i``)
message was discarded in ``_handle_message`` before it could reach the Heber
sink — even though Heber contracts all four as typed Silver datasets.

Two things have to be right for these to land in Silver rather than the DLQ:

1. The message type must map to the canonical feed name Heber contracts
   (``lulds`` / ``statuses`` / ``corrections`` / ``auctions``) — anything else
   is DLQ'd with reason ``uncontracted_feed``.
2. The payload keys must be the Silver column names. Heber's ``FIELD_MAPPINGS``
   entry for these feeds is EMPTY, so its normalizer reads Silver column names
   straight off the payload; Alpaca's two-letter wire keys (``u``/``d``/``sc``/
   ``oi``…) would normalize to an all-null row and then fail Heber's
   ``REQUIRED_FIELDS_BY_FEED`` check. The rename has to happen gateway-side.

The expected column names below mirror ``SILVER_SCHEMAS`` in
``Heber/heber/schemas/silver.py`` — keep them in lockstep.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import msgpack
import orjson
import pytest
from websockets.protocol import State

from gateway.core.stream import (
    MESSAGE_TYPE_TO_DATA_TYPE,
    AlpacaStreamType,
    StreamMultiplexer,
    UpstreamConnection,
)

# Heber Silver columns per feed (heber/schemas/silver.py), minus the envelope
# fields the writer fills in itself (event_id, provider, ts_*, quality_flags…).
HEBER_SILVER_COLUMNS: dict[str, set[str]] = {
    "lulds": {"timestamp", "upper_limit", "lower_limit", "indicator"},
    "statuses": {"timestamp", "status_code", "status_message", "reason_code", "reason_message", "tape"},
    "corrections": {
        "timestamp",
        "exchange",
        "original_trade_id",
        "original_price",
        "original_size",
        "original_conditions",
        "corrected_trade_id",
        "corrected_price",
        "corrected_size",
        "corrected_conditions",
        "tape",
    },
    "auctions": {
        "auction_type",
        "auction_price",
        "imbalance_size",
        "imbalance_side",
        "paired_shares",
        "reference_price",
    },
}

# Heber REQUIRED_FIELDS_BY_FEED (heber/writer/ingest_contracts.py) — a row with
# any of these null is rejected before it reaches Silver. ``auctions`` has no
# entry, so it has no required columns.
HEBER_REQUIRED_COLUMNS: dict[str, set[str]] = {
    "lulds": {"upper_limit", "lower_limit"},
    "statuses": {"status_code"},
    "corrections": {"original_trade_id", "original_price", "original_size"},
    "auctions": set(),
}

# Verbatim Alpaca wire messages (docs.alpaca.markets real-time stock pricing).
ALPACA_WIRE_MESSAGES: dict[str, dict[str, Any]] = {
    "lulds": {
        "T": "l",
        "S": "AAPL",
        "u": 3.24,
        "d": 2.65,
        "i": "B",
        "t": "2026-08-14T13:31:01.531584Z",
        "z": "C",
    },
    "statuses": {
        "T": "s",
        "S": "AAPL",
        "sc": "H",
        "sm": "Trading Halt",
        "rc": "T12",
        "rm": "Trading Halted; For information requested by NASDAQ",
        "t": "2026-08-14T13:59:16.412340Z",
        "z": "C",
    },
    "corrections": {
        "T": "c",
        "S": "AAPL",
        "x": "M",
        "oi": 52983525033527,
        "op": 39.1582,
        "os": 100,
        "oc": ["@", "I"],
        "ci": 52983525033528,
        "cp": 39.16,
        "cs": 100,
        "cc": ["@"],
        "t": "2026-08-14T13:45:00.000000Z",
        "z": "C",
    },
    "auctions": {
        "T": "i",
        "S": "AAPL",
        "p": 9.12,
        "t": "2026-08-14T19:59:00.000000Z",
        "z": "C",
    },
}


async def _capture_envelope(message: dict[str, Any]) -> dict[str, Any]:
    """Run one upstream message through the SIP handler, return the envelope."""
    captured: list[dict[str, Any]] = []

    async def _on_envelope(envelope: dict[str, Any]) -> None:
        captured.append(envelope)

    async def _on_data(_client_id: str, _data_type: str, _envelope: dict) -> None:  # pragma: no cover
        raise AssertionError("no client is subscribed; fanout must not run")

    multiplexer = StreamMultiplexer(
        api_key="test-key",  # pragma: allowlist secret
        api_secret="test-secret",  # pragma: allowlist secret
        on_data=_on_data,
        lazy_connect=True,
        on_envelope=_on_envelope,
    )

    class _Subscriptions:
        def get_clients_for_symbol_view(self, _symbol: str, _data_type: str) -> frozenset[str]:
            return frozenset()

    multiplexer._connections[AlpacaStreamType.STOCKS_SIP] = cast(Any, SimpleNamespace(subscriptions=_Subscriptions()))

    await multiplexer._handle_message(AlpacaStreamType.STOCKS_SIP, message)

    assert len(captured) == 1, f"expected exactly one envelope, got {len(captured)}"
    return captured[0]


def _make_upstream(stream_type: AlpacaStreamType) -> UpstreamConnection:
    async def _noop(_msg: dict) -> None:
        return

    return UpstreamConnection(
        stream_type=stream_type,
        api_key="k",  # pragma: allowlist secret
        api_secret="s",  # pragma: allowlist secret
        on_message=_noop,
    )


class _RecordingWebSocket:
    """Captures the frames a subscribe/unsubscribe would put on the wire."""

    # is_connected reads .state off the socket; report OPEN so the real
    # connected-path code runs against the fake.
    state = State.OPEN

    def __init__(self) -> None:
        self.frames: list[dict[str, Any]] = []

    async def send(self, payload: str | bytes) -> None:
        # The OPRA stream encodes with msgpack, every other stream with JSON.
        if isinstance(payload, bytes):
            self.frames.append(msgpack.unpackb(payload))
        else:
            self.frames.append(orjson.loads(payload))


def _connected_upstream(stream_type: AlpacaStreamType) -> tuple[UpstreamConnection, _RecordingWebSocket]:
    conn = _make_upstream(stream_type)
    ws = _RecordingWebSocket()
    conn._ws = cast(Any, ws)
    conn._authenticated = True
    return conn, ws


# ---------------------------------------------------------------------------
# 1. Message-type mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("msg_type", "feed"),
    [("l", "lulds"), ("s", "statuses"), ("c", "corrections"), ("i", "auctions")],
)
def test_ancillary_message_types_map_to_heber_contracted_feeds(msg_type: str, feed: str) -> None:
    """Each ancillary wire type resolves to the feed name Heber contracts."""
    assert MESSAGE_TYPE_TO_DATA_TYPE.get(msg_type) == feed


def test_existing_message_type_mapping_is_unchanged() -> None:
    """The four original mappings are a frozen wire contract with Heber."""
    for msg_type, feed in (("b", "bars"), ("q", "quotes"), ("t", "trades"), ("n", "news")):
        assert MESSAGE_TYPE_TO_DATA_TYPE[msg_type] == feed


# ---------------------------------------------------------------------------
# 2. Envelope shape per feed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("feed", sorted(ALPACA_WIRE_MESSAGES))
async def test_ancillary_envelope_matches_heber_envelope_contract(feed: str) -> None:
    """Feed name, instrument type and instrument key match Heber's validator."""
    envelope = await _capture_envelope(dict(ALPACA_WIRE_MESSAGES[feed]))

    assert envelope["feed"] == feed
    assert envelope["instrument_type"] == "equity"
    assert envelope["instrument_key"] == "equity:AAPL"
    assert envelope["symbol"] == "AAPL"
    assert envelope["ts_event"].startswith("2026-08-14T")


@pytest.mark.parametrize("feed", sorted(ALPACA_WIRE_MESSAGES))
async def test_ancillary_payload_uses_heber_silver_column_names(feed: str) -> None:
    """Every required Silver column is present and non-null in the payload.

    Heber's FIELD_MAPPINGS for these feeds is empty — the payload keys ARE the
    Silver column names, so the two-letter Alpaca keys must be renamed here.
    """
    payload = (await _capture_envelope(dict(ALPACA_WIRE_MESSAGES[feed])))["payload"]

    for column in HEBER_REQUIRED_COLUMNS[feed]:
        assert payload.get(column) is not None, (
            f"{feed}.{column} would be rejected by Heber as a missing required field"
        )


async def test_luld_payload_carries_the_band_prices() -> None:
    payload = (await _capture_envelope(dict(ALPACA_WIRE_MESSAGES["lulds"])))["payload"]

    assert payload["upper_limit"] == 3.24
    assert payload["lower_limit"] == 2.65
    assert payload["indicator"] == "B"
    assert payload["timestamp"] == "2026-08-14T13:31:01.531584Z"


async def test_status_payload_carries_the_halt_reason() -> None:
    payload = (await _capture_envelope(dict(ALPACA_WIRE_MESSAGES["statuses"])))["payload"]

    assert payload["status_code"] == "H"
    assert payload["status_message"] == "Trading Halt"
    assert payload["reason_code"] == "T12"
    assert payload["reason_message"].startswith("Trading Halted")
    assert payload["tape"] == "C"


async def test_correction_payload_carries_both_sides_of_the_correction() -> None:
    payload = (await _capture_envelope(dict(ALPACA_WIRE_MESSAGES["corrections"])))["payload"]

    assert payload["exchange"] == "M"
    assert payload["original_trade_id"] == 52983525033527
    assert payload["original_price"] == 39.1582
    assert payload["original_size"] == 100
    assert payload["original_conditions"] == ["@", "I"]
    assert payload["corrected_trade_id"] == 52983525033528
    assert payload["corrected_price"] == 39.16
    assert payload["corrected_size"] == 100
    assert payload["corrected_conditions"] == ["@"]
    assert payload["tape"] == "C"


async def test_auction_payload_carries_the_imbalance_price() -> None:
    """Alpaca's imbalance message carries only a price — the other five
    ``auctions`` columns come from the REST /v2/stocks/auctions endpoint and
    stay null here rather than being invented."""
    payload = (await _capture_envelope(dict(ALPACA_WIRE_MESSAGES["auctions"])))["payload"]

    assert payload["auction_price"] == 9.12
    assert "imbalance_size" not in payload


@pytest.mark.parametrize("feed", sorted(ALPACA_WIRE_MESSAGES))
async def test_ancillary_payload_emits_no_unknown_silver_columns(feed: str) -> None:
    """Renamed keys must all be real Silver columns — a typo here writes a
    column Heber silently drops, which reads as 'the feed has no data'."""
    payload = (await _capture_envelope(dict(ALPACA_WIRE_MESSAGES[feed])))["payload"]
    raw_wire_keys = set(ALPACA_WIRE_MESSAGES[feed])

    renamed = set(payload) - raw_wire_keys
    assert renamed <= HEBER_SILVER_COLUMNS[feed], f"{feed}: not Silver columns: {renamed - HEBER_SILVER_COLUMNS[feed]}"


async def test_ancillary_envelopes_reach_the_sink_with_no_subscribed_clients() -> None:
    """No client can subscribe to these feeds — the sink dispatch is the whole
    point, so it must fire on the empty-clients path."""
    envelope = await _capture_envelope(dict(ALPACA_WIRE_MESSAGES["statuses"]))

    assert envelope["source"] == "websocket"
    assert envelope["event_id"]


# ---------------------------------------------------------------------------
# 3. Upstream subscription — nothing arrives without it
# ---------------------------------------------------------------------------


async def test_stock_subscribe_requests_statuses_and_lulds_in_a_separate_frame() -> None:
    """statuses/lulds need an explicit upstream subscribe. It goes in its own
    frame so an Alpaca rejection cannot take the bars/quotes/trades
    subscription down with it."""
    conn, ws = _connected_upstream(AlpacaStreamType.STOCKS_SIP)

    await conn.subscribe(bars={"AAPL"}, quotes=set(), trades={"MSFT"}, news=set())

    assert len(ws.frames) == 2
    core, ancillary = ws.frames
    assert core["action"] == "subscribe"
    assert sorted(core["bars"]) == ["AAPL"]
    assert sorted(core["trades"]) == ["MSFT"]
    assert "statuses" not in core and "lulds" not in core

    assert ancillary["action"] == "subscribe"
    # Ancillary channels cover every symbol we already stream, not just trades.
    assert sorted(ancillary["statuses"]) == ["AAPL", "MSFT"]
    assert sorted(ancillary["lulds"]) == ["AAPL", "MSFT"]


async def test_stock_subscribe_does_not_request_imbalances() -> None:
    """The imbalances channel is unverified on this account's feed and its
    payload fills only one of six ``auctions`` columns — the ``i`` mapping is
    in place, but we do not subscribe."""
    conn, ws = _connected_upstream(AlpacaStreamType.STOCKS_SIP)

    await conn.subscribe(trades={"AAPL"})

    assert all("imbalances" not in frame for frame in ws.frames)


async def test_stock_unsubscribe_drops_statuses_and_lulds() -> None:
    conn, ws = _connected_upstream(AlpacaStreamType.STOCKS_SIP)

    await conn.unsubscribe(trades={"AAPL"})

    assert len(ws.frames) == 2
    assert ws.frames[1] == {"action": "unsubscribe", "statuses": ["AAPL"], "lulds": ["AAPL"]}


async def test_ancillary_unsubscribe_spares_symbols_still_streamed_on_another_feed() -> None:
    """The removed_* sets handed to unsubscribe() are PER FEED — a symbol
    dropped from trades may still be streamed on quotes. Unsubscribing its
    ancillary channels anyway would silently stop halts and LULD bands for a
    symbol we are still streaming, which is the exact class of silent Heber gap
    this change exists to close."""
    conn, ws = _connected_upstream(AlpacaStreamType.STOCKS_SIP)
    conn.subscriptions.subscribe("client-1", quotes=["AAPL"], trades=["AAPL", "MSFT"])
    ws.frames.clear()

    # Client drops trades only. AAPL survives on quotes; MSFT is gone entirely.
    _bars, _quotes, removed_trades, _news = conn.subscriptions.unsubscribe("client-1", trades=["AAPL", "MSFT"])
    await conn.unsubscribe(trades=removed_trades)

    assert len(ws.frames) == 2
    assert ws.frames[1] == {"action": "unsubscribe", "statuses": ["MSFT"], "lulds": ["MSFT"]}


async def test_ancillary_unsubscribe_is_skipped_when_every_symbol_is_still_streamed() -> None:
    conn, ws = _connected_upstream(AlpacaStreamType.STOCKS_SIP)
    conn.subscriptions.subscribe("client-1", quotes=["AAPL"], trades=["AAPL"])
    ws.frames.clear()

    _bars, _quotes, removed_trades, _news = conn.subscriptions.unsubscribe("client-1", trades=["AAPL"])
    await conn.unsubscribe(trades=removed_trades)

    assert len(ws.frames) == 1  # core unsubscribe only, no ancillary frame
    assert "statuses" not in ws.frames[0]


async def test_options_stream_does_not_request_stock_ancillary_channels() -> None:
    """OPRA has no statuses/lulds channels — sending them would be an error."""
    conn, ws = _connected_upstream(AlpacaStreamType.OPTIONS)

    await conn.subscribe(quotes={"AAPL260116C00200000"}, trades={"AAPL260116C00200000"})

    assert len(ws.frames) == 1
    assert "statuses" not in ws.frames[0]


async def test_empty_stock_subscribe_sends_no_ancillary_frame() -> None:
    conn, ws = _connected_upstream(AlpacaStreamType.STOCKS_SIP)

    await conn.subscribe(bars=set(), quotes=set(), trades=set(), news=set())

    assert ws.frames == []
