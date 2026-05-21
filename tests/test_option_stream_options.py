from __future__ import annotations

import json

import msgpack
import pytest

from gateway.core.stream import AlpacaStreamType, StreamMultiplexer, UpstreamConnection


def _decode(stream_type: AlpacaStreamType, raw: bytes | str) -> dict:
    """Decode the upstream payload using the format the stream expects.

    OPRA (OPTIONS) is msgpack; SIP/IEX/CRYPTO/NEWS are JSON.
    """
    if stream_type == AlpacaStreamType.OPTIONS:
        return msgpack.unpackb(raw, raw=False)
    return json.loads(raw)


class _FakeWebSocket:
    def __init__(self) -> None:
        self.messages: list[bytes | str] = []
        self.open = True

    async def send(self, message: bytes | str) -> None:
        self.messages.append(message)


@pytest.mark.asyncio
async def test_option_upstream_connection_strips_bar_subscriptions() -> None:
    connection = UpstreamConnection(
        stream_type=AlpacaStreamType.OPTIONS,
        api_key="test",  # pragma: allowlist secret
        api_secret="test",  # pragma: allowlist secret
        on_message=lambda _: None,
    )
    connection._ws = _FakeWebSocket()
    connection._authenticated = True

    await connection.subscribe(
        bars={"SPY260310C00500000"}, quotes={"SPY260310C00500000"}, trades={"SPY260310C00500000"}
    )

    assert len(connection._ws.messages) == 1
    decoded = msgpack.unpackb(connection._ws.messages[0], raw=False)
    assert decoded["action"] == "subscribe"
    assert "bars" not in decoded
    assert decoded["quotes"] == ["SPY260310C00500000"]
    assert decoded["trades"] == ["SPY260310C00500000"]


@pytest.mark.asyncio
async def test_options_stream_drops_equity_tickers_silently_invalid_symbols() -> None:
    """The OPRA upstream rejects non-OCC symbols. Mixing equity tickers
    in causes Alpaca to silently drop the entire subscribe (2026-05-01 RCA)."""
    connection = UpstreamConnection(
        stream_type=AlpacaStreamType.OPTIONS,
        api_key="test",  # pragma: allowlist secret
        api_secret="test",  # pragma: allowlist secret
        on_message=lambda _: None,
    )
    connection._ws = _FakeWebSocket()
    connection._authenticated = True

    warnings = await connection.subscribe(
        quotes={"BMY", "BMY260529P00057000", "DHR", "DHR260618P00160000"},
    )

    assert len(connection._ws.messages) == 1
    decoded = _decode(AlpacaStreamType.OPTIONS, connection._ws.messages[0])
    assert decoded["action"] == "subscribe"
    # Only OCC contracts forwarded upstream
    assert sorted(decoded["quotes"]) == ["BMY260529P00057000", "DHR260618P00160000"]
    # Warning surfaced for the dropped equity tickers
    assert any("options stream ignoring" in w.lower() and "quotes" in w for w in warnings)


@pytest.mark.asyncio
async def test_stocks_sip_stream_drops_option_contracts() -> None:
    """The SIP upstream rejects OCC symbols. Filter them at sanitize time."""
    connection = UpstreamConnection(
        stream_type=AlpacaStreamType.STOCKS_SIP,
        api_key="test",  # pragma: allowlist secret
        api_secret="test",  # pragma: allowlist secret
        on_message=lambda _: None,
    )
    connection._ws = _FakeWebSocket()
    connection._authenticated = True

    warnings = await connection.subscribe(
        quotes={"BMY", "BMY260529P00057000", "DHR", "DHR260618P00160000"},
        trades={"AAPL", "AAPL260116C00200000"},
    )

    assert len(connection._ws.messages) == 1
    decoded = _decode(AlpacaStreamType.STOCKS_SIP, connection._ws.messages[0])
    assert sorted(decoded["quotes"]) == ["BMY", "DHR"]
    assert sorted(decoded["trades"]) == ["AAPL"]
    assert any("stocks stream ignoring" in w.lower() for w in warnings)


@pytest.mark.asyncio
async def test_stocks_iex_stream_drops_option_contracts() -> None:
    """Same shape filter applies to STOCKS_IEX (not just SIP)."""
    connection = UpstreamConnection(
        stream_type=AlpacaStreamType.STOCKS_IEX,
        api_key="test",  # pragma: allowlist secret
        api_secret="test",  # pragma: allowlist secret
        on_message=lambda _: None,
    )
    connection._ws = _FakeWebSocket()
    connection._authenticated = True

    await connection.subscribe(quotes={"BMY", "BMY260529P00057000"})

    assert len(connection._ws.messages) == 1
    decoded = _decode(AlpacaStreamType.STOCKS_IEX, connection._ws.messages[0])
    assert decoded["quotes"] == ["BMY"]


@pytest.mark.asyncio
async def test_options_stream_skips_subscribe_when_only_equity_tickers_provided() -> None:
    """If every symbol is filtered out, no upstream message should be sent —
    sending an empty subscribe payload would be wasted and may confuse the
    upstream receiver."""
    connection = UpstreamConnection(
        stream_type=AlpacaStreamType.OPTIONS,
        api_key="test",  # pragma: allowlist secret
        api_secret="test",  # pragma: allowlist secret
        on_message=lambda _: None,
    )
    connection._ws = _FakeWebSocket()
    connection._authenticated = True

    warnings = await connection.subscribe(quotes={"BMY", "DHR"})

    assert connection._ws.messages == []
    assert any("options stream ignoring" in w.lower() for w in warnings)


def test_stream_multiplexer_uses_configured_option_feed() -> None:
    multiplexer = StreamMultiplexer(
        api_key="test",  # pragma: allowlist secret
        api_secret="test",  # pragma: allowlist secret
        on_data=lambda *_: None,
        lazy_connect=True,
        options_feed="indicative",
    )

    options_connection = multiplexer._connections[AlpacaStreamType.OPTIONS]
    assert options_connection.endpoint == "wss://stream.data.alpaca.markets/v1beta1/indicative"
