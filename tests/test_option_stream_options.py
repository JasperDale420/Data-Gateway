from __future__ import annotations

import msgpack
import pytest

from gateway.core.stream import AlpacaStreamType, StreamMultiplexer, UpstreamConnection


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
