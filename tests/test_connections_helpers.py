from __future__ import annotations

import asyncio

import pytest

from gateway.core.auth import Client, ClientPermissions
from gateway.core.connections import (
    _BROADCAST_SEND_TIMEOUT_SECONDS,
    Connection,
    ConnectionManager,
    is_benign_ws_close_error,
)


@pytest.mark.parametrize(
    "message",
    [
        # Existing patterns kept working.
        "(1006, None)",
        "ConnectionClosedError: no close frame received (1006)",
        "WebSocketDisconnect: 1001",
        "Unexpected ASGI message 'websocket.send', after transfer_data_task completed.",
        # Patterns added to silence the close race noise observed in today's logs.
        'Cannot call "send" once a close message has been sent.',
        "RuntimeError: WebSocket is not connected.",
    ],
)
def test_is_benign_ws_close_error_matches_known_close_races(message: str) -> None:
    assert is_benign_ws_close_error(RuntimeError(message)) is True


@pytest.mark.parametrize(
    "message",
    [
        "ValueError: 'filled' is not a valid QueryOrderStatus",
        "RuntimeError: upstream API returned 500",
        "TimeoutError",
    ],
)
def test_is_benign_ws_close_error_ignores_unrelated_errors(message: str) -> None:
    assert is_benign_ws_close_error(RuntimeError(message)) is False


def test_is_benign_ws_close_error_recognizes_websocketdisconnect_with_empty_str() -> None:
    from starlette.websockets import WebSocketDisconnect

    exc = WebSocketDisconnect(code=1006, reason=None)
    assert str(exc) == ""
    assert is_benign_ws_close_error(exc) is True


def test_is_benign_ws_close_error_recognizes_connectionclosed_with_empty_str() -> None:
    from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

    assert is_benign_ws_close_error(ConnectionClosedOK(None, None)) is True
    assert is_benign_ws_close_error(ConnectionClosedError(None, None)) is True


class _SlowWebSocket:
    """Mock WebSocket whose `send_*` and `close` block forever until released."""

    def __init__(self) -> None:
        self._release = asyncio.Event()
        self.close_called = False

    async def send_text(self, payload: str) -> None:
        await self._release.wait()

    async def send_bytes(self, payload: bytes) -> None:
        await self._release.wait()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_called = True


class _FastWebSocket:
    """Mock WebSocket that records sent payloads immediately."""

    def __init__(self) -> None:
        self.sent: list[str | bytes] = []

    async def send_text(self, payload: str) -> None:
        self.sent.append(payload)

    async def send_bytes(self, payload: bytes) -> None:
        self.sent.append(payload)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        pass


def _make_client(client_id: str) -> Client:
    return Client(
        id=client_id,
        permissions=ClientPermissions(providers=["alpaca"]),
    )


@pytest.mark.asyncio
async def test_broadcast_slow_client_does_not_stall_other_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One client that never drains its send buffer must not block the rest
    of the fanout. The slow client should time out within
    ``_BROADCAST_SEND_TIMEOUT_SECONDS`` and be force-closed so subsequent
    broadcasts don't keep waiting on it.

    Regression — codex caught: `broadcast` / `broadcast_to_connection_ids`
    did unbounded `await connection.websocket.send_text(...)` inside an
    `asyncio.gather`. One stuck client backpressured every other client
    AND the upstream Alpaca read loop (which awaits the broadcast inline),
    risking Alpaca's 1006 keepalive disconnect.
    """
    # Use a tight timeout so the test doesn't hang for 2 seconds.
    monkeypatch.setattr("gateway.core.connections._BROADCAST_SEND_TIMEOUT_SECONDS", 0.2)

    manager = ConnectionManager()
    slow_ws = _SlowWebSocket()
    fast_ws = _FastWebSocket()

    slow_conn = Connection(
        websocket=slow_ws,  # type: ignore[arg-type]
        client=_make_client("slow"),
        authenticated=True,
    )
    fast_conn = Connection(
        websocket=fast_ws,  # type: ignore[arg-type]
        client=_make_client("fast"),
        authenticated=True,
    )
    manager._connections["slow"] = slow_conn
    manager._connections["fast"] = fast_conn
    manager._client_map["slow"] = {"slow"}
    manager._client_map["fast"] = {"fast"}

    # Broadcast must complete within ~the send-timeout budget, NOT hang
    # waiting on the slow client.
    start = asyncio.get_running_loop().time()
    delivered = await asyncio.wait_for(manager.broadcast({"hello": "world"}), timeout=1.0)
    elapsed = asyncio.get_running_loop().time() - start

    # Only the fast client received the payload.
    assert delivered == 1
    assert len(fast_ws.sent) == 1
    # Slow client was force-closed by the timeout handler.
    assert slow_ws.close_called is True
    # Sanity: broadcast finished in roughly the timeout window, not the
    # default 2.0s production budget. Gives 0.5s of slack for CI jitter.
    assert elapsed < 0.7, f"broadcast should not have stalled on slow client (elapsed={elapsed:.2f}s)"


@pytest.mark.asyncio
async def test_broadcast_to_connection_ids_slow_client_does_not_stall_fanout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same regression as `broadcast`, but for the per-connection-id variant
    used by the stream multiplexer's per-symbol fanout path."""
    monkeypatch.setattr("gateway.core.connections._BROADCAST_SEND_TIMEOUT_SECONDS", 0.2)

    manager = ConnectionManager()
    slow_ws = _SlowWebSocket()
    fast_ws = _FastWebSocket()

    slow_conn = Connection(
        websocket=slow_ws,  # type: ignore[arg-type]
        client=_make_client("slow"),
        authenticated=True,
    )
    fast_conn = Connection(
        websocket=fast_ws,  # type: ignore[arg-type]
        client=_make_client("fast"),
        authenticated=True,
    )
    manager._connections["slow"] = slow_conn
    manager._connections["fast"] = fast_conn

    delivered = await asyncio.wait_for(
        manager.broadcast_to_connection_ids({"hello": "world"}, ["slow", "fast"]),
        timeout=1.0,
    )

    assert delivered == 1
    assert len(fast_ws.sent) == 1
    assert slow_ws.close_called is True


def test_broadcast_send_timeout_is_sane_default() -> None:
    """The production timeout should be small enough to keep fanout flowing
    under a stuck client, but large enough to absorb realistic GC jitter.
    """
    assert 1.0 <= _BROADCAST_SEND_TIMEOUT_SECONDS <= 5.0


@pytest.mark.asyncio
async def test_slow_client_skipped_on_subsequent_broadcasts(monkeypatch: pytest.MonkeyPatch) -> None:
    """After a slow client times out and is force-closed, subsequent broadcasts
    must skip it entirely — not re-pay the timeout cost on every event.

    Regression — codex caught: the initial fix closed the slow WebSocket but
    left the Connection in `_connections` / `_client_map` still flagged as
    authenticated, so target selection in subsequent broadcasts kept including
    it and re-paid the full timeout per event.
    """
    monkeypatch.setattr("gateway.core.connections._BROADCAST_SEND_TIMEOUT_SECONDS", 0.2)

    manager = ConnectionManager()
    slow_ws = _SlowWebSocket()
    fast_ws = _FastWebSocket()

    slow_conn = Connection(
        websocket=slow_ws,  # type: ignore[arg-type]
        client=_make_client("slow"),
        authenticated=True,
    )
    fast_conn = Connection(
        websocket=fast_ws,  # type: ignore[arg-type]
        client=_make_client("fast"),
        authenticated=True,
    )
    manager._connections["slow"] = slow_conn
    manager._connections["fast"] = fast_conn
    manager._client_map["slow"] = {"slow"}
    manager._client_map["fast"] = {"fast"}

    # First broadcast — slow client times out and is force-closed.
    await asyncio.wait_for(manager.broadcast({"seq": 1}), timeout=1.0)
    assert slow_ws.close_called is True
    assert slow_conn.authenticated is False, "slow client must be unauthenticated to be skipped next time"

    # Second broadcast — must complete fast because the slow client is now
    # skipped by target selection.
    start = asyncio.get_running_loop().time()
    delivered = await asyncio.wait_for(manager.broadcast({"seq": 2}), timeout=1.0)
    elapsed = asyncio.get_running_loop().time() - start

    # Only the fast client received the second broadcast.
    assert delivered == 1
    assert len(fast_ws.sent) == 2
    # Second broadcast must be fast — no second 200ms timeout against the slow client.
    assert elapsed < 0.1, f"second broadcast still paid timeout for slow client (elapsed={elapsed:.3f}s)"
