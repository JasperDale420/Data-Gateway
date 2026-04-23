from __future__ import annotations

import pytest

from gateway.core.connections import is_benign_ws_close_error


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
