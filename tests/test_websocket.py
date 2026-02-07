"""WebSocket integration tests."""

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import gateway.api.websocket as websocket_module


class _FakeWebSocket:
    def __init__(self, messages):
        self._messages = list(messages)
        self.sent: list[dict] = []

    async def receive(self) -> dict:
        if self._messages:
            return self._messages.pop(0)
        raise WebSocketDisconnect(code=1000)

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)


@pytest.mark.asyncio
async def test_message_loop_uses_provided_max_size_without_settings_lookup(monkeypatch):
    fake_ws = _FakeWebSocket(messages=[{"text": '{"action":"ping"}'}])
    settings_calls = {"count": 0}

    def _count_settings_calls():
        settings_calls["count"] += 1
        raise AssertionError("get_settings should not be called when max_message_size is provided")

    monkeypatch.setattr(websocket_module, "get_settings", _count_settings_calls)

    with pytest.raises(WebSocketDisconnect):
        await websocket_module._message_loop(
            websocket=fake_ws,
            connection_id="conn-1",
            connections=None,  # Not used for oversize payload handling.
            max_message_size=1,
        )

    assert settings_calls["count"] == 0
    assert fake_ws.sent
    assert fake_ws.sent[0]["error_code"] == "GW-E8005"


def test_websocket_subscribe_with_feeds(client: TestClient, test_api_key: str):
    """Subscribe using feeds list (preferred schema)."""
    with client.websocket_connect("/ws") as websocket:
        websocket.send_json({"action": "auth", "key": test_api_key})
        auth = websocket.receive_json()
        assert auth["type"] == "auth_result"
        assert auth["status"] == "ok"

        websocket.send_json(
            {
                "action": "subscribe",
                "feeds": ["stock_bars"],
                "symbols": ["AAPL"],
            }
        )
        response = websocket.receive_json()
        assert response["type"] == "subscription_ack"
        assert response["status"] == "ok"
        assert response["feeds"] == ["stock_bars"]
        assert "AAPL" in response["subscribed"]
