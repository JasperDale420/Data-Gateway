"""WebSocket integration tests."""

from fastapi.testclient import TestClient


def test_websocket_subscribe_with_feeds(client: TestClient, test_api_key: str):
    """Subscribe without multiplexer returns honest error (no upstream data)."""
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
        assert response["feeds"] == ["stock_bars"]
        # Without a running multiplexer, Gateway returns honest error
        assert response["status"] == "error"
        assert response["error_code"] == "GW-E5001"
        assert "AAPL" in response["failed"]
        assert response["subscribed"] == []
