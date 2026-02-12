"""WebSocket integration tests."""

from fastapi.testclient import TestClient


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
