"""Tests for middleware behavior with streaming and large responses."""

from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.testclient import TestClient

from gateway.api import deps as deps_module
from gateway.api.middleware import CacheMiddleware, EventEnvelopeMiddleware


async def _json_stream(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


def test_cache_middleware_bypasses_streaming_json() -> None:
    app = FastAPI()
    app.add_middleware(CacheMiddleware, default_ttl=60, max_body_bytes=1024)

    @app.get("/health/stream")
    async def stream_endpoint():
        return StreamingResponse(_json_stream(b'{"success":true,"data":{"value":1}}'))

    client = TestClient(app)

    response1 = client.get("/health/stream")
    response2 = client.get("/health/stream")

    assert response1.status_code == 200
    assert response2.status_code == 200
    assert response1.headers["X-Gateway-Cache"] == "BYPASS"
    assert response2.headers["X-Gateway-Cache"] == "BYPASS"
    assert response1.json()["data"]["value"] == 1
    assert response2.json()["data"]["value"] == 1


def test_envelope_middleware_wraps_small_json() -> None:
    app = FastAPI()
    app.add_middleware(EventEnvelopeMiddleware, max_body_bytes=4096)

    @app.get("/api/v1/alpaca/bars")
    async def bars_endpoint():
        return JSONResponse({"success": True, "data": {"symbol": "AAPL", "close": 123.45}})

    client = TestClient(app)
    response = client.get("/api/v1/alpaca/bars")
    data = response.json()

    assert response.status_code == 200
    assert response.headers["X-Gateway-Envelope"] == "true"
    assert data["success"] is True
    assert "envelope" in data
    assert data["data"]["symbol"] == "AAPL"


def test_envelope_middleware_bypasses_streaming_json() -> None:
    app = FastAPI()
    app.add_middleware(EventEnvelopeMiddleware, max_body_bytes=4096)

    @app.get("/api/v1/alpaca/bars")
    async def stream_endpoint():
        return StreamingResponse(_json_stream(b'{"success":true,"data":{"symbol":"AAPL"}}'))

    client = TestClient(app)
    response = client.get("/api/v1/alpaca/bars")
    data = response.json()

    assert response.status_code == 200
    assert "X-Gateway-Envelope" not in response.headers
    assert "envelope" not in data
    assert data["data"]["symbol"] == "AAPL"


def test_envelope_middleware_bypasses_large_json() -> None:
    app = FastAPI()
    app.add_middleware(EventEnvelopeMiddleware, max_body_bytes=128)

    @app.get("/api/v1/alpaca/bars")
    async def large_endpoint():
        return JSONResponse(
            {
                "success": True,
                "data": {"items": [{"symbol": "AAPL", "close": i} for i in range(200)]},
            }
        )

    client = TestClient(app)
    response = client.get("/api/v1/alpaca/bars")
    data = response.json()

    assert response.status_code == 200
    assert "X-Gateway-Envelope" not in response.headers
    assert "envelope" not in data
    assert "items" in data["data"]


def test_envelope_middleware_wraps_uw_flow_list_with_sink_enabled() -> None:
    class _FakeSinkRegistry:
        async def publish_all(self, topic: str, data: dict) -> None:
            return None

    previous_sink = deps_module.get_sink_registry()
    deps_module.set_sink_registry(_FakeSinkRegistry())

    app = FastAPI()
    app.add_middleware(EventEnvelopeMiddleware, max_body_bytes=4096)

    @app.get("/api/v1/uw/flow/all")
    async def flow_endpoint():
        return JSONResponse(
            {"success": True, "data": [{"symbol": "AAPL"}, {"symbol": "MSFT"}]},
        )

    try:
        client = TestClient(app)
        response = client.get("/api/v1/uw/flow/all")
        data = response.json()
    finally:
        deps_module.set_sink_registry(previous_sink)

    assert response.status_code == 200
    assert response.headers["X-Gateway-Envelope"] == "true"
    assert "envelope" in data
    assert len(data["data"]) == 2


def test_sink_envelopes_explode_alpaca_bars_dict_payload() -> None:
    middleware = EventEnvelopeMiddleware(FastAPI())

    payload = {
        "symbol": "AAPL",
        "timeframe": "1Min",
        "bars": [
            {"timestamp": "2026-02-12T14:30:00Z", "open": "100.0", "high": "101.0", "low": "99.5", "close": "100.5"},
            {"timestamp": "2026-02-12T14:31:00Z", "open": "100.5", "high": "101.5", "low": "100.0", "close": "101.0"},
        ],
    }

    envelopes = middleware._build_sink_envelopes(  # noqa: SLF001
        path="/api/v1/alpaca/stocks/AAPL/bars",
        provider="alpaca",
        feed="bars",
        payload=payload,
    )

    assert len(envelopes) == 2
    assert all(env["feed"] == "bars" for env in envelopes)
    assert all(env["symbol"] == "AAPL" for env in envelopes)
    assert envelopes[0]["payload"]["open"] == "100.0"
    assert envelopes[1]["payload"]["close"] == "101.0"


def test_sink_envelopes_explode_alpaca_trades_dict_payload() -> None:
    middleware = EventEnvelopeMiddleware(FastAPI())

    payload = {
        "symbol": "SPY",
        "trades": [
            {"timestamp": "2026-02-12T14:30:00Z", "price": "501.25", "size": 100, "trade_id": "t1"},
            {"timestamp": "2026-02-12T14:30:01Z", "price": "501.30", "size": 50, "trade_id": "t2"},
        ],
    }

    envelopes = middleware._build_sink_envelopes(  # noqa: SLF001
        path="/api/v1/alpaca/stocks/SPY/trades",
        provider="alpaca",
        feed="trades",
        payload=payload,
    )

    assert len(envelopes) == 2
    assert all(env["feed"] == "trades" for env in envelopes)
    assert all(env["symbol"] == "SPY" for env in envelopes)
    assert envelopes[0]["payload"]["price"] == "501.25"
    assert envelopes[1]["payload"]["size"] == 50


def test_sink_envelopes_skip_empty_alpaca_lists() -> None:
    middleware = EventEnvelopeMiddleware(FastAPI())

    empty_bars = middleware._build_sink_envelopes(  # noqa: SLF001
        path="/api/v1/alpaca/stocks/AAPL/bars",
        provider="alpaca",
        feed="bars",
        payload={"symbol": "AAPL", "bars": []},
    )
    empty_trades = middleware._build_sink_envelopes(  # noqa: SLF001
        path="/api/v1/alpaca/stocks/AAPL/trades",
        provider="alpaca",
        feed="trades",
        payload={"symbol": "AAPL", "trades": []},
    )

    assert empty_bars == []
    assert empty_trades == []
