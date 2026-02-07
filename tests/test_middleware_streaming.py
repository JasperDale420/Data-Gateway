"""Tests for middleware behavior with streaming and large responses."""

from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.testclient import TestClient
from starlette.requests import Request

from gateway.api.middleware import CacheMiddleware, EventEnvelopeMiddleware


async def _json_stream(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


async def _raising_stream() -> AsyncIterator[bytes]:
    raise AssertionError("response body iterator should not be consumed")
    yield b""


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


@pytest.mark.asyncio
async def test_envelope_middleware_prefers_prebuffered_body_state() -> None:
    app = FastAPI()
    middleware = EventEnvelopeMiddleware(app, max_body_bytes=4096)
    request = Request(
        {"type": "http", "method": "GET", "path": "/api/v1/alpaca/bars", "headers": []}
    )
    request.state._gateway_cached_response_body = b'{"success":true,"data":{"symbol":"AAPL"}}'
    response = StreamingResponse(_raising_stream(), media_type="application/json")

    body = await middleware._get_response_body(request, response)

    assert body == b'{"success":true,"data":{"symbol":"AAPL"}}'


@pytest.mark.asyncio
async def test_envelope_middleware_buffers_when_state_missing() -> None:
    app = FastAPI()
    middleware = EventEnvelopeMiddleware(app, max_body_bytes=4096)
    request = Request(
        {"type": "http", "method": "GET", "path": "/api/v1/alpaca/bars", "headers": []}
    )
    payload = b'{"success":true,"data":{"symbol":"AAPL"}}'
    response = StreamingResponse(_json_stream(payload), media_type="application/json")

    body = await middleware._get_response_body(request, response)

    assert body == payload
    assert request.state._gateway_cached_response_body == payload


def test_cache_and_envelope_work_together_on_hit_and_miss(test_api_key: str) -> None:
    app = FastAPI()
    app.add_middleware(EventEnvelopeMiddleware, max_body_bytes=4096)
    app.add_middleware(CacheMiddleware, default_ttl=60, max_body_bytes=4096)

    @app.get("/api/v1/alpaca/bars")
    async def bars_endpoint():
        return JSONResponse({"success": True, "data": {"symbol": "AAPL", "close": 123.45}})

    client = TestClient(app)
    headers = {"X-Gateway-Key": test_api_key}

    miss_response = client.get("/api/v1/alpaca/bars", headers=headers)
    hit_response = client.get("/api/v1/alpaca/bars", headers=headers)

    assert miss_response.status_code == 200
    assert hit_response.status_code == 200
    assert miss_response.headers["X-Gateway-Cache"] == "MISS"
    assert hit_response.headers["X-Gateway-Cache"] == "HIT"
    assert miss_response.headers["X-Gateway-Envelope"] == "true"
    assert hit_response.headers["X-Gateway-Envelope"] == "true"
    assert miss_response.json()["data"]["symbol"] == "AAPL"
    assert hit_response.json()["data"]["symbol"] == "AAPL"
