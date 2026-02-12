"""Tests for middleware behavior with streaming and large responses."""

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response, StreamingResponse
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


def test_envelope_middleware_short_circuits_prewrapped_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.add_middleware(EventEnvelopeMiddleware, max_body_bytes=4096)

    @app.get("/api/v1/alpaca/bars")
    async def wrapped_endpoint():
        return JSONResponse(
            {
                "success": True,
                "envelope": {"event_id": "evt_cached"},
                "data": {"symbol": "AAPL"},
            },
            headers={"X-Gateway-Envelope": "true"},
        )

    async def _explode_body_read(*_args, **_kwargs):
        raise AssertionError("_get_response_body should not run for pre-wrapped payloads")

    monkeypatch.setattr(EventEnvelopeMiddleware, "_get_response_body", _explode_body_read)

    client = TestClient(app)
    response = client.get("/api/v1/alpaca/bars")

    assert response.status_code == 200
    assert response.headers["X-Gateway-Envelope"] == "true"
    assert response.json()["envelope"]["event_id"] == "evt_cached"


@pytest.mark.asyncio
async def test_envelope_middleware_prefers_prebuffered_body_state() -> None:
    app = FastAPI()
    middleware = EventEnvelopeMiddleware(app, max_body_bytes=4096)
    request = Request({"type": "http", "method": "GET", "path": "/api/v1/alpaca/bars", "headers": []})
    request.state._gateway_cached_response_body = b'{"success":true,"data":{"symbol":"AAPL"}}'
    response = StreamingResponse(_raising_stream(), media_type="application/json")

    body = await middleware._get_response_body(request, response)

    assert body == b'{"success":true,"data":{"symbol":"AAPL"}}'


@pytest.mark.asyncio
async def test_envelope_middleware_prefers_response_body_attribute() -> None:
    app = FastAPI()
    middleware = EventEnvelopeMiddleware(app, max_body_bytes=4096)
    request = Request({"type": "http", "method": "GET", "path": "/api/v1/alpaca/bars", "headers": []})
    payload = b'{"success":true,"data":{"symbol":"AAPL"}}'
    response = Response(content=payload, media_type="application/json")
    response.body_iterator = _raising_stream()

    body = await middleware._get_response_body(request, response)

    assert body == payload
    assert request.state._gateway_cached_response_body == payload


@pytest.mark.asyncio
async def test_envelope_middleware_buffers_when_state_missing() -> None:
    app = FastAPI()
    middleware = EventEnvelopeMiddleware(app, max_body_bytes=4096)
    request = Request({"type": "http", "method": "GET", "path": "/api/v1/alpaca/bars", "headers": []})
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
    cache_buster = uuid4().hex

    miss_response = client.get(f"/api/v1/alpaca/bars?cache_buster={cache_buster}", headers=headers)
    hit_response = client.get(f"/api/v1/alpaca/bars?cache_buster={cache_buster}", headers=headers)

    assert miss_response.status_code == 200
    assert hit_response.status_code == 200
    assert miss_response.headers["X-Gateway-Cache"] == "MISS"
    assert hit_response.headers["X-Gateway-Cache"] == "HIT"
    assert miss_response.headers["X-Gateway-Envelope"] == "true"
    assert hit_response.headers["X-Gateway-Envelope"] == "true"
    assert miss_response.json()["data"]["symbol"] == "AAPL"
    assert hit_response.json()["data"]["symbol"] == "AAPL"


@pytest.mark.parametrize(
    ("path", "expected_provider", "expected_feed"),
    [
        ("/api/v1/alpaca/stocks/AAPL/bars", "alpaca", "bars"),
        ("/api/v1/alpaca/stocks/AAPL/trades", "alpaca", "trades"),
        ("/api/v1/uw/flow/AAPL", "unusual_whales", "flow_alerts"),
        ("/api/v1/uw/gex/AAPL", "unusual_whales", "greek_exposure"),
    ],
)
def test_extract_route_info_maps_cerberus_critical_paths(
    path: str,
    expected_provider: str,
    expected_feed: str,
) -> None:
    middleware = EventEnvelopeMiddleware(FastAPI(), max_body_bytes=4096)
    provider, feed = middleware._extract_route_info(path)
    assert provider == expected_provider
    assert feed == expected_feed


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        (
            "/api/v1/alpaca/stocks/AAPL/bars",
            {"symbol": "AAPL", "bars": [{"timestamp": "2026-02-12T14:31:00Z"}]},
        ),
        (
            "/api/v1/alpaca/stocks/AAPL/trades",
            {"symbol": "AAPL", "trades": [{"timestamp": "2026-02-12T14:31:00Z"}]},
        ),
        (
            "/api/v1/uw/flow/AAPL",
            [{"symbol": "AAPL", "premium": 10000}],
        ),
        (
            "/api/v1/uw/gex/AAPL",
            [{"strike": 200, "net_gex": 1234.5}],
        ),
    ],
)
def test_sink_publish_eligibility_accepts_cerberus_critical_paths(path: str, payload: object) -> None:
    middleware = EventEnvelopeMiddleware(FastAPI(), max_body_bytes=4096)
    provider, feed = middleware._extract_route_info(path)
    assert middleware._is_sink_publish_eligible(path=path, payload=payload, feed=feed) is True
    assert provider in {"alpaca", "unusual_whales"}


def test_sink_publish_eligibility_rejects_screener_aggregate_payloads() -> None:
    middleware = EventEnvelopeMiddleware(FastAPI(), max_body_bytes=4096)
    path = "/api/v1/alpaca/screener/most-actives"
    payload = {"most_actives": [{"symbol": "AAPL"}, {"symbol": "MSFT"}]}
    _, feed = middleware._extract_route_info(path)
    assert middleware._is_sink_publish_eligible(path=path, payload=payload, feed=feed) is False
