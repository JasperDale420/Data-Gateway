from __future__ import annotations

import asyncio

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

import gateway.api.deps as deps_module
from gateway.api.middleware.envelope import EventEnvelopeMiddleware


def test_rest_sink_skips_gex_when_heavy_feed_disabled(monkeypatch):
    monkeypatch.setenv("GATEWAY_REST_SINK_EXCLUDED_FEEDS", "greek_exposure,iv_rank")
    from gateway.config import get_settings

    get_settings.cache_clear()
    middleware = EventEnvelopeMiddleware(app=lambda scope, receive, send: None)

    assert (
        middleware._is_sink_publish_eligible(
            path="/api/v1/uw/gex/SPY",
            payload=[{"symbol": "SPY", "call_gamma": 1}],
            feed="greek_exposure",
        )
        is False
    )


def test_rest_sink_skips_darkpool_rest_when_poller_owns_live_feed(monkeypatch):
    monkeypatch.setenv("GATEWAY_REST_SINK_EXCLUDED_FEEDS", "darkpool,flow_alerts,greek_exposure")
    from gateway.config import get_settings

    get_settings.cache_clear()
    middleware = EventEnvelopeMiddleware(app=lambda scope, receive, send: None)

    assert (
        middleware._is_sink_publish_eligible(
            path="/api/v1/uw/darkpool/SPY",
            payload=[{"symbol": "SPY", "price": 500, "size": 1000}],
            feed="darkpool",
        )
        is False
    )


def test_rest_sink_skips_flow_rest_when_poller_owns_live_feed(monkeypatch):
    monkeypatch.setenv("GATEWAY_REST_SINK_EXCLUDED_FEEDS", "flow_alerts")
    from gateway.config import get_settings

    get_settings.cache_clear()
    middleware = EventEnvelopeMiddleware(app=lambda scope, receive, send: None)

    assert (
        middleware._is_sink_publish_eligible(
            path="/api/v1/uw/flow/SPY",
            payload=[{"ticker": "SPY", "option_chain": "SPY260619C00500000"}],
            feed="flow_alerts",
        )
        is False
    )


def test_rest_sink_allows_alpaca_bars_when_queue_pressure_is_normal(monkeypatch):
    monkeypatch.setenv("GATEWAY_REST_SINK_EXCLUDED_FEEDS", "greek_exposure,flow_alerts,darkpool")
    from gateway.config import get_settings

    get_settings.cache_clear()
    middleware = EventEnvelopeMiddleware(app=lambda scope, receive, send: None)

    assert (
        middleware._is_sink_publish_eligible(
            path="/api/v1/alpaca/stocks/SPY/bars",
            payload={"symbol": "SPY", "bars": [{"timestamp": "2026-06-12T13:30:00Z", "close": "500.0"}]},
            feed="bars",
        )
        is True
    )


def test_queue_pressure_skips_background_publish_but_keeps_response_enveloped(monkeypatch):
    class _PressureSinkRegistry:
        def __init__(self) -> None:
            self.published: list[tuple[str, dict]] = []

        def can_accept_low_priority(self, sink_name: str, *, max_utilization: float) -> bool:
            assert sink_name == "redis_streams"
            assert max_utilization == 0.70
            return False

        async def publish_all(self, topic: str, data: dict) -> None:
            self.published.append((topic, data))

    monkeypatch.delenv("GATEWAY_REST_SINK_LOW_PRIORITY_MAX_QUEUE_UTILIZATION", raising=False)
    monkeypatch.setenv("GATEWAY_REST_SINK_EXCLUDED_FEEDS", "greek_exposure,flow_alerts,darkpool")
    from gateway.config import get_settings

    get_settings.cache_clear()
    sink_registry = _PressureSinkRegistry()
    previous_sink = deps_module.get_sink_registry()
    deps_module.set_sink_registry(sink_registry)

    app = FastAPI()
    app.add_middleware(EventEnvelopeMiddleware, max_body_bytes=4096)

    @app.get("/api/v1/alpaca/stocks/SPY/bars")
    async def bars_endpoint():
        return JSONResponse(
            {
                "success": True,
                "data": {
                    "symbol": "SPY",
                    "timeframe": "1Min",
                    "bars": [
                        {
                            "timestamp": "2026-06-12T13:30:00Z",
                            "open": "500.0",
                            "high": "501.0",
                            "low": "499.5",
                            "close": "500.5",
                            "volume": 1000,
                        }
                    ],
                },
            }
        )

    try:
        client = TestClient(app)
        response = client.get("/api/v1/alpaca/stocks/SPY/bars")
        asyncio.run(asyncio.sleep(0))
    finally:
        deps_module.set_sink_registry(previous_sink)
        get_settings.cache_clear()

    data = response.json()
    assert response.status_code == 200
    assert response.headers["X-Gateway-Envelope"] == "true"
    assert data["envelope"]["feed"] == "bars"
    assert data["data"]["symbol"] == "SPY"
    assert sink_registry.published == []
