from __future__ import annotations

import asyncio
import json
import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

import gateway.api.deps as deps_module
from gateway.api.middleware.envelope import EventEnvelopeMiddleware


async def _wrap_response(middleware: EventEnvelopeMiddleware, *, path: str, payload: dict) -> list[dict]:
    messages: list[dict] = []
    body = json.dumps(payload).encode()
    initial_message = {
        "type": "http.response.start",
        "status": 200,
        "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
    }

    async def send(message: dict) -> None:
        messages.append(message)

    await middleware._wrap_and_send(path=path, body=body, initial_message=initial_message, send=send)
    if middleware._background_tasks:
        await asyncio.gather(*middleware._background_tasks)
    return messages


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


def test_rest_sink_allows_alpaca_trades_when_queue_pressure_is_normal(monkeypatch):
    monkeypatch.setenv("GATEWAY_REST_SINK_EXCLUDED_FEEDS", "greek_exposure,flow_alerts,darkpool")
    from gateway.config import get_settings

    get_settings.cache_clear()
    middleware = EventEnvelopeMiddleware(app=lambda scope, receive, send: None)

    assert (
        middleware._is_sink_publish_eligible(
            path="/api/v1/alpaca/stocks/SPY/trades",
            payload={
                "symbol": "SPY",
                "trades": [
                    {
                        "timestamp": "2026-06-12T13:30:00Z",
                        "price": "500.25",
                        "size": 100,
                        "trade_id": "t1",
                    }
                ],
            },
            feed="trades",
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


def test_fake_sink_registry_without_pressure_hook_still_publishes_eligible_rest_event(monkeypatch):
    class _LegacySinkRegistry:
        def __init__(self) -> None:
            self.published: list[tuple[str, dict]] = []

        async def publish_all(self, topic: str, data: dict) -> None:
            self.published.append((topic, data))

    monkeypatch.setenv("GATEWAY_REST_SINK_EXCLUDED_FEEDS", "greek_exposure,flow_alerts,darkpool")
    from gateway.config import get_settings

    get_settings.cache_clear()
    sink_registry = _LegacySinkRegistry()
    previous_sink = deps_module.get_sink_registry()
    deps_module.set_sink_registry(sink_registry)
    middleware = EventEnvelopeMiddleware(app=lambda scope, receive, send: None)

    try:
        messages = asyncio.run(
            _wrap_response(
                middleware,
                path="/api/v1/alpaca/stocks/SPY/trades",
                payload={
                    "success": True,
                    "data": {
                        "symbol": "SPY",
                        "trades": [
                            {
                                "timestamp": "2026-06-12T13:30:00Z",
                                "price": "500.25",
                                "size": 100,
                                "trade_id": "t1",
                            }
                        ],
                    },
                },
            )
        )
    finally:
        deps_module.set_sink_registry(previous_sink)
        get_settings.cache_clear()

    response_body = json.loads(messages[-1]["body"])
    assert response_body["envelope"]["feed"] == "trades"
    assert len(sink_registry.published) == 1
    assert sink_registry.published[0][0] == "heber:events"
    assert sink_registry.published[0][1]["feed"] == "trades"


def test_excluded_feed_skip_logs_reason_for_uw_rest_feed(monkeypatch, caplog):
    class _FakeSinkRegistry:
        async def publish_all(self, topic: str, data: dict) -> None:
            raise AssertionError("excluded feed must not publish")

    monkeypatch.setenv("GATEWAY_REST_SINK_EXCLUDED_FEEDS", "greek_exposure,flow_alerts,darkpool")
    from gateway.config import get_settings

    get_settings.cache_clear()
    previous_sink = deps_module.get_sink_registry()
    deps_module.set_sink_registry(_FakeSinkRegistry())
    middleware = EventEnvelopeMiddleware(app=lambda scope, receive, send: None)

    try:
        with caplog.at_level(logging.DEBUG, logger="data-gateway"):
            messages = asyncio.run(
                _wrap_response(
                    middleware,
                    path="/api/v1/uw/gex/SPY",
                    payload={
                        "success": True,
                        "data": [{"symbol": "SPY", "call_gamma": 1}],
                    },
                )
            )
    finally:
        deps_module.set_sink_registry(previous_sink)
        get_settings.cache_clear()

    response_body = json.loads(messages[-1]["body"])
    assert response_body["envelope"]["feed"] == "greek_exposure"

    records = [json.loads(record.getMessage()) for record in caplog.records if record.getMessage().startswith("{")]
    skip_logs = [record for record in records if record.get("message") == "rest_envelope_sink_publish_skipped"]
    assert skip_logs
    assert skip_logs[-1]["path"] == "/api/v1/uw/gex/SPY"
    assert skip_logs[-1]["feed"] == "greek_exposure"
    assert skip_logs[-1]["reason"] == "excluded_feed"


def test_gex_by_strike_uses_equity_key_not_malformed_option():
    """GEX by-strike/by-expiry rows carry strike/expiry but are per-underlying
    analytics — they must get equity:{SYM}, not a malformed option:{SYM} key that
    Heber rejects (which silently dropped every by-strike/by-expiry row)."""
    middleware = EventEnvelopeMiddleware(app=lambda scope, receive, send: None)
    items = [
        {
            "symbol": "AAPL",
            "strike": 200,
            "expiry": "2026-01-17",
            "call_gamma": 1.5,
            "timestamp": "2026-06-25T15:00:00Z",
        }
    ]
    envs = middleware._wrap_list_payload(
        provider="unusual_whales", feed="greek_exposure", items=items, path="/api/v1/uw/gex/AAPL/strike"
    )
    assert len(envs) == 1
    assert envs[0]["instrument_type"] == "equity"
    assert envs[0]["instrument_key"] == "equity:AAPL"
