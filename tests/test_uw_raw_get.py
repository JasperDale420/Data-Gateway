"""Param-coercion contract for UWBaseMixin._raw_get (raw-HTTP endpoint primitive)."""

from typing import Any

from gateway.providers.uw import UnusualWhalesProvider


class _FakeResponse:
    def __init__(self, payload: Any):
        self._payload = payload

    def raise_for_status(self) -> None:  # noqa: D401
        return None

    def json(self) -> Any:
        return self._payload


async def test_raw_get_coerces_params_and_unwraps_data(monkeypatch):
    p = UnusualWhalesProvider()
    p._client = object()  # non-None so _raw_get does not short-circuit
    captured: dict[str, Any] = {}

    async def fake_call_sync(func, *args, **kwargs):
        captured["args"] = args
        captured["params"] = kwargs.get("params")
        return _FakeResponse({"data": [{"ok": 1}]})

    # avoid touching the real SDK httpx client
    fake_httpx = type("H", (), {"get": lambda *a, **k: None})()
    monkeypatch.setattr(p, "_client", type("C", (), {"get_httpx_client": lambda self: fake_httpx})())
    monkeypatch.setattr(p, "_call_sync", fake_call_sync)

    out = await p._raw_get(
        "/api/option-trades/exchange-breakdown/2026-07-03",
        {"ticker[]": ["AAPL", "MSFT"], "by_trade_code": True, "limit": 100, "min_premium": None},
    )

    params = captured["params"]
    # None dropped
    assert "min_premium" not in params
    # list preserved as a list (httpx emits repeated keys) — NOT stringified
    assert params["ticker[]"] == ["AAPL", "MSFT"]
    # scalars passed through for httpx to serialize
    assert params["by_trade_code"] is True
    assert params["limit"] == 100
    # path forwarded verbatim; envelope's "data" unwrapped
    assert captured["args"][0] == "/api/option-trades/exchange-breakdown/2026-07-03"
    assert out == [{"ok": 1}]


async def test_raw_get_returns_raw_body_when_no_data_envelope(monkeypatch):
    p = UnusualWhalesProvider()

    async def fake_call_sync(func, *args, **kwargs):
        return _FakeResponse({"price": "1.23"})  # no "data" key

    fake_httpx = type("H", (), {"get": lambda *a, **k: None})()
    monkeypatch.setattr(p, "_client", type("C", (), {"get_httpx_client": lambda self: fake_httpx})())
    monkeypatch.setattr(p, "_call_sync", fake_call_sync)

    out = await p._raw_get("/api/forex/rate", {"from": "USD", "to": "EUR"})
    assert out == {"price": "1.23"}


async def test_raw_get_returns_empty_when_uninitialized():
    p = UnusualWhalesProvider()
    p._client = None
    assert await p._raw_get("/api/anything") == []
