"""Provider-layer (SDK-translation) tests for AlpacaTradingMixin money-path methods.

The router tests in test_alpaca_trading_router.py drive the FastAPI layer with a
STUBBED provider, so they never exercise the real AlpacaTradingMixin → alpaca-py
request construction. These tests close that gap for the capital-moving methods:
replace_order, cancel_all_orders, close_all_positions, get_positions,
exercise/do_not_exercise, plus the error branches of create_order/close_position.
A wrong field name here would ship a malformed live order with nothing to catch it.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from alpaca.common.exceptions import APIError
from alpaca.trading.enums import TimeInForce
from alpaca.trading.requests import ReplaceOrderRequest
from fastapi import HTTPException

import gateway.providers.alpaca.trading as trading_mod
from gateway.providers.alpaca._base import AlpacaBaseMixin
from gateway.providers.alpaca.trading import AlpacaTradingMixin


def _api_error(code: int, message: str = "boom") -> APIError:
    return APIError(json.dumps({"code": code, "message": message}))


def _raises(exc: Exception):
    """Return a fake SDK method that raises ``exc`` when called."""

    def _fn(*args: Any, **kwargs: Any) -> Any:
        raise exc

    return _fn


class _Provider(AlpacaTradingMixin):
    """Minimal mixin instance with a fake trading client + credentials.

    Borrows the real ``_model_to_dict`` from the base mixin so result shaping
    matches production exactly.
    """

    _model_to_dict = AlpacaBaseMixin._model_to_dict

    def __init__(self, **client_methods: Any) -> None:
        self._trading_client = SimpleNamespace(**client_methods)
        self._trading_base_url = "https://paper-api.alpaca.markets"
        self._api_key = "key-123"  # pragma: allowlist secret
        self._secret_key = "secret-456"  # pragma: allowlist secret


@pytest.fixture(autouse=True)
def _silence_logger(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Capture log events and keep test output quiet."""
    events: list[tuple[str, str]] = []
    monkeypatch.setattr(
        trading_mod,
        "logger",
        SimpleNamespace(
            debug=lambda event, **kw: events.append(("debug", event)),
            info=lambda event, **kw: events.append(("info", event)),
            warning=lambda event, **kw: events.append(("warning", event)),
            error=lambda event, **kw: events.append(("error", event)),
        ),
    )
    return events


# ── replace_order ────────────────────────────────────────────────────────────


def test_replace_order_builds_request_and_returns_dict() -> None:
    captured: dict[str, Any] = {}

    def _replace(order_id: str, request: ReplaceOrderRequest) -> Any:
        captured["order_id"] = order_id
        captured["request"] = request
        return SimpleNamespace(id="r1", status="replaced")

    prov = _Provider(replace_order_by_id=_replace)
    result = prov.replace_order("o1", qty=5, limit_price=10.5, time_in_force="gtc")

    assert result == {"id": "r1", "status": "replaced"}
    assert captured["order_id"] == "o1"
    req = captured["request"]
    assert req.qty == 5
    assert req.limit_price == 10.5
    assert req.time_in_force == TimeInForce.GTC


def test_replace_order_qty_zero_becomes_none() -> None:
    # qty=0 is falsy → the method passes None so the SDK keeps the original qty,
    # rather than submitting a zero-quantity replace.
    captured: dict[str, Any] = {}
    prov = _Provider(replace_order_by_id=lambda oid, req: captured.setdefault("req", req) or SimpleNamespace(id="r"))
    prov.replace_order("o1", qty=0)
    assert captured["req"].qty is None


def test_replace_order_invalid_tif_raises_value_error() -> None:
    prov = _Provider(replace_order_by_id=lambda oid, req: SimpleNamespace(id="r"))
    with pytest.raises(ValueError):
        prov.replace_order("o1", time_in_force="not-a-tif")


def test_replace_order_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    def _replace(order_id: str, request: ReplaceOrderRequest) -> Any:
        raise _api_error(40010000)

    prov = _Provider(replace_order_by_id=_replace)
    with pytest.raises(APIError):
        prov.replace_order("o1", qty=1)
    assert ("error", "alpaca_order_replace_error") in _silence_logger


# ── cancel_all_orders ────────────────────────────────────────────────────────


def test_cancel_all_orders_returns_list() -> None:
    prov = _Provider(cancel_orders=lambda: [SimpleNamespace(id="a", status="canceled"), SimpleNamespace(id="b")])
    result = prov.cancel_all_orders()
    assert result == [{"id": "a", "status": "canceled"}, {"id": "b"}]


def test_cancel_all_orders_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    def _cancel() -> Any:
        raise _api_error(40010000)

    prov = _Provider(cancel_orders=_cancel)
    with pytest.raises(APIError):
        prov.cancel_all_orders()
    assert ("error", "alpaca_orders_cancel_all_error") in _silence_logger


# ── close_all_positions ──────────────────────────────────────────────────────


def test_close_all_positions_default_cancel_orders_true() -> None:
    captured: dict[str, Any] = {}

    def _close(cancel_orders: bool) -> Any:
        captured["cancel_orders"] = cancel_orders
        return [SimpleNamespace(symbol="AAPL", status="closed")]

    prov = _Provider(close_all_positions=_close)
    result = prov.close_all_positions()
    assert captured["cancel_orders"] is True
    assert result == [{"symbol": "AAPL", "status": "closed"}]


def test_close_all_positions_cancel_orders_false_propagates() -> None:
    captured: dict[str, Any] = {}
    prov = _Provider(close_all_positions=lambda cancel_orders: captured.setdefault("co", cancel_orders) or [])
    prov.close_all_positions(cancel_orders=False)
    assert captured["co"] is False


def test_close_all_positions_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(close_all_positions=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.close_all_positions()
    assert ("error", "alpaca_positions_close_all_error") in _silence_logger


# ── get_positions ────────────────────────────────────────────────────────────


def test_get_positions_returns_list() -> None:
    prov = _Provider(get_all_positions=lambda: [SimpleNamespace(symbol="AAPL", qty="10")])
    assert prov.get_positions() == [{"symbol": "AAPL", "qty": "10"}]


def test_get_positions_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_all_positions=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.get_positions()
    assert ("error", "alpaca_positions_error") in _silence_logger


# ── close_position error branches ────────────────────────────────────────────


def test_close_position_not_found_raises_404() -> None:
    prov = _Provider(close_position=_raises(_api_error(40410000)))
    with pytest.raises(HTTPException) as exc:
        prov.close_position("AAPL")
    assert exc.value.status_code == 404
    detail = cast(dict, exc.value.detail)
    assert detail["code"] == "POSITION_NOT_FOUND"


def test_close_position_generic_error_reraises_apierror(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(close_position=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.close_position("AAPL")
    assert ("error", "alpaca_position_close_error") in _silence_logger


# ── create_order error branches ──────────────────────────────────────────────


def test_create_order_unsupported_type_raises_value_error() -> None:
    prov = _Provider(submit_order=lambda req: SimpleNamespace(id="x"))
    with pytest.raises(ValueError, match="Unsupported order type"):
        prov.create_order("AAPL", qty=1, order_type="trailing_stop")


def test_create_order_submit_failure_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(submit_order=_raises(_api_error(40310000)))
    with pytest.raises(APIError):
        prov.create_order("AAPL", qty=1)
    assert ("warning", "alpaca_order_create_error") in _silence_logger


# ── exercise / do_not_exercise ───────────────────────────────────────────────


def test_exercise_option_position_returns_status() -> None:
    captured: dict[str, Any] = {}
    prov = _Provider(exercise_options_position=lambda s: captured.setdefault("sym", s))
    result = prov.exercise_option_position("AAPL250117C00200000")
    assert result == {"status": "exercised", "symbol": "AAPL250117C00200000"}
    assert captured["sym"] == "AAPL250117C00200000"


def test_do_not_exercise_posts_with_auth_and_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _Resp:
        content = b'{"status": "do_not_exercise"}'

        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict[str, str]:
            return {"status": "do_not_exercise"}

    def _post(url: str, **kwargs: Any) -> _Resp:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return _Resp()

    monkeypatch.setattr(trading_mod.httpx, "post", _post)
    prov = _Provider()
    result = prov.do_not_exercise_option("AAPL250117C00200000")

    assert result == {"status": "do_not_exercise"}
    assert captured["url"].endswith("/v2/positions/AAPL250117C00200000/do-not-exercise")
    assert captured["kwargs"]["headers"]["APCA-API-KEY-ID"] == "key-123"
    assert captured["kwargs"]["headers"]["APCA-API-SECRET-KEY"] == "secret-456"
    # The regression guard: a timeout MUST be passed (httpx.post defaults to none).
    assert captured["kwargs"].get("timeout") is not None
    assert captured["kwargs"]["timeout"] > 0


def test_do_not_exercise_empty_body_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Resp:
        content = b""

        def raise_for_status(self) -> None:
            pass

    monkeypatch.setattr(trading_mod.httpx, "post", lambda url, **kw: _Resp())
    prov = _Provider()
    assert prov.do_not_exercise_option("AAPL250117C00200000") == {"status": "do_not_exercise"}


def test_do_not_exercise_http_error_reraises(
    monkeypatch: pytest.MonkeyPatch, _silence_logger: list[tuple[str, str]]
) -> None:
    request = httpx.Request("POST", "https://paper-api.alpaca.markets/x")
    response = httpx.Response(404, request=request)

    def _post(url: str, **kwargs: Any) -> Any:
        raise httpx.HTTPStatusError("not found", request=request, response=response)

    monkeypatch.setattr(trading_mod.httpx, "post", _post)
    prov = _Provider()
    with pytest.raises(httpx.HTTPStatusError):
        prov.do_not_exercise_option("AAPL250117C00200000")
    assert ("error", "alpaca_option_dne_error") in _silence_logger


# ── get_account_activities ───────────────────────────────────────────────────


def test_get_account_activities_calls_low_level_endpoint() -> None:
    # alpaca-py >=0.41 dropped TradingClient.get_account_activities (it moved to
    # BrokerClient). The provider must hit the Trading API endpoint directly via
    # the SDK's low-level get(), returning the raw dicts unchanged.
    captured: dict[str, Any] = {}

    def _get(path: str, data: Any = None) -> Any:
        captured["path"] = path
        captured["data"] = data
        return [
            {"id": "1", "activity_type": "FILL", "symbol": "AAPL", "order_id": "o1"},
            {"id": "2", "activity_type": "FILL", "symbol": "MSFT", "order_id": "o2"},
        ]

    prov = _Provider(get=_get)
    result = prov.get_account_activities(["FILL"])

    assert captured["path"] == "/account/activities"
    assert captured["data"] == {"activity_types": "FILL"}
    assert result == [
        {"id": "1", "activity_type": "FILL", "symbol": "AAPL", "order_id": "o1"},
        {"id": "2", "activity_type": "FILL", "symbol": "MSFT", "order_id": "o2"},
    ]


def test_get_account_activities_joins_multiple_types() -> None:
    captured: dict[str, Any] = {}

    def _get(path: str, data: Any = None) -> Any:
        captured["data"] = data
        return []

    prov = _Provider(get=_get)
    prov.get_account_activities(["FILL", "DIV"])
    assert captured["data"] == {"activity_types": "FILL,DIV"}


def test_get_account_activities_no_types_passes_none() -> None:
    captured: dict[str, Any] = {}

    def _get(path: str, data: Any = None) -> Any:
        captured["data"] = data
        return []

    prov = _Provider(get=_get)
    prov.get_account_activities(None)
    assert captured["data"] is None


def test_get_account_activities_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.get_account_activities(["FILL"])
    assert ("error", "alpaca_activities_error") in _silence_logger
