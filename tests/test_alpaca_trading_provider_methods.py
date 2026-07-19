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
from collections.abc import Callable
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from alpaca.common.enums import Sort
from alpaca.common.exceptions import APIError
from alpaca.trading.enums import (
    AssetClass,
    AssetExchange,
    AssetStatus,
    OrderClass,
    OrderSide,
    PositionIntent,
    QueryOrderStatus,
    TimeInForce,
)
from alpaca.trading.requests import (
    CreateWatchlistRequest,
    GetAssetsRequest,
    GetCalendarRequest,
    GetOrdersRequest,
    LimitOrderRequest,
    MarketOrderRequest,
    ReplaceOrderRequest,
    StopLimitOrderRequest,
    StopOrderRequest,
    UpdateWatchlistRequest,
)
from fastapi import HTTPException

import gateway.providers.alpaca.trading as trading_mod
from gateway.providers.alpaca._base import ERR_TRADING_CLIENT_NOT_INITIALIZED, AlpacaBaseMixin
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


# ── trading client not initialized ───────────────────────────────────────────

_NOT_INITIALIZED_CALLS = [
    pytest.param(lambda p: p.get_account(), id="get_account"),
    pytest.param(lambda p: p.create_order("AAPL", qty=1), id="create_order"),
    pytest.param(lambda p: p.get_orders(), id="get_orders"),
    pytest.param(lambda p: p.get_order("o1"), id="get_order"),
    pytest.param(lambda p: p.get_order_by_client_id("c1"), id="get_order_by_client_id"),
    pytest.param(lambda p: p.cancel_order("o1"), id="cancel_order"),
    pytest.param(lambda p: p.cancel_all_orders(), id="cancel_all_orders"),
    pytest.param(lambda p: p.replace_order("o1"), id="replace_order"),
    pytest.param(lambda p: p.get_positions(), id="get_positions"),
    pytest.param(lambda p: p.get_position("AAPL"), id="get_position"),
    pytest.param(lambda p: p.close_position("AAPL"), id="close_position"),
    pytest.param(lambda p: p.close_all_positions(), id="close_all_positions"),
    pytest.param(lambda p: p.exercise_option_position("AAPL"), id="exercise_option_position"),
    pytest.param(lambda p: p.do_not_exercise_option("AAPL"), id="do_not_exercise_option"),
    pytest.param(lambda p: p.get_portfolio_history(), id="get_portfolio_history"),
    pytest.param(lambda p: p.get_assets(), id="get_assets"),
    pytest.param(lambda p: p.get_asset("AAPL"), id="get_asset"),
    pytest.param(lambda p: p.get_clock(), id="get_clock"),
    pytest.param(lambda p: p.get_calendar(), id="get_calendar"),
    pytest.param(lambda p: p.get_account_configurations(), id="get_account_configurations"),
    pytest.param(lambda p: p.set_account_configurations(), id="set_account_configurations"),
    pytest.param(lambda p: p.get_account_activities(), id="get_account_activities"),
    pytest.param(lambda p: p.get_watchlists(), id="get_watchlists"),
    pytest.param(lambda p: p.create_watchlist("w"), id="create_watchlist"),
    pytest.param(lambda p: p.get_watchlist("w1"), id="get_watchlist"),
    pytest.param(lambda p: p.update_watchlist("w1", name="n"), id="update_watchlist"),
    pytest.param(lambda p: p.delete_watchlist("w1"), id="delete_watchlist"),
    pytest.param(lambda p: p.add_asset_to_watchlist("w1", "AAPL"), id="add_asset_to_watchlist"),
    pytest.param(lambda p: p.remove_asset_from_watchlist("w1", "AAPL"), id="remove_asset_from_watchlist"),
]


@pytest.mark.parametrize("call", _NOT_INITIALIZED_CALLS)
def test_methods_raise_when_trading_client_not_initialized(call: Callable[[_Provider], Any]) -> None:
    prov = _Provider()
    prov._trading_client = None
    with pytest.raises(RuntimeError, match=ERR_TRADING_CLIENT_NOT_INITIALIZED):
        call(prov)


# ── error-code helpers ───────────────────────────────────────────────────────


def test_extract_alpaca_error_code_unparseable_returns_none() -> None:
    assert AlpacaTradingMixin._extract_alpaca_error_code(APIError("not-json")) is None


def test_is_benign_cancel_race_requires_code_and_message() -> None:
    benign = _api_error(42210000, 'order is already in "filled" state')
    assert AlpacaTradingMixin._is_benign_cancel_race(benign) is True
    # Right code, wrong message → NOT benign (Alpaca reuses 42210000).
    assert AlpacaTradingMixin._is_benign_cancel_race(_api_error(42210000, "insufficient qty")) is False
    # Wrong code entirely.
    assert AlpacaTradingMixin._is_benign_cancel_race(_api_error(40010000, "already in state")) is False
    # Unparseable body (code is None) → not benign.
    assert AlpacaTradingMixin._is_benign_cancel_race(APIError("already in plain text")) is False


# ── get_account ──────────────────────────────────────────────────────────────


def test_get_account_returns_dict() -> None:
    prov = _Provider(get_account=lambda: SimpleNamespace(status="ACTIVE", cash="100000"))
    assert prov.get_account() == {"status": "ACTIVE", "cash": "100000"}


def test_get_account_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_account=_raises(_api_error(40110000)))
    with pytest.raises(APIError):
        prov.get_account()
    assert ("error", "alpaca_account_error") in _silence_logger


# ── create_order success variants ────────────────────────────────────────────


def _submit_capture(captured: dict[str, Any]) -> Callable[[Any], Any]:
    """Fake submit_order that records the SDK request and returns an order."""

    def _submit(request: Any) -> Any:
        captured["request"] = request
        return SimpleNamespace(id="o1", status="accepted")

    return _submit


def test_create_order_market_notional_extended_hours() -> None:
    captured: dict[str, Any] = {}
    prov = _Provider(submit_order=_submit_capture(captured))
    result = prov.create_order(
        "spy",
        notional=500.0,
        side="sell",
        extended_hours=True,
        client_order_id="cid-1",
    )
    assert result == {"id": "o1", "status": "accepted"}
    req = captured["request"]
    assert isinstance(req, MarketOrderRequest)
    assert req.symbol == "SPY"
    assert req.notional == 500.0
    assert req.qty is None
    assert req.side == OrderSide.SELL
    assert req.extended_hours is True
    assert req.client_order_id == "cid-1"


def test_create_order_bracket_builds_take_profit_and_stop_loss() -> None:
    captured: dict[str, Any] = {}
    prov = _Provider(submit_order=_submit_capture(captured))
    prov.create_order(
        "aapl",
        qty=10,
        side="buy",
        order_type="market",
        order_class="bracket",
        take_profit_limit_price=210.0,
        stop_loss_stop_price=190.0,
        stop_loss_limit_price=189.5,
    )
    req = captured["request"]
    assert req.symbol == "AAPL"
    assert req.order_class == OrderClass.BRACKET
    assert req.take_profit.limit_price == 210.0
    assert req.stop_loss.stop_price == 190.0
    assert req.stop_loss.limit_price == 189.5


def test_create_order_limit_with_position_intent_and_gtc() -> None:
    captured: dict[str, Any] = {}
    prov = _Provider(submit_order=_submit_capture(captured))
    prov.create_order(
        "AAPL",
        qty=5,
        side="sell",
        order_type="limit",
        limit_price=189.5,
        time_in_force="gtc",
        position_intent="sell_to_close",
    )
    req = captured["request"]
    assert isinstance(req, LimitOrderRequest)
    assert req.limit_price == 189.5
    assert req.position_intent == PositionIntent.SELL_TO_CLOSE
    assert req.time_in_force == TimeInForce.GTC


def test_create_order_stop() -> None:
    captured: dict[str, Any] = {}
    prov = _Provider(submit_order=_submit_capture(captured))
    prov.create_order("AAPL", qty=5, side="sell", order_type="stop", stop_price=180.0)
    req = captured["request"]
    assert isinstance(req, StopOrderRequest)
    assert req.stop_price == 180.0
    assert req.time_in_force == TimeInForce.DAY


def test_create_order_stop_limit_unknown_tif_falls_back_to_day() -> None:
    captured: dict[str, Any] = {}
    prov = _Provider(submit_order=_submit_capture(captured))
    prov.create_order(
        "AAPL",
        qty=5,
        side="buy",
        order_type="stop_limit",
        limit_price=201.0,
        stop_price=200.0,
        time_in_force="mystery",
    )
    req = captured["request"]
    assert isinstance(req, StopLimitOrderRequest)
    assert req.limit_price == 201.0
    assert req.stop_price == 200.0
    assert req.time_in_force == TimeInForce.DAY


def test_create_order_invalid_side_raises_value_error() -> None:
    prov = _Provider(submit_order=lambda req: SimpleNamespace(id="x"))
    with pytest.raises(ValueError, match="Invalid order side"):
        prov.create_order("AAPL", qty=1, side="hold")


def test_create_order_invalid_position_intent_raises_value_error() -> None:
    prov = _Provider(submit_order=lambda req: SimpleNamespace(id="x"))
    with pytest.raises(ValueError, match="Invalid position_intent"):
        prov.create_order("AAPL", qty=1, position_intent="yolo")


# ── get_orders / get_order / get_order_by_client_id ──────────────────────────


def test_get_orders_builds_request_with_filters() -> None:
    captured: dict[str, Any] = {}

    def _get_orders(request: GetOrdersRequest) -> Any:
        captured["request"] = request
        return [SimpleNamespace(id="o1", symbol="AAPL")]

    after = datetime(2026, 7, 1, tzinfo=UTC)
    until = datetime(2026, 7, 15, tzinfo=UTC)
    prov = _Provider(get_orders=_get_orders)
    result = prov.get_orders(
        status="closed",
        limit=1000,
        direction="asc",
        symbols=["AAPL"],
        nested=False,
        side="buy",
        after=after,
        until=until,
    )
    assert result == [{"id": "o1", "symbol": "AAPL"}]
    req = captured["request"]
    assert req.status == QueryOrderStatus.CLOSED
    assert req.limit == 500  # capped at Alpaca's page maximum
    assert req.direction == Sort.ASC
    assert req.symbols == ["AAPL"]
    assert req.nested is False
    assert req.side == OrderSide.BUY
    assert req.after == after
    assert req.until == until


def test_get_orders_defaults_no_side() -> None:
    captured: dict[str, Any] = {}

    def _get_orders(request: GetOrdersRequest) -> Any:
        captured["request"] = request
        return []

    prov = _Provider(get_orders=_get_orders)
    assert prov.get_orders() == []
    req = captured["request"]
    assert req.status == QueryOrderStatus.OPEN
    assert req.limit == 100
    assert req.side is None


def test_get_orders_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_orders=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.get_orders()
    assert ("error", "alpaca_orders_error") in _silence_logger


def test_get_order_returns_dict() -> None:
    captured: dict[str, Any] = {}

    def _get(order_id: str) -> Any:
        captured["order_id"] = order_id
        return SimpleNamespace(id=order_id, status="filled")

    prov = _Provider(get_order_by_id=_get)
    assert prov.get_order("o1") == {"id": "o1", "status": "filled"}
    assert captured["order_id"] == "o1"


def test_get_order_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_order_by_id=_raises(_api_error(40410000)))
    with pytest.raises(APIError):
        prov.get_order("o1")
    assert ("error", "alpaca_order_get_error") in _silence_logger


def test_get_order_by_client_id_returns_dict() -> None:
    prov = _Provider(get_order_by_client_id=lambda cid: SimpleNamespace(client_order_id=cid, id="o9"))
    assert prov.get_order_by_client_id("cid-7") == {"client_order_id": "cid-7", "id": "o9"}


def test_get_order_by_client_id_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_order_by_client_id=_raises(_api_error(40410000)))
    with pytest.raises(APIError):
        prov.get_order_by_client_id("cid-7")
    assert ("error", "alpaca_order_get_by_client_error") in _silence_logger


# ── cancel_order ─────────────────────────────────────────────────────────────


def test_cancel_order_success_returns_true(_silence_logger: list[tuple[str, str]]) -> None:
    captured: dict[str, Any] = {}
    prov = _Provider(cancel_order_by_id=lambda oid: captured.setdefault("order_id", oid))
    assert prov.cancel_order("o1") is True
    assert captured["order_id"] == "o1"
    assert ("info", "alpaca_order_cancelled") in _silence_logger


def test_cancel_order_benign_race_logs_info_and_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    exc = _api_error(42210000, 'order is already in "filled" state')
    prov = _Provider(cancel_order_by_id=_raises(exc))
    with pytest.raises(APIError):
        prov.cancel_order("o1")
    assert ("info", "alpaca_order_cancel_noop") in _silence_logger
    assert ("error", "alpaca_order_cancel_error") not in _silence_logger


def test_cancel_order_actionable_failure_logs_error(_silence_logger: list[tuple[str, str]]) -> None:
    # Same 42210000 code but without the "already in" signature → actionable.
    prov = _Provider(cancel_order_by_id=_raises(_api_error(42210000, "cannot cancel")))
    with pytest.raises(APIError):
        prov.cancel_order("o1")
    assert ("error", "alpaca_order_cancel_error") in _silence_logger


# ── get_position ─────────────────────────────────────────────────────────────


def test_get_position_uppercases_symbol_and_returns_dict() -> None:
    captured: dict[str, Any] = {}

    def _get(symbol: str) -> Any:
        captured["symbol"] = symbol
        return SimpleNamespace(symbol=symbol, qty="10")

    prov = _Provider(get_open_position=_get)
    assert prov.get_position("aapl") == {"symbol": "AAPL", "qty": "10"}
    assert captured["symbol"] == "AAPL"


def test_get_position_not_found_raises_404() -> None:
    prov = _Provider(get_open_position=_raises(_api_error(40410000)))
    with pytest.raises(HTTPException) as exc:
        prov.get_position("AAPL")
    assert exc.value.status_code == 404
    detail = cast(dict, exc.value.detail)
    assert detail["code"] == "POSITION_NOT_FOUND"
    assert detail["alpaca_code"] == 40410000


def test_get_position_generic_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_open_position=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.get_position("AAPL")
    assert ("error", "alpaca_position_error") in _silence_logger


# ── close_position success paths ─────────────────────────────────────────────


def _close_capture(captured: dict[str, Any]) -> Callable[..., Any]:
    def _close(symbol: str, close_options: Any = None) -> Any:
        captured["symbol"] = symbol
        captured["options"] = close_options
        return SimpleNamespace(id="c1", status="accepted")

    return _close


def test_close_position_defaults_to_full_close() -> None:
    # Neither qty nor percentage → percentage=100, so a naked
    # DELETE /positions/<symbol> closes the whole position instead of 502ing.
    captured: dict[str, Any] = {}
    prov = _Provider(close_position=_close_capture(captured))
    result = prov.close_position("aapl")
    assert result == {"id": "c1", "status": "accepted"}
    assert captured["symbol"] == "AAPL"
    assert captured["options"].percentage == "100.0"
    assert captured["options"].qty is None


def test_close_position_qty_round_trips_as_string() -> None:
    captured: dict[str, Any] = {}
    prov = _Provider(close_position=_close_capture(captured))
    prov.close_position("AAPL", qty=2.5)
    assert captured["options"].qty == "2.5"
    assert captured["options"].percentage is None


# ── exercise error branch ────────────────────────────────────────────────────


def test_exercise_option_position_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(exercise_options_position=_raises(_api_error(40310000)))
    with pytest.raises(APIError):
        prov.exercise_option_position("AAPL250117C00200000")
    assert ("error", "alpaca_option_exercise_error") in _silence_logger


# ── portfolio history / assets / clock / calendar ────────────────────────────


def test_get_portfolio_history_passes_range() -> None:
    captured: dict[str, Any] = {}

    def _hist(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return SimpleNamespace(equity=[100.0, 101.0], timeframe="1D")

    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = datetime(2026, 7, 15, tzinfo=UTC)
    prov = _Provider(get_portfolio_history=_hist)
    result = prov.get_portfolio_history(period="1M", timeframe="1D", start=start, end=end, extended_hours=True)
    assert result == {"equity": [100.0, 101.0], "timeframe": "1D"}
    assert captured == {
        "period": "1M",
        "timeframe": "1D",
        "date_start": start,
        "date_end": end,
        "extended_hours": True,
    }


def test_get_portfolio_history_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_portfolio_history=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.get_portfolio_history()
    assert ("error", "alpaca_portfolio_history_error") in _silence_logger


def test_get_assets_maps_filters_to_enums() -> None:
    captured: dict[str, Any] = {}

    def _assets(request: GetAssetsRequest) -> Any:
        captured["request"] = request
        return [SimpleNamespace(symbol="AAPL", tradable=True)]

    prov = _Provider(get_all_assets=_assets)
    result = prov.get_assets(status="active", asset_class="us_equity", exchange="NASDAQ")
    assert result == [{"symbol": "AAPL", "tradable": True}]
    req = captured["request"]
    assert req.status == AssetStatus.ACTIVE
    assert req.asset_class == AssetClass.US_EQUITY
    assert req.exchange == AssetExchange.NASDAQ


def test_get_assets_no_filters_pass_none() -> None:
    captured: dict[str, Any] = {}

    def _assets(request: GetAssetsRequest) -> Any:
        captured["request"] = request
        return []

    prov = _Provider(get_all_assets=_assets)
    assert prov.get_assets() == []
    req = captured["request"]
    assert req.status is None
    assert req.asset_class is None
    assert req.exchange is None


def test_get_assets_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_all_assets=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.get_assets()
    assert ("error", "alpaca_assets_error") in _silence_logger


def test_get_asset_uppercases_symbol() -> None:
    captured: dict[str, Any] = {}

    def _asset(symbol: str) -> Any:
        captured["symbol"] = symbol
        return SimpleNamespace(symbol=symbol)

    prov = _Provider(get_asset=_asset)
    assert prov.get_asset("msft") == {"symbol": "MSFT"}
    assert captured["symbol"] == "MSFT"


def test_get_asset_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_asset=_raises(_api_error(40410000)))
    with pytest.raises(APIError):
        prov.get_asset("MSFT")
    assert ("error", "alpaca_asset_error") in _silence_logger


def test_get_clock_returns_dict() -> None:
    prov = _Provider(get_clock=lambda: SimpleNamespace(is_open=True))
    assert prov.get_clock() == {"is_open": True}


def test_get_clock_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_clock=_raises(_api_error(50010000)))
    with pytest.raises(APIError):
        prov.get_clock()
    assert ("error", "alpaca_clock_error") in _silence_logger


def test_get_calendar_passes_date_range() -> None:
    captured: dict[str, Any] = {}

    def _cal(request: GetCalendarRequest) -> Any:
        captured["request"] = request
        return [SimpleNamespace(open="09:30", close="16:00")]

    prov = _Provider(get_calendar=_cal)
    result = prov.get_calendar(start=date(2026, 7, 1), end=date(2026, 7, 31))
    assert result == [{"open": "09:30", "close": "16:00"}]
    assert captured["request"].start == date(2026, 7, 1)
    assert captured["request"].end == date(2026, 7, 31)


def test_get_calendar_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_calendar=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.get_calendar()
    assert ("error", "alpaca_calendar_error") in _silence_logger


# ── account configurations ───────────────────────────────────────────────────


def test_get_account_configurations_returns_dict() -> None:
    prov = _Provider(get_account_configurations=lambda: SimpleNamespace(dtbp_check="both", suspend_trade=False))
    assert prov.get_account_configurations() == {"dtbp_check": "both", "suspend_trade": False}


def test_get_account_configurations_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_account_configurations=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.get_account_configurations()
    assert ("error", "alpaca_account_config_error") in _silence_logger


def test_set_account_configurations_passes_all_kwargs() -> None:
    captured: dict[str, Any] = {}

    def _set(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return SimpleNamespace(suspend_trade=True)

    prov = _Provider(set_account_configurations=_set)
    result = prov.set_account_configurations(
        dtbp_check="entry",
        trade_confirm_email="none",
        suspend_trade=True,
        no_shorting=True,
        fractional_trading=False,
        max_margin_multiplier="2",
        pdt_check="exit",
    )
    assert result == {"suspend_trade": True}
    assert captured == {
        "dtbp_check": "entry",
        "trade_confirm_email": "none",
        "suspend_trade": True,
        "no_shorting": True,
        "fractional_trading": False,
        "max_margin_multiplier": "2",
        "pdt_check": "exit",
    }


def test_set_account_configurations_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(set_account_configurations=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.set_account_configurations(suspend_trade=True)
    assert ("error", "alpaca_account_config_set_error") in _silence_logger


# ── watchlists ───────────────────────────────────────────────────────────────


def test_get_watchlists_returns_list() -> None:
    prov = _Provider(get_watchlists=lambda: [SimpleNamespace(id="w1", name="tech")])
    assert prov.get_watchlists() == [{"id": "w1", "name": "tech"}]


def test_get_watchlists_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_watchlists=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.get_watchlists()
    assert ("error", "alpaca_watchlists_error") in _silence_logger


def test_create_watchlist_defaults_symbols_to_empty_list() -> None:
    captured: dict[str, Any] = {}

    def _create(request: CreateWatchlistRequest) -> Any:
        captured["request"] = request
        return SimpleNamespace(id="w1", name=request.name)

    prov = _Provider(create_watchlist=_create)
    result = prov.create_watchlist("tech")
    assert result == {"id": "w1", "name": "tech"}
    assert captured["request"].symbols == []


def test_create_watchlist_with_symbols() -> None:
    captured: dict[str, Any] = {}

    def _create(request: CreateWatchlistRequest) -> Any:
        captured["request"] = request
        return SimpleNamespace(id="w1", name=request.name)

    prov = _Provider(create_watchlist=_create)
    prov.create_watchlist("tech", ["AAPL", "MSFT"])
    assert captured["request"].symbols == ["AAPL", "MSFT"]


def test_create_watchlist_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(create_watchlist=_raises(_api_error(40010000)))
    with pytest.raises(APIError):
        prov.create_watchlist("tech")
    assert ("error", "alpaca_watchlist_create_error") in _silence_logger


def test_get_watchlist_returns_dict() -> None:
    prov = _Provider(get_watchlist_by_id=lambda wid: SimpleNamespace(id=wid, name="tech"))
    assert prov.get_watchlist("w1") == {"id": "w1", "name": "tech"}


def test_get_watchlist_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(get_watchlist_by_id=_raises(_api_error(40410000)))
    with pytest.raises(APIError):
        prov.get_watchlist("w1")
    assert ("error", "alpaca_watchlist_error") in _silence_logger


def test_update_watchlist_builds_request() -> None:
    captured: dict[str, Any] = {}

    def _update(watchlist_id: str, request: UpdateWatchlistRequest) -> Any:
        captured["watchlist_id"] = watchlist_id
        captured["request"] = request
        return SimpleNamespace(id=watchlist_id, name=request.name)

    prov = _Provider(update_watchlist_by_id=_update)
    result = prov.update_watchlist("w1", name="renamed", symbols=["AAPL"])
    assert result == {"id": "w1", "name": "renamed"}
    assert captured["watchlist_id"] == "w1"
    assert captured["request"].symbols == ["AAPL"]


def test_update_watchlist_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(update_watchlist_by_id=_raises(_api_error(40410000)))
    with pytest.raises(APIError):
        prov.update_watchlist("w1", name="renamed")
    assert ("error", "alpaca_watchlist_update_error") in _silence_logger


def test_delete_watchlist_returns_true(_silence_logger: list[tuple[str, str]]) -> None:
    captured: dict[str, Any] = {}
    prov = _Provider(delete_watchlist_by_id=lambda wid: captured.setdefault("watchlist_id", wid))
    assert prov.delete_watchlist("w1") is True
    assert captured["watchlist_id"] == "w1"
    assert ("info", "alpaca_watchlist_deleted") in _silence_logger


def test_delete_watchlist_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(delete_watchlist_by_id=_raises(_api_error(40410000)))
    with pytest.raises(APIError):
        prov.delete_watchlist("w1")
    assert ("error", "alpaca_watchlist_delete_error") in _silence_logger


def test_add_asset_to_watchlist_returns_dict() -> None:
    captured: dict[str, Any] = {}

    def _add(watchlist_id: str, symbol: str) -> Any:
        captured["args"] = (watchlist_id, symbol)
        return SimpleNamespace(id=watchlist_id, symbols=[symbol])

    prov = _Provider(add_asset_to_watchlist_by_id=_add)
    assert prov.add_asset_to_watchlist("w1", "AAPL") == {"id": "w1", "symbols": ["AAPL"]}
    assert captured["args"] == ("w1", "AAPL")


def test_add_asset_to_watchlist_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(add_asset_to_watchlist_by_id=_raises(_api_error(40410000)))
    with pytest.raises(APIError):
        prov.add_asset_to_watchlist("w1", "AAPL")
    assert ("error", "alpaca_watchlist_add_asset_error") in _silence_logger


def test_remove_asset_from_watchlist_returns_dict() -> None:
    captured: dict[str, Any] = {}

    def _remove(watchlist_id: str, symbol: str) -> Any:
        captured["args"] = (watchlist_id, symbol)
        return SimpleNamespace(id=watchlist_id, symbols=[])

    prov = _Provider(remove_asset_from_watchlist_by_id=_remove)
    assert prov.remove_asset_from_watchlist("w1", "AAPL") == {"id": "w1", "symbols": []}
    assert captured["args"] == ("w1", "AAPL")


def test_remove_asset_from_watchlist_api_error_reraises(_silence_logger: list[tuple[str, str]]) -> None:
    prov = _Provider(remove_asset_from_watchlist_by_id=_raises(_api_error(40410000)))
    with pytest.raises(APIError):
        prov.remove_asset_from_watchlist("w1", "AAPL")
    assert ("error", "alpaca_watchlist_remove_asset_error") in _silence_logger
