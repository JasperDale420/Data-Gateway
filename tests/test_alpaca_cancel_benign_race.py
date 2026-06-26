"""AlpacaTradingMixin.cancel_order log-severity for benign cancel/fill races.

A cancel that loses the race with the order's own terminal transition (Alpaca
422, code 42210000 "order is already in ...") is expected during active
trading. It must still re-raise so the caller learns the true order state, but
it must be logged at INFO, not ERROR — these were flooding the error log with
~1300 non-actionable lines per trading day.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from alpaca.common.exceptions import APIError

import gateway.providers.alpaca.trading as trading_mod
from gateway.providers.alpaca.trading import AlpacaTradingMixin


def _api_error(code: int, message: str = 'order is already in "filled" state') -> APIError:
    import json

    return APIError(json.dumps({"code": code, "message": message}))


class _CancelStub(AlpacaTradingMixin):
    def __init__(self, exc: Exception) -> None:
        def _cancel(order_id: str) -> Any:
            raise exc

        self._trading_client = SimpleNamespace(cancel_order_by_id=_cancel)


@pytest.fixture
def captured_logs(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    events: list[tuple[str, str]] = []
    monkeypatch.setattr(
        trading_mod,
        "logger",
        SimpleNamespace(
            info=lambda event, **kw: events.append(("info", event)),
            error=lambda event, **kw: events.append(("error", event)),
        ),
    )
    return events


def test_benign_cancel_race_logs_info_and_reraises(captured_logs: list[tuple[str, str]]) -> None:
    prov = _CancelStub(_api_error(42210000))
    with pytest.raises(APIError):
        prov.cancel_order("order-1")
    assert ("info", "alpaca_order_cancel_noop") in captured_logs
    assert not any(level == "error" for level, _ in captured_logs)


def test_real_cancel_error_logs_error_and_reraises(captured_logs: list[tuple[str, str]]) -> None:
    prov = _CancelStub(_api_error(40010000))  # non-benign code → genuine error
    with pytest.raises(APIError):
        prov.cancel_order("order-2")
    assert ("error", "alpaca_order_cancel_error") in captured_logs
    assert not any(event == "alpaca_order_cancel_noop" for _, event in captured_logs)


def test_benign_code_with_non_terminal_message_logs_error(captured_logs: list[tuple[str, str]]) -> None:
    # Same code 42210000 but NOT the "already in ... state" race — Alpaca reuses
    # the code for other rejections, which must still surface as ERROR.
    prov = _CancelStub(_api_error(42210000, message="order is not cancelable"))
    with pytest.raises(APIError):
        prov.cancel_order("order-3")
    assert ("error", "alpaca_order_cancel_error") in captured_logs
    assert not any(event == "alpaca_order_cancel_noop" for _, event in captured_logs)


def test_malformed_error_body_logs_error_without_throwing(captured_logs: list[tuple[str, str]]) -> None:
    # A non-JSON error body makes code extraction return None — the except
    # block must not raise a SECONDARY error, and must fall through to ERROR.
    prov = _CancelStub(APIError("not-json-garbage"))
    with pytest.raises(APIError):
        prov.cancel_order("order-4")
    assert ("error", "alpaca_order_cancel_error") in captured_logs
    assert not any(event == "alpaca_order_cancel_noop" for _, event in captured_logs)
