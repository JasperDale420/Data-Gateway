"""AlpacaTradingMixin.create_order maps the position_intent string onto the
submitted Alpaca OrderRequest.

Lets callers (e.g. Orion's reduce-only close path) force sell_to_close /
buy_to_close so Alpaca never converts a close into an opening (naked short)
position.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from alpaca.trading.enums import PositionIntent

from gateway.providers.alpaca.trading import AlpacaTradingMixin


class _StubProvider(AlpacaTradingMixin):
    def __init__(self) -> None:
        self.captured: dict[str, Any] = {}

        def _submit(req: Any) -> Any:
            self.captured["request"] = req
            return SimpleNamespace(id="order-x")

        self._trading_client = SimpleNamespace(submit_order=_submit)

    def _model_to_dict(self, order: Any) -> dict[str, Any]:
        return {"id": getattr(order, "id", None)}


def test_sell_to_close_maps_to_request_position_intent():
    prov = _StubProvider()
    prov.create_order(
        symbol="AAPL260529P00315000",
        side="sell",
        qty=10,
        order_type="limit",
        limit_price=1.0,
        position_intent="sell_to_close",
    )
    assert prov.captured["request"].position_intent == PositionIntent.SELL_TO_CLOSE


def test_buy_to_close_maps_to_request_position_intent():
    prov = _StubProvider()
    prov.create_order(
        symbol="AAPL260529P00315000",
        side="buy",
        qty=10,
        order_type="limit",
        limit_price=1.0,
        position_intent="buy_to_close",
    )
    assert prov.captured["request"].position_intent == PositionIntent.BUY_TO_CLOSE


def test_omitted_position_intent_is_none():
    prov = _StubProvider()
    prov.create_order(symbol="AAPL", side="buy", qty=1, order_type="market")
    assert prov.captured["request"].position_intent is None


def test_invalid_position_intent_raises_value_error():
    prov = _StubProvider()
    with pytest.raises(ValueError, match="Invalid position_intent"):
        prov.create_order(
            symbol="AAPL",
            side="sell",
            qty=1,
            order_type="market",
            position_intent="not_a_real_intent",
        )
