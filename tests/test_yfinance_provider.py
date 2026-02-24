"""Tests for YFinance provider performance behavior."""

from __future__ import annotations

import sys
import types
from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

from gateway.providers.yfinance import YFinanceProvider


@pytest.mark.asyncio
async def test_get_history_avoids_iterrows(monkeypatch: pytest.MonkeyPatch) -> None:
    """History conversion should avoid iterrows for performance."""
    df = pd.DataFrame(
        {
            "Open": [101.0, 102.0],
            "High": [105.0, 106.0],
            "Low": [99.0, 100.0],
            "Close": [104.0, 103.5],
            "Volume": [1000, 2000],
        },
        index=pd.DatetimeIndex(
            [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            name="Date",
        ),
    )

    def _iterrows_fail(self):  # pragma: no cover - only called on regression
        raise AssertionError("iterrows should not be used for history conversion")

    monkeypatch.setattr(pd.DataFrame, "iterrows", _iterrows_fail, raising=True)

    class _Ticker:
        def __init__(self, symbol: str) -> None:
            self.symbol = symbol

        def history(self, *args, **kwargs):
            return df

    fake_yfinance = types.SimpleNamespace(Ticker=lambda symbol: _Ticker(symbol))
    monkeypatch.setitem(sys.modules, "yfinance", fake_yfinance)

    provider = YFinanceProvider()
    results = await provider.get_history("aapl", period="1mo", interval="1d")

    assert [bar.symbol for bar in results] == ["AAPL", "AAPL"]
    assert results[0].open == Decimal("101.0")
    assert results[0].close == Decimal("104.0")
    assert results[1].volume == 2000
