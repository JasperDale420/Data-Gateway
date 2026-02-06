from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pandas as pd

from gateway.providers.yfinance import YFinanceProvider


def test_bars_from_history_df_uses_itertuples_and_builds_normalized_bars(
    monkeypatch,
) -> None:
    provider = YFinanceProvider()
    index = [datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 3, tzinfo=UTC)]
    df = pd.DataFrame(
        {
            "Open": [100.5, 101.25],
            "High": [102.0, 103.5],
            "Low": [99.8, 100.9],
            "Close": [101.1, 103.0],
            "Volume": [1500, 1900],
        },
        index=index,
    )

    def _iterrows_not_expected():
        raise AssertionError("iterrows should not be used")

    monkeypatch.setattr(df, "iterrows", _iterrows_not_expected)

    bars = provider._bars_from_history_df(df=df, symbol="aapl", interval="1d")

    assert len(bars) == 2
    assert bars[0].symbol == "AAPL"
    assert bars[0].timestamp == index[0]
    assert bars[0].open == Decimal("100.5")
    assert bars[0].high == Decimal("102.0")
    assert bars[0].low == Decimal("99.8")
    assert bars[0].close == Decimal("101.1")
    assert bars[0].volume == 1500
    assert bars[0].provider == "yfinance"
    assert bars[0].timeframe == "1d"


def test_bars_from_history_df_handles_empty_dataframe() -> None:
    provider = YFinanceProvider()
    df = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

    bars = provider._bars_from_history_df(df=df, symbol="MSFT", interval="1d")

    assert bars == []


def test_major_holders_from_df_uses_itertuples(monkeypatch) -> None:
    provider = YFinanceProvider()
    df = pd.DataFrame(
        [
            ["45.10%", "% of Shares Held by Institutions"],
            ["1.30%", "% of Shares Held by Insiders"],
        ]
    )

    def _iterrows_not_expected():
        raise AssertionError("iterrows should not be used")

    monkeypatch.setattr(df, "iterrows", _iterrows_not_expected)

    holders = provider._major_holders_from_df(df)

    assert holders == {
        "% of Shares Held by Institutions": "45.10%",
        "% of Shares Held by Insiders": "1.30%",
    }
