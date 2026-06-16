from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
import pytest

from gateway.providers.yfinance import YFinanceProvider
from gateway.schemas import NormalizedBar


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


# ─────────────────────────────────────────────────────────────────────
# BLOCKER 5: canonical get_bars dispatcher
# ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_bars_dispatches_to_get_history_with_translated_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`get_bars(..., "1Day", ...)` routes to `get_history(interval="1d")`."""
    provider = YFinanceProvider()
    captured: list[dict[str, Any]] = []

    async def _fake_history(
        symbol: str,
        period: str = "1mo",
        interval: str = "1d",
        start: str | None = None,
        end: str | None = None,
    ) -> list[NormalizedBar]:
        captured.append({"symbol": symbol, "interval": interval, "start": start, "end": end})
        return [
            NormalizedBar(
                symbol=symbol.upper(),
                timestamp=datetime(2026, 2, 3, tzinfo=UTC),
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.5"),
                volume=Decimal("10"),
                provider="yfinance",
                timeframe=interval,
            )
        ]

    monkeypatch.setattr(provider, "get_history", _fake_history)
    bars = await provider.get_bars(
        symbols=["aapl"],
        timeframe="1Day",
        start=datetime(2026, 2, 1, tzinfo=UTC),
        end=datetime(2026, 2, 5, tzinfo=UTC),
    )
    assert len(bars) == 1
    assert captured[0]["interval"] == "1d"
    assert captured[0]["start"] == "2026-02-01"
    assert captured[0]["end"] == "2026-02-05"


@pytest.mark.asyncio
async def test_get_bars_accepts_raw_yfinance_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raw yfinance intervals like `1m` pass through unchanged."""
    provider = YFinanceProvider()
    captured: list[str] = []

    async def _fake_history(symbol: str, **kw: Any) -> list[NormalizedBar]:
        captured.append(kw["interval"])
        return []

    monkeypatch.setattr(provider, "get_history", _fake_history)
    await provider.get_bars(["AAPL"], "1m", datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 2, 2, tzinfo=UTC))
    assert captured == ["1m"]


@pytest.mark.asyncio
async def test_get_bars_fanout_across_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = YFinanceProvider()
    seen: list[str] = []

    async def _fake_history(symbol: str, **_kw: Any) -> list[NormalizedBar]:
        seen.append(symbol.upper())
        return [
            NormalizedBar(
                symbol=symbol.upper(),
                timestamp=datetime(2026, 2, 3, tzinfo=UTC),
                open=Decimal("1"),
                high=Decimal("2"),
                low=Decimal("1"),
                close=Decimal("1"),
                volume=Decimal("0"),
                provider="yfinance",
            )
        ]

    monkeypatch.setattr(provider, "get_history", _fake_history)
    bars = await provider.get_bars(
        symbols=["AAPL", "MSFT", "GOOG"],
        timeframe="1Day",
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 12, 31, tzinfo=UTC),
    )
    assert len(bars) == 3
    assert seen == ["AAPL", "MSFT", "GOOG"]
