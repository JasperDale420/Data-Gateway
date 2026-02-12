"""Tests for ticker_universe.py."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.core.ticker_universe import DEFAULT_CORE_TICKERS, TickerUniverse


class TestTickerUniverse:
    """Test suite for TickerUniverse."""

    def test_default_core_tickers(self) -> None:
        """Core tickers use DEFAULT_CORE_TICKERS when none provided."""
        tu = TickerUniverse()
        assert tu.core_tickers == [t.upper() for t in DEFAULT_CORE_TICKERS]
        assert len(tu.core_tickers) > 0

    def test_custom_core_tickers(self) -> None:
        """Custom core tickers override defaults."""
        tu = TickerUniverse(core_tickers=["AAPL", "msft", "tsla"])
        assert tu.core_tickers == ["AAPL", "MSFT", "TSLA"]

    def test_initial_dynamic_empty(self) -> None:
        """Dynamic tickers are empty before first refresh."""
        tu = TickerUniverse()
        assert tu.dynamic_tickers == []
        assert tu.last_refresh is None

    def test_all_tickers_deduplicates(self) -> None:
        """all_tickers deduplicates core + dynamic."""
        tu = TickerUniverse(core_tickers=["AAPL", "MSFT"])
        tu._dynamic = ["MSFT", "NVDA"]
        result = tu.all_tickers
        assert result == ["AAPL", "MSFT", "NVDA"]

    def test_core_tickers_returns_copy(self) -> None:
        """Modifying returned list does not change internal state."""
        tu = TickerUniverse(core_tickers=["SPY"])
        tickers = tu.core_tickers
        tickers.append("QQQ")
        assert tu.core_tickers == ["SPY"]


class TestTickerUniverseRefresh:
    """Tests for dynamic ticker refresh."""

    @pytest.mark.asyncio
    async def test_refresh_populates_dynamic(self) -> None:
        """refresh_dynamic populates dynamic tickers from screener."""
        tu = TickerUniverse(core_tickers=["SPY", "QQQ"], dynamic_count=3)

        mock_provider = AsyncMock()
        mock_provider.get_stock_screener.return_value = [
            SimpleNamespace(symbol="TSLA"),
            SimpleNamespace(symbol="NVDA"),
            SimpleNamespace(symbol="AMD"),
            SimpleNamespace(symbol="SPY"),  # already in core, should be skipped
            SimpleNamespace(symbol="PLTR"),
        ]

        count = await tu.refresh_dynamic(mock_provider)
        assert count == 3
        assert tu.dynamic_tickers == ["TSLA", "NVDA", "AMD"]
        assert tu.last_refresh is not None

    @pytest.mark.asyncio
    async def test_refresh_skips_core_tickers(self) -> None:
        """Screener results that are already core tickers are excluded."""
        tu = TickerUniverse(core_tickers=["AAPL", "MSFT"], dynamic_count=5)

        mock_provider = AsyncMock()
        mock_provider.get_stock_screener.return_value = [
            SimpleNamespace(symbol="AAPL"),
            SimpleNamespace(symbol="MSFT"),
            SimpleNamespace(symbol="GOOG"),
        ]

        count = await tu.refresh_dynamic(mock_provider)
        assert count == 1
        assert tu.dynamic_tickers == ["GOOG"]

    @pytest.mark.asyncio
    async def test_refresh_once_per_day(self) -> None:
        """refresh_dynamic only calls screener once per calendar day."""
        tu = TickerUniverse(core_tickers=["SPY"], dynamic_count=2)

        mock_provider = AsyncMock()
        mock_provider.get_stock_screener.return_value = [
            SimpleNamespace(symbol="TSLA"),
        ]

        await tu.refresh_dynamic(mock_provider)
        assert mock_provider.get_stock_screener.call_count == 1

        # Second call same day should be a no-op
        await tu.refresh_dynamic(mock_provider)
        assert mock_provider.get_stock_screener.call_count == 1

    @pytest.mark.asyncio
    async def test_refresh_handles_provider_error(self) -> None:
        """Provider errors are caught and logged, previous dynamic tickers retained."""
        tu = TickerUniverse(core_tickers=["SPY"], dynamic_count=3)
        tu._dynamic = ["AAPL"]

        mock_provider = AsyncMock()
        mock_provider.get_stock_screener.side_effect = RuntimeError("API error")

        count = await tu.refresh_dynamic(mock_provider)
        assert count == 1  # retained old tickers
        assert tu.dynamic_tickers == ["AAPL"]

    @pytest.mark.asyncio
    async def test_refresh_handles_dict_results(self) -> None:
        """Screener results as dicts (not objects) are handled."""
        tu = TickerUniverse(core_tickers=["SPY"], dynamic_count=2)

        mock_provider = AsyncMock()
        mock_provider.get_stock_screener.return_value = [
            {"symbol": "RIVN"},
            {"symbol": "SOFI"},
        ]

        count = await tu.refresh_dynamic(mock_provider)
        assert count == 2
        assert tu.dynamic_tickers == ["RIVN", "SOFI"]

    @pytest.mark.asyncio
    async def test_zero_dynamic_count(self) -> None:
        """dynamic_count=0 means no dynamic tickers are added."""
        tu = TickerUniverse(core_tickers=["SPY"], dynamic_count=0)

        mock_provider = AsyncMock()
        mock_provider.get_stock_screener.return_value = [
            SimpleNamespace(symbol="TSLA"),
        ]

        count = await tu.refresh_dynamic(mock_provider)
        assert count == 0
        assert tu.dynamic_tickers == []
