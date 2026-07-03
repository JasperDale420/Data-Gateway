"""UW fundamentals mixin.

Raw-HTTP endpoints for UnusualWhales stock financials, company profiles,
IPO calendar, and the ticker-exchange directory not covered by the vendored
SDK (v5.1).
"""

from typing import Any
from urllib.parse import quote


class UWFundamentalsMixin:
    """Mixin providing UW fundamentals REST endpoints."""

    async def get_ipo_calendar(self) -> Any:
        """Upcoming IPOs in the next 3 months."""
        return await self._raw_get("/api/calendar/ipo", {})

    async def get_company_listings(self, status: str | None = None, date_str: str | None = None) -> Any:
        """All US-traded securities, optionally filtered to delisted as of a date."""
        return await self._raw_get("/api/companies/listings", {"status": status, "date": date_str})

    async def get_company_dividends(self, symbol: str) -> Any:
        """Historical dividend events for a ticker."""
        return await self._raw_get(f"/api/companies/{quote(symbol)}/dividends", {})

    async def get_earnings_estimates(self, symbol: str) -> Any:
        """Analyst-driven forward earnings estimates by quarter/year."""
        return await self._raw_get(f"/api/companies/{quote(symbol)}/earnings-estimates", {})

    async def get_company_profile(self, symbol: str) -> Any:
        """Normalized company profile (sector, industry, market cap, P/E)."""
        return await self._raw_get(f"/api/companies/{quote(symbol)}/profile", {})

    async def get_company_splits(self, symbol: str) -> Any:
        """Historical stock split events for a ticker."""
        return await self._raw_get(f"/api/companies/{quote(symbol)}/splits", {})

    async def get_earnings_transcript(self, symbol: str, quarter: str) -> Any:
        """Full earnings-call transcript for a ticker and quarter (e.g. 2024Q1)."""
        return await self._raw_get(f"/api/companies/{quote(symbol)}/transcripts/{quote(quarter)}", {})

    async def get_ticker_exchanges(self) -> Any:
        """Mapping of all tickers to their exchanges."""
        return await self._raw_get("/api/stock-directory/ticker-exchanges", {})

    async def get_balance_sheets(self, symbol: str) -> Any:
        """Balance sheet data for a ticker."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/balance-sheets", {})

    async def get_cash_flows(self, symbol: str) -> Any:
        """Cash flow statement data for a ticker."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/cash-flows", {})

    async def get_stock_earnings(self, symbol: str) -> Any:
        """Earnings history for a ticker."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/earnings", {})

    async def get_stock_financials(self, symbol: str) -> Any:
        """Full financial data (income, balance sheets, cash flows, earnings)."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/financials", {})

    async def get_fundamental_breakdown(self, symbol: str) -> Any:
        """Fundamental financial data (EPS, revenue, dividends, share counts)."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/fundamental-breakdown", {})

    async def get_income_statements(self, symbol: str) -> Any:
        """Income statement data for a ticker."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/income-statements", {})

    async def get_stock_ownership(self, symbol: str, limit: int | None = None) -> Any:
        """Institutions, insider trades, and politicians with the most shares."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/ownership", {"limit": limit})

    async def get_technical_indicator(
        self,
        symbol: str,
        function: str,
        interval: str | None = None,
        time_period: int | None = None,
        series_type: str | None = None,
        month: str | None = None,
    ) -> Any:
        """Technical indicator time series for a ticker."""
        return await self._raw_get(
            f"/api/stock/{quote(symbol)}/technical-indicator/{quote(function)}",
            {
                "interval": interval,
                "time_period": time_period,
                "series_type": series_type,
                "month": month,
            },
        )
