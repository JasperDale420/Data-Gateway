"""UW congress / institutional / shorts / analytics mixin.

Raw-HTTP endpoints for UnusualWhales congress-extension analytics not covered by
the vendored SDK (v5.1): sliding/fixed-window analytics, unusual congressional
trades and their by-ticker / chart / stats views, politician portfolios &
disclosures, v2 institutional activity, and v2 short-interest / short-screener.
"""

from typing import Any
from urllib.parse import quote

from ._base import _UWMixinBase


class UWCongressExtMixin(_UWMixinBase):
    """Mixin providing UW congress-extension REST endpoints."""

    async def get_sliding_window_analytics(
        self,
        symbols: str,
        range: str,
        calculations: str,
        range_end: str | None = None,
        interval: str | None = None,
        ohlc: str | None = None,
        window_size: int | None = None,
    ) -> Any:
        """Sliding-window statistical analytics across one or more tickers."""
        return await self._raw_get(
            "/api/analytics/sliding",
            {
                "symbols": symbols,
                "range": range,
                "range_end": range_end,
                "interval": interval,
                "ohlc": ohlc,
                "window_size": window_size,
                "calculations": calculations,
            },
        )

    async def get_fixed_window_analytics(
        self,
        symbols: str,
        range: str,
        calculations: str,
        range_end: str | None = None,
        interval: str | None = None,
        ohlc: str | None = None,
    ) -> Any:
        """Fixed-window statistical analytics across one or more tickers."""
        return await self._raw_get(
            "/api/analytics/window",
            {
                "symbols": symbols,
                "range": range,
                "range_end": range_end,
                "interval": interval,
                "ohlc": ohlc,
                "calculations": calculations,
            },
        )

    async def get_congress_trader_recent_reports(
        self,
        limit: int | None = None,
        date_str: str | None = None,
        ticker: str | None = None,
        name: str | None = None,
        page: int | None = None,
        date_from: str | None = None,
    ) -> Any:
        """Recent reports by the given congress member."""
        return await self._raw_get(
            "/api/congress/congress-trader",
            {
                "limit": limit,
                "date": date_str,
                "ticker": ticker,
                "name": name,
                "page": page,
                "date_from": date_from,
            },
        )

    async def get_congress_politicians(self, last_traded_within_months: int | None = None) -> Any:
        """Distinct list of politicians for which trade data exists."""
        return await self._raw_get(
            "/api/congress/politicians",
            {"last_traded_within_months": last_traded_within_months},
        )

    async def get_unusual_congress_trades(
        self,
        types: str | None = None,
        limit: int | None = None,
        page: int | None = None,
    ) -> Any:
        """Congressional trades flagged as unusual, optionally filtered by reason tags."""
        return await self._raw_get(
            "/api/congress/unusual-trades",
            {"types": types, "limit": limit, "page": page},
        )

    async def get_unusual_congress_trades_by_tickers(
        self,
        tickers: str | None = None,
        transaction_type: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        politician: str | None = None,
        limit: int | None = None,
        page: int | None = None,
    ) -> Any:
        """Unusual congressional trades filtered by one or more tickers."""
        return await self._raw_get(
            "/api/congress/unusual-trades/by-tickers",
            {
                "tickers": tickers,
                "transaction_type": transaction_type,
                "date_from": date_from,
                "date_to": date_to,
                "politician": politician,
                "limit": limit,
                "page": page,
            },
        )

    async def get_unusual_congress_trades_chart_data(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> Any:
        """Trade points and SPY daily closes over the requested date range."""
        return await self._raw_get(
            "/api/congress/unusual-trades/chart-data",
            {"date_from": date_from, "date_to": date_to},
        )

    async def get_unusual_congress_trades_stats(self) -> Any:
        """Most recent cached overview statistics for unusual congressional trades."""
        return await self._raw_get("/api/congress/unusual-trades/stats", {})

    async def get_institution_activity_v2(
        self,
        name: str,
        ticker_symbol: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        page: int | None = None,
        order: str | None = None,
        order_direction: str | None = None,
    ) -> Any:
        """Trading activities for a given institution (v2)."""
        return await self._raw_get(
            f"/api/institution/{quote(name)}/activity/v2",
            {
                "ticker_symbol": ticker_symbol,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
                "page": page,
                "order": order,
                "order_direction": order_direction,
            },
        )

    async def get_politician_disclosures(
        self,
        politician_id: str | None = None,
        latest_only: bool | None = None,
        year: int | None = None,
    ) -> Any:
        """Annual disclosure file records for politicians."""
        return await self._raw_get(
            "/api/politician-portfolios/disclosures",
            {"politician_id": politician_id, "latest_only": latest_only, "year": year},
        )

    async def get_short_screener(
        self,
        tickers: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        min_short_interest: float | None = None,
        max_short_interest: float | None = None,
        min_days_to_cover: float | None = None,
        max_days_to_cover: float | None = None,
        min_si_float: float | None = None,
        max_si_float: float | None = None,
        min_si_float_with_synth_long_pct_of_total_shares: float | None = None,
        max_si_float_with_synth_long_pct_of_total_shares: float | None = None,
        min_total_float: float | None = None,
        max_total_float: float | None = None,
        order_by: str | None = None,
        order_direction: str | None = None,
        min_market_date: str | None = None,
        max_market_date: str | None = None,
        min_fee_rate: float | None = None,
        max_fee_rate: float | None = None,
        min_rebate_rate: float | None = None,
        max_rebate_rate: float | None = None,
        min_short_shares_available: float | None = None,
        max_short_shares_available: float | None = None,
    ) -> Any:
        """Short interest and float data for percentage calculations based off search params."""
        return await self._raw_get(
            "/api/short_screener",
            {
                "tickers": tickers,
                "limit": limit,
                "offset": offset,
                "min_short_interest": min_short_interest,
                "max_short_interest": max_short_interest,
                "min_days_to_cover": min_days_to_cover,
                "max_days_to_cover": max_days_to_cover,
                "min_si_float": min_si_float,
                "max_si_float": max_si_float,
                "min_si_float_with_synth_long_pct_of_total_shares": (min_si_float_with_synth_long_pct_of_total_shares),
                "max_si_float_with_synth_long_pct_of_total_shares": (max_si_float_with_synth_long_pct_of_total_shares),
                "min_total_float": min_total_float,
                "max_total_float": max_total_float,
                "order_by": order_by,
                "order_direction": order_direction,
                "min_market_date": min_market_date,
                "max_market_date": max_market_date,
                "min_fee_rate": min_fee_rate,
                "max_fee_rate": max_fee_rate,
                "min_rebate_rate": min_rebate_rate,
                "max_rebate_rate": max_rebate_rate,
                "min_short_shares_available": min_short_shares_available,
                "max_short_shares_available": max_short_shares_available,
            },
        )

    async def get_short_interest_float_v2(self, symbol: str) -> Any:
        """V2 short interest, float size, and days-to-cover for a ticker."""
        return await self._raw_get(f"/api/shorts/{quote(symbol)}/interest-float/v2", {})
