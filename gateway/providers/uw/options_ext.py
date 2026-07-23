"""UW options-flow / greeks / GEX / options-pulse mixin.

Raw-HTTP endpoints for UnusualWhales options analytics not covered by the
vendored SDK (v5.1): group greek-flow by expiry, lit-flow tape, market movers /
OI change, option-trade exchange breakdown & flow-alert detail, optionable
tickers, options-pulse (sector / scanner / market-wide / per-ticker), plus
per-ticker flow-per-expiry / flow-per-strike / GEX levels and spot exposures.
"""

from typing import Any
from urllib.parse import quote

from ._base import _UWMixinBase


class UWOptionsExtMixin(_UWMixinBase):
    """Mixin providing UW options-flow / greeks / GEX / options-pulse REST endpoints."""

    async def get_group_flow_greek_flow_by_expiry(
        self,
        flow_group: str,
        expiry: str,
        date_str: str | None = None,
    ) -> Any:
        """Group flow greek flow (delta & vega) per minute for a given expiry."""
        return await self._raw_get(
            f"/api/group-flow/{quote(flow_group)}/greek-flow/{quote(expiry)}",
            {"date": date_str},
        )

    async def get_lit_flow_recent(
        self,
        limit: int | None = None,
        date_str: str | None = None,
        min_premium: int | None = None,
        max_premium: int | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
        min_volume: int | None = None,
        max_volume: int | None = None,
    ) -> Any:
        """Latest lit exchange trades market-wide."""
        return await self._raw_get(
            "/api/lit-flow/recent",
            {
                "limit": limit,
                "date": date_str,
                "min_premium": min_premium,
                "max_premium": max_premium,
                "min_size": min_size,
                "max_size": max_size,
                "min_volume": min_volume,
                "max_volume": max_volume,
            },
        )

    async def get_ticker_lit_flow(
        self,
        symbol: str,
        date_str: str | None = None,
        newer_than: str | None = None,
        older_than: str | None = None,
        min_premium: int | None = None,
        max_premium: int | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
        min_volume: int | None = None,
        max_volume: int | None = None,
        limit: int | None = None,
    ) -> Any:
        """Lit exchange trades for a ticker on a given day."""
        return await self._raw_get(
            f"/api/lit-flow/{quote(symbol)}",
            {
                "date": date_str,
                "newer_than": newer_than,
                "older_than": older_than,
                "min_premium": min_premium,
                "max_premium": max_premium,
                "min_size": min_size,
                "max_size": max_size,
                "min_volume": min_volume,
                "max_volume": max_volume,
                "limit": limit,
            },
        )

    async def get_market_movers(self) -> Any:
        """Top gainers, losers, and most actively traded US tickers for the latest session."""
        return await self._raw_get("/api/market/movers", {})

    async def get_market_oi_change(
        self,
        date_str: str | None = None,
        limit: int | None = None,
        order: str | None = None,
    ) -> Any:
        """Contracts with the highest OI (open interest) change market-wide."""
        return await self._raw_get(
            "/api/market/oi-change",
            {"date": date_str, "limit": limit, "order": order},
        )

    async def get_option_trades_exchange_breakdown(
        self,
        date_str: str,
        tickers: list[str] | None = None,
        by_trade_code: bool | None = None,
        min_premium: str | None = None,
        limit: int = 100,
        page: int = 1,
        order: str | None = None,
    ) -> Any:
        """Option tape aggregated by options exchange for a trading date."""
        return await self._raw_get(
            f"/api/option-trades/exchange-breakdown/{quote(date_str)}",
            {
                "ticker[]": tickers,
                "by_trade_code": by_trade_code,
                "min_premium": min_premium,
                "limit": limit,
                "page": page,
                "order": order,
            },
        )

    async def get_flow_alert_by_id(self, id: str, older_than: str | None = None) -> Any:
        """Trades that made up a specific flow alert."""
        return await self._raw_get(
            f"/api/option-trades/flow-alerts/{quote(id)}",
            {"older_than": older_than},
        )

    async def get_optionable_tickers(self, ticker: str | None = None) -> Any:
        """Current universe of underlying symbols that have listed options."""
        return await self._raw_get("/api/option-trades/optionable-tickers", {"ticker": ticker})

    async def get_options_pulse_sectors(self, date_str: str | None = None) -> Any:
        """Latest Options Pulse sentiment per sector and industry on a date."""
        return await self._raw_get("/api/options-pulse/sectors", {"date": date_str})

    async def get_options_pulse_top(
        self,
        direction: str | None = None,
        date_str: str | None = None,
        ticker: str | None = None,
        min_score: float | None = None,
        max_score: float | None = None,
        min_txn: int | None = None,
        limit: int = 50,
    ) -> Any:
        """Cross-symbol Options Pulse scanner ranked by sentiment."""
        return await self._raw_get(
            "/api/options-pulse/top",
            {
                "direction": direction,
                "date": date_str,
                "ticker": ticker,
                "min_score": min_score,
                "max_score": max_score,
                "min_txn": min_txn,
                "limit": limit,
            },
        )

    async def get_options_pulse_total(self, date_str: str | None = None) -> Any:
        """Market-wide Options Pulse gauge snapshot + intraday series for a date."""
        return await self._raw_get("/api/options-pulse/total", {"date": date_str})

    async def get_ticker_flow_alerts(
        self,
        symbol: str,
        limit: int | None = None,
        is_ask_side: bool | None = None,
        is_bid_side: bool | None = None,
    ) -> Any:
        """Flow alerts for a ticker (deprecated upstream endpoint)."""
        return await self._raw_get(
            f"/api/stock/{quote(symbol)}/flow-alerts",
            {"limit": limit, "is_ask_side": is_ask_side, "is_bid_side": is_bid_side},
        )

    async def get_ticker_flow_per_expiry(self, symbol: str) -> Any:
        """Option flow per expiry for the last trading day."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/flow-per-expiry", {})

    async def get_ticker_flow_per_strike(self, symbol: str, date_str: str | None = None) -> Any:
        """Option flow per strike for a given trading day."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/flow-per-strike", {"date": date_str})

    async def get_gex_levels(self, symbol: str, date_str: str | None = None) -> Any:
        """Key gamma-exposure (GEX) price levels for a ticker on a market date."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/gex-levels", {"date": date_str})

    async def get_ticker_options_pulse(self, symbol: str, date_str: str | None = None) -> Any:
        """Options Pulse sentiment for a single ticker (snapshot + intraday series)."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/options-pulse", {"date": date_str})

    async def get_ticker_spot_exposures_expiry_strike(
        self,
        symbol: str,
        expirations: list[str],
        date_str: str | None = None,
        limit: int | None = None,
        page: int | None = None,
        min_strike: float | None = None,
        max_strike: float | None = None,
        min_dte: int | None = None,
        max_dte: int | None = None,
    ) -> Any:
        """Most recent spot GEX exposures across strikes for a ticker & expiration."""
        return await self._raw_get(
            f"/api/stock/{quote(symbol)}/spot-exposures/expiry-strike",
            {
                "expirations[]": expirations,
                "date": date_str,
                "limit": limit,
                "page": page,
                "min_strike": min_strike,
                "max_strike": max_strike,
                "min_dte": min_dte,
                "max_dte": max_dte,
            },
        )
