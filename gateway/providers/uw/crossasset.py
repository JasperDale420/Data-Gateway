"""UW cross-asset mixin.

Raw-HTTP endpoints for UnusualWhales cross-asset data not covered by the
vendored SDK (v5.1): commodities, crypto whale/OHLC/state, digital-currency
and FX historical/intraday/spot series, and US economic indicators.
"""

from typing import Any
from urllib.parse import quote


class UWCrossAssetMixin:
    """Mixin providing UW cross-asset REST endpoints."""

    async def get_commodity_series(self, name: str, interval: str | None = None) -> Any:
        """Long-running price series for a commodity (wti, brent, natural-gas, etc.)."""
        return await self._raw_get(f"/api/commodities/{quote(name)}", {"interval": interval})

    async def get_crypto_whale_transactions(self, limit: int | None = None) -> Any:
        """Recent crypto whale transactions."""
        return await self._raw_get("/api/crypto/whale-transactions", {"limit": limit})

    async def get_recent_crypto_whale_trades(self, limit: int | None = None) -> Any:
        """Recent large crypto trades (whale trades) across all pairs."""
        return await self._raw_get("/api/crypto/whales/recent", {"limit": limit})

    async def get_crypto_ohlc_candles(
        self,
        pair: str,
        candle_size: str,
        limit: int | None = None,
        date_str: str | None = None,
    ) -> Any:
        """OHLC candle data for a crypto pair at a given candle size."""
        return await self._raw_get(
            f"/api/crypto/{quote(pair)}/ohlc/{quote(candle_size)}",
            {"limit": limit, "date": date_str},
        )

    async def get_crypto_pair_state(self, pair: str) -> Any:
        """Current state for a crypto pair including 24h OHLCV data."""
        return await self._raw_get(f"/api/crypto/{quote(pair)}/state", {})

    async def get_digital_currency_history(
        self,
        symbol: str,
        market: str,
        interval: str | None = None,
    ) -> Any:
        """Daily, weekly, or monthly OHLC bars for a digital asset."""
        return await self._raw_get(
            "/api/digital-currencies/history",
            {"symbol": symbol, "market": market, "interval": interval},
        )

    async def get_digital_currency_intraday(
        self,
        symbol: str,
        market: str,
        interval: str | None = None,
    ) -> Any:
        """Intraday OHLC bars for a digital asset against a fiat market."""
        return await self._raw_get(
            "/api/digital-currencies/intraday",
            {"symbol": symbol, "market": market, "interval": interval},
        )

    async def get_economic_indicator(
        self,
        indicator: str,
        interval: str | None = None,
        maturity: str | None = None,
    ) -> Any:
        """Long-running US economic indicator series (gdp, cpi, treasury-yield, etc.)."""
        return await self._raw_get(
            f"/api/economy/{quote(indicator)}",
            {"interval": interval, "maturity": maturity},
        )

    async def get_forex_history(
        self,
        from_currency: str,
        to_currency: str,
        interval: str | None = None,
    ) -> Any:
        """Daily, weekly, or monthly OHLC bars for a currency pair."""
        return await self._raw_get(
            "/api/forex/history",
            {"from": from_currency, "to": to_currency, "interval": interval},
        )

    async def get_forex_intraday(
        self,
        from_currency: str,
        to_currency: str,
        interval: str | None = None,
    ) -> Any:
        """Intraday OHLC bars for a currency pair."""
        return await self._raw_get(
            "/api/forex/intraday",
            {"from": from_currency, "to": to_currency, "interval": interval},
        )

    async def get_forex_rate(self, from_currency: str, to_currency: str) -> Any:
        """Realtime spot exchange rate between two currencies."""
        return await self._raw_get(
            "/api/forex/rate",
            {"from": from_currency, "to": to_currency},
        )
