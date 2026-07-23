"""UW volatility-analytics mixin.

Raw-HTTP endpoints for UnusualWhales volatility analytics not covered by the
vendored SDK (v5.1): per-ticker anomaly / character / stats / variance-risk-premium,
plus market-wide anomaly & character tops and the VIX term structure.
"""

from typing import Any
from urllib.parse import quote

from ._base import _UWMixinBase


class UWVolatilityExtMixin(_UWMixinBase):
    """Mixin providing UW volatility-analytics REST endpoints."""

    async def get_volatility_anomaly(self, symbol: str, date_str: str | None = None) -> Any:
        """Volatility anomaly score for a ticker."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/volatility/anomaly", {"date": date_str})

    async def get_volatility_character(self, symbol: str, date_str: str | None = None) -> Any:
        """Volatility character for a ticker."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/volatility/character", {"date": date_str})

    async def get_ticker_volatility_stats(self, symbol: str, date_str: str | None = None) -> Any:
        """Volatility statistics for a ticker (dedicated UW volatility/stats endpoint)."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/volatility/stats", {"date": date_str})

    async def get_variance_risk_premium(self, symbol: str, date_str: str | None = None) -> Any:
        """Variance risk premium for a ticker."""
        return await self._raw_get(f"/api/stock/{quote(symbol)}/volatility/variance-risk-premium", {"date": date_str})

    async def get_top_volatility_anomalies(
        self,
        direction: str,
        date_str: str | None = None,
        limit: int = 50,
    ) -> Any:
        """Top volatility anomalies market-wide (direction: ``short_vol`` or ``long_vol``)."""
        return await self._raw_get(
            "/api/volatility/anomaly/top",
            {"direction": direction, "date": date_str, "limit": limit},
        )

    async def get_top_volatility_character(
        self,
        date_str: str | None = None,
        limit: int = 50,
        sort: str | None = None,
        direction: str | None = None,
    ) -> Any:
        """Top volatility character market-wide."""
        return await self._raw_get(
            "/api/volatility/character/top",
            {"date": date_str, "limit": limit, "sort": sort, "dir": direction},
        )

    async def get_vix_term_structure(self, history_days: int = 90) -> Any:
        """VIX term structure with optional history window."""
        return await self._raw_get("/api/volatility/vix-term-structure", {"history_days": history_days})
