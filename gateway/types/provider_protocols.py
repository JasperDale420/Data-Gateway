"""Typed provider protocols for registry-based provider access."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Protocol


class SupportsAlpacaCalendar(Protocol):
    """Subset of Alpaca provider used by calendar endpoints."""

    def get_calendar(self, start: date, end: date) -> list[dict[str, Any]]:
        """Return trading calendar entries."""


class SupportsAlpacaOptionsChain(Protocol):
    """Subset of Alpaca provider used by bulk options endpoints."""

    async def get_option_chain(
        self,
        *,
        underlying: str,
        expiration_gte: str | None = None,
        expiration_lte: str | None = None,
    ) -> list[Any]:
        """Return option chain records."""


class SupportsAlpacaBars(Protocol):
    """Subset of Alpaca provider used by bulk bars endpoint."""

    async def get_bars(
        self,
        *,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
        adjustment: str,
    ) -> list[Any]:
        """Return normalized bar records."""


class SupportsAlpacaCorporateActions(Protocol):
    """Subset of Alpaca provider used by corporate actions endpoints."""

    async def get_corporate_actions(
        self,
        *,
        symbols: list[str],
        types: list[str] | None,
        start: datetime,
        end: datetime,
    ) -> list[Any]:
        """Return corporate action records."""


class SupportsFinnhubEarningsCalendar(Protocol):
    """Subset of Finnhub provider used by earnings calendar endpoint."""

    async def get_earnings_calendar(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        """Return earnings calendar rows."""
