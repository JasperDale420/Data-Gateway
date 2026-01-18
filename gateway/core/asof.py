"""As-Of Query Support.

Point-in-time query handling for historical data as specified in PRD (lines 823-854).
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import structlog

logger = structlog.get_logger()


# ─────────────────────────────────────────────────────────────────────────────
# As-Of Metadata
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class AsOfMeta:
    """Metadata for as-of query results."""

    as_of: datetime
    pit_guaranteed: bool  # Point-in-time accuracy guaranteed
    data_version: str  # "original" or "restated"
    provider_support: bool  # Whether provider supports as-of queries

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "pit_guaranteed": self.pit_guaranteed,
            "data_version": self.data_version,
            "provider_support": self.provider_support,
        }


@dataclass
class AsOfResult:
    """Result of an as-of query."""

    data: Any
    meta: AsOfMeta

    def to_dict(self) -> dict[str, Any]:
        return {
            "data": self.data,
            "meta": self.meta.to_dict(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# As-Of Query Handler
# ─────────────────────────────────────────────────────────────────────────────


class AsOfQueryHandler:
    """Handles point-in-time (as-of) queries.

    Provides as-of filtering and metadata for historical data queries,
    enabling backtesting with data as it existed at a specific timestamp.

    Provider Support:
    - Alpaca: Full support for bars, quotes, trades
    - yfinance: No support (returns current data)
    - UW: Limited support (flow data only)

    Example:
        handler = AsOfQueryHandler()

        # Check if provider supports as-of
        if handler.supports_as_of("alpaca", "bars"):
            result = handler.apply_as_of_filter(bars, as_of_timestamp)
    """

    # Provider support matrix per PRD
    PROVIDER_SUPPORT = {
        "alpaca": {
            "bars": True,
            "quotes": True,
            "trades": True,
            "options": False,
        },
        "yfinance": {
            "bars": False,
            "quotes": False,
            "trades": False,
        },
        "uw": {
            "flow": True,
            "chain": False,
            "dark_pool": True,
        },
    }

    def supports_as_of(self, provider: str, data_type: str) -> bool:
        """Check if a provider supports as-of queries for a data type.

        Args:
            provider: Provider name (alpaca, yfinance, uw)
            data_type: Data type (bars, quotes, trades, options, flow, etc.)

        Returns:
            True if as-of queries are supported
        """
        provider_matrix = self.PROVIDER_SUPPORT.get(provider.lower(), {})
        return provider_matrix.get(data_type.lower(), False)

    def apply_as_of_filter(
        self,
        data: list[dict],
        as_of: datetime,
        timestamp_field: str = "timestamp",
    ) -> list[dict]:
        """Filter data to only include records before as-of timestamp.

        Args:
            data: List of data records
            as_of: Point-in-time cutoff
            timestamp_field: Name of timestamp field in records

        Returns:
            Filtered data with only records before as-of
        """
        filtered = []

        for record in data:
            ts = record.get(timestamp_field)
            if ts is None:
                continue

            # Parse timestamp if string
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))

            # Ensure timezone-aware comparison
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            if as_of.tzinfo is None:
                as_of = as_of.replace(tzinfo=UTC)

            if ts <= as_of:
                filtered.append(record)

        return filtered

    def build_meta(
        self,
        provider: str,
        data_type: str,
        as_of: datetime,
    ) -> AsOfMeta:
        """Build metadata for an as-of query result.

        Args:
            provider: Provider name
            data_type: Type of data queried
            as_of: As-of timestamp

        Returns:
            AsOfMeta with provider support info
        """
        supports = self.supports_as_of(provider, data_type)

        return AsOfMeta(
            as_of=as_of,
            pit_guaranteed=supports,
            data_version="original" if supports else "current",
            provider_support=supports,
        )

    def wrap_result(
        self,
        data: Any,
        provider: str,
        data_type: str,
        as_of: datetime,
    ) -> AsOfResult:
        """Wrap query result with as-of metadata.

        Args:
            data: Query result data
            provider: Provider name
            data_type: Type of data
            as_of: As-of timestamp

        Returns:
            AsOfResult with data and metadata
        """
        meta = self.build_meta(provider, data_type, as_of)

        # Apply filter if provider supports as-of
        if meta.pit_guaranteed and isinstance(data, list):
            data = self.apply_as_of_filter(data, as_of)

        return AsOfResult(data=data, meta=meta)


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

_handler: AsOfQueryHandler | None = None


def get_asof_handler() -> AsOfQueryHandler:
    """Get singleton AsOfQueryHandler instance."""
    global _handler
    if _handler is None:
        _handler = AsOfQueryHandler()
    return _handler
