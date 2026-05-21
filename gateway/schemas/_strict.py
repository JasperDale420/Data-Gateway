"""Gateway-strict subclasses of empire-schemas models.

Each subclass adds Data-Gateway-side invariants on top of the canonical
shared schema. See `_validators.py` for the rationale and the list of
invariants enforced.

These subclasses are re-exported from `gateway.schemas` so any code
that builds a `NormalizedBar` etc. inside Data-Gateway gets the strict
form automatically. `isinstance(obj, EmpireSchemaModel)` continues to
work because subclasses inherit from the canonical model.

The cleanup of these invariants in `empire-schemas` proper is tracked
as a follow-up; the gateway is the source of truth until that lands.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from empire_schemas.alternative import (
    NormalizedDarkpoolTrade as _DarkpoolTradeBase,
)
from empire_schemas.alternative import (
    NormalizedInsiderTrade as _InsiderTradeBase,
)
from empire_schemas.alternative import (
    NormalizedPoliticianTrade as _PoliticianTradeBase,
)
from empire_schemas.analytics import (
    NormalizedMarketTide as _MarketTideBase,
)
from empire_schemas.analytics import (
    NormalizedNetPremiumTick as _NetPremiumTickBase,
)
from empire_schemas.analytics import (
    NormalizedSectorTide as _SectorTideBase,
)
from empire_schemas.market_data import (
    NormalizedBar as _NormalizedBarBase,
)
from empire_schemas.market_data import (
    NormalizedForexRate as _NormalizedForexRateBase,
)
from empire_schemas.market_data import (
    NormalizedQuote as _NormalizedQuoteBase,
)
from empire_schemas.market_data import (
    NormalizedTrade as _NormalizedTradeBase,
)
from empire_schemas.options import (
    NormalizedFlowAlert as _FlowAlertBase,
)
from empire_schemas.options import (
    NormalizedGreekExposure as _GreekExposureBase,
)
from empire_schemas.options import (
    NormalizedOptionContract as _OptionContractBase,
)
from empire_schemas.responses import (
    NormalizedNewsArticle as _NewsArticleBase,
)
from empire_schemas.responses import (
    ResponseMeta as _ResponseMetaBase,
)
from pydantic import field_validator

from gateway.schemas._validators import (
    require_aware,
    require_non_negative,
    require_positive,
)

# ─────────────────────────────────────────────────────────────────────
# Market data
# ─────────────────────────────────────────────────────────────────────


class NormalizedBar(_NormalizedBarBase):
    """Strict bar: tz-aware timestamp; positive OHLC; non-negative volume."""

    @field_validator("timestamp", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)

    @field_validator("open", "high", "low", "close", mode="after")
    @classmethod
    def _positive_ohlc(cls, v: Decimal | None) -> Decimal | None:
        return require_positive(v)

    @field_validator("volume", "trade_count", mode="after")
    @classmethod
    def _non_negative(cls, v: Any) -> Any:
        return require_non_negative(v)


class NormalizedQuote(_NormalizedQuoteBase):
    """Strict quote: tz-aware timestamp; non-negative bid/ask price and size."""

    @field_validator("timestamp", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)

    @field_validator("bid_price", "ask_price", "bid_size", "ask_size", mode="after")
    @classmethod
    def _non_negative(cls, v: Decimal | None) -> Decimal | None:
        return require_non_negative(v)


class NormalizedTrade(_NormalizedTradeBase):
    """Strict trade: tz-aware timestamp; positive price; non-negative size."""

    @field_validator("timestamp", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)

    @field_validator("price", mode="after")
    @classmethod
    def _positive_price(cls, v: Decimal | None) -> Decimal | None:
        return require_positive(v)

    @field_validator("size", mode="after")
    @classmethod
    def _non_negative_size(cls, v: Decimal | None) -> Decimal | None:
        return require_non_negative(v)


class NormalizedForexRate(_NormalizedForexRateBase):
    """Strict forex rate: tz-aware timestamp; positive bid/ask."""

    @field_validator("timestamp", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)

    @field_validator("bid", "ask", "mid", "open", "high", "low", "close", mode="after")
    @classmethod
    def _positive_rates(cls, v: Decimal | None) -> Decimal | None:
        return require_positive(v)


# ─────────────────────────────────────────────────────────────────────
# Options
# ─────────────────────────────────────────────────────────────────────


class NormalizedFlowAlert(_FlowAlertBase):
    """Strict flow alert: tz-aware timestamp; positive strike; non-neg counts."""

    @field_validator("timestamp", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)

    @field_validator("strike", mode="after")
    @classmethod
    def _positive_strike(cls, v: Decimal | None) -> Decimal | None:
        return require_positive(v)

    @field_validator("volume", "open_interest", "total_size", "trade_count", "expiry_count", mode="after")
    @classmethod
    def _non_negative_counts(cls, v: int | None) -> int | None:
        return require_non_negative(v)


class NormalizedOptionContract(_OptionContractBase):
    """Strict option contract: tz-aware timestamp; positive strike/bid/ask/last; non-neg counts."""

    @field_validator("timestamp", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)

    @field_validator("strike", mode="after")
    @classmethod
    def _positive_strike(cls, v: Decimal | None) -> Decimal | None:
        return require_positive(v)

    @field_validator("bid", "ask", "last", mode="after")
    @classmethod
    def _non_neg_prices(cls, v: Decimal | None) -> Decimal | None:
        # Allow 0 for last/bid/ask (no trade yet / locked market) but reject negative.
        return require_non_negative(v)

    @field_validator("volume", "open_interest", mode="after")
    @classmethod
    def _non_negative_counts(cls, v: int | None) -> int | None:
        return require_non_negative(v)


class NormalizedGreekExposure(_GreekExposureBase):
    """Strict greek exposure: tz-aware timestamp; positive strike when present."""

    @field_validator("timestamp", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)

    @field_validator("strike", mode="after")
    @classmethod
    def _positive_strike(cls, v: Decimal | None) -> Decimal | None:
        return require_positive(v)


# ─────────────────────────────────────────────────────────────────────
# Analytics / flow
# ─────────────────────────────────────────────────────────────────────


class NormalizedMarketTide(_MarketTideBase):
    @field_validator("timestamp", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)


class NormalizedSectorTide(_SectorTideBase):
    @field_validator("timestamp", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)


class NormalizedNetPremiumTick(_NetPremiumTickBase):
    @field_validator("timestamp", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)


# ─────────────────────────────────────────────────────────────────────
# Alternative
# ─────────────────────────────────────────────────────────────────────


class NormalizedDarkpoolTrade(_DarkpoolTradeBase):
    @field_validator("timestamp", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)


class NormalizedInsiderTrade(_InsiderTradeBase):
    @field_validator("transaction_date", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)


class NormalizedPoliticianTrade(_PoliticianTradeBase):
    @field_validator("transaction_date", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)


# ─────────────────────────────────────────────────────────────────────
# News
# ─────────────────────────────────────────────────────────────────────


class NormalizedNewsArticle(_NewsArticleBase):
    @field_validator("published_at", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return require_aware(v)


# ─────────────────────────────────────────────────────────────────────
# ResponseMeta — require explicit provider (no silent "alpaca" default)
# ─────────────────────────────────────────────────────────────────────


class ResponseMeta(_ResponseMetaBase):
    """Strict ResponseMeta: `provider` must be supplied explicitly.

    The canonical `empire_schemas.responses.ResponseMeta` defaults
    `provider="alpaca"`, which silently mis-stamps the lineage on any
    response built without an explicit provider. Gateway tests pin
    that this strict variant rejects construction without `provider`.
    """

    # The Field(...) (no default) forces Pydantic to require the value
    # even though the parent class supplies a default.
    provider: str  # type: ignore[assignment]


__all__ = [
    "NormalizedBar",
    "NormalizedQuote",
    "NormalizedTrade",
    "NormalizedForexRate",
    "NormalizedFlowAlert",
    "NormalizedOptionContract",
    "NormalizedGreekExposure",
    "NormalizedMarketTide",
    "NormalizedSectorTide",
    "NormalizedNetPremiumTick",
    "NormalizedDarkpoolTrade",
    "NormalizedInsiderTrade",
    "NormalizedPoliticianTrade",
    "NormalizedNewsArticle",
    "ResponseMeta",
]
