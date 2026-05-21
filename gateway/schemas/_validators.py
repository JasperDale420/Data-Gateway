"""Gateway-side strict validators for shared empire-schemas models.

These wrappers add validation invariants that are missing from the shared
`empire-schemas` package. We subclass rather than mutate so:

1. The upstream package stays untouched (no coordinated rollout required
   across Heber/Cerberus/3Roses/etc).
2. `isinstance(obj, EmpireSchemaModel)` still works because subclasses
   inherit from the canonical model.
3. Gateway boundaries (REST handlers, sink publish, REST/WS construction
   in providers) automatically get the stricter checks.

Invariants enforced here:

* **Timezone-aware datetimes** — every `datetime` field rejects
  `tzinfo=None`. The Data-Gateway → Heber contract requires UTC for all
  timestamps; naive datetimes upstream silently corrupted lineage data.

* **Positivity** — prices and strikes must be `> 0`; volume, open
  interest, and sizes must be `>= 0`. Negative/zero values previously
  validated and propagated into downstream analytics.

The cleanup of these invariants in `empire-schemas` proper is tracked as
a follow-up; until that lands, gateway is the source of truth.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

# Reason strings — kept as module constants so tests and operators can
# grep for the exact message that fires.
NAIVE_DT_MSG = (
    "timestamp must be timezone-aware (UTC required); naive datetimes (tzinfo=None) are not accepted by the gateway"
)
NON_POSITIVE_MSG = "price/strike must be > 0"
NEGATIVE_QTY_MSG = "volume/size/open_interest must be >= 0"


def require_aware(value: datetime | None) -> datetime | None:
    """Reject `datetime` values with `tzinfo=None`.

    Used as the body of `@field_validator(..., mode="after")` on every
    datetime field across gateway-strict schemas.
    """
    if value is None:
        return value
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None:
        raise ValueError(NAIVE_DT_MSG)
    return value


def require_positive(value: Decimal | int | float | None) -> Any:
    """Require `> 0` — for prices, strikes, etc."""
    if value is None:
        return value
    if value <= 0:
        raise ValueError(NON_POSITIVE_MSG)
    return value


def require_non_negative(value: Decimal | int | float | None) -> Any:
    """Require `>= 0` — for volume, OI, sizes."""
    if value is None:
        return value
    if value < 0:
        raise ValueError(NEGATIVE_QTY_MSG)
    return value
