"""Shared timestamp parsing utilities.

Provides a single canonical implementation for parsing timestamps across
the gateway, handling Z-suffix normalization, naive-to-UTC promotion,
and ISO 8601 parsing.
"""

from datetime import UTC, datetime


def parse_timestamp(value: str | datetime | None) -> datetime | None:
    """Parse a timestamp value to a timezone-aware datetime.

    Handles:
    - ``datetime`` pass-through (naive datetimes promoted to UTC)
    - ISO 8601 strings with ``Z`` suffix (converted to ``+00:00``)
    - ISO 8601 strings with explicit offset
    - Naive ISO strings (assumed UTC)

    Args:
        value: A datetime, ISO 8601 string, or ``None``.

    Returns:
        Timezone-aware ``datetime`` in UTC, or ``None`` if parsing fails
        or *value* is ``None``.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
        except (ValueError, AttributeError):
            return None
    return None
