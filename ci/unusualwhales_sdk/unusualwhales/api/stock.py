"""unusualwhales.api.stock CI stub."""

from unittest.mock import MagicMock


def __getattr__(name: str) -> MagicMock:  # type: ignore[return]
    return MagicMock()
