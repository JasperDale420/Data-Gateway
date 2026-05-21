"""unusualwhales CI stub — provides only the subset imported at module level."""

from unittest.mock import MagicMock

UnusualWhalesClient = MagicMock


def __getattr__(name: str) -> MagicMock:  # type: ignore[return]
    return MagicMock()
