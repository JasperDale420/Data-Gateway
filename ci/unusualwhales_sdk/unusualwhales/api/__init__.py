"""unusualwhales.api CI stub — all submodules resolve to MagicMock objects."""

from unittest.mock import MagicMock


def __getattr__(name: str) -> MagicMock:  # type: ignore[return]
    return MagicMock()
