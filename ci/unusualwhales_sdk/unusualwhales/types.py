"""Minimal types stub — only UNSET/Unset are imported at module level by gateway."""

from __future__ import annotations


class Unset:
    """Sentinel for unset/absent API fields."""

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "UNSET"


UNSET: Unset = Unset()
