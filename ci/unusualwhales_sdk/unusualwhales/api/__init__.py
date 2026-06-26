"""unusualwhales.api CI stub.

Real submodules (e.g. ``insider.py``) load normally; every other name resolves
to a STABLE per-name MagicMock. The previous ``return MagicMock()`` made
``hasattr(api, name)`` true for every name, so ``from unusualwhales.api import
insider`` skipped the real submodule import and handed back a throwaway mock —
and two such imports returned *different* mocks, breaking identity assertions
(test_get_insiders_uses_transactions_endpoint).
"""

import importlib
from unittest.mock import MagicMock

_mocks: dict[str, MagicMock] = {}


def __getattr__(name: str) -> object:
    if name.startswith("__") and name.endswith("__"):
        raise AttributeError(name)  # don't mock dunders — keep package introspection sane
    try:
        return importlib.import_module(f"{__name__}.{name}")
    except ModuleNotFoundError:
        return _mocks.setdefault(name, MagicMock())
