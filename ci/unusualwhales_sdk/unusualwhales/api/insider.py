"""unusualwhales.api.insider CI stub.

The package ``__getattr__`` fallback returns a FRESH ``MagicMock()`` for every
missing submodule access, so the provider's ``insider.get_transactions.sync``
and a test's reference to the same path resolve to different objects — breaking
identity assertions (test_get_insiders_uses_transactions_endpoint). Defining the
module here gives a single, stable ``get_transactions`` so both references point
at the same object, matching the real vendored SDK's behaviour.
"""

from unittest.mock import MagicMock

# Stable module-level handle: ``get_transactions.sync`` is the same child mock on
# every access, so `provider.get_insiders` and the test see an identical func.
get_transactions = MagicMock()
