"""Coverage for the REAL ``get_order_ownership_guard`` lazy-init helper.

``tests/test_alpaca_trading_router.py`` has an autouse fixture
(``_reset_trading_inflight_sem``) that unconditionally monkeypatches
``trading.get_order_ownership_guard`` to a fake for every test in that file,
so the real function's body (module-level singleton construction, the
missing-Redis-URL 503, and the re-init-on-URL-change branch) is never
exercised there. This file imports ``gateway.api.alpaca.trading`` fresh and
calls the real function directly (not through an HTTP route) so autouse
fixtures from the other test file don't apply.

Confirmed by hand: ``redis.asyncio.BlockingConnectionPool.from_url(...)``
and ``aioredis.Redis(connection_pool=pool)`` do not connect eagerly at
construction time, so these tests need no real Redis server.
"""

from __future__ import annotations

import os

import pytest
from fastapi import HTTPException
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE

from gateway.api.alpaca import trading
from gateway.config import Settings
from gateway.core.order_ownership import OrderOwnershipGuard


@pytest.fixture(autouse=True)
def _isolate_gateway_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip GATEWAY_* out of the process environment for this module.

    ``gateway.main`` calls ``load_dotenv()`` at import time (and
    pydantic-settings' own dotenv read walks UP from cwd, so it finds the
    real repo's ``.env`` even when this worktree has none of its own), so
    the developer's ``GATEWAY_DATA_SINK_REDIS_URL`` is already in
    ``os.environ`` by the time a test runs — silently defeating the
    ``data_sink_redis_url=""`` case below. Matches the isolation pattern in
    ``tests/test_jetstream.py``.
    """
    for key in list(os.environ):
        if key.startswith("GATEWAY_"):
            monkeypatch.delenv(key, raising=False)


@pytest.fixture(autouse=True)
def _reset_ownership_guard_module_globals():
    """Isolate the module-level singleton across tests in this file.

    ``get_order_ownership_guard`` mutates the module globals
    ``_order_ownership_guard`` / ``_order_ownership_redis_url`` as a caching
    side effect. Without resetting these between tests, one test's
    constructed guard would leak into the next and mask the re-init branch.
    """
    trading._order_ownership_guard = None
    trading._order_ownership_redis_url = None
    yield
    trading._order_ownership_guard = None
    trading._order_ownership_redis_url = None


def test_get_order_ownership_guard_raises_503_when_redis_not_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No ``GATEWAY_DATA_SINK_REDIS_URL`` means order ownership can never be
    durably recorded, so the guard must fail loudly (503 GW-E5301) rather
    than silently construct a client pointed at nothing."""
    monkeypatch.setattr(trading, "get_settings", lambda: Settings(_env_file=None, GATEWAY_DATA_SINK_REDIS_URL=""))

    with pytest.raises(HTTPException) as exc:
        trading.get_order_ownership_guard()

    assert exc.value.status_code == HTTP_503_SERVICE_UNAVAILABLE
    assert exc.value.detail["code"] == "GW-E5301"
    # Nothing should have been cached on a failed construction.
    assert trading._order_ownership_guard is None


def test_get_order_ownership_guard_constructs_and_caches_singleton(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A configured URL builds a real ``OrderOwnershipGuard`` without
    connecting to Redis (connection pools are lazy), and a second call with
    the SAME url returns the identical cached instance rather than
    reconstructing the pool on every request."""
    settings = Settings(_env_file=None, GATEWAY_DATA_SINK_REDIS_URL="redis://localhost:59999/0")
    monkeypatch.setattr(trading, "get_settings", lambda: settings)

    guard1 = trading.get_order_ownership_guard()
    assert isinstance(guard1, OrderOwnershipGuard)

    guard2 = trading.get_order_ownership_guard()
    assert guard2 is guard1, "second call with the same redis url must reuse the cached singleton"


def test_get_order_ownership_guard_reinitializes_on_url_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DIFFERENT redis url (e.g. a config reload) must construct a NEW
    guard rather than keep serving the stale cached one."""
    settings_a = Settings(_env_file=None, GATEWAY_DATA_SINK_REDIS_URL="redis://localhost:59999/0")
    monkeypatch.setattr(trading, "get_settings", lambda: settings_a)
    guard_a = trading.get_order_ownership_guard()

    settings_b = Settings(_env_file=None, GATEWAY_DATA_SINK_REDIS_URL="redis://localhost:59998/0")
    monkeypatch.setattr(trading, "get_settings", lambda: settings_b)
    guard_b = trading.get_order_ownership_guard()

    assert guard_b is not guard_a, "a changed redis url must trigger re-init, not reuse the stale singleton"
    assert trading._order_ownership_redis_url == "redis://localhost:59998/0"
