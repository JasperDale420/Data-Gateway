"""Tests for permissions hash caching behavior."""

from fastapi import FastAPI

from gateway.api.middleware import CacheMiddleware
from gateway.core.auth import Client, ClientPermissions


def _make_client() -> Client:
    return Client(
        id="client-1",
        permissions=ClientPermissions(
            providers=["alpaca"],
            feeds=["bars"],
            max_symbols=100,
            rate_limit=60,
            ws_subscriptions_max=500,
        ),
    )


def test_permissions_hash_cached_for_same_permissions(monkeypatch) -> None:
    """Repeated calls should reuse cached hash for unchanged permissions."""
    app = FastAPI()
    middleware = CacheMiddleware(app)
    client = _make_client()

    import json as json_module

    calls = {"count": 0}
    original_dumps = json_module.dumps

    def _spy_dumps(*args, **kwargs):
        calls["count"] += 1
        return original_dumps(*args, **kwargs)

    monkeypatch.setattr(json_module, "dumps", _spy_dumps)

    first = middleware._permissions_hash(client)
    second = middleware._permissions_hash(client)

    assert first == second
    assert calls["count"] == 1


def test_permissions_hash_recomputes_on_change(monkeypatch) -> None:
    """Changing permissions should invalidate the cached hash."""
    app = FastAPI()
    middleware = CacheMiddleware(app)
    client = _make_client()

    import json as json_module

    calls = {"count": 0}
    original_dumps = json_module.dumps

    def _spy_dumps(*args, **kwargs):
        calls["count"] += 1
        return original_dumps(*args, **kwargs)

    monkeypatch.setattr(json_module, "dumps", _spy_dumps)

    first = middleware._permissions_hash(client)
    client.permissions.providers.append("finnhub")
    second = middleware._permissions_hash(client)

    assert first != second
    assert calls["count"] == 2
