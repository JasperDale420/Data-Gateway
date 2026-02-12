"""Tests for Tier 2 admin operations endpoints (PRD §Security Architecture)."""

import asyncio
from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from gateway.api.deps import require_api_key
from gateway.core.auth import Client, ClientPermissions
from gateway.core.cache import InMemoryCache
from gateway.core.circuit_breaker import (
    CircuitBreakerRegistry,
    CircuitState,
    get_circuit_registry,
)
from gateway.core.shutdown import ShutdownCoordinator
from gateway.main import app

from .conftest import TEST_API_KEY

BYPASS_CACHE = {"X-Gateway-Cache": "bypass"}


def _headers(api_key: str = TEST_API_KEY) -> dict:
    return {"X-Gateway-Key": api_key, **BYPASS_CACHE}


def _make_client(role: str = "admin") -> Client:
    return Client(
        id=f"test_{role}",
        permissions=ClientPermissions(
            providers=["alpaca", "uw"],
            feeds=["bars", "quotes"],
            max_symbols=100,
            rate_limit=60,
        ),
        role=role,
    )


@contextmanager
def _override_role(role: str):
    """Override require_api_key to return a client with the given role."""
    client = _make_client(role)
    app.dependency_overrides[require_api_key] = lambda: client
    try:
        yield
    finally:
        app.dependency_overrides.pop(require_api_key, None)


# ─────────────────────────────────────────────────────────────────────────────
# Cache Management
# ─────────────────────────────────────────────────────────────────────────────


class TestCacheManagement:
    """Tests for /api/v1/admin/cache/* endpoints."""

    def test_cache_clear_returns_count(self, client: TestClient, test_cache: InMemoryCache):
        # Use internal _cache directly (set/get are async)
        test_cache._cache["foo"] = "bar"
        test_cache._cache["baz"] = "qux"
        test_cache._stats.size = len(test_cache._cache)

        with _override_role("admin"):
            resp = client.post("/api/v1/admin/cache/clear", headers=_headers())

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["data"]["entries_cleared"] >= 2

    def test_cache_clear_empties_cache(self, client: TestClient, test_cache: InMemoryCache):
        test_cache._cache["key1"] = "val1"
        test_cache._stats.size = 1

        with _override_role("admin"):
            client.post("/api/v1/admin/cache/clear", headers=_headers())

        # After clearing, cache size should be 0
        stats = test_cache.get_stats_dict()
        assert stats["size"] == 0

    def test_cache_stats(self, client: TestClient, test_cache: InMemoryCache):
        test_cache._cache["a"] = 1
        test_cache._stats.size = 1
        test_cache._stats.hits = 5
        test_cache._stats.misses = 2

        with _override_role("admin"):
            resp = client.get("/api/v1/admin/cache/stats", headers=_headers())

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert "hits" in data
        assert "misses" in data
        assert "size" in data

    def test_cache_endpoints_reject_client_role(self, client: TestClient):
        with _override_role("client"):
            resp = client.post("/api/v1/admin/cache/clear", headers=_headers())
        assert resp.status_code == 403


# ─────────────────────────────────────────────────────────────────────────────
# Circuit Breaker Management
# ─────────────────────────────────────────────────────────────────────────────


class TestCircuitBreakerManagement:
    """Tests for /api/v1/admin/circuits/* endpoints."""

    @pytest.fixture(autouse=True)
    def _reset_circuit_registry(self):
        """Ensure a clean circuit registry per test."""
        import gateway.core.circuit_breaker as cb_mod

        old = cb_mod._registry
        cb_mod._registry = CircuitBreakerRegistry()
        yield
        cb_mod._registry = old

    def test_list_circuits(self, client: TestClient):
        """GET /admin/circuits returns all registered breakers."""
        registry = get_circuit_registry()
        asyncio.get_event_loop().run_until_complete(registry.get("alpaca_rest"))

        with _override_role("admin"):
            resp = client.get("/api/v1/admin/circuits", headers=_headers())

        assert resp.status_code == 200
        circuits = resp.json()["data"]["circuits"]
        assert len(circuits) == 1
        assert circuits[0]["name"] == "alpaca_rest"
        assert circuits[0]["state"] == "closed"

    def test_reset_all_circuits(self, client: TestClient):
        """POST /admin/circuits/reset resets all breakers."""
        registry = get_circuit_registry()
        loop = asyncio.get_event_loop()
        breaker = loop.run_until_complete(registry.get("test_breaker"))
        breaker.state = CircuitState.OPEN

        with _override_role("admin"):
            resp = client.post("/api/v1/admin/circuits/reset", headers=_headers())

        assert resp.status_code == 200
        assert resp.json()["data"]["reset_count"] == 1
        assert breaker.state == CircuitState.CLOSED

    def test_reset_single_circuit(self, client: TestClient):
        """POST /admin/circuits/{name}/reset resets one breaker."""
        registry = get_circuit_registry()
        loop = asyncio.get_event_loop()
        breaker = loop.run_until_complete(registry.get("alpaca_rest"))
        breaker.state = CircuitState.OPEN

        with _override_role("admin"):
            resp = client.post("/api/v1/admin/circuits/alpaca_rest/reset", headers=_headers())

        assert resp.status_code == 200
        assert resp.json()["data"]["reset"] is True
        assert breaker.state == CircuitState.CLOSED

    def test_reset_unknown_circuit_returns_404(self, client: TestClient):
        with _override_role("admin"):
            resp = client.post("/api/v1/admin/circuits/nonexistent/reset", headers=_headers())
        assert resp.status_code == 404

    def test_circuits_reject_client_role(self, client: TestClient):
        with _override_role("client"):
            resp = client.get("/api/v1/admin/circuits", headers=_headers())
        assert resp.status_code == 403


# ─────────────────────────────────────────────────────────────────────────────
# Provider Reload
# ─────────────────────────────────────────────────────────────────────────────


class TestProviderReload:
    """Tests for POST /api/v1/admin/providers/{name}/reload."""

    def test_reload_provider(self, client: TestClient, test_registry):
        test_registry.reload_provider.return_value = True

        with _override_role("admin"):
            resp = client.post("/api/v1/admin/providers/alpaca/reload", headers=_headers())

        assert resp.status_code == 200
        assert resp.json()["data"]["provider"] == "alpaca"
        assert resp.json()["data"]["reloaded"] is True

    def test_reload_unknown_provider(self, client: TestClient, test_registry):
        test_registry.reload_provider.side_effect = KeyError("unknown")

        with _override_role("admin"):
            resp = client.post("/api/v1/admin/providers/unknown/reload", headers=_headers())

        assert resp.status_code == 404


# ─────────────────────────────────────────────────────────────────────────────
# Admin Shutdown
# ─────────────────────────────────────────────────────────────────────────────


class TestAdminShutdown:
    """Tests for POST /api/v1/admin/shutdown (super_admin only)."""

    @pytest.fixture(autouse=True)
    def _reset_shutdown(self):
        yield
        coord = ShutdownCoordinator.get_instance()
        coord._is_shutting_down = False
        ShutdownCoordinator._instance = None

    def test_shutdown_triggers_coordinator(self, client: TestClient):
        with _override_role("super_admin"):
            resp = client.post("/api/v1/admin/shutdown", headers=_headers())

        assert resp.status_code == 200
        assert resp.json()["data"]["shutting_down"] is True

        coord = ShutdownCoordinator.get_instance()
        assert coord.is_shutting_down is True

    def test_shutdown_rejects_admin_role(self, client: TestClient):
        """Only super_admin can trigger shutdown."""
        with _override_role("admin"):
            resp = client.post("/api/v1/admin/shutdown", headers=_headers())
        assert resp.status_code == 403

    def test_shutdown_rejects_client_role(self, client: TestClient):
        with _override_role("client"):
            resp = client.post("/api/v1/admin/shutdown", headers=_headers())
        assert resp.status_code == 403


# ─────────────────────────────────────────────────────────────────────────────
# Security Blocklist
# ─────────────────────────────────────────────────────────────────────────────


class TestSecurityBlocklist:
    """Tests for /api/v1/admin/security/blocklist."""

    @pytest.fixture(autouse=True)
    def _clear_blocklist(self):
        """Ensure clean blocklist per test."""
        from gateway.api.admin import _ip_blocklist

        _ip_blocklist.clear()
        yield
        _ip_blocklist.clear()

    def test_add_to_blocklist(self, client: TestClient):
        with _override_role("admin"):
            resp = client.post(
                "/api/v1/admin/security/blocklist",
                headers=_headers(),
                json={
                    "ip": "203.0.113.50",
                    "reason": "Brute force attempt",
                    "duration_hours": 24,
                },
            )

        assert resp.status_code == 200
        assert resp.json()["data"]["blocked"] is True

    def test_list_blocklist(self, client: TestClient):
        with _override_role("admin"):
            resp = client.get("/api/v1/admin/security/blocklist", headers=_headers())

        assert resp.status_code == 200
        assert "blocked" in resp.json()["data"]

    def test_blocklist_rejects_client_role(self, client: TestClient):
        with _override_role("client"):
            resp = client.get("/api/v1/admin/security/blocklist", headers=_headers())
        assert resp.status_code == 403
