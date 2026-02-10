"""Tests for graceful shutdown coordination (PRD §Graceful Shutdown)."""

import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import WebSocket

from gateway.core.auth import Client, ClientPermissions
from gateway.core.connections import ConnectionManager
from gateway.core.shutdown import ShutdownCoordinator

# ── ShutdownCoordinator ──────────────────────────────────────────────────────


class TestShutdownCoordinator:
    """Test ShutdownCoordinator state machine."""

    @pytest.fixture(autouse=True)
    def _reset(self):
        ShutdownCoordinator.reset()
        yield
        ShutdownCoordinator.reset()

    def test_singleton(self):
        a = ShutdownCoordinator.get_instance()
        b = ShutdownCoordinator.get_instance()
        assert a is b

    def test_initial_state(self):
        coord = ShutdownCoordinator.get_instance()
        assert coord.is_shutting_down is False
        assert coord.drain_remaining() == 0.0

    def test_start_shutdown(self):
        coord = ShutdownCoordinator.get_instance()
        coord.start_shutdown(drain_seconds=10)
        assert coord.is_shutting_down is True
        assert coord.drain_seconds == 10
        assert coord.drain_remaining() > 0

    def test_drain_remaining_decreases(self):
        coord = ShutdownCoordinator.get_instance()
        coord.start_shutdown(drain_seconds=10)
        remaining_before = coord.drain_remaining()
        time.sleep(0.05)
        remaining_after = coord.drain_remaining()
        assert remaining_after < remaining_before

    def test_drain_remaining_floors_at_zero(self):
        coord = ShutdownCoordinator.get_instance()
        coord.start_shutdown(drain_seconds=0)
        time.sleep(0.01)
        assert coord.drain_remaining() == 0.0

    def test_start_shutdown_idempotent(self):
        coord = ShutdownCoordinator.get_instance()
        coord.start_shutdown(drain_seconds=30)
        first_start = coord._drain_started_at
        coord.start_shutdown(drain_seconds=60)
        assert coord._drain_started_at == first_start
        assert coord.drain_seconds == 30  # not updated


# ── ConnectionManager shutdown helpers ───────────────────────────────────────


class TestConnectionManagerShutdown:
    """Test broadcast_shutdown and close_all on ConnectionManager."""

    @pytest.fixture
    def manager(self):
        return ConnectionManager()

    def _make_ws(self):
        ws = MagicMock(spec=WebSocket)
        ws.send_json = AsyncMock()
        ws.close = AsyncMock()
        return ws

    @pytest.mark.asyncio
    async def test_broadcast_shutdown_sends_to_authenticated(self, manager):
        ws = self._make_ws()
        await manager.connect("c1", ws)
        perms = ClientPermissions(providers=["alpaca"], feeds=["bars"])
        client = Client(id="test", permissions=perms)
        await manager.authenticate("c1", client)

        count = await manager.broadcast_shutdown(timeout_seconds=30)

        assert count == 1
        ws.send_json.assert_called_once()
        msg = ws.send_json.call_args[0][0]
        assert msg["type"] == "system"
        assert msg["event"] == "shutdown"
        assert msg["timeout_seconds"] == 30

    @pytest.mark.asyncio
    async def test_broadcast_shutdown_skips_unauthenticated(self, manager):
        ws = self._make_ws()
        await manager.connect("c1", ws)

        count = await manager.broadcast_shutdown(timeout_seconds=30)
        assert count == 0
        ws.send_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_all_closes_with_code(self, manager):
        ws1 = self._make_ws()
        ws2 = self._make_ws()
        await manager.connect("c1", ws1)
        await manager.connect("c2", ws2)

        await manager.close_all(code=1001, reason="Going Away")

        ws1.close.assert_called_once_with(code=1001, reason="Going Away")
        ws2.close.assert_called_once_with(code=1001, reason="Going Away")

    @pytest.mark.asyncio
    async def test_close_all_tolerates_errors(self, manager):
        ws = self._make_ws()
        ws.close = AsyncMock(side_effect=RuntimeError("already closed"))
        await manager.connect("c1", ws)

        # Should not raise
        await manager.close_all()


# ── Health endpoint during shutdown ──────────────────────────────────────────


class TestHealthDuringShutdown:
    """Test that health returns 503 during shutdown."""

    @pytest.fixture(autouse=True)
    def _reset(self):
        ShutdownCoordinator.reset()
        yield
        ShutdownCoordinator.reset()

    def test_liveness_returns_503_during_shutdown(self, client):
        coord = ShutdownCoordinator.get_instance()
        coord.start_shutdown(drain_seconds=30)
        response = client.get("/health", headers={"X-Gateway-Cache": "bypass"})
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "shutting_down"
        assert "drain_remaining_seconds" in data

    def test_readiness_returns_503_during_shutdown(self, client):
        coord = ShutdownCoordinator.get_instance()
        coord.start_shutdown(drain_seconds=30)
        response = client.get("/health/ready", headers={"X-Gateway-Cache": "bypass"})
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "shutting_down"

    def test_liveness_returns_200_when_not_shutting_down(self, client):
        response = client.get("/health", headers={"X-Gateway-Cache": "bypass"})
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
