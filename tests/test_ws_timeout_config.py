"""Verify WebSocket timeout configuration values.

These tests guard against accidental regression of timeout values
that were increased to prevent cascading disconnects under load.
"""

from __future__ import annotations

import pytest


@pytest.mark.unit
class TestWebSocketTimeoutConfig:
    """Ensure timeout constants are set to their required minimum values."""

    def test_default_heartbeat_interval(self):
        from gateway.config import Settings

        settings = Settings()
        assert settings.ws_heartbeat_interval >= 30, (
            f"ws_heartbeat_interval={settings.ws_heartbeat_interval} too low; must be >= 30s"
        )

    def test_client_heartbeat_timeout(self):
        from gateway.api.websocket import HEARTBEAT_TIMEOUT

        assert HEARTBEAT_TIMEOUT >= 15, f"HEARTBEAT_TIMEOUT={HEARTBEAT_TIMEOUT} too low; must be >= 15s"

    def test_client_max_missed_heartbeats(self):
        from gateway.api.websocket import MAX_MISSED_HEARTBEATS

        assert MAX_MISSED_HEARTBEATS >= 4, f"MAX_MISSED_HEARTBEATS={MAX_MISSED_HEARTBEATS} too low; must be >= 4"

    def test_max_silence_at_least_2_minutes(self):
        from gateway.api.websocket import MAX_MISSED_HEARTBEATS
        from gateway.config import Settings

        settings = Settings()
        max_silence = settings.ws_heartbeat_interval * MAX_MISSED_HEARTBEATS
        assert max_silence >= 120, f"Max silence ({max_silence}s) too low; must allow >= 120s before disconnect"
