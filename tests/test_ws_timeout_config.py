"""Verify WebSocket timeout configuration values.

These tests guard against accidental regression of timeout values
that were increased to prevent cascading disconnects under load.
"""

from __future__ import annotations

import pytest


@pytest.mark.unit
class TestWebSocketTimeoutConfig:
    """Ensure timeout constants are set to their required minimum values."""

    def test_client_heartbeat_interval(self):
        from gateway.api.websocket import HEARTBEAT_INTERVAL

        assert HEARTBEAT_INTERVAL >= 45, f"HEARTBEAT_INTERVAL={HEARTBEAT_INTERVAL} too low; must be >= 45s"

    def test_client_heartbeat_timeout(self):
        from gateway.api.websocket import HEARTBEAT_TIMEOUT

        assert HEARTBEAT_TIMEOUT >= 15, f"HEARTBEAT_TIMEOUT={HEARTBEAT_TIMEOUT} too low; must be >= 15s"

    def test_client_max_missed_heartbeats(self):
        from gateway.api.websocket import MAX_MISSED_HEARTBEATS

        assert MAX_MISSED_HEARTBEATS >= 4, f"MAX_MISSED_HEARTBEATS={MAX_MISSED_HEARTBEATS} too low; must be >= 4"

    def test_max_silence_at_least_3_minutes(self):
        from gateway.api.websocket import HEARTBEAT_INTERVAL, MAX_MISSED_HEARTBEATS

        max_silence = HEARTBEAT_INTERVAL * MAX_MISSED_HEARTBEATS
        assert max_silence >= 180, f"Max silence ({max_silence}s) too low; must allow >= 180s before disconnect"
