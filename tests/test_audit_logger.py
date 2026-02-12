"""Tests for structured audit logging (PRD §Audit Logging)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from gateway.core.audit import AuditEvent, AuditLogger

# ─────────────────────────────────────────────────────────────────────────────
# Unit tests — AuditLogger core
# ─────────────────────────────────────────────────────────────────────────────


class TestAuditLoggerBasics:
    """Core AuditLogger functionality."""

    def test_singleton_returns_same_instance(self):
        AuditLogger._instance = None
        a = AuditLogger.get_instance()
        b = AuditLogger.get_instance()
        assert a is b
        AuditLogger._instance = None

    def test_fresh_logger_has_empty_buffer(self):
        logger = AuditLogger(buffer_size=100)
        assert logger.get_recent() == []

    def test_get_stats_empty(self):
        logger = AuditLogger(buffer_size=100)
        stats = logger.get_stats()
        assert stats["total"] == 0
        assert stats["by_event"] == {}

    def test_clear_empties_buffer(self):
        logger = AuditLogger(buffer_size=100)
        logger.auth_success(client_id="test", ip="127.0.0.1")
        assert len(logger.get_recent()) == 1
        logger.clear()
        assert len(logger.get_recent()) == 0


class TestAuditEventMethods:
    """Each PRD-specified event type emits correctly."""

    def setup_method(self):
        self.logger = AuditLogger(buffer_size=1000)

    def test_auth_success(self):
        self.logger.auth_success(
            client_id="cerberus",
            ip="10.0.1.50",
            user_agent="Python/3.11",
            metadata={"auth_method": "api_key", "connection_type": "rest"},
        )
        entries = self.logger.get_recent()
        assert len(entries) == 1
        entry = entries[0]
        assert entry["event"] == "auth_success"
        assert entry["level"] == "INFO"
        assert entry["actor"]["client_id"] == "cerberus"
        assert entry["actor"]["ip"] == "10.0.1.50"
        assert entry["result"] == "success"
        assert entry["action"] == "authenticate"
        assert "timestamp" in entry

    def test_auth_failure(self):
        self.logger.auth_failure(
            ip="203.0.113.50",
            user_agent="curl/7.88",
            metadata={"reason": "invalid_key", "key_prefix": "gw_unknown_"},
        )
        entries = self.logger.get_recent()
        assert len(entries) == 1
        entry = entries[0]
        assert entry["event"] == "auth_failure"
        assert entry["level"] == "WARN"
        assert entry["result"] == "failure"
        assert entry["actor"]["ip"] == "203.0.113.50"
        # No client_id for failed auth
        assert entry["actor"].get("client_id") is None

    def test_auth_timeout(self):
        self.logger.auth_timeout(ip="10.0.0.1", connection_id="conn-123")
        entries = self.logger.get_recent()
        assert len(entries) == 1
        assert entries[0]["event"] == "auth_timeout"
        assert entries[0]["level"] == "WARN"

    def test_key_created(self):
        self.logger.key_created(
            client_id="admin",
            ip="10.0.0.1",
            resource_id="key_new_abc",
        )
        entry = self.logger.get_recent()[0]
        assert entry["event"] == "key_created"
        assert entry["level"] == "INFO"
        assert entry["resource"]["type"] == "api_key"
        assert entry["resource"]["id"] == "key_new_abc"

    def test_key_rotated(self):
        self.logger.key_rotated(client_id="admin", ip="10.0.0.1", resource_id="key_abc")
        entry = self.logger.get_recent()[0]
        assert entry["event"] == "key_rotated"
        assert entry["level"] == "INFO"

    def test_key_revoked(self):
        self.logger.key_revoked(client_id="admin", ip="10.0.0.1", resource_id="key_abc")
        entry = self.logger.get_recent()[0]
        assert entry["event"] == "key_revoked"
        assert entry["level"] == "WARN"

    def test_admin_action(self):
        self.logger.admin_action(
            client_id="admin",
            ip="10.0.0.1",
            action="get_status",
            resource_type="admin",
            resource_id="/api/v1/admin/status",
        )
        entry = self.logger.get_recent()[0]
        assert entry["event"] == "admin_action"
        assert entry["level"] == "INFO"
        assert entry["action"] == "get_status"
        assert entry["resource"]["type"] == "admin"

    def test_config_changed(self):
        self.logger.config_changed(
            client_id="super_admin",
            ip="10.0.0.1",
            metadata={"key": "rate_limit", "old": 100, "new": 200},
        )
        entry = self.logger.get_recent()[0]
        assert entry["event"] == "config_changed"
        assert entry["level"] == "WARN"

    def test_rate_limited(self):
        self.logger.rate_limited(
            client_id="cerberus",
            ip="10.0.0.1",
            metadata={"limit_type": "per_client", "limit": 600},
        )
        entry = self.logger.get_recent()[0]
        assert entry["event"] == "rate_limited"
        assert entry["level"] == "WARN"
        assert entry["metadata"]["limit_type"] == "per_client"

    def test_ip_blocked(self):
        self.logger.ip_blocked(
            ip="203.0.113.50",
            metadata={"reason": "rate_limit_exceeded", "duration_seconds": 3600},
        )
        entry = self.logger.get_recent()[0]
        assert entry["event"] == "ip_blocked"
        assert entry["level"] == "WARN"

    def test_permission_denied(self):
        self.logger.permission_denied(
            client_id="cerberus",
            ip="10.0.0.1",
            resource_type="provider",
            resource_id="alphavantage",
            metadata={"required_role": "admin"},
        )
        entry = self.logger.get_recent()[0]
        assert entry["event"] == "permission_denied"
        assert entry["level"] == "WARN"
        assert entry["resource"]["type"] == "provider"


class TestAuditLoggerRingBuffer:
    """Ring buffer behavior — bounded memory, oldest evicted first."""

    def test_buffer_evicts_oldest(self):
        logger = AuditLogger(buffer_size=3)
        logger.auth_success(client_id="c1", ip="1.1.1.1")
        logger.auth_success(client_id="c2", ip="2.2.2.2")
        logger.auth_success(client_id="c3", ip="3.3.3.3")
        logger.auth_success(client_id="c4", ip="4.4.4.4")
        entries = logger.get_recent()
        assert len(entries) == 3
        # Oldest (c1) should be evicted; most recent first
        assert entries[0]["actor"]["client_id"] == "c4"
        assert entries[2]["actor"]["client_id"] == "c2"

    def test_get_recent_with_limit(self):
        logger = AuditLogger(buffer_size=100)
        for i in range(10):
            logger.auth_success(client_id=f"c{i}", ip="1.1.1.1")
        entries = logger.get_recent(limit=3)
        assert len(entries) == 3
        # Most recent first
        assert entries[0]["actor"]["client_id"] == "c9"


class TestAuditLoggerFiltering:
    """Filtering by level and event type."""

    def setup_method(self):
        self.logger = AuditLogger(buffer_size=1000)
        self.logger.auth_success(client_id="c1", ip="1.1.1.1")
        self.logger.auth_failure(ip="2.2.2.2")
        self.logger.rate_limited(client_id="c1", ip="1.1.1.1")
        self.logger.admin_action(client_id="admin", ip="10.0.0.1", action="status")

    def test_filter_by_level(self):
        warn_entries = self.logger.get_recent(level="WARN")
        assert len(warn_entries) == 2
        assert all(e["level"] == "WARN" for e in warn_entries)

    def test_filter_by_event_type(self):
        auth_entries = self.logger.get_recent(event_type="auth_success")
        assert len(auth_entries) == 1
        assert auth_entries[0]["event"] == "auth_success"

    def test_filter_by_level_and_event(self):
        entries = self.logger.get_recent(level="WARN", event_type="rate_limited")
        assert len(entries) == 1
        assert entries[0]["event"] == "rate_limited"


class TestAuditLoggerStats:
    """Stats reporting."""

    def test_stats_counts_by_event(self):
        logger = AuditLogger(buffer_size=1000)
        logger.auth_success(client_id="c1", ip="1.1.1.1")
        logger.auth_success(client_id="c2", ip="2.2.2.2")
        logger.auth_failure(ip="3.3.3.3")
        stats = logger.get_stats()
        assert stats["total"] == 3
        assert stats["by_event"]["auth_success"] == 2
        assert stats["by_event"]["auth_failure"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# Integration tests — auth module emits audit events
# ─────────────────────────────────────────────────────────────────────────────


class TestAuthAuditIntegration:
    """ClientAuthenticator emits audit events on auth success/failure."""

    def test_authenticate_success_emits_audit(self, tmp_path):
        config = tmp_path / "clients.yaml"
        config.write_text(
            "clients:\n"
            "  - id: tester\n"
            "    key: gw_test_key123\n"  # pragma: allowlist secret
            "    role: client\n"
            "    permissions:\n"
            "      providers: [alpaca]\n"
        )
        from gateway.core.auth import ClientAuthenticator

        authenticator = ClientAuthenticator(config)
        audit = AuditLogger(buffer_size=100)

        with patch("gateway.core.auth.get_audit_logger", return_value=audit):
            client = authenticator.authenticate(
                "gw_test_key123",  # pragma: allowlist secret
                ip="10.0.0.1",
                user_agent="test-agent",
            )

        assert client is not None
        assert client.id == "tester"
        entries = audit.get_recent()
        assert len(entries) == 1
        assert entries[0]["event"] == "auth_success"
        assert entries[0]["actor"]["client_id"] == "tester"
        assert entries[0]["actor"]["ip"] == "10.0.0.1"

    def test_authenticate_failure_emits_audit(self, tmp_path):
        config = tmp_path / "clients.yaml"
        config.write_text(
            "clients:\n"
            "  - id: tester\n"
            "    key: gw_test_key123\n"  # pragma: allowlist secret
            "    role: client\n"
        )
        from gateway.core.auth import ClientAuthenticator

        authenticator = ClientAuthenticator(config)
        audit = AuditLogger(buffer_size=100)

        with patch("gateway.core.auth.get_audit_logger", return_value=audit):
            client = authenticator.authenticate(
                "gw_wrong_key",  # pragma: allowlist secret
                ip="203.0.113.50",
                user_agent="curl/7.88",
            )

        assert client is None
        entries = audit.get_recent()
        assert len(entries) == 1
        assert entries[0]["event"] == "auth_failure"
        assert entries[0]["actor"]["ip"] == "203.0.113.50"


# ─────────────────────────────────────────────────────────────────────────────
# Integration tests — admin audit endpoint
# ─────────────────────────────────────────────────────────────────────────────


class TestAdminAuditEndpoint:
    """Admin endpoint for querying recent audit logs."""

    @pytest.fixture
    def audit_logger(self):
        logger = AuditLogger(buffer_size=100)
        logger.auth_success(client_id="c1", ip="10.0.0.1")
        logger.auth_failure(ip="203.0.113.50")
        logger.rate_limited(client_id="c1", ip="10.0.0.1")
        return logger

    def test_recent_audit_returns_entries(self, audit_logger):
        entries = audit_logger.get_recent()
        assert len(entries) == 3

    def test_recent_audit_filter_by_level(self, audit_logger):
        entries = audit_logger.get_recent(level="WARN")
        assert len(entries) == 2

    def test_recent_audit_filter_by_event(self, audit_logger):
        entries = audit_logger.get_recent(event_type="auth_success")
        assert len(entries) == 1

    def test_recent_audit_limit(self, audit_logger):
        entries = audit_logger.get_recent(limit=1)
        assert len(entries) == 1
        # Most recent first
        assert entries[0]["event"] == "rate_limited"


class TestAuditEventEnum:
    """AuditEvent enum covers all PRD-specified events."""

    def test_all_events_defined(self):
        expected = {
            "auth_success",
            "auth_failure",
            "auth_timeout",
            "key_created",
            "key_rotated",
            "key_revoked",
            "admin_action",
            "config_changed",
            "rate_limited",
            "ip_blocked",
            "permission_denied",
        }
        actual = {e.value for e in AuditEvent}
        assert expected == actual

    def test_event_levels(self):
        from gateway.core.audit import AUDIT_EVENT_LEVELS

        assert AUDIT_EVENT_LEVELS[AuditEvent.AUTH_SUCCESS] == "INFO"
        assert AUDIT_EVENT_LEVELS[AuditEvent.AUTH_FAILURE] == "WARN"
        assert AUDIT_EVENT_LEVELS[AuditEvent.RATE_LIMITED] == "WARN"
        assert AUDIT_EVENT_LEVELS[AuditEvent.ADMIN_ACTION] == "INFO"
