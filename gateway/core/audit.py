"""Structured audit logging per PRD §Audit Logging.

Provides an append-only audit trail for security-relevant events with:
- Typed methods for each of the 11 PRD-specified event types
- In-memory ring buffer for admin queries via GET /admin/audit/recent
- Structured JSON output via dedicated structlog logger (gateway.audit)
"""

from __future__ import annotations

import enum
import threading
from collections import deque
from datetime import UTC, datetime
from typing import Any

import structlog

# Dedicated audit logger for filtering/routing
_audit_logger = structlog.get_logger("gateway.audit")


class AuditEvent(enum.Enum):
    """PRD-specified audit event types."""

    AUTH_SUCCESS = "auth_success"
    AUTH_FAILURE = "auth_failure"
    AUTH_TIMEOUT = "auth_timeout"
    KEY_CREATED = "key_created"
    KEY_ROTATED = "key_rotated"
    KEY_REVOKED = "key_revoked"
    ADMIN_ACTION = "admin_action"
    CONFIG_CHANGED = "config_changed"
    RATE_LIMITED = "rate_limited"
    IP_BLOCKED = "ip_blocked"
    PERMISSION_DENIED = "permission_denied"


AUDIT_EVENT_LEVELS: dict[AuditEvent, str] = {
    AuditEvent.AUTH_SUCCESS: "INFO",
    AuditEvent.AUTH_FAILURE: "WARN",
    AuditEvent.AUTH_TIMEOUT: "WARN",
    AuditEvent.KEY_CREATED: "INFO",
    AuditEvent.KEY_ROTATED: "INFO",
    AuditEvent.KEY_REVOKED: "WARN",
    AuditEvent.ADMIN_ACTION: "INFO",
    AuditEvent.CONFIG_CHANGED: "WARN",
    AuditEvent.RATE_LIMITED: "WARN",
    AuditEvent.IP_BLOCKED: "WARN",
    AuditEvent.PERMISSION_DENIED: "WARN",
}


class AuditLogger:
    """Structured audit logger with in-memory ring buffer.

    Emits PRD-formatted audit entries via structlog and stores them in a
    bounded deque for admin query access.
    """

    _instance: AuditLogger | None = None
    _lock = threading.Lock()

    def __init__(self, buffer_size: int = 10_000) -> None:
        self._buffer: deque[dict[str, Any]] = deque(maxlen=buffer_size)
        self._stats: dict[str, int] = {}
        self._buffer_lock = threading.Lock()

    @classmethod
    def get_instance(cls, buffer_size: int = 10_000) -> AuditLogger:
        """Get or create the singleton AuditLogger."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(buffer_size=buffer_size)
        return cls._instance

    # ── Core emit ────────────────────────────────────────────────────────

    def _emit(
        self,
        event: AuditEvent,
        *,
        action: str = "",
        result: str = "",
        client_id: str | None = None,
        ip: str | None = None,
        user_agent: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        correlation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        """Build and store a structured audit entry."""
        level = AUDIT_EVENT_LEVELS[event]

        actor: dict[str, Any] = {}
        if client_id is not None:
            actor["client_id"] = client_id
        if ip is not None:
            actor["ip"] = ip
        if user_agent is not None:
            actor["user_agent"] = user_agent

        resource: dict[str, Any] = {}
        if resource_type is not None:
            resource["type"] = resource_type
        if resource_id is not None:
            resource["id"] = resource_id

        entry: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "event": event.value,
            "level": level,
            "actor": actor,
            "action": action,
            "result": result,
        }

        if resource:
            entry["resource"] = resource
        if metadata:
            entry["metadata"] = metadata
        if correlation_id:
            entry["correlation_id"] = correlation_id

        # Store in ring buffer
        with self._buffer_lock:
            self._buffer.append(entry)
            self._stats[event.value] = self._stats.get(event.value, 0) + 1

        # Emit via structlog
        log_method = _audit_logger.info if level == "INFO" else _audit_logger.warning
        log_method(
            event.value,
            actor=actor,
            action=action,
            result=result,
            resource=resource or None,
            metadata=metadata,
            correlation_id=correlation_id,
        )

        return entry

    # ── Typed event methods (PRD §Audit Logging) ─────────────────────────

    def auth_success(
        self,
        *,
        client_id: str,
        ip: str,
        user_agent: str | None = None,
        metadata: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Log successful authentication."""
        return self._emit(
            AuditEvent.AUTH_SUCCESS,
            action="authenticate",
            result="success",
            client_id=client_id,
            ip=ip,
            user_agent=user_agent,
            metadata=metadata,
            correlation_id=correlation_id,
        )

    def auth_failure(
        self,
        *,
        ip: str,
        user_agent: str | None = None,
        client_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Log failed authentication."""
        return self._emit(
            AuditEvent.AUTH_FAILURE,
            action="authenticate",
            result="failure",
            client_id=client_id,
            ip=ip,
            user_agent=user_agent,
            metadata=metadata,
            correlation_id=correlation_id,
        )

    def auth_timeout(
        self,
        *,
        ip: str,
        connection_id: str | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Log authentication timeout."""
        return self._emit(
            AuditEvent.AUTH_TIMEOUT,
            action="authenticate",
            result="timeout",
            ip=ip,
            metadata={"connection_id": connection_id} if connection_id else None,
            correlation_id=correlation_id,
        )

    def key_created(
        self,
        *,
        client_id: str,
        ip: str,
        resource_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Log API key creation."""
        return self._emit(
            AuditEvent.KEY_CREATED,
            action="create_key",
            result="success",
            client_id=client_id,
            ip=ip,
            resource_type="api_key",
            resource_id=resource_id,
            metadata=metadata,
        )

    def key_rotated(
        self,
        *,
        client_id: str,
        ip: str,
        resource_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Log API key rotation."""
        return self._emit(
            AuditEvent.KEY_ROTATED,
            action="rotate_key",
            result="success",
            client_id=client_id,
            ip=ip,
            resource_type="api_key",
            resource_id=resource_id,
            metadata=metadata,
        )

    def key_revoked(
        self,
        *,
        client_id: str,
        ip: str,
        resource_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Log API key revocation."""
        return self._emit(
            AuditEvent.KEY_REVOKED,
            action="revoke_key",
            result="success",
            client_id=client_id,
            ip=ip,
            resource_type="api_key",
            resource_id=resource_id,
            metadata=metadata,
        )

    def admin_action(
        self,
        *,
        client_id: str,
        ip: str,
        action: str,
        resource_type: str = "admin",
        resource_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Log admin endpoint access."""
        return self._emit(
            AuditEvent.ADMIN_ACTION,
            action=action,
            result="success",
            client_id=client_id,
            ip=ip,
            resource_type=resource_type,
            resource_id=resource_id,
            metadata=metadata,
        )

    def config_changed(
        self,
        *,
        client_id: str,
        ip: str,
        metadata: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Log configuration change."""
        return self._emit(
            AuditEvent.CONFIG_CHANGED,
            action="config_change",
            result="success",
            client_id=client_id,
            ip=ip,
            metadata=metadata,
            correlation_id=correlation_id,
        )

    def rate_limited(
        self,
        *,
        client_id: str | None = None,
        ip: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Log rate limit enforcement."""
        return self._emit(
            AuditEvent.RATE_LIMITED,
            action="rate_limit",
            result="blocked",
            client_id=client_id,
            ip=ip,
            metadata=metadata,
        )

    def ip_blocked(
        self,
        *,
        ip: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Log IP block."""
        return self._emit(
            AuditEvent.IP_BLOCKED,
            action="block_ip",
            result="blocked",
            ip=ip,
            metadata=metadata,
        )

    def permission_denied(
        self,
        *,
        client_id: str,
        ip: str,
        resource_type: str | None = None,
        resource_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Log permission denied."""
        return self._emit(
            AuditEvent.PERMISSION_DENIED,
            action="access",
            result="denied",
            client_id=client_id,
            ip=ip,
            resource_type=resource_type,
            resource_id=resource_id,
            metadata=metadata,
        )

    # ── Query methods ────────────────────────────────────────────────────

    def get_recent(
        self,
        limit: int = 100,
        level: str | None = None,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Query recent audit entries from the ring buffer.

        Args:
            limit: Max entries to return (most recent first).
            level: Filter by log level (INFO, WARN).
            event_type: Filter by event type name.

        Returns:
            List of audit entries, most recent first.
        """
        with self._buffer_lock:
            entries = list(self._buffer)

        # Apply filters
        if level:
            entries = [e for e in entries if e["level"] == level]
        if event_type:
            entries = [e for e in entries if e["event"] == event_type]

        # Most recent first, limited
        entries.reverse()
        return entries[:limit]

    def get_stats(self) -> dict[str, Any]:
        """Get audit event statistics."""
        with self._buffer_lock:
            total = sum(self._stats.values())
            by_event = dict(self._stats)
        return {"total": total, "by_event": by_event}

    def clear(self) -> None:
        """Clear the audit buffer and stats. For testing only."""
        with self._buffer_lock:
            self._buffer.clear()
            self._stats.clear()


def get_audit_logger() -> AuditLogger:
    """Dependency-injection accessor for the singleton AuditLogger."""
    return AuditLogger.get_instance()
