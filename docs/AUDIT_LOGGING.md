# Audit Logging

The Data Gateway includes a structured audit logging system designed to track security-critical and operational events. This system provides visibility into authentication attempts, administrative actions, and configuration changes.

## Overview

Audit logs are distinct from application logs. While application logs capture debug and error information for developers, audit logs capture **who** did **what** and **when** for security and compliance purposes.

The `AuditLogger` is a singleton service that:

1. Writes structured JSON events to a dedicated `audit` logger (output to stdout/file depending on config).
2. Maintains a fixed-size in-memory ring buffer of recent events for admin inspection.

## Event Types

| Event Type | Description | Criticality |
| :--- | :--- | :--- |
| `auth_success` | Successful API key authentication | Low |
| `auth_failure` | Invalid API key or missing credentials | High |
| `auth_timeout` | WebSocket authentication timeout | Medium |
| `key_created` | New API key provisioning | High |
| `key_rotated` | API key rotation | High |
| `key_revoked` | API key revocation | High |
| `admin_action` | Privileged administrative operation | High |
| `config_changed` | Dynamic configuration update | Medium |
| `rate_limited` | Rate limit exceeded (429) | Low |
| `ip_blocked` | IP address added to blocklist | High |
| `permission_denied` | Valid key but insufficient permissions | High |

## Usage

### dependency Injection

Access the logger via the dependency injection system:

```python
from gateway.api.deps import get_audit_logger
from gateway.core.audit import AuditLogger

@router.post("/critical-op")
async def critical_op(
    audit: AuditLogger = Depends(get_audit_logger)
):
    audit.log_admin_action(
        actor="user_123",
        action="critical_op",
        target="resource_abc",
        details={"status": "success"}
    )
```

### Inspecting Logs

Recent audit events can be inspected via the internal admin interface (future implementation) or by consuming the structured logs from the container output.

**Log Format:**

```json
{
  "timestamp": "2026-02-10T08:00:00.000000Z",
  "level": "info",
  "logger": "audit",
  "event": "admin_action",
  "actor": "user_123",
  "action": "provider_reload",
  "target": "alpaca",
  "details": {
    "ip_address": "192.168.1.50"
  }
}
```

## Security Considerations

- **No PII**: Audit logs should not contain PII other than necessary actor identifiers (API key IDs, usernames).
- **No Secrets**: Never log raw API keys, passwords, or secrets. The system automatically masks sensitive fields if they accidentally pass through, but developers must be vigilant.
- **Immutability**: The in-memory ring buffer is append-only and overwrites oldest entries when full.
