"""Security Module - Input Validation, API Key Management, RBAC.

Implements security architecture as specified in PRD:
- Input validation with GW-E8xxx error codes
- API key structure and lifecycle management
- Role-based access control (RBAC)
- Session token support
"""

import hashlib
import os
import re
import secrets
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger()


# ─────────────────────────────────────────────────────────────────────────────
# Validation Error Codes (GW-E8xxx)
# ─────────────────────────────────────────────────────────────────────────────


class ValidationErrorCode(str, Enum):
    """Input validation error codes per PRD."""

    INVALID_SYMBOL_FORMAT = "GW-E8001"
    PARAMETER_EXCEEDS_LIMIT = "GW-E8002"
    INVALID_DATETIME_FORMAT = "GW-E8003"
    INVALID_ENUM_VALUE = "GW-E8004"
    REQUEST_BODY_TOO_LARGE = "GW-E8005"
    FORBIDDEN_CHARACTERS = "GW-E8006"
    REQUIRED_PARAMETER_MISSING = "GW-E8007"


# ─────────────────────────────────────────────────────────────────────────────
# Symbol Patterns
# ─────────────────────────────────────────────────────────────────────────────

SYMBOL_PATTERNS = {
    "stock": re.compile(r"^[A-Z]{1,5}$"),
    "option_occ": re.compile(r"^[A-Z]{1,6}\d{6}[CP]\d{8}$"),
    "crypto": re.compile(r"^[A-Z]{2,5}/[A-Z]{3,4}$"),
    "forex": re.compile(r"^[A-Z]{3}/[A-Z]{3}$"),
}

# Forbidden characters (SQL injection, XSS, etc.)
FORBIDDEN_PATTERN = re.compile(r"[<>\"';]|--|\*/|/\*|\x00|script", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# Parameter Limits
# ─────────────────────────────────────────────────────────────────────────────

PARAM_LIMITS = {
    "symbols_array_max": 500,
    "limit_max": 10000,
    "limit_default": 1000,
    "date_range_max_days": 5 * 365,  # 5 years
    "replay_speed_max": 100.0,
    "request_body_max_bytes": 1 * 1024 * 1024,  # 1 MB
    "url_max_bytes": 8 * 1024,  # 8 KB
    "websocket_message_max_bytes": 64 * 1024,  # 64 KB
}

VALID_TIMEFRAMES = {
    "1Min",
    "1Minute",
    "5Min",
    "5Minute",
    "15Min",
    "15Minute",
    "30Min",
    "30Minute",
    "1Hour",
    "1H",
    "1Day",
    "1D",
    "1Week",
    "1W",
}


# ─────────────────────────────────────────────────────────────────────────────
# Validation Result
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class InputValidationError:
    """Validation error with PRD-specified structure."""

    code: str
    message: str
    field: str | None = None
    value: Any | None = None
    expected_pattern: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "code": self.code,
            "message": self.message,
        }
        details: dict[str, Any] = {}
        if self.field:
            details["field"] = self.field
        if self.value is not None:
            details["value"] = str(self.value)[:100]  # Truncate for safety
        if self.expected_pattern:
            details["expected_pattern"] = self.expected_pattern
        if details:
            result["details"] = details
        return result


# ─────────────────────────────────────────────────────────────────────────────
# Input Validator
# ─────────────────────────────────────────────────────────────────────────────


class InputValidator:
    """Validates and sanitizes user input per PRD security requirements."""

    def validate_symbol(
        self,
        symbol: str,
        symbol_type: str = "stock",
    ) -> InputValidationError | None:
        """Validate a symbol against known patterns.

        Args:
            symbol: The symbol to validate
            symbol_type: One of "stock", "option_occ", "crypto", "forex"

        Returns:
            ValidationError if invalid, None if valid
        """
        if not symbol:
            return InputValidationError(
                code=ValidationErrorCode.REQUIRED_PARAMETER_MISSING.value,
                message="Symbol is required",
                field="symbol",
            )

        # Check for forbidden characters first
        if FORBIDDEN_PATTERN.search(symbol):
            return InputValidationError(
                code=ValidationErrorCode.FORBIDDEN_CHARACTERS.value,
                message="Forbidden characters detected in symbol",
                field="symbol",
                value=symbol,
            )

        pattern = SYMBOL_PATTERNS.get(symbol_type)
        if not pattern:
            symbol_type = "stock"
            pattern = SYMBOL_PATTERNS["stock"]

        if not pattern.match(symbol.upper()):
            return InputValidationError(
                code=ValidationErrorCode.INVALID_SYMBOL_FORMAT.value,
                message=f"Invalid {symbol_type} symbol format",
                field="symbol",
                value=symbol,
                expected_pattern=pattern.pattern,
            )

        return None

    def validate_symbols_array(
        self,
        symbols: list[str],
        max_symbols: int | None = None,
    ) -> InputValidationError | None:
        """Validate an array of symbols."""
        max_allowed = max_symbols or PARAM_LIMITS["symbols_array_max"]

        if not symbols:
            return InputValidationError(
                code=ValidationErrorCode.REQUIRED_PARAMETER_MISSING.value,
                message="At least one symbol is required",
                field="symbols",
            )

        if len(symbols) > max_allowed:
            return InputValidationError(
                code=ValidationErrorCode.PARAMETER_EXCEEDS_LIMIT.value,
                message=f"Maximum {max_allowed} symbols allowed",
                field="symbols",
                value=len(symbols),
            )

        seen_symbols: set[str] = set()
        for symbol in symbols:
            normalized = symbol.upper()
            if normalized in seen_symbols:
                continue
            seen_symbols.add(normalized)
            error = self.validate_symbol(normalized)
            if error is not None:
                return error

        return None

    @staticmethod
    def _matches_any_symbol_pattern(symbol: str) -> bool:
        """Return True if symbol matches any known format (stock, crypto, option, forex, wildcard)."""
        if symbol == "*":
            return True
        return any(pattern.match(symbol) for pattern in SYMBOL_PATTERNS.values())

    def validate_timeframe(self, timeframe: str) -> InputValidationError | None:
        """Validate timeframe value."""
        if not timeframe:
            return InputValidationError(
                code=ValidationErrorCode.REQUIRED_PARAMETER_MISSING.value,
                message="Timeframe is required",
                field="timeframe",
            )

        if timeframe not in VALID_TIMEFRAMES:
            return InputValidationError(
                code=ValidationErrorCode.INVALID_ENUM_VALUE.value,
                message="Invalid timeframe value",
                field="timeframe",
                value=timeframe,
            )

        return None

    def validate_date_range(
        self,
        start: datetime,
        end: datetime,
    ) -> InputValidationError | None:
        """Validate date range is within limits."""
        if start >= end:
            return InputValidationError(
                code=ValidationErrorCode.INVALID_DATETIME_FORMAT.value,
                message="Start date must be before end date",
                field="start",
            )

        days_diff = (end - start).days
        max_days = PARAM_LIMITS["date_range_max_days"]

        if days_diff > max_days:
            return InputValidationError(
                code=ValidationErrorCode.PARAMETER_EXCEEDS_LIMIT.value,
                message=f"Date range cannot exceed {max_days // 365} years",
                field="date_range",
                value=f"{days_diff} days",
            )

        return None

    def validate_limit(
        self,
        limit: int,
        max_limit: int | None = None,
    ) -> tuple[int, InputValidationError | None]:
        """Validate and cap limit parameter.

        Returns:
            Tuple of (capped_limit, error_or_none)
        """
        max_allowed = max_limit or PARAM_LIMITS["limit_max"]

        if limit < 1:
            return PARAM_LIMITS["limit_default"], None

        if limit > max_allowed:
            # Cap at max instead of rejecting
            return max_allowed, None

        return limit, None

    def validate_speed(self, speed: float) -> InputValidationError | None:
        """Validate replay speed."""
        if speed <= 0:
            return InputValidationError(
                code=ValidationErrorCode.PARAMETER_EXCEEDS_LIMIT.value,
                message="Speed must be positive",
                field="speed",
                value=speed,
            )

        if speed > PARAM_LIMITS["replay_speed_max"]:
            return InputValidationError(
                code=ValidationErrorCode.PARAMETER_EXCEEDS_LIMIT.value,
                message=f"Speed cannot exceed {PARAM_LIMITS['replay_speed_max']}x",
                field="speed",
                value=speed,
            )

        return None

    def check_forbidden_chars(
        self,
        value: str,
        field_name: str = "input",
    ) -> InputValidationError | None:
        """Check for forbidden characters in any input."""
        if FORBIDDEN_PATTERN.search(value):
            return InputValidationError(
                code=ValidationErrorCode.FORBIDDEN_CHARACTERS.value,
                message="Forbidden characters detected",
                field=field_name,
            )
        return None

    def validate_request_size(
        self,
        content_length: int,
        endpoint_type: str = "rest",
    ) -> InputValidationError | None:
        """Validate request body size."""
        if endpoint_type == "websocket":
            max_size = PARAM_LIMITS["websocket_message_max_bytes"]
        else:
            max_size = PARAM_LIMITS["request_body_max_bytes"]

        if content_length > max_size:
            return InputValidationError(
                code=ValidationErrorCode.REQUEST_BODY_TOO_LARGE.value,
                message=f"Request body exceeds {max_size // 1024}KB limit",
                field="content-length",
                value=content_length,
            )

        return None


# ─────────────────────────────────────────────────────────────────────────────
# RBAC - Roles and Permissions
# ─────────────────────────────────────────────────────────────────────────────


class Role(str, Enum):
    """User roles per PRD RBAC spec."""

    CLIENT = "client"
    ADMIN = "admin"
    SUPER_ADMIN = "super_admin"


@dataclass
class Permission:
    """Permission definition."""

    providers: list[str] = field(default_factory=lambda: ["alpaca", "uw"])
    feeds: list[str] = field(default_factory=lambda: ["bars", "quotes", "trades"])
    symbols: list[str] = field(default_factory=lambda: ["*"])
    max_symbols: int = 1000
    rate_limit_requests_per_minute: int = 600
    allowed_ips: list[str] = field(default_factory=list)
    features: dict[str, bool] = field(
        default_factory=lambda: {
            "replay": True,
            "bulk_download": True,
            "admin": False,
        }
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "providers": self.providers,
            "feeds": self.feeds,
            "symbols": self.symbols,
            "max_symbols": self.max_symbols,
            "rate_limit_requests_per_minute": self.rate_limit_requests_per_minute,
            "allowed_ips": self.allowed_ips,
            "features": self.features,
        }


# Permission matrix per PRD
PERMISSION_MATRIX: dict[str, dict[Role, bool]] = {
    "/api/v1/alpaca/": {Role.CLIENT: True, Role.ADMIN: True, Role.SUPER_ADMIN: True},
    "/api/v1/uw/": {Role.CLIENT: True, Role.ADMIN: True, Role.SUPER_ADMIN: True},
    "/api/v1/bulk/": {Role.CLIENT: True, Role.ADMIN: True, Role.SUPER_ADMIN: True},
    "/api/v1/replay/": {Role.CLIENT: True, Role.ADMIN: True, Role.SUPER_ADMIN: True},
    "/api/v1/calendar/": {Role.CLIENT: True, Role.ADMIN: True, Role.SUPER_ADMIN: True},
    "/api/v1/admin/keys": {Role.CLIENT: False, Role.ADMIN: True, Role.SUPER_ADMIN: True},
    "/api/v1/admin/cache/": {Role.CLIENT: False, Role.ADMIN: True, Role.SUPER_ADMIN: True},
    "/api/v1/admin/circuits/": {Role.CLIENT: False, Role.ADMIN: True, Role.SUPER_ADMIN: True},
    "/api/v1/admin/logs/": {Role.CLIENT: False, Role.ADMIN: True, Role.SUPER_ADMIN: True},
    "/api/v1/admin/config/": {Role.CLIENT: False, Role.ADMIN: False, Role.SUPER_ADMIN: True},
    "/api/v1/admin/shutdown": {Role.CLIENT: False, Role.ADMIN: False, Role.SUPER_ADMIN: True},
}


def check_permission(path: str, role: Role) -> bool:
    """Check if a role has permission for a path."""
    for pattern, perms in PERMISSION_MATRIX.items():
        if path.startswith(pattern):
            return perms.get(role, False)
    # Default allow for unlisted paths
    return True


# ─────────────────────────────────────────────────────────────────────────────
# API Key Management
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class APIKey:
    """API key with PRD-specified structure."""

    key_id: str
    client_id: str
    key_hash: str  # SHA-256 hash, never store raw key
    permissions: Permission
    role: Role = Role.CLIENT
    expires_at: datetime | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_used_at: datetime | None = None
    revoked: bool = False

    def to_dict(self, include_hash: bool = False) -> dict[str, Any]:
        """Convert to dict (never expose raw key)."""
        result = {
            "key_id": self.key_id,
            "client_id": self.client_id,
            "role": self.role.value,
            "permissions": self.permissions.to_dict(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "created_at": self.created_at.isoformat(),
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
            "revoked": self.revoked,
        }
        if include_hash:
            result["key_hash"] = self.key_hash
        return result

    def is_expired(self) -> bool:
        """Check if key is expired."""
        if self.expires_at is None:
            return False
        return datetime.now(UTC) > self.expires_at

    def is_valid(self) -> bool:
        """Check if key is valid for use."""
        return not self.revoked and not self.is_expired()


class APIKeyManager:
    """Manages API key lifecycle per PRD spec."""

    KEY_PREFIX = "gw_"
    KEY_ENTROPY_BYTES = 32  # 256 bits

    def __init__(self) -> None:
        self._keys: dict[str, APIKey] = {}
        self._key_by_hash: dict[str, str] = {}  # hash -> key_id

    def generate_key(
        self,
        client_id: str,
        permissions: Permission | None = None,
        role: Role = Role.CLIENT,
        expires_in_days: int = 365,
    ) -> tuple[str, APIKey]:
        """Generate a new API key.

        Returns:
            Tuple of (raw_key, APIKey object)
            Note: raw_key is only returned once at creation!
        """
        # Generate unique key ID
        key_id = f"key_{secrets.token_hex(6)}"

        # Generate random bytes (256-bit entropy)
        random_part = secrets.token_urlsafe(self.KEY_ENTROPY_BYTES)

        # Build key: gw_<client_id>_<random>
        raw_key = f"{self.KEY_PREFIX}{client_id}_{random_part}"

        # Hash for storage
        key_hash = self._hash_key(raw_key)

        # Create key object
        api_key = APIKey(
            key_id=key_id,
            client_id=client_id,
            key_hash=key_hash,
            permissions=permissions or Permission(),
            role=role,
            expires_at=datetime.now(UTC) + timedelta(days=expires_in_days),
        )

        # Store
        self._keys[key_id] = api_key
        self._key_by_hash[key_hash] = key_id

        logger.info(
            "api_key_created",
            key_id=key_id,
            client_id=client_id,
            role=role.value,
            expires_in_days=expires_in_days,
        )

        return raw_key, api_key

    def validate_key(self, raw_key: str) -> APIKey | None:
        """Validate an API key and return the associated key object.

        Returns:
            APIKey if valid, None if invalid
        """
        if not raw_key or not raw_key.startswith(self.KEY_PREFIX):
            return None

        key_hash = self._hash_key(raw_key)
        key_id = self._key_by_hash.get(key_hash)

        if not key_id:
            return None

        api_key = self._keys.get(key_id)
        if not api_key or not api_key.is_valid():
            return None

        # Update last used
        api_key.last_used_at = datetime.now(UTC)

        return api_key

    def rotate_key(
        self,
        key_id: str,
        expires_in_days: int = 365,
    ) -> tuple[str, APIKey] | None:
        """Rotate an existing key (invalidate old, create new).

        Returns:
            Tuple of (new_raw_key, new_APIKey) or None if key not found
        """
        old_key = self._keys.get(key_id)
        if not old_key:
            return None

        # Revoke old key
        old_key.revoked = True
        del self._key_by_hash[old_key.key_hash]

        # Generate new key with same client/permissions
        new_raw, new_key = self.generate_key(
            client_id=old_key.client_id,
            permissions=old_key.permissions,
            role=old_key.role,
            expires_in_days=expires_in_days,
        )

        logger.info("api_key_rotated", old_key_id=key_id, new_key_id=new_key.key_id)

        return new_raw, new_key

    def revoke_key(self, key_id: str) -> bool:
        """Revoke an API key immediately."""
        api_key = self._keys.get(key_id)
        if not api_key:
            return False

        api_key.revoked = True
        if api_key.key_hash in self._key_by_hash:
            del self._key_by_hash[api_key.key_hash]

        logger.info("api_key_revoked", key_id=key_id)
        return True

    def list_keys(self, client_id: str | None = None) -> list[APIKey]:
        """List all keys (optionally filtered by client)."""
        keys = self._keys.values()
        if client_id:
            keys = [k for k in keys if k.client_id == client_id]
        return list(keys)

    def get_key(self, key_id: str) -> APIKey | None:
        """Get a key by ID."""
        return self._keys.get(key_id)

    def _hash_key(self, raw_key: str) -> str:
        """Hash an API key using SHA-256."""
        return hashlib.sha256(raw_key.encode()).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# Secrets Manager (Multi-source)
# ─────────────────────────────────────────────────────────────────────────────


class SecretsManager:
    """Load secrets from multiple sources per PRD priority order.

    Priority:
    1. HashiCorp Vault
    2. AWS/GCP Secrets Manager
    3. SOPS-encrypted files
    4. Docker Secrets
    5. Environment variables
    """

    def __init__(self) -> None:
        self._vault_client = None
        self._cache: dict[str, str] = {}

    def get_secret(self, name: str) -> str | None:
        """Get a secret from the highest-priority available source."""
        # Check cache first
        if name in self._cache:
            return self._cache[name]

        # Try sources in priority order
        value = self._from_vault(name) or self._from_docker_secrets(name) or self._from_env(name)

        if value:
            self._cache[name] = value

        return value

    def get_alpaca_credentials(self, key_num: int = 1) -> tuple[str | None, str | None]:
        """Get Alpaca API credentials."""
        api_key = self.get_secret(f"ALPACA_API_KEY_{key_num}")
        secret_key = self.get_secret(f"ALPACA_SECRET_KEY_{key_num}")
        return api_key, secret_key

    def _from_vault(self, name: str) -> str | None:
        """Load from HashiCorp Vault (stub for production)."""
        # In production, implement Vault client
        return None

    def _from_docker_secrets(self, name: str) -> str | None:
        """Load from Docker secrets file."""
        file_path = os.environ.get(f"{name}_FILE")
        if file_path and os.path.isfile(file_path):
            try:
                with open(file_path) as f:
                    return f.read().strip()
            except Exception:
                pass
        return None

    def _from_env(self, name: str) -> str | None:
        """Load from environment variable (dev only)."""
        return os.environ.get(name)


# ─────────────────────────────────────────────────────────────────────────────
# Singletons
# ─────────────────────────────────────────────────────────────────────────────

_validator: InputValidator | None = None
_key_manager: APIKeyManager | None = None
_secrets: SecretsManager | None = None


def get_input_validator() -> InputValidator:
    """Get singleton input validator."""
    global _validator
    if _validator is None:
        _validator = InputValidator()
    return _validator


def get_api_key_manager() -> APIKeyManager:
    """Get singleton API key manager."""
    global _key_manager
    if _key_manager is None:
        _key_manager = APIKeyManager()
    return _key_manager


def get_secrets_manager() -> SecretsManager:
    """Get singleton secrets manager."""
    global _secrets
    if _secrets is None:
        _secrets = SecretsManager()
    return _secrets
