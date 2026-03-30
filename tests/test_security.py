"""Tests for Security Module."""

from datetime import UTC, datetime, timedelta

import pytest

from gateway.core.security import (
    APIKeyManager,
    InputValidationError,
    InputValidator,
    Permission,
    Role,
    ValidationErrorCode,
    check_permission,
    get_api_key_manager,
    get_input_validator,
)


class TestInputValidator:
    """Test InputValidator functionality."""

    @pytest.fixture
    def validator(self) -> InputValidator:
        return InputValidator()

    # ─────────────────────────────────────────────────────────────────────
    # Symbol Validation
    # ─────────────────────────────────────────────────────────────────────

    def test_valid_stock_symbols(self, validator):
        """Valid stock symbols should pass."""
        for symbol in ["AAPL", "MSFT", "GOOGL", "A", "NFLX"]:
            assert validator.validate_symbol(symbol) is None

    def test_invalid_stock_symbols(self, validator):
        """Invalid stock symbols should return errors."""
        error = validator.validate_symbol("INVALID$")
        assert error is not None
        assert error.code == ValidationErrorCode.INVALID_SYMBOL_FORMAT.value

        error = validator.validate_symbol("TOOLONGNAME")
        assert error is not None

    def test_empty_symbol(self, validator):
        """Empty symbol should return required error."""
        error = validator.validate_symbol("")
        assert error is not None
        assert error.code == ValidationErrorCode.REQUIRED_PARAMETER_MISSING.value

    def test_forbidden_chars_in_symbol(self, validator):
        """Forbidden characters should be detected."""
        error = validator.validate_symbol("AAPL<script>")
        assert error is not None
        assert error.code == ValidationErrorCode.FORBIDDEN_CHARACTERS.value

        error = validator.validate_symbol("AAPL'; DROP TABLE")
        assert error is not None

    def test_option_symbol(self, validator):
        """OCC option symbols should validate."""
        # Valid OCC format: AAPL250117C00200000
        error = validator.validate_symbol("AAPL250117C00200000", "option_occ")
        assert error is None

    def test_crypto_symbol(self, validator):
        """Crypto pair symbols should validate."""
        error = validator.validate_symbol("BTC/USD", "crypto")
        assert error is None

        error = validator.validate_symbol("ETH/USDT", "crypto")
        assert error is None

    def test_forex_symbol(self, validator):
        """Forex pair symbols should validate."""
        error = validator.validate_symbol("EUR/USD", "forex")
        assert error is None

    # ─────────────────────────────────────────────────────────────────────
    # Symbols Array Validation
    # ─────────────────────────────────────────────────────────────────────

    def test_valid_symbols_array(self, validator):
        """Valid symbols array should pass."""
        error = validator.validate_symbols_array(["AAPL", "MSFT", "GOOGL"])
        assert error is None

    def test_empty_symbols_array(self, validator):
        """Empty array should fail."""
        error = validator.validate_symbols_array([])
        assert error is not None
        assert error.code == ValidationErrorCode.REQUIRED_PARAMETER_MISSING.value

    def test_symbols_array_exceeds_limit(self, validator):
        """Array exceeding limit should fail."""
        symbols = [f"SYM{i}" for i in range(600)]
        error = validator.validate_symbols_array(symbols)
        assert error is not None
        assert error.code == ValidationErrorCode.PARAMETER_EXCEEDS_LIMIT.value

    def test_symbols_array_with_custom_limit(self, validator):
        """Custom max_symbols should be respected."""
        symbols = ["AAPL", "MSFT", "GOOGL"]
        error = validator.validate_symbols_array(symbols, max_symbols=2)
        assert error is not None

    def test_symbols_array_dedupes_before_validation(self, validator, monkeypatch):
        """Duplicate symbols should not be revalidated."""
        calls: list[str] = []
        original = validator.validate_symbol

        def _count(symbol: str, symbol_type: str = "stock"):
            calls.append(symbol)
            return original(symbol, symbol_type)

        monkeypatch.setattr(validator, "validate_symbol", _count)

        # With explicit symbol_type, validate_symbol is called per unique symbol
        error = validator.validate_symbols_array(["AAPL", "aapl", "MSFT", "AAPL"], symbol_type="stock")

        assert error is None
        assert calls == ["AAPL", "MSFT"]

    # ─────────────────────────────────────────────────────────────────────
    # Timeframe Validation
    # ─────────────────────────────────────────────────────────────────────

    def test_valid_timeframes(self, validator):
        """Valid timeframes should pass."""
        for tf in ["1Min", "5Min", "1Hour", "1Day", "1Week"]:
            assert validator.validate_timeframe(tf) is None

    def test_invalid_timeframe(self, validator):
        """Invalid timeframe should fail."""
        error = validator.validate_timeframe("InvalidTF")
        assert error is not None
        assert error.code == ValidationErrorCode.INVALID_ENUM_VALUE.value

    def test_empty_timeframe(self, validator):
        """Empty timeframe should fail."""
        error = validator.validate_timeframe("")
        assert error is not None
        assert error.code == ValidationErrorCode.REQUIRED_PARAMETER_MISSING.value

    # ─────────────────────────────────────────────────────────────────────
    # Date Range Validation
    # ─────────────────────────────────────────────────────────────────────

    def test_valid_date_range(self, validator):
        """Valid date range should pass."""
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 31, tzinfo=UTC)
        assert validator.validate_date_range(start, end) is None

    def test_start_after_end(self, validator):
        """Start after end should fail."""
        start = datetime(2024, 2, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, tzinfo=UTC)
        error = validator.validate_date_range(start, end)
        assert error is not None
        assert error.code == ValidationErrorCode.INVALID_DATETIME_FORMAT.value

    def test_date_range_exceeds_limit(self, validator):
        """Date range exceeding 5 years should fail."""
        start = datetime(2018, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, tzinfo=UTC)
        error = validator.validate_date_range(start, end)
        assert error is not None
        assert error.code == ValidationErrorCode.PARAMETER_EXCEEDS_LIMIT.value

    # ─────────────────────────────────────────────────────────────────────
    # Limit Validation
    # ─────────────────────────────────────────────────────────────────────

    def test_valid_limit(self, validator):
        """Valid limit should pass."""
        limit, error = validator.validate_limit(100)
        assert limit == 100
        assert error is None

    def test_limit_capped_at_max(self, validator):
        """Limit exceeding max should be capped."""
        limit, error = validator.validate_limit(50000)
        assert limit == 10000  # Max
        assert error is None

    def test_negative_limit_uses_default(self, validator):
        """Negative limit should use default."""
        limit, error = validator.validate_limit(-1)
        assert limit == 1000  # Default
        assert error is None

    # ─────────────────────────────────────────────────────────────────────
    # Speed Validation
    # ─────────────────────────────────────────────────────────────────────

    def test_valid_speed(self, validator):
        """Valid speed should pass."""
        assert validator.validate_speed(1.0) is None
        assert validator.validate_speed(10.0) is None
        assert validator.validate_speed(100.0) is None

    def test_speed_exceeds_max(self, validator):
        """Speed exceeding max should fail."""
        error = validator.validate_speed(150.0)
        assert error is not None
        assert error.code == ValidationErrorCode.PARAMETER_EXCEEDS_LIMIT.value

    def test_negative_speed(self, validator):
        """Negative speed should fail."""
        error = validator.validate_speed(-1.0)
        assert error is not None

    # ─────────────────────────────────────────────────────────────────────
    # Request Size Validation
    # ─────────────────────────────────────────────────────────────────────

    def test_valid_request_size(self, validator):
        """Valid request size should pass."""
        assert validator.validate_request_size(1000) is None

    def test_request_size_exceeds_limit(self, validator):
        """Request size exceeding limit should fail."""
        error = validator.validate_request_size(10 * 1024 * 1024)  # 10MB
        assert error is not None
        assert error.code == ValidationErrorCode.REQUEST_BODY_TOO_LARGE.value

    def test_singleton(self):
        """get_input_validator should return singleton."""
        v1 = get_input_validator()
        v2 = get_input_validator()
        assert v1 is v2


class TestAPIKeyManager:
    """Test APIKeyManager functionality."""

    @pytest.fixture
    def manager(self) -> APIKeyManager:
        return APIKeyManager()

    def test_generate_key(self, manager):
        """Should generate valid API keys."""
        raw_key, api_key = manager.generate_key("test_client")

        assert raw_key.startswith("gw_test_client_")
        assert len(raw_key) >= 48
        assert api_key.key_id.startswith("key_")
        assert api_key.client_id == "test_client"
        assert api_key.is_valid()

    def test_key_has_correct_entropy(self, manager):
        """Key should have sufficient entropy."""
        raw_key, _ = manager.generate_key("test")
        # Key format: gw_<client>_<random>
        parts = raw_key.split("_", 2)
        random_part = parts[2]
        # 32 bytes base64url = ~43 chars
        assert len(random_part) >= 40

    def test_validate_key(self, manager):
        """Should validate generated keys."""
        raw_key, original = manager.generate_key("test")

        validated = manager.validate_key(raw_key)
        assert validated is not None
        assert validated.key_id == original.key_id

    def test_invalid_key_returns_none(self, manager):
        """Invalid keys should return None."""
        assert manager.validate_key("") is None
        assert manager.validate_key("invalid") is None
        assert manager.validate_key("gw_fake_key123") is None

    def test_key_expiration(self, manager):
        """Expired keys should be invalid."""
        raw_key, api_key = manager.generate_key("test", expires_in_days=0)
        # Force expiration
        api_key.expires_at = datetime.now(UTC) - timedelta(days=1)

        assert api_key.is_expired()
        assert not api_key.is_valid()

    def test_revoke_key(self, manager):
        """Revoked keys should be invalid."""
        raw_key, api_key = manager.generate_key("test")

        assert manager.revoke_key(api_key.key_id)
        assert api_key.revoked
        assert not api_key.is_valid()
        assert manager.validate_key(raw_key) is None

    def test_rotate_key(self, manager):
        """Key rotation should invalidate old and create new."""
        raw_key, old_key = manager.generate_key("test")

        result = manager.rotate_key(old_key.key_id)
        assert result is not None

        new_raw, new_key = result

        # Old key should be revoked
        assert old_key.revoked
        assert manager.validate_key(raw_key) is None

        # New key should be valid
        assert new_key.is_valid()
        assert manager.validate_key(new_raw) is not None

    def test_list_keys(self, manager):
        """Should list all keys."""
        manager.generate_key("client1")
        manager.generate_key("client2")
        manager.generate_key("client1")

        all_keys = manager.list_keys()
        assert len(all_keys) == 3

        client1_keys = manager.list_keys("client1")
        assert len(client1_keys) == 2

    def test_key_with_role(self, manager):
        """Should support different roles."""
        _, admin_key = manager.generate_key("admin", role=Role.ADMIN)
        assert admin_key.role == Role.ADMIN

    def test_key_with_custom_permissions(self, manager):
        """Should support custom permissions."""
        perms = Permission(
            providers=["alpaca"],
            max_symbols=100,
        )
        _, api_key = manager.generate_key("limited", permissions=perms)

        assert api_key.permissions.providers == ["alpaca"]
        assert api_key.permissions.max_symbols == 100

    def test_singleton(self):
        """get_api_key_manager should return singleton."""
        m1 = get_api_key_manager()
        m2 = get_api_key_manager()
        assert m1 is m2


class TestRBAC:
    """Test RBAC permission checks."""

    def test_client_data_access(self):
        """Clients should access data endpoints."""
        assert check_permission("/api/v1/alpaca/stocks/AAPL/bars", Role.CLIENT)
        assert check_permission("/api/v1/uw/flow", Role.CLIENT)
        assert check_permission("/api/v1/bulk/bars", Role.CLIENT)
        assert check_permission("/api/v1/replay/sessions", Role.CLIENT)

    def test_client_no_admin_access(self):
        """Clients should not access admin endpoints."""
        assert not check_permission("/api/v1/admin/keys", Role.CLIENT)
        assert not check_permission("/api/v1/admin/cache/clear", Role.CLIENT)

    def test_admin_can_manage_keys(self):
        """Admins should access key management."""
        assert check_permission("/api/v1/admin/keys", Role.ADMIN)
        assert check_permission("/api/v1/admin/cache/clear", Role.ADMIN)

    def test_admin_no_shutdown(self):
        """Admins should not shutdown."""
        assert not check_permission("/api/v1/admin/shutdown", Role.ADMIN)
        assert not check_permission("/api/v1/admin/config/reload", Role.ADMIN)

    def test_super_admin_full_access(self):
        """Super admins should have full access."""
        assert check_permission("/api/v1/admin/shutdown", Role.SUPER_ADMIN)
        assert check_permission("/api/v1/admin/config/reload", Role.SUPER_ADMIN)
        assert check_permission("/api/v1/admin/keys", Role.SUPER_ADMIN)


class TestInputValidationError:
    """Test ValidationError formatting."""

    def test_to_dict_basic(self):
        """Basic error should format correctly."""
        error = InputValidationError(
            code="GW-E8001",
            message="Invalid symbol format",
        )
        data = error.to_dict()

        assert data["code"] == "GW-E8001"
        assert data["message"] == "Invalid symbol format"
        assert "details" not in data

    def test_to_dict_with_details(self):
        """Error with details should format correctly."""
        error = InputValidationError(
            code="GW-E8001",
            message="Invalid symbol format",
            field="symbol",
            value="INVALID$",
            expected_pattern="^[A-Z]{1,5}$",
        )
        data = error.to_dict()

        assert data["details"]["field"] == "symbol"
        assert data["details"]["value"] == "INVALID$"
        assert data["details"]["expected_pattern"] == "^[A-Z]{1,5}$"
