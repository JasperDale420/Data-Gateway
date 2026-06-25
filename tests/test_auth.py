"""Tests for authentication."""

from pathlib import Path

import pytest

import gateway.core.auth as auth_module
from gateway.core.auth import ClientAuthenticator, InvalidClientIdError


def test_authenticate_valid_key(test_authenticator, test_api_key):
    """Valid key returns client."""
    client = test_authenticator.authenticate(test_api_key)
    assert client is not None
    assert client.id == "test"
    assert client.enabled is True


def test_authenticate_invalid_key(test_authenticator):
    """Invalid key returns None."""
    client = test_authenticator.authenticate("invalid_key")
    assert client is None


def test_authenticate_disabled_client(test_authenticator, disabled_api_key):
    """Disabled client returns None."""
    client = test_authenticator.authenticate(disabled_api_key)
    assert client is None


def test_client_permissions(test_authenticator, test_api_key):
    """Client has expected permissions."""
    client = test_authenticator.authenticate(test_api_key)
    assert client is not None

    assert "alpaca" in client.permissions.providers
    assert "bars" in client.permissions.feeds
    assert client.permissions.max_symbols == 100
    assert client.permissions.rate_limit == 60


def test_heber_watch_can_read_uw_enrichment_feeds():
    """Heber watch must be allowed to read the UW endpoints it enriches from."""
    authenticator = ClientAuthenticator(Path("config/clients.yaml"))

    client = authenticator.get_client("heber-watch")

    assert client is not None
    assert "uw" in client.permissions.providers or "unusual_whales" in client.permissions.providers
    assert "greek_exposure" in client.permissions.feeds
    assert "market_tide" in client.permissions.feeds
    assert "options" in client.permissions.feeds


def test_get_client_by_id(test_authenticator):
    """Get client by ID."""
    client = test_authenticator.get_client("test")
    assert client is not None
    assert client.id == "test"


def test_get_nonexistent_client(test_authenticator):
    """Nonexistent client returns None."""
    client = test_authenticator.get_client("nonexistent")
    assert client is None


def test_hash_key():
    """Key hashing produces consistent hash with sha256: prefix."""
    from gateway.core.auth import ClientAuthenticator

    hash1 = ClientAuthenticator.hash_key("test_key")
    hash2 = ClientAuthenticator.hash_key("test_key")
    hash3 = ClientAuthenticator.hash_key("different_key")

    assert hash1 == hash2
    assert hash1 != hash3
    assert hash1.startswith("sha256:")
    assert len(hash1) == 71  # "sha256:" (7) + 64 hex chars


def test_authenticate_valid_key_logs_debug_not_info(test_authenticator, test_api_key, monkeypatch):
    """Successful auth should use debug-level logging to reduce hot-path log volume."""

    class _FakeLogger:
        def __init__(self) -> None:
            self.info_calls = 0
            self.debug_calls = 0

        def info(self, *_args, **_kwargs) -> None:
            self.info_calls += 1

        def debug(self, *_args, **_kwargs) -> None:
            self.debug_calls += 1

        def warning(self, *_args, **_kwargs) -> None:
            return None

    fake_logger = _FakeLogger()
    monkeypatch.setattr(auth_module, "logger", fake_logger)

    client = test_authenticator.authenticate(test_api_key)

    assert client is not None
    assert fake_logger.debug_calls == 2
    assert fake_logger.info_calls == 0


def test_reload_keeps_previous_clients_on_bad_config(tmp_path):
    """A malformed clients.yaml on SIGHUP reload must not wipe live client maps.

    Regression guard: reload() used to clear the maps before re-parsing, so a
    single bad edit + SIGHUP 401-locked every trading client until restart.
    """
    config = tmp_path / "clients.yaml"
    config.write_text(
        "clients:\n  - id: kairos\n    key: gw_live_key_123\n    role: trader\n    permissions:\n      trading: true\n"
    )
    auth = ClientAuthenticator(config)
    assert auth.authenticate("gw_live_key_123") is not None

    # Corrupt the config with an invalid client id and reload.
    config.write_text("clients:\n  - id: 'bad id with spaces'\n    key: gw_other\n")
    with pytest.raises(InvalidClientIdError):
        auth.reload()

    # The previously-loaded client must still authenticate (maps rolled back).
    assert auth.authenticate("gw_live_key_123") is not None
    assert auth.get_client("kairos") is not None
