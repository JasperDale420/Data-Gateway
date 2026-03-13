"""pytest configuration and fixtures."""

from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

import gateway.core.globals as _globals_module
from gateway.api.deps import (
    get_authenticator,
    get_cache,
    get_connection_manager,
    get_registry,
    get_sink_registry,
)
from gateway.config import Settings, get_settings
from gateway.core.auth import ClientAuthenticator
from gateway.core.cache import InMemoryCache
from gateway.core.connections import ConnectionManager
from gateway.core.globals import set_sink_registry
from gateway.main import app

DEFAULT_TEST_API_KEY = "gw_test_dev_key_67890"  # pragma: allowlist secret
DEFAULT_DISABLED_API_KEY = "gw_disabled_key"  # pragma: allowlist secret


def _load_repo_test_api_key() -> str:
    """Load the test client key from repo config when available."""
    config_path = Path(__file__).resolve().parents[1] / "config" / "clients.yaml"
    if not config_path.exists():
        return DEFAULT_TEST_API_KEY

    try:
        config = yaml.safe_load(config_path.read_text()) or {}
    except Exception:
        return DEFAULT_TEST_API_KEY

    clients = config.get("clients", [])
    for client_data in clients:
        if client_data.get("id") == "test":
            key = client_data.get("key")
            if isinstance(key, str) and key:
                return key

    return DEFAULT_TEST_API_KEY


TEST_API_KEY = _load_repo_test_api_key()


# Test client fixture
@pytest.fixture
def client():
    """FastAPI test client."""
    return TestClient(app)


# Override settings for tests
@pytest.fixture
def test_settings(tmp_path: Path):
    """Test settings with temp clients file."""
    clients_file = tmp_path / "clients.yaml"
    clients_file.write_text(
        f"""
clients:
  - id: test
    key: "{TEST_API_KEY}"
    permissions:
      providers: [alpaca]
      feeds: [bars]
      max_symbols: 100
      rate_limit: 60
    enabled: true
  - id: disabled
    key: "{DEFAULT_DISABLED_API_KEY}"
    permissions:
      providers: []
      feeds: []
      max_symbols: 0
      rate_limit: 0
    enabled: false
"""
    )

    return Settings(
        debug=True,
        clients_config_path=clients_file,
        cache_max_size=100,
        cache_default_ttl=60,
    )


@pytest.fixture
def test_authenticator(test_settings: Settings):
    """Client authenticator with test config."""
    return ClientAuthenticator(test_settings.clients_config_path)


@pytest.fixture
def test_cache():
    """Fresh cache for testing."""
    return InMemoryCache(max_size=100, default_ttl=60)


@pytest.fixture
def test_connection_manager():
    """Fresh connection manager for testing."""
    return ConnectionManager()


@pytest.fixture
def test_registry():
    """Mock provider registry for tests."""
    from unittest.mock import AsyncMock, MagicMock

    registry = MagicMock()  # No spec to allow arbitrary attributes
    # Mock common provider methods to return empty/error responses
    mock_provider = MagicMock()
    mock_provider.get_bars = AsyncMock(return_value={"bars": [], "symbol": "TEST"})
    mock_provider.get_quote = AsyncMock(return_value={"quote": {}, "symbol": "TEST"})
    mock_provider.get_chain = AsyncMock(return_value={"chain": []})
    registry.get_provider.return_value = mock_provider
    registry.list_providers.return_value = ["alpaca"]
    return registry


# Override dependencies in app
@pytest.fixture(autouse=True)
def override_deps(test_settings, test_authenticator, test_cache, test_connection_manager, test_registry):
    """Override FastAPI dependencies with test instances."""

    # Clear LRU caches so require_api_key picks up the test authenticator
    get_authenticator.cache_clear()
    get_cache.cache_clear()
    get_connection_manager.cache_clear()

    # Prevent test data from leaking to production Redis stream
    _original_sink = get_sink_registry()
    set_sink_registry(None)
    app.dependency_overrides[get_sink_registry] = lambda: None

    # Nullify the multiplexer so WebSocket subscribe uses stub responses
    # instead of hitting real Alpaca upstream (the lifespan creates a real
    # multiplexer because it calls get_settings() directly, bypassing DI).
    _original_mux = _globals_module._multiplexer
    _globals_module._multiplexer = None

    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_authenticator] = lambda: test_authenticator
    app.dependency_overrides[get_cache] = lambda: test_cache
    app.dependency_overrides[get_connection_manager] = lambda: test_connection_manager
    app.dependency_overrides[get_registry] = lambda: test_registry

    yield

    app.dependency_overrides.clear()
    # Restore original sink registry, multiplexer, and clear caches
    set_sink_registry(_original_sink)
    _globals_module._multiplexer = _original_mux
    get_authenticator.cache_clear()
    get_cache.cache_clear()
    get_connection_manager.cache_clear()


# ─────────────────────────────────────────────────────────────────────────────
# Data Fixtures (PRD Test Data Requirements)
# ─────────────────────────────────────────────────────────────────────────────

import json

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def valid_bars():
    """Load valid bar data from fixtures."""
    path = FIXTURES_DIR / "alpaca_bars_valid.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


@pytest.fixture
def invalid_bars():
    """Load invalid bar data from fixtures."""
    path = FIXTURES_DIR / "alpaca_bars_invalid.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


@pytest.fixture
def valid_quotes():
    """Load quote data from fixtures."""
    path = FIXTURES_DIR / "alpaca_quotes.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


@pytest.fixture
def valid_trades():
    """Load trade data from fixtures."""
    path = FIXTURES_DIR / "alpaca_trades.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


@pytest.fixture
def option_chains():
    """Load option chain data from fixtures."""
    path = FIXTURES_DIR / "option_chains.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


@pytest.fixture
def uw_flow_samples():
    """Load UW flow samples from fixtures."""
    path = FIXTURES_DIR / "uw_flow_samples.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


@pytest.fixture
def symbols_1000():
    """Load 1000 symbols for load testing."""
    path = FIXTURES_DIR / "symbols_1000.txt"
    if path.exists():
        with open(path) as f:
            return [line.strip() for line in f if line.strip()]
    return []


@pytest.fixture
def symbols_5000():
    """Load 5000 symbols for stress testing."""
    path = FIXTURES_DIR / "symbols_5000.txt"
    if path.exists():
        with open(path) as f:
            return [line.strip() for line in f if line.strip()]
    return []


# Edge case fixtures
@pytest.fixture
def invalid_high_low():
    """Bar with high < low."""
    path = FIXTURES_DIR / "invalid_high_low.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


@pytest.fixture
def crossed_quote():
    """Quote with bid > ask."""
    path = FIXTURES_DIR / "crossed_quote.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


@pytest.fixture
def auth_headers():
    """Standard authentication headers for tests."""
    return {"X-Gateway-Key": TEST_API_KEY}


@pytest.fixture
def test_api_key() -> str:
    """Canonical API key used for authenticated tests."""
    return TEST_API_KEY


@pytest.fixture
def disabled_api_key() -> str:
    """Canonical disabled API key used for auth tests."""
    return DEFAULT_DISABLED_API_KEY
