"""pytest configuration and fixtures."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from gateway.api.deps import (
    get_authenticator,
    get_cache,
    get_connection_manager,
    get_registry,
)
from gateway.config import Settings, get_settings
from gateway.core.auth import ClientAuthenticator
from gateway.core.cache import InMemoryCache
from gateway.core.connections import ConnectionManager
from gateway.main import app


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
        """
clients:
  - id: test
    key: gw_test_key_12345
    permissions:
      providers: [alpaca]
      feeds: [bars]
      max_symbols: 100
      rate_limit: 60
    enabled: true
  - id: disabled
    key: gw_disabled_key
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
def override_deps(
    test_settings, test_authenticator, test_cache, test_connection_manager, test_registry
):
    """Override FastAPI dependencies with test instances."""
    # Clear LRU caches so require_api_key picks up the test authenticator
    get_authenticator.cache_clear()
    get_cache.cache_clear()
    get_connection_manager.cache_clear()

    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_authenticator] = lambda: test_authenticator
    app.dependency_overrides[get_cache] = lambda: test_cache
    app.dependency_overrides[get_connection_manager] = lambda: test_connection_manager
    app.dependency_overrides[get_registry] = lambda: test_registry

    yield

    app.dependency_overrides.clear()
    # Clear caches again after test to clean up
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
    return {"X-Gateway-Key": "gw_test_key_12345"}
