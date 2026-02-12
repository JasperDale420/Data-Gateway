"""Tests for Unified Market Data API."""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from gateway.api.deps import get_registry
from gateway.core.provider import DataProvider
from gateway.core.registry import ProviderRegistry
from gateway.main import app
from gateway.schemas import NormalizedBar, NormalizedQuote, NormalizedTrade


class MockProvider(DataProvider):
    """Test provider returning canned normalized data."""

    def __init__(self, name: str):
        self._name = name
        self._capabilities = ["stock_bars", "stock_quotes", "stock_trades", "news"]

    @property
    def name(self) -> str:
        return self._name

    @property
    def capabilities(self) -> list[str]:
        return self._capabilities

    @property
    def supported_feeds(self) -> list[str]:
        return ["sip"]

    async def initialize(self, config: dict):
        pass  # No-op for tests

    async def shutdown(self):
        pass  # No-op for tests

    async def health_check(self):
        pass  # No-op for tests

    async def get_bars(self, symbols, timeframe="1Day", start=None, end=None, **kwargs):
        return [
            NormalizedBar(
                symbol=symbols[0],
                timestamp="2023-01-01T00:00:00Z",
                open=100,
                high=105,
                low=99,
                close=102,
                volume=1000,
                provider=self._name,
            )
        ]

    async def get_quotes(self, symbols, **kwargs):
        return [
            NormalizedQuote(
                symbol=symbols[0],
                timestamp="2023-01-01T00:00:00Z",
                bid_price=101,
                ask_price=102,
                bid_size=100,
                ask_size=100,
                provider=self._name,
            )
        ]

    async def get_trades(self, symbols, start=None, end=None, **kwargs):
        return [
            NormalizedTrade(
                symbol=symbols[0],
                timestamp="2023-01-01T00:00:00Z",
                price=101,
                size=50,
                trade_id="t1",
                tape="A",
                provider=self._name,
            )
        ]

    async def get_news(self, **kwargs):
        return [
            {
                "headline": "Test News",
                "summary": "This is a test article.",
                "url": "http://example.com/news/1",
                "source": self.name,
                "published_at": "2023-01-01T00:00:00Z",
            }
        ]


@pytest.fixture
def mock_registry():
    registry = MagicMock(spec=ProviderRegistry)
    p1 = MockProvider("provider1")
    registry.get_ordered_providers.return_value = [p1]
    return registry


@pytest.fixture
def client(mock_registry):
    # Reset circuit breaker state leaked from other test modules
    from gateway.core.circuit_breaker import get_circuit_registry

    get_circuit_registry()._breakers.clear()

    app.dependency_overrides[get_registry] = lambda: mock_registry

    valid_key = "gw_test_dev_key_67890"

    with TestClient(app) as tc:
        tc.headers.update({"X-Gateway-Key": valid_key})
        yield tc

    app.dependency_overrides.clear()


# ── Endpoint Tests ───────────────────────────────────────────────


def test_get_bars(client):
    response = client.get("/api/v1/market/stocks/AAPL/bars")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["symbol"] == "AAPL"
    assert len(data["data"]["bars"]) == 1
    assert data["meta"]["provider"] == "unified"


def test_get_quotes(client):
    response = client.get("/api/v1/market/stocks/AAPL/quotes")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert float(data["data"]["bid_price"]) == pytest.approx(101.0)


def test_get_trades(client):
    response = client.get("/api/v1/market/stocks/AAPL/trades")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]["trades"]) == 1


def test_get_news(client):
    response = client.get("/api/v1/market/news/AAPL")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]["articles"]) == 1
    assert data["data"]["articles"][0]["source"] == "provider1"


def test_no_providers_available(client, mock_registry):
    """Verify 503 when no providers are available for a capability."""
    mock_registry.get_ordered_providers.return_value = []
    app.dependency_overrides[get_registry] = lambda: mock_registry

    # Bypass cache middleware to ensure the endpoint actually runs
    response = client.get(
        "/api/v1/market/stocks/AAPL/bars",
        headers={"X-Gateway-Cache": "bypass"},
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "No providers configured for stock_bars"
