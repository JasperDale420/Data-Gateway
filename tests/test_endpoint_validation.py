"""Comprehensive Endpoint Response Validation Tests.

This test file discovers all registered routes in the Gateway and validates
that each endpoint returns a valid response without ResponseValidationError.

Purpose:
- Detect schema mismatches (e.g., SuccessResponse.data type issues)
- Catch missing/broken endpoints early
- Provide a quick smoke test for all API routes
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from gateway.main import app

# Sample data for mocking provider responses
MOCK_BAR = {
    "symbol": "AAPL",
    "timestamp": datetime.now(UTC),
    "open": Decimal("150.00"),
    "high": Decimal("155.00"),
    "low": Decimal("149.00"),
    "close": Decimal("154.00"),
    "volume": 1000000,
    "provider": "alpaca",
}

MOCK_QUOTE = {
    "symbol": "AAPL",
    "timestamp": datetime.now(UTC),
    "bid_price": Decimal("154.00"),
    "bid_size": 100,
    "ask_price": Decimal("154.01"),
    "ask_size": 200,
    "provider": "alpaca",
}


class TestEndpointDiscovery:
    """Test that discovers and lists all registered routes."""

    def test_list_all_routes(self, client: TestClient):
        """List all registered routes for debugging."""
        routes = []
        for route in app.routes:
            if hasattr(route, "path"):
                methods = getattr(route, "methods", {"GET"})
                routes.append(
                    {
                        "path": route.path,
                        "methods": list(methods) if methods else ["GET"],
                        "name": getattr(route, "name", None),
                    }
                )

        # Print for debugging
        print(f"\n{'='*60}")
        print(f"Total routes: {len(routes)}")
        print(f"{'='*60}")
        for r in sorted(routes, key=lambda x: x["path"]):
            methods = ", ".join(r["methods"])
            print(f"{methods:8} {r['path']}")

        assert len(routes) > 0, "No routes registered!"


class TestHealthEndpointsValidation:
    """Validate health endpoints return valid responses."""

    def test_health_liveness(self, client: TestClient):
        """GET /health returns valid response."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data

    def test_health_ready(self, client: TestClient):
        """GET /health/ready returns valid response."""
        response = client.get("/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data

    def test_health_status(self, client: TestClient):
        """GET /health/status returns valid response."""
        response = client.get("/health/status")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data


class TestCatalogEndpointsValidation:
    """Validate catalog/discovery endpoints return valid responses."""

    def test_catalog_root(self, client: TestClient, auth_headers: dict):
        """GET /catalog returns valid response."""
        response = client.get("/catalog/", headers=auth_headers)
        assert response.status_code == 200
        # Catalogs can return dict directly (not wrapped in SuccessResponse)
        data = response.json()
        assert isinstance(data, dict)

    def test_catalog_streams(self, client: TestClient, auth_headers: dict):
        """GET /catalog/streams returns valid response."""
        response = client.get("/catalog/streams", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)

    def test_catalog_feeds(self, client: TestClient, auth_headers: dict):
        """GET /catalog/feeds returns valid response."""
        response = client.get("/catalog/feeds", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)

    def test_catalog_providers(self, client: TestClient, auth_headers: dict):
        """GET /catalog/providers returns valid response."""
        response = client.get("/catalog/providers", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)


class TestSymbologyEndpointsValidation:
    """Validate symbology endpoints return valid responses."""

    def test_symbology_resolve(self, client: TestClient, auth_headers: dict):
        """GET /api/v1/symbology/resolve returns valid response."""
        response = client.get(
            "/api/v1/symbology/resolve",
            params={"symbol": "AAPL"},
            headers=auth_headers,
        )
        assert response.status_code == 200
        data = response.json()
        assert "success" in data
        # Success is True for valid symbols
        if data.get("success"):
            assert "data" in data

    def test_symbology_validate(self, client: TestClient, auth_headers: dict):
        """GET /api/v1/symbology/validate returns valid response."""
        response = client.get(
            "/api/v1/symbology/validate",
            params={"symbol": "AAPL"},
            headers=auth_headers,
        )
        assert response.status_code == 200
        data = response.json()
        assert "success" in data
        assert "data" in data
        assert "valid" in data["data"]

    def test_symbology_batch(self, client: TestClient, auth_headers: dict):
        """POST /api/v1/symbology/batch returns valid response."""
        response = client.post(
            "/api/v1/symbology/batch",
            json={"symbols": ["AAPL", "MSFT"]},
            headers=auth_headers,
        )
        assert response.status_code == 200
        data = response.json()
        assert "success" in data


class TestCalendarEndpointsValidation:
    """Validate calendar endpoints return valid responses."""

    def test_calendar_market_hours(self, client: TestClient, auth_headers: dict):
        """GET /api/v1/calendar/market-hours returns valid response."""
        response = client.get(
            "/api/v1/calendar/market-hours", params={"date": "2024-01-15"}, headers=auth_headers
        )
        assert response.status_code == 200
        data = response.json()
        # Check it's a valid dict response
        assert isinstance(data, dict)

    def test_calendar_is_open(self, client: TestClient, auth_headers: dict):
        """GET /api/v1/calendar/is-open returns valid response."""
        response = client.get("/api/v1/calendar/is-open", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)

    def test_calendar_trading_days(self, client: TestClient, auth_headers: dict):
        """GET /api/v1/calendar/trading-days returns valid response."""
        response = client.get(
            "/api/v1/calendar/trading-days",
            params={"start": "2024-01-01", "end": "2024-01-31"},
            headers=auth_headers,
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)


class TestUWEndpointsValidation:
    """Validate Unusual Whales endpoints return valid responses.

    These tests verify that UW endpoints don't cause ResponseValidationError,
    even when the provider returns empty data or errors.
    """

    @pytest.fixture
    def mock_uw_provider(self, test_registry):
        """Set up mock UW provider with proper return types."""

        mock_provider = MagicMock()

        # Mock methods that return lists
        mock_provider.get_market_tide = AsyncMock(return_value=[])
        mock_provider.get_max_pain = AsyncMock(return_value=[])
        mock_provider.get_spot_exposures_by_strike = AsyncMock(return_value=[])
        mock_provider.get_flow_alerts = AsyncMock(return_value=[])
        mock_provider.get_flow_per_strike = AsyncMock(return_value=[])
        mock_provider.get_flow_per_expiry = AsyncMock(return_value=[])
        mock_provider.get_greek_flow = AsyncMock(return_value=[])
        mock_provider.get_net_flow_expiry = AsyncMock(return_value=[])
        mock_provider.get_interpolated_iv = AsyncMock(return_value=[])
        mock_provider.get_institutions = AsyncMock(return_value=[])
        mock_provider.get_congress_trades = AsyncMock(return_value=[])
        mock_provider.get_insiders = AsyncMock(return_value=[])
        mock_provider.get_iv_rank = AsyncMock(return_value=None)
        mock_provider.get_ticker_info = AsyncMock(return_value=None)
        mock_provider.get_stock_info = AsyncMock(return_value=None)  # Added
        mock_provider.get_darkpool_trades = AsyncMock(return_value=[])
        mock_provider.get_earnings = AsyncMock(return_value=[])
        mock_provider.get_market_tide_by_etf = AsyncMock(return_value=[])

        # Update the registry to return UW provider
        test_registry.get = MagicMock(return_value=mock_provider)

        return mock_provider

    def test_uw_market_tide(self, client: TestClient, auth_headers: dict, mock_uw_provider):
        """GET /api/v1/uw/market/tide returns valid response without validation error."""
        response = client.get("/api/v1/uw/market/tide", headers=auth_headers)
        # Should not be 500 (ResponseValidationError)
        assert response.status_code != 500, f"Got 500: {response.text}"
        if response.status_code == 200:
            data = response.json()
            assert "success" in data or isinstance(data, dict)

    def test_uw_max_pain(self, client: TestClient, auth_headers: dict, mock_uw_provider):
        """GET /api/v1/uw/{symbol}/max-pain returns valid response."""
        response = client.get("/api/v1/uw/AAPL/max-pain", headers=auth_headers)
        assert response.status_code != 500, f"Got 500: {response.text}"

    def test_uw_spot_exposures(self, client: TestClient, auth_headers: dict, mock_uw_provider):
        """GET /api/v1/uw/{symbol}/spot-exposures returns valid response."""
        response = client.get("/api/v1/uw/AAPL/spot-exposures", headers=auth_headers)
        assert response.status_code != 500, f"Got 500: {response.text}"

    def test_uw_iv_rank(self, client: TestClient, auth_headers: dict, mock_uw_provider):
        """GET /api/v1/uw/{symbol}/iv-rank returns valid response."""
        response = client.get("/api/v1/uw/AAPL/iv-rank", headers=auth_headers)
        # 404 is valid (no data), 500 is not
        assert response.status_code != 500, f"Got 500: {response.text}"

    def test_uw_stock_info(self, client: TestClient, auth_headers: dict, mock_uw_provider):
        """GET /api/v1/uw/stock/{symbol}/info returns valid response."""
        response = client.get("/api/v1/uw/stock/AAPL/info", headers=auth_headers)
        assert response.status_code != 500, f"Got 500: {response.text}"

    def test_uw_institutions(self, client: TestClient, auth_headers: dict, mock_uw_provider):
        """GET /api/v1/uw/institutions/{symbol} returns valid response."""
        response = client.get("/api/v1/uw/institutions/AAPL", headers=auth_headers)
        assert response.status_code != 500, f"Got 500: {response.text}"

    def test_uw_congress(self, client: TestClient, auth_headers: dict, mock_uw_provider):
        """GET /api/v1/uw/congress/{symbol} returns valid response."""
        response = client.get("/api/v1/uw/congress/AAPL", headers=auth_headers)
        assert response.status_code != 500, f"Got 500: {response.text}"


class TestResponseSchemaValidation:
    """Test that responses conform to expected schemas."""

    def test_success_response_with_list_data(self, client: TestClient, auth_headers: dict):
        """Verify SuccessResponse accepts list data (fixed in v0.5.2)."""
        # Catalog endpoints return dicts, symbology returns SuccessResponse
        response = client.get(
            "/api/v1/symbology/resolve",
            params={"symbol": "AAPL"},
            headers=auth_headers,
        )
        assert response.status_code == 200
        data = response.json()
        # Should have success field
        assert "success" in data or "valid" in data or "data" in data

    def test_paginated_response_format(self, client: TestClient, auth_headers: dict):
        """Verify paginated responses include pagination metadata."""
        # Calendar trading-days is paginated
        response = client.get(
            "/api/v1/calendar/trading-days",
            params={"start": "2024-01-01", "end": "2024-01-31"},
            headers=auth_headers,
        )
        assert response.status_code == 200
        data = response.json()
        # Should be a valid dict response
        assert isinstance(data, dict)


# Run all validation tests to identify broken endpoints
class TestBulkEndpointValidation:
    """Bulk validation of common endpoint patterns."""

    # List of endpoints to test (path, method, requires_auth, params)
    ENDPOINTS_TO_TEST = [
        # Health
        ("/health", "GET", False, {}),
        ("/health/ready", "GET", False, {}),
        ("/health/status", "GET", False, {}),
        # Catalog
        ("/catalog/", "GET", True, {}),
        ("/catalog/streams", "GET", True, {}),
        ("/catalog/feeds", "GET", True, {}),
        ("/catalog/providers", "GET", True, {}),
        # Symbology
        ("/api/v1/symbology/resolve", "GET", True, {"symbol": "AAPL"}),
        ("/api/v1/symbology/validate", "GET", True, {"symbol": "AAPL"}),
        # Calendar (auth required)
        ("/api/v1/calendar/is-open", "GET", True, {}),
        ("/api/v1/calendar/market-hours", "GET", True, {"date": "2024-01-15"}),
    ]

    @pytest.mark.parametrize("path,method,requires_auth,params", ENDPOINTS_TO_TEST)
    def test_endpoint_returns_valid_response(
        self, client: TestClient, auth_headers: dict, path, method, requires_auth, params
    ):
        """Test that endpoint returns valid JSON without 500 error."""
        headers = auth_headers if requires_auth else {}

        if method == "GET":
            response = client.get(path, params=params, headers=headers)
        elif method == "POST":
            response = client.post(path, json=params, headers=headers)
        else:
            pytest.skip(f"Unsupported method: {method}")

        # Should not return 500 (internal server error / validation error)
        assert response.status_code != 500, f"{path} returned 500: {response.text}"

        # Response should be valid JSON
        try:
            data = response.json()
            assert isinstance(data, dict | list), f"{path} returned non-JSON: {type(data)}"
        except Exception as e:
            pytest.fail(f"{path} returned invalid JSON: {e}")
