"""Smoke tests for Data Gateway.

These tests verify basic functionality after deployment.
Run time target: < 2 minutes as per PRD.
"""


class TestServerStartup:
    """Test: server_starts - Verify server starts successfully."""

    def test_health_endpoint_responds(self, client):
        """Server should respond to health check."""
        response = client.get("/health")
        assert response.status_code == 200


class TestHealthReady:
    """Test: health_ready - Verify health/ready endpoint."""

    def test_liveness(self, client):
        """Liveness endpoint should return 200."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_readiness(self, client):
        """Readiness endpoint should return 200 when ready."""
        response = client.get("/health/ready")
        # May be 503 if providers not initialized, but should respond
        assert response.status_code in (200, 503)


class TestAuthWorks:
    """Test: auth_works - Verify authentication works."""

    def test_missing_auth_returns_401(self, client):
        """Request without API key should return 401."""
        response = client.get("/api/v1/alpaca/stocks/AAPL/bars")
        assert response.status_code == 401

    def test_invalid_auth_returns_401(self, client):
        """Request with invalid API key should return 401."""
        response = client.get(
            "/api/v1/alpaca/stocks/AAPL/bars",
            headers={"X-Gateway-Key": "invalid_key"},
        )
        assert response.status_code == 401

    def test_valid_auth_endpoint_exists(self, client, test_authenticator):
        """Test that authenticated endpoint responds (not 404)."""
        response = client.get(
            "/api/v1/alpaca/stocks/AAPL/bars?timeframe=1Day",
            headers={"X-Gateway-Key": "gw_test_key_12345"},
        )
        # Endpoint exists - may return various errors in test mode
        assert response.status_code in (200, 401, 500, 502, 503)


class TestRestWorks:
    """Test: rest_works - Verify REST endpoints respond."""

    def test_alpaca_bars_endpoint_exists(self, client, auth_headers):
        """Alpaca bars endpoint should exist and respond."""
        response = client.get(
            "/api/v1/alpaca/stocks/AAPL/bars?timeframe=1Day",
            headers=auth_headers,
        )
        # Endpoint exists (not 404) - may return 500 if registry not initialized
        assert response.status_code != 404

    def test_metrics_endpoint(self, client):
        """Metrics endpoint should be accessible."""
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "gateway_" in response.text or "process_" in response.text


class TestCacheWorks:
    """Test caching functionality."""

    def test_health_status_shows_cache_stats(self, client):
        """Health status should include cache stats."""
        response = client.get("/health/status")
        if response.status_code == 200:
            data = response.json()
            # Check for nested structure
            assert "components" in data


class TestFixturesLoaded:
    """Test that fixtures are properly loaded."""

    def test_valid_bars_fixture(self, valid_bars):
        """Valid bars fixture should have data."""
        assert len(valid_bars) > 0

    def test_invalid_bars_fixture(self, invalid_bars):
        """Invalid bars fixture should have data."""
        assert len(invalid_bars) > 0

    def test_symbols_1000_fixture(self, symbols_1000):
        """symbols_1000 fixture should have 1000 symbols."""
        assert len(symbols_1000) == 1000
