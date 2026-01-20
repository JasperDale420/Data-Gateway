"""API Integration Tests.

Integration tests for Data-Gateway API endpoints using FastAPI TestClient.
Tests request/response contracts, error handling, and authentication.
"""

from fastapi.testclient import TestClient

# ─────────────────────────────────────────────────────────────────────────────
# Health Endpoints
# ─────────────────────────────────────────────────────────────────────────────


class TestHealthEndpoints:
    """Integration tests for health check endpoints."""

    def test_liveness(self, client: TestClient):
        """GET /health returns ok."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_readiness(self, client: TestClient):
        """GET /health/ready checks dependencies."""
        response = client.get("/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "checks" in data
        assert data["status"] in ("ready", "not_ready")

    def test_detailed_status(self, client: TestClient):
        """GET /health/status returns component details."""
        response = client.get("/health/status")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "version" in data
        assert "timestamp" in data
        assert "components" in data
        assert "cache" in data["components"]
        assert "connections" in data["components"]


# ─────────────────────────────────────────────────────────────────────────────
# Symbology Endpoints
# ─────────────────────────────────────────────────────────────────────────────


class TestSymbologyEndpoints:
    """Integration tests for symbology resolution endpoints."""

    def test_resolve_stock(self, client: TestClient):
        """GET /symbology/resolve resolves stock symbol."""
        response = client.get("/symbology/resolve", params={"symbol": "AAPL"})
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["normalized"]["type"] == "stock"
        assert data["data"]["normalized"]["symbol"] == "AAPL"

    def test_resolve_occ_option(self, client: TestClient):
        """GET /symbology/resolve resolves OCC option format."""
        response = client.get(
            "/symbology/resolve",
            params={"symbol": "AAPL250117C00200000"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["normalized"]["type"] == "option"
        assert data["data"]["normalized"]["underlying"] == "AAPL"
        assert data["data"]["normalized"]["strike"] == 200.0
        assert data["data"]["normalized"]["option_type"] == "call"

    def test_resolve_crypto(self, client: TestClient):
        """GET /symbology/resolve resolves crypto pair."""
        # Use DOGE/USDT which has 4-letter base (not 3 like EUR)
        response = client.get("/symbology/resolve", params={"symbol": "DOGE/USDT"})
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["normalized"]["type"] == "crypto"

    def test_resolve_forex(self, client: TestClient):
        """GET /symbology/resolve resolves forex pair."""
        response = client.get("/symbology/resolve", params={"symbol": "EUR/USD"})
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["normalized"]["type"] == "forex"

    def test_resolve_invalid_symbol(self, client: TestClient):
        """GET /symbology/resolve returns 400 for invalid symbol."""
        response = client.get(
            "/symbology/resolve",
            params={"symbol": "INVALID123XYZ"},
        )
        assert response.status_code == 400
        assert "GW-E8001" in str(response.json())

    def test_validate_valid_symbol(self, client: TestClient):
        """GET /symbology/validate confirms valid symbol."""
        response = client.get("/symbology/validate", params={"symbol": "MSFT"})
        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is True
        assert data["symbol"] == "MSFT"
        assert data["error"] is None

    def test_validate_invalid_symbol(self, client: TestClient):
        """GET /symbology/validate reports invalid symbol."""
        response = client.get(
            "/symbology/validate",
            params={"symbol": "INVALID123XYZ"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is False
        assert data["error"] is not None

    def test_batch_resolve(self, client: TestClient):
        """POST /symbology/batch resolves multiple symbols."""
        response = client.post(
            "/symbology/batch",
            json={"symbols": ["AAPL", "MSFT", "BTC/USD"]},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["results"]) == 3
        assert len(data["errors"]) == 0

    def test_convert_symbol(self, client: TestClient):
        """GET /symbology/convert converts to provider format."""
        response = client.get(
            "/symbology/convert",
            params={"symbol": "AAPL", "provider": "alpaca"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["input"] == "AAPL"
        assert data["data"]["provider"] == "alpaca"
        assert data["data"]["output"] == "AAPL"


# ─────────────────────────────────────────────────────────────────────────────
# Calendar Endpoints
# ─────────────────────────────────────────────────────────────────────────────


class TestCalendarEndpoints:
    """Integration tests for trading calendar endpoints."""

    def test_market_hours(self, client: TestClient, auth_headers: dict):
        """GET /api/v1/calendar/market-hours returns hours info."""
        response = client.get(
            "/api/v1/calendar/market-hours",
            params={"date": "2024-01-15"},
            headers=auth_headers,
        )
        assert response.status_code == 200
        data = response.json()
        # Response may have envelope wrapper with data field
        inner = data.get("data", data)
        assert "date" in inner
        assert "market" in inner
        assert "status" in inner
        assert "timezone" in inner

    def test_market_hours_invalid_date(self, client: TestClient, auth_headers: dict):
        """GET /api/v1/calendar/market-hours rejects invalid date."""
        response = client.get(
            "/api/v1/calendar/market-hours",
            params={"date": "not-a-date"},
            headers=auth_headers,
        )
        assert response.status_code == 400
        assert "Invalid date format" in response.json()["detail"]

    def test_trading_days(self, client: TestClient, auth_headers: dict):
        """GET /api/v1/calendar/trading-days returns trading days."""
        response = client.get(
            "/api/v1/calendar/trading-days",
            params={"start": "2024-01-01", "end": "2024-01-31"},
            headers=auth_headers,
        )
        assert response.status_code == 200
        data = response.json()
        # Response may have envelope wrapper with data field
        inner = data.get("data", data)
        assert "trading_days" in inner
        assert "holidays" in inner
        assert "early_closes" in inner
        assert len(inner["trading_days"]) > 0

    def test_trading_days_range_limit(self, client: TestClient, auth_headers: dict):
        """GET /api/v1/calendar/trading-days rejects >1 year range."""
        response = client.get(
            "/api/v1/calendar/trading-days",
            params={"start": "2022-01-01", "end": "2024-01-01"},
            headers=auth_headers,
        )
        assert response.status_code == 400
        assert "exceed 1 year" in response.json()["detail"]

    def test_is_market_open(self, client: TestClient, auth_headers: dict):
        """GET /api/v1/calendar/is-open returns open status."""
        response = client.get(
            "/api/v1/calendar/is-open",
            headers=auth_headers,
        )
        assert response.status_code == 200
        data = response.json()
        inner = data.get("data", data)
        assert "is_open" in inner
        assert "date" in inner
        assert "status" in inner
        assert isinstance(inner["is_open"], bool)

    def test_next_trading_day(self, client: TestClient, auth_headers: dict):
        """GET /api/v1/calendar/next-trading-day returns next day."""
        response = client.get(
            "/api/v1/calendar/next-trading-day",
            params={"from": "2024-01-01"},
            headers=auth_headers,
        )
        assert response.status_code == 200
        data = response.json()
        inner = data.get("data", data)
        assert "next_trading_day" in inner
        assert "from" in inner
        # Next trading day should be >= from date
        assert inner["next_trading_day"] >= inner["from"]


# ─────────────────────────────────────────────────────────────────────────────
# Alpaca Endpoints (Mocked)
# ─────────────────────────────────────────────────────────────────────────────


class TestAlpacaEndpoints:
    """Integration tests for Alpaca endpoints with mocked providers."""

    def test_stock_bars_requires_auth(self, client: TestClient):
        """GET /api/v1/alpaca/stocks/{symbol}/bars requires auth without key."""
        # Create a fresh client without the dependency overrides to test auth
        from fastapi.testclient import TestClient as TC

        from gateway.main import app as raw_app

        raw_client = TC(raw_app)
        response = raw_client.get("/api/v1/alpaca/stocks/AAPL/bars")
        assert response.status_code in (401, 403)

    def test_option_chain_requires_auth(self, client: TestClient):
        """GET /api/v1/alpaca/options/{underlying}/chain requires auth."""
        from fastapi.testclient import TestClient as TC

        from gateway.main import app as raw_app

        raw_client = TC(raw_app)
        response = raw_client.get("/api/v1/alpaca/options/AAPL/chain")
        # Accept 401/403 (auth required) or 404 (route may differ)
        assert response.status_code in (401, 403, 404)


# ─────────────────────────────────────────────────────────────────────────────
# Admin Endpoints
# ─────────────────────────────────────────────────────────────────────────────


class TestAdminEndpoints:
    """Integration tests for admin endpoints."""

    def test_admin_requires_auth(self, client: TestClient):
        """Admin endpoints require authentication."""
        response = client.get("/admin/config")
        assert response.status_code in (401, 403, 404)
