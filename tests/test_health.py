"""Tests for health endpoints."""


def test_liveness_returns_ok(client):
    """GET /health returns status ok."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readiness_returns_ready(client):
    """GET /health/ready returns ready status."""
    response = client.get("/health/ready")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ready"
    assert "checks" in data
    assert data["checks"]["cache"] == "ok"


def test_status_returns_detailed_info(client):
    """GET /health/status returns detailed status."""
    response = client.get("/health/status")
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "ok"
    assert "version" in data
    assert "timestamp" in data
    assert "components" in data

    # Check cache stats
    assert "cache" in data["components"]
    assert data["components"]["cache"]["status"] == "ok"

    # Check connection stats
    assert "connections" in data["components"]
    assert data["components"]["connections"]["status"] == "ok"


def test_root_endpoint(client):
    """GET / returns gateway info."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()

    assert data["name"] == "Data Gateway"
    assert "version" in data
    assert data["status"] == "ok"
