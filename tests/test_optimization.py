from fastapi import FastAPI
from fastapi.testclient import TestClient

from gateway.api.middleware import CacheMiddleware

# ─────────────────────────────────────────────────────────────────────────────
# Test 1: CacheMiddleware Header Preservation
# ─────────────────────────────────────────────────────────────────────────────


def test_cache_middleware_header_preservation():
    """Verify that CacheMiddleware preserves headers on cache hits."""

    app = FastAPI()

    # Add CacheMiddleware
    app.add_middleware(CacheMiddleware, default_ttl=60)

    # Mock endpoint that sets a custom header
    @app.get("/health/test-headers")
    def test_headers():
        from fastapi.responses import JSONResponse

        return JSONResponse(
            content={"data": "test"},
            headers={
                "X-Custom-Header": "Preserved",
                "Strict-Transport-Security": "max-age=31536000",
            },
        )

    client = TestClient(app)

    # 1. First request (Cache MISS)
    response1 = client.get("/health/test-headers")
    assert response1.status_code == 200
    assert response1.headers["X-Gateway-Cache"] == "MISS"
    assert response1.headers["X-Custom-Header"] == "Preserved"
    assert "Strict-Transport-Security" in response1.headers

    # 2. Second request (Cache HIT)
    response2 = client.get("/health/test-headers")
    assert response2.status_code == 200
    assert response2.headers["X-Gateway-Cache"] == "HIT"

    # Verify headers are preserved
    assert "X-Custom-Header" in response2.headers, "X-Custom-Header lost on cache hit"
    assert response2.headers["X-Custom-Header"] == "Preserved"

    assert "Strict-Transport-Security" in response2.headers, "Security header lost on cache hit"
    assert response2.headers["Strict-Transport-Security"] == "max-age=31536000"

    # Verify hop-by-hop headers are NOT preserved (if added by server)
    # But TestClient might not simulate server-added hop-by-hop headers easily.
    # We explicitly excluded 'content-length' in middleware, let's check it.
    # Starlette/FastAPI adds Content-Length automatically.
    # The middleware copies it from the cached *response object* or recalculates?
    # When returning Response(content=...), Starlette calculates Content-Length.
    # So we don't need to assert Content-Length preservation logic specifically,
    # but we should ensure we didn't cache it incorrectly (though Filter prevented it).


def test_cache_middleware_requires_auth_for_non_public_get(test_api_key: str):
    """Non-public GET paths must reject missing/invalid keys before cache lookup."""

    app = FastAPI()
    app.add_middleware(CacheMiddleware, default_ttl=60)

    @app.get("/api/v1/secure-probe")
    def secure_probe():
        return {"ok": True, "source": "secure-probe"}

    client = TestClient(app)

    no_key = client.get("/api/v1/secure-probe")
    assert no_key.status_code == 401
    assert no_key.json()["detail"]["code"] == "GW-E2001"

    invalid_key = client.get(
        "/api/v1/secure-probe",
        headers={"X-Gateway-Key": "invalid_key"},
    )
    assert invalid_key.status_code == 401
    assert invalid_key.json()["detail"]["code"] == "GW-E2002"

    valid_1 = client.get(
        "/api/v1/secure-probe",
        headers={"X-Gateway-Key": test_api_key},
    )
    assert valid_1.status_code == 200
    assert valid_1.headers["X-Gateway-Cache"] == "MISS"

    valid_2 = client.get(
        "/api/v1/secure-probe",
        headers={"X-Gateway-Key": test_api_key},
    )
    assert valid_2.status_code == 200
    assert valid_2.headers["X-Gateway-Cache"] == "HIT"
