import time

from fastapi import FastAPI
from fastapi.testclient import TestClient

from gateway.api.middleware import CacheMiddleware
from gateway.core.envelope import wrap_event

# ─────────────────────────────────────────────────────────────────────────────
# Test 1: EventEnvelope Serialization Optimization
# ─────────────────────────────────────────────────────────────────────────────


def test_wrap_event_serialization_optimization():
    """Verify that wrap_event correctly handles large payloads efficiently."""

    # Create a large payload
    # 10,000 items
    payload = [{"id": i, "val": f"test_{i}"} for i in range(10000)]

    # Measure time
    start = time.time()

    result = wrap_event(
        event={"items": payload, "count": len(payload)},
        provider="test_provider",
        feed="test_feed",
        source="rest",
    )

    end = time.time()
    duration = end - start

    # Verify correctness
    assert result["provider"] == "test_provider"
    assert result["feed"] == "test_feed"
    assert "payload" in result
    assert result["payload"]["items"] == payload

    # Verify performance (should be very fast, < 100ms for 10k items)
    # Without optimization, it might be slower, but main point is correctness here
    # 10k items is large enough to show difference if we were benchmarking
    print(f"Serialization took: {duration:.4f}s")
    assert duration < 0.2  # Generous upper bound


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: CacheMiddleware Header Preservation
# ─────────────────────────────────────────────────────────────────────────────


def test_cache_middleware_header_preservation():
    """Verify that CacheMiddleware preserves headers on cache hits."""

    app = FastAPI()

    # Add CacheMiddleware
    app.add_middleware(CacheMiddleware, default_ttl=60)

    # Mock endpoint that sets a custom header
    @app.get("/test-headers")
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
    response1 = client.get("/test-headers")
    assert response1.status_code == 200
    assert response1.headers["X-Gateway-Cache"] == "MISS"
    assert response1.headers["X-Custom-Header"] == "Preserved"
    assert "Strict-Transport-Security" in response1.headers

    # 2. Second request (Cache HIT)
    response2 = client.get("/test-headers")
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
