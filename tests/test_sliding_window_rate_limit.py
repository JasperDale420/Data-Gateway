"""Tests for sliding window rate limiting and Retry-After headers.

Covers:
- Sliding window bucket behavior under burst conditions
- Retry-After header presence on 429 responses
- Per-client rate limiting with sliding window
"""

from __future__ import annotations

import time

from starlette.testclient import TestClient

from gateway.api.middleware import RateLimitBucket, RateLimitMiddleware

# ─────────────────────────────────────────────────────────────────────────────
# Sliding Window Bucket Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestSlidingWindowBucket:
    """Test the sliding window RateLimitBucket implementation."""

    def test_allows_requests_within_limit(self) -> None:
        bucket = RateLimitBucket(limit=5, remaining=5)
        for _ in range(5):
            assert bucket.consume() is True
        assert bucket.remaining == 0

    def test_rejects_requests_over_limit(self) -> None:
        bucket = RateLimitBucket(limit=3, remaining=3)
        for _ in range(3):
            assert bucket.consume() is True
        assert bucket.consume() is False
        assert bucket.remaining == 0

    def test_sliding_window_expires_oldest_requests(self) -> None:
        """Verify that old requests roll off the window continuously."""
        bucket = RateLimitBucket(limit=2, remaining=2)
        bucket._window = 0.5  # 500ms window for fast testing

        # Fill the bucket
        assert bucket.consume() is True
        assert bucket.consume() is True
        assert bucket.consume() is False  # Full

        # Wait for oldest request to expire
        time.sleep(0.6)

        # Window should have rolled — oldest request expired
        assert bucket.consume() is True

    def test_reset_after_reports_correct_time(self) -> None:
        bucket = RateLimitBucket(limit=5, remaining=5)
        assert bucket.reset_after == 0  # No requests yet

        bucket.consume()
        # Should be close to 60 seconds (the default window)
        assert 55 <= bucket.reset_after <= 60

    def test_reset_after_zero_when_empty(self) -> None:
        bucket = RateLimitBucket(limit=5, remaining=5)
        assert bucket.reset_after == 0

    def test_remaining_tracks_available_slots(self) -> None:
        bucket = RateLimitBucket(limit=10, remaining=10)
        for i in range(7):
            bucket.consume()
        assert bucket.remaining == 3

    def test_burst_then_gradual_recovery(self) -> None:
        """Simulate burst traffic followed by gradual slot recovery."""
        bucket = RateLimitBucket(limit=3, remaining=3)
        bucket._window = 0.3  # 300ms window

        # Burst: consume all 3 at once
        for _ in range(3):
            assert bucket.consume() is True
        assert bucket.consume() is False

        # Wait for window to expire
        time.sleep(0.35)

        # All 3 slots should be available again
        assert bucket.consume() is True
        assert bucket.consume() is True
        assert bucket.consume() is True


# ─────────────────────────────────────────────────────────────────────────────
# Retry-After Header Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestRetryAfterHeader:
    """Test that 429 responses include the Retry-After header."""

    def _make_app(self, limit: int = 3):
        """Create a minimal ASGI app with rate limiting middleware."""
        from starlette.applications import Starlette
        from starlette.responses import PlainTextResponse
        from starlette.routing import Route

        async def homepage(request):
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/test", homepage)])
        app.add_middleware(RateLimitMiddleware, default_limit=limit)
        return app

    def test_429_includes_retry_after_header(self) -> None:
        app = self._make_app(limit=2)
        client = TestClient(app)

        # Exhaust the limit
        client.get("/test")
        client.get("/test")

        # Third request should be rate limited
        response = client.get("/test")
        assert response.status_code == 429
        assert "Retry-After" in response.headers
        retry_after = int(response.headers["Retry-After"])
        assert retry_after > 0
        assert retry_after <= 60

    def test_429_includes_rate_limit_headers(self) -> None:
        app = self._make_app(limit=2)
        client = TestClient(app)

        # Exhaust the limit
        client.get("/test")
        client.get("/test")

        # Third request should have all rate limit headers
        response = client.get("/test")
        assert response.status_code == 429
        assert "X-RateLimit-Limit" in response.headers
        assert "X-RateLimit-Remaining" in response.headers
        assert "X-RateLimit-Reset" in response.headers
        assert "X-RateLimit-Reset-After" in response.headers
        assert response.headers["X-RateLimit-Remaining"] == "0"

    def test_successful_response_includes_rate_limit_headers(self) -> None:
        app = self._make_app(limit=10)
        client = TestClient(app)

        response = client.get("/test")
        assert response.status_code == 200
        assert "X-RateLimit-Limit" in response.headers
        assert response.headers["X-RateLimit-Limit"] == "10"
