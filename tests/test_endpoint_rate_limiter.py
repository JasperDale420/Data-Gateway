"""Tests for endpoint-level rate limiting.

Covers:
- Sliding window rate limiting per endpoint group
- Concurrent session limits
- Client-scoped limits
- Limit reset behavior
- Status reporting
"""

from __future__ import annotations

import time

import pytest

from gateway.core.rate_limiter import (
    EndpointRateLimitConfig,
    EndpointRateLimiter,
    EndpointRateLimitExceeded,
)

# ─────────────────────────────────────────────────────────────────────────────
# Sliding window tests
# ─────────────────────────────────────────────────────────────────────────────


class TestEndpointRateLimiterSlidingWindow:
    """Test sliding-window rate limiting per endpoint group."""

    def test_allows_requests_within_limit(self) -> None:
        config = EndpointRateLimitConfig(
            requests_per_window=5,
            window_seconds=60,
        )
        limiter = EndpointRateLimiter()
        limiter.configure("bulk_create", config)

        for _ in range(5):
            limiter.acquire("bulk_create", client_id="client-a")

    def test_rejects_requests_over_limit(self) -> None:
        config = EndpointRateLimitConfig(
            requests_per_window=3,
            window_seconds=60,
        )
        limiter = EndpointRateLimiter()
        limiter.configure("bulk_create", config)

        for _ in range(3):
            limiter.acquire("bulk_create", client_id="client-a")

        with pytest.raises(EndpointRateLimitExceeded) as exc_info:
            limiter.acquire("bulk_create", client_id="client-a")

        assert exc_info.value.endpoint == "bulk_create"
        assert exc_info.value.retry_after > 0

    def test_different_clients_have_separate_limits(self) -> None:
        config = EndpointRateLimitConfig(
            requests_per_window=2,
            window_seconds=60,
        )
        limiter = EndpointRateLimiter()
        limiter.configure("bulk_create", config)

        # Client A uses both slots
        for _ in range(2):
            limiter.acquire("bulk_create", client_id="client-a")

        # Client B should still have their own budget
        limiter.acquire("bulk_create", client_id="client-b")

    def test_different_endpoints_have_separate_limits(self) -> None:
        limiter = EndpointRateLimiter()
        limiter.configure(
            "bulk_create",
            EndpointRateLimitConfig(requests_per_window=2, window_seconds=60),
        )
        limiter.configure(
            "replay_create",
            EndpointRateLimitConfig(requests_per_window=2, window_seconds=60),
        )

        for _ in range(2):
            limiter.acquire("bulk_create", client_id="test")

        # Different endpoint should still work
        limiter.acquire("replay_create", client_id="test")

    def test_window_resets_after_expiry(self) -> None:
        config = EndpointRateLimitConfig(
            requests_per_window=2,
            window_seconds=0.1,  # 100ms window for fast test
        )
        limiter = EndpointRateLimiter()
        limiter.configure("test_ep", config)

        # Exhaust limit
        for _ in range(2):
            limiter.acquire("test_ep", client_id="test")

        with pytest.raises(EndpointRateLimitExceeded):
            limiter.acquire("test_ep", client_id="test")

        # Wait for window to expire
        time.sleep(0.15)

        # Should be allowed again
        limiter.acquire("test_ep", client_id="test")

    def test_unconfigured_endpoint_is_allowed(self) -> None:
        limiter = EndpointRateLimiter()
        # No configuration for "unknown_ep"
        limiter.acquire("unknown_ep", client_id="test")


# ─────────────────────────────────────────────────────────────────────────────
# Concurrent session limit tests
# ─────────────────────────────────────────────────────────────────────────────


class TestEndpointRateLimiterConcurrency:
    """Test concurrent session limits for long-running operations."""

    def test_allows_sessions_within_limit(self) -> None:
        config = EndpointRateLimitConfig(
            max_concurrent=3,
        )
        limiter = EndpointRateLimiter()
        limiter.configure("replay_session", config)

        # Acquire 3 concurrent slots
        for i in range(3):
            limiter.acquire_concurrent("replay_session", session_id=f"s{i}")

    def test_rejects_sessions_over_concurrent_limit(self) -> None:
        config = EndpointRateLimitConfig(
            max_concurrent=2,
        )
        limiter = EndpointRateLimiter()
        limiter.configure("replay_session", config)

        limiter.acquire_concurrent("replay_session", session_id="s1")
        limiter.acquire_concurrent("replay_session", session_id="s2")

        with pytest.raises(EndpointRateLimitExceeded) as exc_info:
            limiter.acquire_concurrent("replay_session", session_id="s3")

        assert exc_info.value.endpoint == "replay_session"
        assert "concurrent" in exc_info.value.message.lower()

    def test_release_frees_slot(self) -> None:
        config = EndpointRateLimitConfig(max_concurrent=1)
        limiter = EndpointRateLimiter()
        limiter.configure("replay_session", config)

        limiter.acquire_concurrent("replay_session", session_id="s1")

        with pytest.raises(EndpointRateLimitExceeded):
            limiter.acquire_concurrent("replay_session", session_id="s2")

        # Release first session
        limiter.release_concurrent("replay_session", session_id="s1")

        # Now should be allowed
        limiter.acquire_concurrent("replay_session", session_id="s2")

    def test_duplicate_session_id_is_idempotent(self) -> None:
        config = EndpointRateLimitConfig(max_concurrent=1)
        limiter = EndpointRateLimiter()
        limiter.configure("replay_session", config)

        limiter.acquire_concurrent("replay_session", session_id="s1")
        # Same session ID should not consume another slot
        limiter.acquire_concurrent("replay_session", session_id="s1")

    def test_release_nonexistent_session_is_safe(self) -> None:
        limiter = EndpointRateLimiter()
        limiter.configure("replay_session", EndpointRateLimitConfig(max_concurrent=5))

        # Should not raise
        limiter.release_concurrent("replay_session", session_id="does-not-exist")


# ─────────────────────────────────────────────────────────────────────────────
# Combined limits (window + concurrent)
# ─────────────────────────────────────────────────────────────────────────────


class TestEndpointRateLimiterCombined:
    """Test combined window + concurrent limits."""

    def test_both_limits_enforced(self) -> None:
        config = EndpointRateLimitConfig(
            requests_per_window=10,
            window_seconds=60,
            max_concurrent=2,
        )
        limiter = EndpointRateLimiter()
        limiter.configure("replay", config)

        # Concurrent check
        limiter.acquire_concurrent("replay", session_id="s1")
        limiter.acquire_concurrent("replay", session_id="s2")

        with pytest.raises(EndpointRateLimitExceeded):
            limiter.acquire_concurrent("replay", session_id="s3")


# ─────────────────────────────────────────────────────────────────────────────
# Status reporting
# ─────────────────────────────────────────────────────────────────────────────


class TestEndpointRateLimiterStatus:
    """Test status reporting for admin visibility."""

    def test_get_status_returns_all_endpoints(self) -> None:
        limiter = EndpointRateLimiter()
        limiter.configure(
            "bulk_create",
            EndpointRateLimitConfig(requests_per_window=10, window_seconds=3600),
        )
        limiter.configure(
            "replay_session",
            EndpointRateLimitConfig(max_concurrent=5),
        )

        status = limiter.get_status()

        assert "bulk_create" in status
        assert "replay_session" in status

    def test_get_status_shows_usage(self) -> None:
        limiter = EndpointRateLimiter()
        limiter.configure(
            "bulk_create",
            EndpointRateLimitConfig(requests_per_window=10, window_seconds=3600),
        )

        limiter.acquire("bulk_create", client_id="test")
        limiter.acquire("bulk_create", client_id="test")

        status = limiter.get_status()
        bulk_status = status["bulk_create"]

        assert bulk_status["config"]["requests_per_window"] == 10
        assert bulk_status["clients"]["test"]["used"] == 2

    def test_get_status_single_endpoint(self) -> None:
        limiter = EndpointRateLimiter()
        limiter.configure(
            "bulk_create",
            EndpointRateLimitConfig(requests_per_window=10, window_seconds=3600),
        )

        status = limiter.get_status("bulk_create")
        assert "config" in status

    def test_get_status_unknown_endpoint(self) -> None:
        limiter = EndpointRateLimiter()
        status = limiter.get_status("unknown")
        assert status == {"endpoint": "unknown", "status": "not_configured"}
