"""Per-client rate limit middleware."""

import time
from collections import deque
from dataclasses import dataclass, field

from fastapi import Request, Response
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from gateway.core.logger import logger
from gateway.core.metrics import record_rate_limit_exceeded


@dataclass
class RateLimitBucket:
    """Per-client sliding window rate limit tracking.

    Uses a deque of timestamps instead of a fixed-window counter so that
    expired requests roll off continuously.  This prevents burst starvation
    where the entire allowance is consumed in <1 second.
    """

    limit: int
    remaining: int  # kept for compatibility but computed from deque
    reset_at: float = field(default_factory=lambda: time.time() + 60)
    last_seen: float = field(default_factory=time.time)
    _timestamps: deque = field(default_factory=deque)
    _window: float = 60.0

    def _cleanup(self, now: float) -> None:
        """Remove timestamps older than the window."""
        cutoff = now - self._window
        while self._timestamps and self._timestamps[0] <= cutoff:
            self._timestamps.popleft()

    def consume(self) -> bool:
        """Consume one request. Returns True if allowed."""
        now = time.time()
        self.last_seen = now
        self._cleanup(now)

        if len(self._timestamps) < self.limit:
            self._timestamps.append(now)
            self.remaining = max(0, self.limit - len(self._timestamps))
            self.reset_at = (self._timestamps[0] + self._window) if self._timestamps else now + self._window
            return True

        # Update bookkeeping for headers
        self.remaining = 0
        self.reset_at = (self._timestamps[0] + self._window) if self._timestamps else now + self._window
        return False

    @property
    def reset_after(self) -> int:
        """Seconds until the oldest request expires from the window."""
        if not self._timestamps:
            return 0
        return max(0, int(self._timestamps[0] + self._window - time.time()))


class RateLimitMiddleware:
    """Adds rate limit headers to responses.

    Headers added:
    - X-RateLimit-Limit: Max requests per minute
    - X-RateLimit-Remaining: Requests left in window
    - X-RateLimit-Reset: Unix timestamp of reset
    - X-RateLimit-Reset-After: Seconds until reset

    Per-client limits are read from client permissions if available.
    """

    def __init__(self, app: ASGIApp, default_limit: int = 600) -> None:
        self.app = app
        self.default_limit = default_limit
        self._buckets: dict[str, RateLimitBucket] = {}
        self._last_prune = time.time()
        self._prune_interval = 60.0
        self._bucket_ttl = 120.0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        now = time.time()
        self._prune_buckets(now)

        # Build a Request just for extracting client info
        request = Request(scope)
        client_id, client_limit = self._get_client_info(request)

        # Get or create bucket with client-specific limit
        if client_id not in self._buckets:
            self._buckets[client_id] = RateLimitBucket(
                limit=client_limit,
                remaining=client_limit,
            )
        bucket = self._buckets[client_id]

        # Update limit if client permissions changed
        if bucket.limit != client_limit:
            bucket.limit = client_limit
            bucket.remaining = min(bucket.remaining, client_limit)

        # Check rate limit
        if not bucket.consume():
            logger.warning(
                "rate_limit_exceeded",
                client_id=client_id,
                limit=bucket.limit,
            )
            record_rate_limit_exceeded(client_id)
            headers = self._rate_limit_headers(bucket)
            headers["Retry-After"] = str(bucket.reset_after)
            response = Response(
                content='{"error": {"code": "GW-E4001", "message": "Rate limit exceeded"}}',
                status_code=429,
                media_type="application/json",
                headers=headers,
            )
            await response(scope, receive, send)
            return

        # Inject rate limit headers into downstream response
        rl_headers = self._rate_limit_headers(bucket)

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                for key, value in rl_headers.items():
                    headers.append(key, value)
            await send(message)

        await self.app(scope, receive, send_wrapper)

    def _prune_buckets(self, now: float) -> None:
        """Prune idle buckets to prevent unbounded growth."""
        if now - self._last_prune < self._prune_interval:
            return

        cutoff = now - self._bucket_ttl
        for client_id, bucket in list(self._buckets.items()):
            if bucket.last_seen < cutoff:
                self._buckets.pop(client_id, None)

        self._last_prune = now

    def _get_client_info(self, request: Request) -> tuple[str, int]:
        """Extract client ID and rate limit from request."""
        # Check if client is already authenticated (from deps)
        if hasattr(request.state, "client"):
            client = request.state.client
            return client.id, client.permissions.rate_limit

        # Check X-Gateway-Key header for identification. This middleware runs
        # BEFORE the auth dependency sets request.state.client, so resolve the
        # client here to honor its per-client rate_limit (e.g. 3roses=6000)
        # instead of always falling back to the default.
        api_key = request.headers.get("X-Gateway-Key", "")
        if api_key:
            from gateway.api.deps import get_authenticator

            client = get_authenticator().client_for_key(api_key)
            if client is not None:
                return client.id, client.permissions.rate_limit
            return f"key:{api_key[:16]}", self.default_limit

        # Fall back to IP
        client_ip = request.client.host if request.client else "unknown"
        return f"ip:{client_ip}", self.default_limit

    def _rate_limit_headers(self, bucket: RateLimitBucket) -> dict[str, str]:
        """Generate rate limit headers."""
        return {
            "X-RateLimit-Limit": str(bucket.limit),
            "X-RateLimit-Remaining": str(bucket.remaining),
            "X-RateLimit-Reset": str(int(bucket.reset_at)),
            "X-RateLimit-Reset-After": str(bucket.reset_after),
        }
