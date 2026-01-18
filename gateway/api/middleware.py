"""API middleware for rate limiting, cache headers, and envelope wrapping."""

import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from gateway.core.envelope import wrap_event

logger = structlog.get_logger()


@dataclass
class RateLimitBucket:
    """Per-client rate limit tracking."""

    limit: int
    remaining: int
    reset_at: float = field(default_factory=lambda: time.time() + 60)

    def consume(self) -> bool:
        """Consume one request. Returns True if allowed."""
        now = time.time()

        # Reset bucket if window expired
        if now >= self.reset_at:
            self.remaining = self.limit
            self.reset_at = now + 60

        if self.remaining > 0:
            self.remaining -= 1
            return True
        return False

    @property
    def reset_after(self) -> int:
        """Seconds until reset."""
        return max(0, int(self.reset_at - time.time()))


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Adds rate limit headers to responses.

    Headers added:
    - X-RateLimit-Limit: Max requests per minute
    - X-RateLimit-Remaining: Requests left in window
    - X-RateLimit-Reset: Unix timestamp of reset
    - X-RateLimit-Reset-After: Seconds until reset

    Per-client limits are read from client permissions if available.
    """

    def __init__(self, app, default_limit: int = 600):
        super().__init__(app)
        self.default_limit = default_limit
        self._buckets: dict[str, RateLimitBucket] = {}

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Extract client identifier and limit
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
            return Response(
                content='{"error": {"code": "GW-E4001", "message": "Rate limit exceeded"}}',
                status_code=429,
                media_type="application/json",
                headers=self._rate_limit_headers(bucket),
            )

        # Process request
        response = await call_next(request)

        # Add rate limit headers
        for key, value in self._rate_limit_headers(bucket).items():
            response.headers[key] = value

        return response

    def _get_client_info(self, request: Request) -> tuple[str, int]:
        """Extract client ID and rate limit from request."""
        # Check if client is already authenticated (from deps)
        if hasattr(request.state, "client"):
            client = request.state.client
            return client.id, client.permissions.rate_limit

        # Check X-Gateway-Key header for identification
        api_key = request.headers.get("X-Gateway-Key", "")
        if api_key:
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


@dataclass
class CacheEntry:
    """Cached response with metadata."""

    content: bytes
    media_type: str
    created_at: float
    ttl: int

    @property
    def age_ms(self) -> int:
        """Age in milliseconds."""
        return int((time.time() - self.created_at) * 1000)

    @property
    def ttl_remaining(self) -> int:
        """Remaining TTL in milliseconds."""
        elapsed = time.time() - self.created_at
        return max(0, int((self.ttl - elapsed) * 1000))

    def is_expired(self) -> bool:
        """Check if entry has expired."""
        return time.time() - self.created_at > self.ttl


class CacheMiddleware(BaseHTTPMiddleware):
    """Adds cache headers and caching for GET requests.

    Headers added:
    - X-Gateway-Cache: HIT or MISS
    - X-Gateway-Cache-Age: Age in milliseconds
    - X-Gateway-Cache-TTL: Remaining TTL in milliseconds
    """

    def __init__(self, app, default_ttl: int = 60):
        super().__init__(app)
        self.default_ttl = default_ttl
        self._cache: dict[str, CacheEntry] = {}

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Only cache GET requests
        if request.method != "GET":
            return await call_next(request)

        # Check for cache bypass
        if request.headers.get("X-Gateway-Cache") == "bypass":
            response = await call_next(request)
            response.headers["X-Gateway-Cache"] = "BYPASS"
            return response

        # Generate cache key
        cache_key = self._cache_key(request)

        # Check cache
        entry = self._cache.get(cache_key)
        if entry and not entry.is_expired():
            return Response(
                content=entry.content,
                status_code=200,
                media_type=entry.media_type,
                headers={
                    "X-Gateway-Cache": "HIT",
                    "X-Gateway-Cache-Age": str(entry.age_ms),
                    "X-Gateway-Cache-TTL": str(entry.ttl_remaining),
                },
            )

        # Process request
        response = await call_next(request)

        # Cache successful responses
        if response.status_code == 200:
            body = b""
            async for chunk in response.body_iterator:
                body += chunk

            self._cache[cache_key] = CacheEntry(
                content=body,
                media_type=response.media_type or "application/json",
                created_at=time.time(),
                ttl=self.default_ttl,
            )

            return Response(
                content=body,
                status_code=200,
                media_type=response.media_type,
                headers={
                    **dict(response.headers),
                    "X-Gateway-Cache": "MISS",
                    "X-Gateway-Cache-Age": "0",
                    "X-Gateway-Cache-TTL": str(self.default_ttl * 1000),
                },
            )

        response.headers["X-Gateway-Cache"] = "MISS"
        return response

    def _cache_key(self, request: Request) -> str:
        """Generate cache key from request."""
        return f"{request.method}:{request.url.path}:{request.url.query}"


# ─────────────────────────────────────────────────────────────────────────────
# EventEnvelope Middleware - Wraps all REST API responses in universal envelope
# ─────────────────────────────────────────────────────────────────────────────

# Route patterns to extract provider/feed from API paths
ROUTE_PATTERNS = [
    # /api/v1/{provider}/{feed}/{symbol} or /api/v1/{provider}/{endpoint}
    re.compile(r"^/api/v1/(?P<provider>alpaca|finnhub|alphavantage|yf|uw|sec|news)/(?P<feed>\w+)"),
    # /api/v1/{provider} for health/status endpoints
    re.compile(r"^/api/v1/(?P<provider>\w+)$"),
]

# Map endpoint patterns to feed types
FEED_MAPPING = {
    "quote": "quotes",
    "quotes": "quotes",
    "bars": "bars",
    "intraday": "bars",
    "daily": "bars",
    "weekly": "bars",
    "monthly": "bars",
    "trades": "trades",
    "trade": "trades",
    "flow": "flow",
    "darkpool": "darkpool",
    "news": "news",
    "articles": "news",
    "profile": "fundamentals",
    "overview": "fundamentals",
    "earnings": "fundamentals",
    "financials": "fundamentals",
    "filings": "filings",
    "facts": "filings",
    "insiders": "filings",
    "options": "options",
    "chain": "options",
    "greeks": "greeks",
    "gex": "greeks",
}

# Paths to skip envelope wrapping (health, metrics, admin, etc)
SKIP_PATHS = {
    "/health",
    "/ready",
    "/metrics",
    "/api/v1/health",
    "/api/v1/admin",
    "/docs",
    "/redoc",
    "/openapi.json",
}


class EventEnvelopeMiddleware(BaseHTTPMiddleware):
    """Wraps all REST API responses in EventEnvelope for universal routing/storage.

    This middleware intercepts successful API responses and wraps the data payload
    in an EventEnvelope, providing:
    - Idempotent event_id for deduplication
    - Consistent instrument_key for routing
    - Schema versioning for evolution
    - Lineage tracking for debugging

    Skipped paths: health, metrics, admin endpoints
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip non-API and system paths
        path = request.url.path
        if not path.startswith("/api/v1/") or any(path.startswith(skip) for skip in SKIP_PATHS):
            return await call_next(request)

        # Skip non-GET requests (POST, PUT, DELETE are typically mutations)
        if request.method != "GET":
            return await call_next(request)

        # Process request
        response = await call_next(request)

        # Only wrap successful JSON responses
        if response.status_code != 200:
            return response

        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type:
            return response

        try:
            # Read response body
            body = b""
            async for chunk in response.body_iterator:
                body += chunk

            # Parse JSON
            data = json.loads(body)

            # Skip if already wrapped or is an error
            if "envelope" in data or not data.get("success", True):
                return Response(
                    content=body,
                    status_code=response.status_code,
                    media_type=response.media_type,
                    headers=dict(response.headers),
                )

            # Extract provider and feed from path
            provider, feed = self._extract_route_info(path)

            # Get the actual data payload
            payload = data.get("data", data)

            # Handle list responses (wrap each item)
            if isinstance(payload, list) and len(payload) > 0:
                # For lists, wrap the entire response as one envelope
                envelope = wrap_event(
                    event={"items": payload, "count": len(payload)},
                    provider=provider,
                    feed=feed,
                    source="rest",
                )
            elif isinstance(payload, dict):
                # For single objects, wrap directly
                envelope = wrap_event(
                    event=payload,
                    provider=provider,
                    feed=feed,
                    source="rest",
                )
            else:
                # Skip wrapping for primitive responses
                return Response(
                    content=body,
                    status_code=response.status_code,
                    media_type=response.media_type,
                    headers=dict(response.headers),
                )

            # Build wrapped response
            wrapped = {
                "success": True,
                "envelope": envelope,
                "data": payload,  # Backward compat
                "meta": data.get("meta", {}),
            }

            wrapped_body = json.dumps(wrapped, default=str)

            logger.debug(
                "rest_envelope_wrapped",
                path=path,
                provider=provider,
                feed=feed,
                event_id=envelope.get("event_id"),
            )

            return Response(
                content=wrapped_body,
                status_code=200,
                media_type="application/json",
                headers={
                    **{k: v for k, v in response.headers.items() if k.lower() != "content-length"},
                    "X-Gateway-Envelope": "true",
                    "X-Gateway-Event-Id": envelope.get("event_id", ""),
                },
            )

        except Exception as e:
            logger.warning(
                "envelope_middleware_error",
                path=path,
                error=str(e),
            )
            # Return original response on error
            return Response(
                content=body if "body" in dir() else b"",
                status_code=response.status_code,
                media_type=response.media_type,
                headers=dict(response.headers),
            )

    def _extract_route_info(self, path: str) -> tuple[str, str]:
        """Extract provider and feed from request path."""
        for pattern in ROUTE_PATTERNS:
            match = pattern.match(path)
            if match:
                groups = match.groupdict()
                provider = groups.get("provider", "unknown")

                # Normalize provider names
                provider_map = {
                    "yf": "yfinance",
                    "uw": "unusual_whales",
                }
                provider = provider_map.get(provider, provider)

                # Extract feed from matched group or path segment
                feed_raw = groups.get("feed", "")
                feed = FEED_MAPPING.get(feed_raw, feed_raw or "data")

                return provider, feed

        return "unknown", "data"
