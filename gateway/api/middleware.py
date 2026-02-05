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
    last_seen: float = field(default_factory=time.time)

    def consume(self) -> bool:
        """Consume one request. Returns True if allowed."""
        now = time.time()
        self.last_seen = now

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
        self._last_prune = time.time()
        self._prune_interval = 60.0
        self._bucket_ttl = 120.0

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        now = time.time()
        self._prune_buckets(now)

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
    headers: dict[str, str]
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

    def to_dict(self) -> dict:
        """Serialize for Redis storage."""
        import base64

        return {
            "content": base64.b64encode(self.content).decode("ascii"),
            "media_type": self.media_type,
            "headers": self.headers,
            "created_at": self.created_at,
            "ttl": self.ttl,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CacheEntry":
        """Deserialize from Redis storage."""
        import base64

        return cls(
            content=base64.b64decode(data["content"]),
            media_type=data["media_type"],
            headers=data.get("headers", {}),
            created_at=data["created_at"],
            ttl=data["ttl"],
        )


class CacheMiddleware(BaseHTTPMiddleware):
    """Adds cache headers and caching for GET requests.

    Uses HybridCache (L1 memory + L2 Redis) when available for durable caching.

    Headers added:
    - X-Gateway-Cache: HIT or MISS
    - X-Gateway-Cache-Age: Age in milliseconds
    - X-Gateway-Cache-TTL: Remaining TTL in milliseconds
    """

    def __init__(self, app, default_ttl: int = 60, max_size: int = 10000):
        super().__init__(app)
        self.default_ttl = default_ttl
        self._cache = None  # Lazy initialization
        self._cache_initialized = False

    def _get_cache(self):
        """Get cache instance (lazy initialization)."""
        if not self._cache_initialized:
            from gateway.api.deps import get_cache

            self._cache = get_cache()
            self._cache_initialized = True
        return self._cache

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

        # Get cache instance
        cache = self._get_cache()

        # Check cache (async for HybridCache)
        try:
            cached_data = await cache.get(cache_key)
            if cached_data:
                entry = CacheEntry.from_dict(cached_data)
                if not entry.is_expired():
                    headers = dict(entry.headers or {})
                    headers.update(
                        {
                            "X-Gateway-Cache": "HIT",
                            "X-Gateway-Cache-Age": str(entry.age_ms),
                            "X-Gateway-Cache-TTL": str(entry.ttl_remaining),
                        }
                    )
                    return Response(
                        content=entry.content,
                        status_code=200,
                        media_type=entry.media_type,
                        headers=headers,
                    )
        except Exception as e:
            logger.debug("cache_read_error", key=cache_key, error=str(e))

        # Process request
        response = await call_next(request)

        # Cache successful responses
        if response.status_code == 200:
            body_chunks: list[bytes] = []
            async for chunk in response.body_iterator:
                body_chunks.append(chunk)
            body = b"".join(body_chunks)

            # Store headers (excluding hop-by-hop)
            cached_headers = {
                k: v
                for k, v in response.headers.items()
                if k.lower() not in ("content-length", "set-cookie", "connection", "keep-alive")
            }

            entry = CacheEntry(
                content=body,
                media_type=response.media_type or "application/json",
                headers=cached_headers,
                created_at=time.time(),
                ttl=self.default_ttl,
            )

            # Store in cache (async)
            try:
                await cache.set(cache_key, entry.to_dict(), ttl=self.default_ttl)
            except Exception as e:
                logger.debug("cache_write_error", key=cache_key, error=str(e))

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
    # Core market data
    "quote": "quotes",
    "quotes": "quotes",
    "bars": "bars",
    "intraday": "bars",
    "daily": "bars",
    "weekly": "bars",
    "monthly": "bars",
    "trades": "trades",
    "trade": "trades",
    # Options & flow
    "flow": "flow",
    "darkpool": "darkpool",
    "options": "options",
    "chain": "options",
    "greeks": "greeks",
    "gex": "greeks",
    # Fundamentals & news
    "news": "news",
    "articles": "news",
    "profile": "fundamentals",
    "overview": "fundamentals",
    "earnings": "fundamentals",
    "financials": "fundamentals",
    "filings": "filings",
    "facts": "filings",
    # UW market sentiment
    "tide": "market_tide",
    "market_tide": "market_tide",
    "sector_tide": "sector_tide",
    # UW alternative data
    "etf": "etf",
    "holdings": "etf",
    "flows": "etf",
    "shorts": "shorts",
    "short_interest": "shorts",
    "ftd": "shorts",
    "screener": "screener",
    # UW political/institutional
    "insiders": "insiders",
    "institutions": "institutions",
    "politicians": "politicians",
    # Analytics
    "volatility": "analytics",
    "iv_rank": "analytics",
    "seasonality": "analytics",
    "max_pain": "analytics",
    # Forex
    "forex": "forex",
    "rates": "forex",
    # Company fundamentals
    "company_overview": "fundamentals",
    "income_statement": "fundamentals",
    "balance_sheet": "fundamentals",
    "cash_flow": "fundamentals",
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
            body_chunks: list[bytes] = []
            async for chunk in response.body_iterator:
                body_chunks.append(chunk)
            body = b"".join(body_chunks)

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

            # Publish to data sink for Heber lakehouse (non-blocking)
            from gateway.api.deps import get_sink_registry

            sink_registry = get_sink_registry()
            if sink_registry:
                import asyncio

                topic = f"gateway.rest.{feed}"
                # Fire-and-forget publish (DataSinkRegistry handles task lifecycle)
                asyncio.create_task(sink_registry.publish_all(topic, envelope))

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


# ─────────────────────────────────────────────────────────────────────────────
# Security Headers Middleware (PRD 7.2.2)
# ─────────────────────────────────────────────────────────────────────────────


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Adds security headers to all responses.

    Headers added:
    - Strict-Transport-Security (HSTS): max-age=31536000; includeSubDomains
    - X-Content-Type-Options: nosniff
    - X-Frame-Options: DENY
    - X-XSS-Protection: 1; mode=block
    - Referrer-Policy: strict-origin-when-cross-origin
    """

    def __init__(self, app, hsts_max_age: int = 31536000, include_subdomains: bool = True):
        super().__init__(app)
        self.hsts_max_age = hsts_max_age
        self.include_subdomains = include_subdomains

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Add security headers to response."""
        response = await call_next(request)

        # HSTS header (PRD 7.2.2)
        hsts_value = f"max-age={self.hsts_max_age}"
        if self.include_subdomains:
            hsts_value += "; includeSubDomains"
        response.headers["Strict-Transport-Security"] = hsts_value

        # Additional security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        return response


# ─────────────────────────────────────────────────────────────────────────────
# Global Rate Limit Middleware (PRD 7.5.1-2, 7.6.1)
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class IPConnectionTracker:
    """Track connections per IP for DDoS protection."""

    requests: int = 0
    first_request: float = field(default_factory=time.time)
    blocked_until: float = 0.0
    last_seen: float = field(default_factory=time.time)


class GlobalRateLimitMiddleware(BaseHTTPMiddleware):
    """Global and per-IP rate limiting.

    Implements:
    - PRD 7.5.1: Global limit 10,000 req/min
    - PRD 7.5.2: Per-IP limit 1,000 req/min
    - PRD 7.6.1: Max 10 concurrent connections per IP

    Uses sliding window for rate limiting.
    """

    def __init__(
        self,
        app,
        global_limit: int = 10000,
        per_ip_limit: int = 1000,
        max_connections_per_ip: int = 10,
        window_seconds: int = 60,
    ):
        super().__init__(app)
        self.global_limit = global_limit
        self.per_ip_limit = per_ip_limit
        self.max_connections_per_ip = max_connections_per_ip
        self.window_seconds = window_seconds

        # Tracking state
        self._global_requests = 0
        self._global_window_start = time.time()
        self._ip_trackers: dict[str, IPConnectionTracker] = {}
        self._blocked_ips: set[str] = set()
        self._last_prune = time.time()
        self._prune_interval = 60.0

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from request."""
        # Check X-Forwarded-For for proxy scenarios
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        # Fall back to direct connection
        return request.client.host if request.client else "unknown"

    def _reset_window_if_needed(self) -> None:
        """Reset global window if expired."""
        now = time.time()
        if now - self._global_window_start >= self.window_seconds:
            self._global_requests = 0
            self._global_window_start = now

    def _check_ip_limit(self, ip: str) -> bool:
        """Check if IP is within rate limit. Returns True if allowed."""
        now = time.time()

        # Check if blocked
        if ip in self._blocked_ips:
            tracker = self._ip_trackers.get(ip)
            if tracker and tracker.blocked_until > now:
                return False
            # Unblock if time expired
            self._blocked_ips.discard(ip)

        # Get or create tracker
        tracker = self._ip_trackers.get(ip)
        if not tracker or now - tracker.first_request >= self.window_seconds:
            self._ip_trackers[ip] = IPConnectionTracker(
                requests=1, first_request=now, last_seen=now
            )
            return True

        # Check limit
        tracker.requests += 1
        tracker.last_seen = now
        if tracker.requests > self.per_ip_limit:
            # Block for remainder of window
            tracker.blocked_until = tracker.first_request + self.window_seconds
            self._blocked_ips.add(ip)
            logger.warning("ip_rate_limited", ip=ip, requests=tracker.requests)
            return False

        return True

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Check global and per-IP limits."""
        self._prune_ip_trackers()
        # Skip for health endpoints
        if request.url.path in ("/health", "/health/ready"):
            return await call_next(request)

        # Reset window if needed
        self._reset_window_if_needed()

        # Check global limit (PRD 7.5.1)
        if self._global_requests >= self.global_limit:
            logger.warning("global_rate_limit_exceeded", current=self._global_requests)
            return Response(
                content=json.dumps(
                    {
                        "success": False,
                        "error": {
                            "code": "GW-E4029",
                            "message": "Global rate limit exceeded",
                        },
                    }
                ),
                status_code=429,
                media_type="application/json",
            )

        # Check per-IP limit (PRD 7.5.2)
        client_ip = self._get_client_ip(request)
        if not self._check_ip_limit(client_ip):
            return Response(
                content=json.dumps(
                    {
                        "success": False,
                        "error": {
                            "code": "GW-E4029",
                            "message": "IP rate limit exceeded",
                        },
                    }
                ),
                status_code=429,
                media_type="application/json",
            )

        # Increment global counter
        self._global_requests += 1

        return await call_next(request)

    def _prune_ip_trackers(self) -> None:
        """Prune idle IP trackers and expired blocks."""
        now = time.time()
        if now - self._last_prune < self._prune_interval:
            return

        cutoff = now - (self.window_seconds * 2)
        for ip, tracker in list(self._ip_trackers.items()):
            if tracker.blocked_until > now:
                continue
            if tracker.last_seen < cutoff:
                self._ip_trackers.pop(ip, None)
                self._blocked_ips.discard(ip)

        # Clean up expired blocks with no tracker
        for ip in list(self._blocked_ips):
            maybe_tracker = self._ip_trackers.get(ip)
            if maybe_tracker is None or maybe_tracker.blocked_until <= now:
                self._blocked_ips.discard(ip)

        self._last_prune = now

    def block_ip(self, ip: str, duration_seconds: int = 3600) -> None:
        """Manually block an IP (PRD 7.6.3)."""
        tracker = self._ip_trackers.get(ip)
        if not tracker:
            tracker = IPConnectionTracker()
            self._ip_trackers[ip] = tracker
        tracker.blocked_until = time.time() + duration_seconds
        self._blocked_ips.add(ip)
        logger.info("ip_blocked", ip=ip, duration=duration_seconds)

    def unblock_ip(self, ip: str) -> None:
        """Remove IP from blocklist."""
        self._blocked_ips.discard(ip)
        if ip in self._ip_trackers:
            self._ip_trackers[ip].blocked_until = 0.0
        logger.info("ip_unblocked", ip=ip)

    def get_blocked_ips(self) -> list[str]:
        """Get list of currently blocked IPs."""
        return list(self._blocked_ips)
