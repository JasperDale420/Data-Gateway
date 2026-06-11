"""API middleware for rate limiting, cache headers, and envelope wrapping.

Re-exports every public middleware class and helper so existing imports
(``from gateway.api.middleware import CacheMiddleware``) keep working after
the module was split into a package.
"""

from gateway.api.middleware.cache import CacheEntry, CacheMiddleware
from gateway.api.middleware.envelope import (
    CANONICAL_ROUTE_FEEDS,
    FEED_MAPPING,
    ROUTE_PATTERNS,
    SKIP_PATHS,
    EventEnvelopeMiddleware,
)
from gateway.api.middleware.global_ratelimit import (
    GlobalRateLimitMiddleware,
    IPConnectionTracker,
)
from gateway.api.middleware.metrics import RequestMetricsMiddleware
from gateway.api.middleware.ratelimit import RateLimitBucket, RateLimitMiddleware
from gateway.api.middleware.security_headers import SecurityHeadersMiddleware
from gateway.api.middleware.validation import InputValidationMiddleware

__all__ = [
    "CANONICAL_ROUTE_FEEDS",
    "FEED_MAPPING",
    "ROUTE_PATTERNS",
    "SKIP_PATHS",
    "CacheEntry",
    "CacheMiddleware",
    "EventEnvelopeMiddleware",
    "GlobalRateLimitMiddleware",
    "IPConnectionTracker",
    "InputValidationMiddleware",
    "RateLimitBucket",
    "RateLimitMiddleware",
    "RequestMetricsMiddleware",
    "SecurityHeadersMiddleware",
]
