"""Per-provider rate limiting for upstream API calls.

Tracks request counts per provider and enforces their specific rate limits
to prevent 429 errors from upstream APIs.

Provider Limits (official, per plan):
- Alpaca (Algo Trader Plus): 10,000/min market data, 200/min trading
- Finnhub (Free): 60/min, 30/sec
- Alpha Vantage (Free): 5/min, 25/day
- NewsAPI (Developer): 100/day
- Unusual Whales: 120/min, 15K/day
- SEC EDGAR: 10/sec (600/min)
- YFinance: No official limit (conservative estimate)
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import structlog

logger = structlog.get_logger()


class RateLimitExceeded(Exception):
    """Raised when provider rate limit is exceeded."""

    def __init__(self, provider: str, retry_after: int, message: str = ""):
        self.provider = provider
        self.retry_after = retry_after
        self.message = message or f"Rate limit exceeded for {provider}"
        super().__init__(self.message)


@dataclass
class ProviderLimits:
    """Rate limit configuration for a provider."""

    requests_per_minute: int
    requests_per_second: int | None = None
    requests_per_day: int | None = None

    # For providers with different limits per endpoint type
    market_data_per_minute: int | None = None
    fundamentals_per_minute: int | None = None


# Provider rate limit configurations (Alpaca uses paid-tier defaults; override via env)
PROVIDER_LIMITS: dict[str, ProviderLimits] = {
    "alpaca": ProviderLimits(
        requests_per_minute=10000,
        requests_per_second=75,
        market_data_per_minute=10000,
    ),
    "finnhub": ProviderLimits(
        requests_per_minute=60,
        requests_per_second=30,
        market_data_per_minute=900,
        fundamentals_per_minute=300,
    ),
    "alphavantage": ProviderLimits(
        requests_per_minute=5,
        requests_per_day=25,
    ),
    "news": ProviderLimits(
        requests_per_minute=100,  # No per-minute limit in docs; 100/day on free plan
        requests_per_day=100,
    ),
    "unusual_whales": ProviderLimits(
        requests_per_minute=120,
        requests_per_day=15000,
    ),
    "sec": ProviderLimits(
        requests_per_minute=600,  # 10/sec = 600/min
        requests_per_second=10,
    ),
    "yfinance": ProviderLimits(
        requests_per_minute=60,  # No official limit, be conservative
        requests_per_second=5,
    ),
}


@dataclass
class RateLimitBucket:
    """Sliding window rate limit bucket."""

    limit: int
    window_seconds: int
    timestamps: deque[float] = field(default_factory=deque)

    def _cleanup(self, now: float) -> None:
        """Remove expired timestamps."""
        cutoff = now - self.window_seconds
        while self.timestamps and self.timestamps[0] <= cutoff:
            self.timestamps.popleft()

    @property
    def remaining(self) -> int:
        """Requests remaining in current window."""
        self._cleanup(time.time())
        return max(0, self.limit - len(self.timestamps))

    @property
    def reset_after(self) -> float:
        """Seconds until oldest request expires."""
        if not self.timestamps:
            return 0
        oldest = self.timestamps[0]
        return max(0, self.window_seconds - (time.time() - oldest))

    def try_acquire(self) -> bool:
        """Try to acquire a request slot. Returns True if allowed."""
        now = time.time()
        self._cleanup(now)

        if len(self.timestamps) < self.limit:
            self.timestamps.append(now)
            return True
        return False

    def record(self) -> None:
        """Record a request (for tracking without blocking)."""
        self.timestamps.append(time.time())


@dataclass
class ProviderRateLimiter:
    """Rate limiter for a single provider with multiple time windows."""

    provider: str
    per_second: RateLimitBucket | None = None
    per_minute: RateLimitBucket = field(default_factory=lambda: RateLimitBucket(60, 60))
    per_day: RateLimitBucket | None = None

    # Stats
    total_requests: int = 0
    total_throttled: int = 0

    def try_acquire(self) -> tuple[bool, int]:
        """Try to acquire a request slot.

        Returns:
            (allowed, retry_after_seconds)
        """
        # Check per-second limit first (most restrictive window)
        if self.per_second and not self.per_second.try_acquire():
            return False, max(1, int(self.per_second.reset_after) + 1)

        # Check per-minute limit
        if not self.per_minute.try_acquire():
            retry = max(1, int(self.per_minute.reset_after) + 1)
            return False, retry

        # Check per-day limit
        if self.per_day and not self.per_day.try_acquire():
            retry = max(60, int(self.per_day.reset_after) + 1)
            return False, retry

        self.total_requests += 1
        return True, 0

    def get_status(self) -> dict:
        """Get current rate limit status."""
        status = {
            "provider": self.provider,
            "total_requests": self.total_requests,
            "total_throttled": self.total_throttled,
        }

        if self.per_second:
            status["per_second"] = {
                "limit": self.per_second.limit,
                "remaining": self.per_second.remaining,
            }

        status["per_minute"] = {
            "limit": self.per_minute.limit,
            "remaining": self.per_minute.remaining,
            "reset_after": int(self.per_minute.reset_after),
        }

        if self.per_day:
            status["per_day"] = {
                "limit": self.per_day.limit,
                "remaining": self.per_day.remaining,
            }

        return status


class ProviderRateLimitManager:
    """Manages rate limiters for all providers."""

    _instance: Optional["ProviderRateLimitManager"] = None

    def __init__(self):
        self._limiters: dict[str, ProviderRateLimiter] = {}
        self._lock = asyncio.Lock()
        self._initialize_limiters()

    @classmethod
    def get_instance(cls) -> "ProviderRateLimitManager":
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _initialize_limiters(self) -> None:
        """Initialize rate limiters from config.

        Alpaca limits can be overridden via GATEWAY_ALPACA_RATE_LIMIT_PER_MINUTE
        and GATEWAY_ALPACA_RATE_LIMIT_PER_SECOND environment variables.
        """
        # Apply config overrides for Alpaca limits
        try:
            from gateway.config import get_settings

            settings = get_settings()
            alpaca_limits = PROVIDER_LIMITS.get("alpaca")
            if alpaca_limits:
                alpaca_limits.requests_per_minute = settings.alpaca_rate_limit_per_minute
                alpaca_limits.requests_per_second = settings.alpaca_rate_limit_per_second
                alpaca_limits.market_data_per_minute = settings.alpaca_rate_limit_per_minute
        except Exception:
            pass  # Fall back to hardcoded defaults if settings unavailable

        for provider, limits in PROVIDER_LIMITS.items():
            per_second = None
            if limits.requests_per_second:
                per_second = RateLimitBucket(limits.requests_per_second, 1)

            per_minute = RateLimitBucket(limits.requests_per_minute, 60)

            per_day = None
            if limits.requests_per_day:
                per_day = RateLimitBucket(limits.requests_per_day, 86400)

            self._limiters[provider] = ProviderRateLimiter(
                provider=provider,
                per_second=per_second,
                per_minute=per_minute,
                per_day=per_day,
            )

        logger.info("provider_rate_limiters_initialized", providers=list(self._limiters.keys()))

    async def acquire(self, provider: str, block: bool = False, max_wait: float = 30.0) -> bool:
        """Acquire a request slot for a provider.

        Args:
            provider: Provider name
            block: If True, wait for slot availability
            max_wait: Maximum seconds to wait if blocking

        Returns:
            True if acquired, False if rate limited

        Raises:
            RateLimitExceeded: If rate limited and not blocking
        """
        provider = provider.lower()

        # Get or create limiter
        if provider not in self._limiters:
            # Unknown provider, use default conservative limit
            self._limiters[provider] = ProviderRateLimiter(
                provider=provider,
                per_minute=RateLimitBucket(60, 60),
            )

        limiter = self._limiters[provider]

        if block:
            # Wait for slot based on limiter-provided retry_after hints.
            loop = asyncio.get_running_loop()
            deadline = loop.time() + max_wait

            while True:
                allowed, retry_after = limiter.try_acquire()
                if allowed:
                    return True

                remaining = deadline - loop.time()
                if remaining <= 0:
                    break

                sleep_for = min(max(0.01, float(retry_after)), remaining)
                await asyncio.sleep(sleep_for)

            limiter.total_throttled += 1
            raise RateLimitExceeded(provider, int(max_wait), "Timeout waiting for rate limit slot")

        else:
            allowed, retry_after = limiter.try_acquire()
            if not allowed:
                limiter.total_throttled += 1
                logger.warning(
                    "provider_rate_limit_exceeded",
                    provider=provider,
                    retry_after=retry_after,
                    remaining_minute=limiter.per_minute.remaining,
                )
                raise RateLimitExceeded(provider, retry_after)

            return True

    def get_status(self, provider: str | None = None) -> dict:
        """Get rate limit status for one or all providers."""
        if provider:
            provider = provider.lower()
            if provider in self._limiters:
                return self._limiters[provider].get_status()
            return {"provider": provider, "status": "not_tracked"}

        return {"providers": {name: limiter.get_status() for name, limiter in self._limiters.items()}}

    def get_headers(self, provider: str) -> dict[str, str]:
        """Get rate limit headers for a provider."""
        provider = provider.lower()
        if provider not in self._limiters:
            return {}

        limiter = self._limiters[provider]
        return {
            "X-Provider-RateLimit-Limit": str(limiter.per_minute.limit),
            "X-Provider-RateLimit-Remaining": str(limiter.per_minute.remaining),
            "X-Provider-RateLimit-Reset": str(int(limiter.per_minute.reset_after)),
            "X-Provider-RateLimit-Provider": provider,
        }


# Convenience functions
def get_rate_limiter() -> ProviderRateLimitManager:
    """Get the global rate limiter instance."""
    return ProviderRateLimitManager.get_instance()


async def check_rate_limit(provider: str, block: bool = False) -> bool:
    """Check and acquire rate limit for a provider.

    Usage:
        await check_rate_limit("finnhub")  # Raises if limited
        await check_rate_limit("alphavantage", block=True)  # Waits if needed
    """
    return await get_rate_limiter().acquire(provider, block=block)


def get_provider_limits(provider: str) -> ProviderLimits | None:
    """Get configured limits for a provider."""
    return PROVIDER_LIMITS.get(provider.lower())


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint-Level Rate Limiting (PRD 7.5.3)
# ─────────────────────────────────────────────────────────────────────────────


class EndpointRateLimitExceeded(Exception):
    """Raised when an endpoint-level rate limit is exceeded."""

    def __init__(self, endpoint: str, retry_after: float, message: str = ""):
        self.endpoint = endpoint
        self.retry_after = retry_after
        self.message = message or f"Endpoint rate limit exceeded for {endpoint}"
        super().__init__(self.message)


@dataclass
class EndpointRateLimitConfig:
    """Configuration for a single endpoint group's rate limits.

    Supports two complementary limit types:
    - requests_per_window: Sliding window rate limit (e.g., bulk: 10 per hour)
    - max_concurrent: Concurrent session limit (e.g., replay: 5 simultaneous)
    """

    requests_per_window: int | None = None
    window_seconds: float = 3600
    max_concurrent: int | None = None


@dataclass
class _ClientWindowBucket:
    """Tracks per-client request timestamps for sliding window enforcement."""

    timestamps: deque = field(default_factory=deque)


class EndpointRateLimiter:
    """Rate limiter for specific API endpoint groups.

    Enforces per-endpoint, per-client request limits and concurrent session
    limits for resource-intensive operations.

    Usage:
        limiter = EndpointRateLimiter()
        limiter.configure("bulk_create",
            EndpointRateLimitConfig(requests_per_window=10, window_seconds=3600))
        limiter.configure("replay_session",
            EndpointRateLimitConfig(max_concurrent=5))

        # For request-count limits
        limiter.acquire("bulk_create", client_id="cerberus")

        # For concurrent limits
        limiter.acquire_concurrent("replay_session", session_id="abc123")
        limiter.release_concurrent("replay_session", session_id="abc123")
    """

    _instance: "EndpointRateLimiter | None" = None

    def __init__(self) -> None:
        self._configs: dict[str, EndpointRateLimitConfig] = {}
        self._client_buckets: dict[str, dict[str, _ClientWindowBucket]] = {}
        self._active_sessions: dict[str, set[str]] = {}

    @classmethod
    def get_instance(cls) -> "EndpointRateLimiter":
        """Get or create the singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def configure(self, endpoint: str, config: EndpointRateLimitConfig) -> None:
        """Register rate limit configuration for an endpoint group."""
        self._configs[endpoint] = config
        if config.requests_per_window is not None:
            self._client_buckets.setdefault(endpoint, {})
        if config.max_concurrent is not None:
            self._active_sessions.setdefault(endpoint, set())
        logger.info(
            "endpoint_rate_limit_configured",
            endpoint=endpoint,
            requests_per_window=config.requests_per_window,
            window_seconds=config.window_seconds,
            max_concurrent=config.max_concurrent,
        )

    def acquire(self, endpoint: str, client_id: str) -> None:
        """Acquire a rate limit slot for a request. Raises EndpointRateLimitExceeded."""
        config = self._configs.get(endpoint)
        if config is None or config.requests_per_window is None:
            return

        buckets = self._client_buckets.setdefault(endpoint, {})
        bucket = buckets.get(client_id)
        if bucket is None:
            bucket = _ClientWindowBucket()
            buckets[client_id] = bucket

        now = time.time()
        window_start = now - config.window_seconds

        # Evict expired timestamps
        while bucket.timestamps and bucket.timestamps[0] <= window_start:
            bucket.timestamps.popleft()

        if len(bucket.timestamps) >= config.requests_per_window:
            # Calculate retry_after from oldest timestamp in window
            oldest = bucket.timestamps[0]
            retry_after = oldest + config.window_seconds - now
            logger.warning(
                "endpoint_rate_limit_exceeded",
                endpoint=endpoint,
                client_id=client_id,
                used=len(bucket.timestamps),
                limit=config.requests_per_window,
                retry_after=round(retry_after, 1),
            )
            raise EndpointRateLimitExceeded(
                endpoint=endpoint,
                retry_after=max(retry_after, 0.1),
                message=(
                    f"Endpoint rate limit exceeded for {endpoint}: "
                    f"{len(bucket.timestamps)}/{config.requests_per_window} "
                    f"requests in {config.window_seconds}s window"
                ),
            )

        bucket.timestamps.append(now)

    def acquire_concurrent(self, endpoint: str, session_id: str) -> None:
        """Acquire a concurrent session slot. Raises EndpointRateLimitExceeded."""
        config = self._configs.get(endpoint)
        if config is None or config.max_concurrent is None:
            return

        sessions = self._active_sessions.setdefault(endpoint, set())

        # Idempotent: same session_id doesn't count twice
        if session_id in sessions:
            return

        if len(sessions) >= config.max_concurrent:
            logger.warning(
                "endpoint_concurrent_limit_exceeded",
                endpoint=endpoint,
                session_id=session_id,
                active=len(sessions),
                limit=config.max_concurrent,
            )
            raise EndpointRateLimitExceeded(
                endpoint=endpoint,
                retry_after=0,
                message=(
                    f"Concurrent session limit exceeded for {endpoint}: "
                    f"{len(sessions)}/{config.max_concurrent} active sessions"
                ),
            )

        sessions.add(session_id)

    def release_concurrent(self, endpoint: str, session_id: str) -> None:
        """Release a concurrent session slot."""
        sessions = self._active_sessions.get(endpoint)
        if sessions is not None:
            sessions.discard(session_id)

    def get_status(self, endpoint: str | None = None) -> dict:
        """Get rate limit status for admin visibility."""
        if endpoint is not None:
            return self._endpoint_status(endpoint)

        return {ep: self._endpoint_status(ep) for ep in self._configs}

    def _endpoint_status(self, endpoint: str) -> dict:
        """Build status dict for a single endpoint."""
        config = self._configs.get(endpoint)
        if config is None:
            return {"endpoint": endpoint, "status": "not_configured"}

        now = time.time()
        result: dict = {
            "config": {
                "requests_per_window": config.requests_per_window,
                "window_seconds": config.window_seconds,
                "max_concurrent": config.max_concurrent,
            },
        }

        # Window usage per client
        if config.requests_per_window is not None:
            buckets = self._client_buckets.get(endpoint, {})
            window_start = now - config.window_seconds
            clients: dict = {}
            for cid, bucket in buckets.items():
                # Count only non-expired timestamps
                used = sum(1 for ts in bucket.timestamps if ts > window_start)
                clients[cid] = {
                    "used": used,
                    "limit": config.requests_per_window,
                    "remaining": max(0, config.requests_per_window - used),
                }
            result["clients"] = clients

        # Concurrent usage
        if config.max_concurrent is not None:
            sessions = self._active_sessions.get(endpoint, set())
            result["concurrent"] = {
                "active": len(sessions),
                "limit": config.max_concurrent,
                "remaining": max(0, config.max_concurrent - len(sessions)),
                "sessions": sorted(sessions),
            }

        return result
