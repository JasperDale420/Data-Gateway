"""Per-provider rate limiting for upstream API calls.

Tracks request counts per provider and enforces their specific rate limits
to prevent 429 errors from upstream APIs.

Provider Limits (Free Tier):
- Alpaca: 200/min (market data can be higher with paid)
- Finnhub: 60/min general, 300/min fundamentals, 900/min market data
- Alpha Vantage: 5/min, 25/day (extremely restrictive)
- NewsAPI: 100/day
- Unusual Whales: 120/min, 15K/day
- SEC EDGAR: 10/sec (600/min)
- YFinance: No official limit (be conservative)
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


# Provider rate limit configurations (conservative free-tier values)
PROVIDER_LIMITS: dict[str, ProviderLimits] = {
    "alpaca": ProviderLimits(
        requests_per_minute=200,
        requests_per_second=10,
        market_data_per_minute=200,  # Can be 10K with paid plan
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
        requests_per_minute=10,  # Conservative: 100/day = ~0.07/min
        requests_per_day=100,
    ),
    "unusual_whales": ProviderLimits(
        requests_per_minute=120,
        requests_per_day=15000,
    ),
    "sec": ProviderLimits(
        requests_per_minute=300,  # 10/sec = 600/min, be conservative
        requests_per_second=8,
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
        """Initialize rate limiters from config."""
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

        return {
            "providers": {name: limiter.get_status() for name, limiter in self._limiters.items()}
        }

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
