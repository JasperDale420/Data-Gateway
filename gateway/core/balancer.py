"""Multi-key load balancer for API rate limit management."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import structlog

logger = structlog.get_logger()


@dataclass
class ApiKey:
    """Tracks an API key's health and usage."""

    key_id: str
    secret: str
    healthy: bool = True
    request_count: int = 0
    last_request: datetime | None = None
    last_error: datetime | None = None
    error_count: int = 0
    cooldown_until: datetime | None = None

    def is_available(self) -> bool:
        """Check if key is available for use."""
        if not self.healthy:
            return False
        if self.cooldown_until and datetime.now(UTC) < self.cooldown_until:
            return False
        return True

    def record_request(self) -> None:
        """Record a successful request."""
        self.request_count += 1
        self.last_request = datetime.now(UTC)

    def record_error(self, cooldown_seconds: int = 60) -> None:
        """Record an error and start cooldown."""
        self.error_count += 1
        self.last_error = datetime.now(UTC)
        self.cooldown_until = datetime.now(UTC) + timedelta(seconds=cooldown_seconds)
        logger.warning(
            "api_key_cooldown",
            key_id=self.key_id[:8] + "...",
            cooldown_seconds=cooldown_seconds,
        )

    def recover(self) -> None:
        """Mark key as recovered."""
        self.healthy = True
        self.cooldown_until = None
        logger.info("api_key_recovered", key_id=self.key_id[:8] + "...")


class KeyLoadBalancer:
    """Load balancer with round-robin selection and health tracking."""

    def __init__(self, cooldown_seconds: int = 60, max_errors: int = 3):
        self._keys: list[ApiKey] = []
        self._current_index: int = 0
        self._cooldown_seconds = cooldown_seconds
        self._max_errors = max_errors
        self._lock = asyncio.Lock()
        self._recovery_task: asyncio.Task | None = None

    def add_key(self, key_id: str, secret: str) -> None:
        """Add an API key to the pool."""
        self._keys.append(ApiKey(key_id=key_id, secret=secret))
        logger.info("api_key_added", key_id=key_id[:8] + "...", total_keys=len(self._keys))

    def add_keys_from_env(self, key_ids: list[str], secrets: list[str]) -> None:
        """Add multiple keys from environment lists."""
        for key_id, secret in zip(key_ids, secrets, strict=False):
            if key_id and secret:
                self.add_key(key_id, secret)

    async def get_key(self) -> ApiKey | None:
        """Get next available key using round-robin."""
        async with self._lock:
            if not self._keys:
                return None

            # Try each key once
            for _ in range(len(self._keys)):
                key = self._keys[self._current_index]
                self._current_index = (self._current_index + 1) % len(self._keys)

                if key.is_available():
                    return key

            # All keys exhausted
            logger.warning("all_api_keys_exhausted")
            return None

    async def record_success(self, key: ApiKey) -> None:
        """Record successful request for a key."""
        async with self._lock:
            key.record_request()

    async def record_rate_limit(self, key: ApiKey, retry_after: int = 60) -> None:
        """Record rate limit hit (429 response)."""
        async with self._lock:
            key.record_error(cooldown_seconds=retry_after)

            if key.error_count >= self._max_errors:
                key.healthy = False
                logger.error(
                    "api_key_disabled",
                    key_id=key.key_id[:8] + "...",
                    error_count=key.error_count,
                )

        # Ensure recovery task is running
        if self._recovery_task is None or self._recovery_task.done():
            self._recovery_task = asyncio.create_task(self._recovery_loop())

    async def _recovery_loop(self) -> None:
        """Periodically check and recover keys."""
        while True:
            await asyncio.sleep(30)

            now = datetime.now(UTC)
            any_recovering = False

            async with self._lock:
                for key in self._keys:
                    if key.cooldown_until and now >= key.cooldown_until:
                        key.cooldown_until = None
                        any_recovering = True

                    if not key.healthy and key.error_count >= self._max_errors:
                        # Gradually reset error count
                        if key.last_error and (now - key.last_error).total_seconds() > 300:
                            key.error_count = 0
                            key.recover()
                            any_recovering = True

            if not any_recovering and all(k.is_available() for k in self._keys):
                break

    def get_available_count(self) -> int:
        """Count of currently available keys."""
        return sum(1 for k in self._keys if k.is_available())

    def get_stats(self) -> dict:
        """Get load balancer statistics."""
        return {
            "total_keys": len(self._keys),
            "available_keys": self.get_available_count(),
            "total_requests": sum(k.request_count for k in self._keys),
            "keys": [
                {
                    "key_id": k.key_id[:8] + "...",
                    "healthy": k.healthy,
                    "available": k.is_available(),
                    "request_count": k.request_count,
                    "error_count": k.error_count,
                }
                for k in self._keys
            ],
        }


# Global instance for Alpaca keys
alpaca_key_balancer = KeyLoadBalancer()
