"""Circuit breaker pattern for upstream connections.

Prevents cascading failures by tracking upstream health and temporarily
rejecting requests when a component is failing, as specified in the PRD.
"""

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeVar

from gateway.core.logger import logger

T = TypeVar("T")


# ─────────────────────────────────────────────────────────────────────────────
# Circuit States
# ─────────────────────────────────────────────────────────────────────────────


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation, requests allowed
    OPEN = "open"  # Failing, requests rejected
    HALF_OPEN = "half_open"  # Probing recovery, limited requests allowed


# ─────────────────────────────────────────────────────────────────────────────
# Circuit Breaker Configuration
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class CircuitBreakerConfig:
    """Configuration for a circuit breaker.

    Attributes:
        failure_threshold: Consecutive failures before opening circuit
        recovery_timeout: Seconds before attempting half-open state
        success_threshold: Successes in half-open to fully close
    """

    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    success_threshold: int = 3


# Pre-configured settings per PRD
CIRCUIT_CONFIGS: dict[str, CircuitBreakerConfig] = {
    "alpaca_stock_ws": CircuitBreakerConfig(failure_threshold=5, recovery_timeout=60),
    "alpaca_options_ws": CircuitBreakerConfig(failure_threshold=5, recovery_timeout=60),
    "alpaca_rest": CircuitBreakerConfig(failure_threshold=10, recovery_timeout=30),
    "uw_rest": CircuitBreakerConfig(failure_threshold=10, recovery_timeout=30),
    "news_rest": CircuitBreakerConfig(failure_threshold=10, recovery_timeout=30),
    "yfinance_rest": CircuitBreakerConfig(failure_threshold=10, recovery_timeout=30),
    "sec_rest": CircuitBreakerConfig(failure_threshold=10, recovery_timeout=30),
    # Redis Streams sink to Heber — high-throughput data pipeline.
    # The default (threshold=5, recovery=60s) was too aggressive: a single
    # transient Redis blip triggered 5 failures in <2 seconds, then the
    # 60-second blackout dropped ~256 events permanently.  With the new
    # retry logic in RedisStreamsSink.publish(), each counted failure
    # already survived 3 retry attempts, so reaching 20 failures means
    # Redis is genuinely down.  The 15-second recovery is short enough to
    # resume quickly once Redis is back.
    "data_sink:redis_streams": CircuitBreakerConfig(
        failure_threshold=20,
        recovery_timeout=15,
        success_threshold=2,
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────────────────────


class CircuitOpenError(Exception):
    """Raised when a request is rejected due to open circuit."""

    def __init__(self, name: str, retry_after: float = 0):
        self.name = name
        self.retry_after = retry_after
        self.error_code = "GW-E1012"
        super().__init__(f"Circuit '{name}' is open, retry after {retry_after:.1f}s")


# ─────────────────────────────────────────────────────────────────────────────
# Circuit Breaker
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class CircuitBreaker:
    """Circuit breaker for upstream connections.

    Implements the circuit breaker pattern to prevent cascading failures.

    States:
        - CLOSED: Normal operation, all requests pass through
        - OPEN: Circuit tripped after failures, requests rejected
        - HALF_OPEN: Testing recovery, limited requests allowed

    Usage:
        breaker = CircuitBreaker("alpaca_rest", config)
        result = await breaker.call(some_async_function, arg1, arg2)
    """

    name: str
    config: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)

    # State tracking
    state: CircuitState = field(default=CircuitState.CLOSED)
    failure_count: int = field(default=0)
    success_count: int = field(default=0)
    last_failure_time: float | None = field(default=None)

    # Lock for thread safety
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # Guards against multiple concurrent HALF_OPEN probes
    _half_open_in_progress: bool = field(default=False)

    async def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> T:
        """Execute a function through the circuit breaker.

        Args:
            func: Async function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the function call

        Raises:
            CircuitOpenError: If circuit is open and recovery timeout not elapsed
            Exception: Any exception from the underlying function
        """
        async with self._lock:
            await self._check_state()

        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure(e)
            raise

    async def _check_state(self) -> None:
        """Check circuit state and potentially transition.

        Raises:
            CircuitOpenError: If circuit is open, or if HALF_OPEN already has
                a probe in flight (only one probe at a time).
        """
        if self.state == CircuitState.OPEN:
            elapsed = time.time() - (self.last_failure_time or 0)

            if elapsed >= self.config.recovery_timeout:
                if self._half_open_in_progress:
                    # Another probe is already running — reject this caller
                    retry_after = self.config.recovery_timeout
                    raise CircuitOpenError(self.name, retry_after)
                # Transition to half-open
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                self._half_open_in_progress = True
                logger.info(
                    "circuit_half_open",
                    circuit=self.name,
                    code="GW-I1009",
                )
            else:
                # Still in recovery timeout
                retry_after = self.config.recovery_timeout - elapsed
                raise CircuitOpenError(self.name, retry_after)
        elif self.state == CircuitState.HALF_OPEN:
            # HALF_OPEN admits exactly ONE probe at a time. Concurrent
            # callers must back off so we don't N-times-amplify load against
            # an upstream that's only tentatively recovered. Without this
            # branch, every caller that arrives while the probe is running
            # falls through `_check_state` and races to call `func(...)`
            # in parallel, defeating the purpose of half-open.
            #
            # The flag works as a single-slot semaphore: the success/failure
            # handlers release it when each probe finishes, and the next
            # sequential probe re-acquires it here.
            if self._half_open_in_progress:
                retry_after = self.config.recovery_timeout
                raise CircuitOpenError(self.name, retry_after)
            # No probe in flight — reserve the slot for this caller.
            self._half_open_in_progress = True

    async def _on_success(self) -> None:
        """Handle successful request."""
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                # Release the half-open probe permit so the next sequential
                # caller can probe. The `_check_state` HALF_OPEN guard keeps
                # CONCURRENT probes from racing; releasing here allows the
                # next SEQUENTIAL probe to proceed once this one finishes.
                self._half_open_in_progress = False
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    logger.info(
                        "circuit_closed",
                        circuit=self.name,
                        success_count=self.success_count,
                        code="GW-I1010",
                    )

            # Reset failure count on any success
            self.failure_count = 0

    async def _on_failure(self, error: Exception) -> None:
        """Handle failed request."""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == CircuitState.HALF_OPEN:
                # Any failure in half-open reopens the circuit
                self.state = CircuitState.OPEN
                self._half_open_in_progress = False
                logger.warning(
                    "circuit_reopened",
                    circuit=self.name,
                    error=str(error),
                    code="GW-W1011",
                )
            elif self.failure_count >= self.config.failure_threshold:
                self.state = CircuitState.OPEN
                # Data sink circuits use WARNING because the sink layer has its
                # own retry/buffer logic — opening the circuit is controlled
                # degradation, not an unrecoverable error.  Other upstream
                # circuits (provider REST/WS) use ERROR.
                if self.name.startswith("data_sink:"):
                    logger.warning(
                        "circuit_opened",
                        circuit=self.name,
                        failure_count=self.failure_count,
                        error=str(error),
                        code="GW-W1013",
                    )
                else:
                    logger.error(
                        "circuit_opened",
                        circuit=self.name,
                        failure_count=self.failure_count,
                        error=str(error),
                        code="GW-E1011",
                    )

    async def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        async with self._lock:
            prev_state = self.state
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
            self._half_open_in_progress = False
            logger.info(
                "circuit_reset",
                circuit=self.name,
                previous_state=prev_state.value,
                code="GW-I1012",
            )

    def get_status(self) -> dict[str, Any]:
        """Get current circuit breaker status."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time,
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "recovery_timeout": self.config.recovery_timeout,
                "success_threshold": self.config.success_threshold,
            },
        }


# ─────────────────────────────────────────────────────────────────────────────
# Circuit Breaker Registry
# ─────────────────────────────────────────────────────────────────────────────


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers.

    Provides a centralized way to get, create, and manage circuit breakers
    for different components.
    """

    def __init__(self) -> None:
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()

    async def get(self, name: str) -> CircuitBreaker:
        """Get or create a circuit breaker by name.

        Args:
            name: Component name (e.g., "alpaca_rest", "uw_rest")

        Returns:
            CircuitBreaker instance for the component
        """
        async with self._lock:
            if name not in self._breakers:
                config = CIRCUIT_CONFIGS.get(name, CircuitBreakerConfig())
                self._breakers[name] = CircuitBreaker(name=name, config=config)
            return self._breakers[name]

    async def reset(self, name: str) -> bool:
        """Reset a specific circuit breaker.

        Args:
            name: Component name

        Returns:
            True if reset, False if not found
        """
        if name in self._breakers:
            await self._breakers[name].reset()
            return True
        return False

    async def reset_all(self) -> int:
        """Reset all circuit breakers.

        Returns:
            Number of breakers reset
        """
        count = 0
        for breaker in self._breakers.values():
            await breaker.reset()
            count += 1
        return count

    def get_all_status(self) -> list[dict[str, Any]]:
        """Get status of all circuit breakers."""
        return [breaker.get_status() for breaker in self._breakers.values()]

    def list_names(self) -> list[str]:
        """List all registered circuit breaker names."""
        return list(self._breakers.keys())


# ─────────────────────────────────────────────────────────────────────────────
# Singleton Registry
# ─────────────────────────────────────────────────────────────────────────────

_registry: CircuitBreakerRegistry | None = None


def get_circuit_registry() -> CircuitBreakerRegistry:
    """Get the singleton circuit breaker registry."""
    global _registry
    if _registry is None:
        _registry = CircuitBreakerRegistry()
    return _registry


async def get_circuit_breaker(name: str) -> CircuitBreaker:
    """Convenience function to get a circuit breaker by name."""
    return await get_circuit_registry().get(name)
