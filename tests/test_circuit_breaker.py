"""Unit tests for CircuitBreaker."""

import asyncio

import pytest

from gateway.core.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerRegistry,
    CircuitOpenError,
    CircuitState,
    get_circuit_breaker,
)


@pytest.fixture
def breaker() -> CircuitBreaker:
    """Create a circuit breaker with fast thresholds for testing."""
    config = CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout=0.1,  # 100ms for fast tests
        success_threshold=2,
    )
    return CircuitBreaker(name="test", config=config)


class TestCircuitBreakerStates:
    """Tests for circuit breaker state transitions."""

    @pytest.mark.asyncio
    async def test_initial_state_is_closed(self, breaker: CircuitBreaker) -> None:
        """Circuit breaker should start in CLOSED state."""
        assert breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_success_keeps_circuit_closed(self, breaker: CircuitBreaker) -> None:
        """Successful calls should keep circuit CLOSED."""

        async def success_func():
            return "ok"

        result = await breaker.call(success_func)
        assert result == "ok"
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_failures_open_circuit(self, breaker: CircuitBreaker) -> None:
        """Consecutive failures should open the circuit."""

        async def failing_func():
            raise ValueError("Upstream error")

        # Trigger failure_threshold (3) failures
        for _ in range(3):
            with pytest.raises(ValueError):
                await breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN
        assert breaker.failure_count == 3

    @pytest.mark.asyncio
    async def test_open_circuit_rejects_calls(self, breaker: CircuitBreaker) -> None:
        """Open circuit should reject calls with CircuitOpenError."""

        async def failing_func():
            raise ValueError("Upstream error")

        async def success_func():
            return "ok"

        # Open the circuit
        for _ in range(3):
            with pytest.raises(ValueError):
                await breaker.call(failing_func)

        # Now calls should be rejected
        with pytest.raises(CircuitOpenError) as exc_info:
            await breaker.call(success_func)

        assert exc_info.value.name == "test"
        assert exc_info.value.retry_after > 0

    @pytest.mark.asyncio
    async def test_circuit_transitions_to_half_open(self, breaker: CircuitBreaker) -> None:
        """Circuit should transition to HALF_OPEN after recovery timeout."""

        async def failing_func():
            raise ValueError("Upstream error")

        async def success_func():
            return "ok"

        # Open the circuit
        for _ in range(3):
            with pytest.raises(ValueError):
                await breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN

        # Wait for recovery timeout
        await asyncio.sleep(0.15)

        # Next call should transition to HALF_OPEN and succeed
        result = await breaker.call(success_func)
        assert result == "ok"
        assert breaker.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_closes_after_successes(self, breaker: CircuitBreaker) -> None:
        """Circuit should close after success_threshold successes in HALF_OPEN."""

        async def failing_func():
            raise ValueError("Upstream error")

        async def success_func():
            return "ok"

        # Open the circuit
        for _ in range(3):
            with pytest.raises(ValueError):
                await breaker.call(failing_func)

        # Wait for recovery timeout
        await asyncio.sleep(0.15)

        # First success transitions to HALF_OPEN
        await breaker.call(success_func)
        assert breaker.state == CircuitState.HALF_OPEN

        # Second success should close the circuit (success_threshold=2)
        await breaker.call(success_func)
        assert breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_half_open_reopens_on_failure(self, breaker: CircuitBreaker) -> None:
        """Circuit should reopen on any failure in HALF_OPEN state."""

        async def failing_func():
            raise ValueError("Upstream error")

        async def success_func():
            return "ok"

        # Open the circuit
        for _ in range(3):
            with pytest.raises(ValueError):
                await breaker.call(failing_func)

        # Wait for recovery timeout
        await asyncio.sleep(0.15)

        # First success transitions to HALF_OPEN
        await breaker.call(success_func)
        assert breaker.state == CircuitState.HALF_OPEN

        # Failure in HALF_OPEN should reopen
        with pytest.raises(ValueError):
            await breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN


class TestCircuitBreakerReset:
    """Tests for manual circuit reset."""

    @pytest.mark.asyncio
    async def test_reset_closes_open_circuit(self, breaker: CircuitBreaker) -> None:
        """Manual reset should close an open circuit."""

        async def failing_func():
            raise ValueError("Upstream error")

        # Open the circuit
        for _ in range(3):
            with pytest.raises(ValueError):
                await breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN

        # Reset
        await breaker.reset()

        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0
        assert breaker.success_count == 0


class TestCircuitBreakerRegistry:
    """Tests for circuit breaker registry."""

    @pytest.mark.asyncio
    async def test_get_creates_breaker(self) -> None:
        """Registry should create breakers on demand."""
        registry = CircuitBreakerRegistry()
        breaker = await registry.get("test_component")

        assert breaker.name == "test_component"
        assert breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_get_returns_same_breaker(self) -> None:
        """Registry should return the same breaker for a name."""
        registry = CircuitBreakerRegistry()
        b1 = await registry.get("test_component")
        b2 = await registry.get("test_component")

        assert b1 is b2

    @pytest.mark.asyncio
    async def test_uses_predefined_config(self) -> None:
        """Registry should use predefined configs for known components."""
        registry = CircuitBreakerRegistry()
        breaker = await registry.get("alpaca_rest")

        # PRD specifies: failure_threshold=10, recovery_timeout=30
        assert breaker.config.failure_threshold == 10
        assert breaker.config.recovery_timeout == 30

    @pytest.mark.asyncio
    async def test_redis_streams_sink_uses_custom_config(self) -> None:
        """data_sink:redis_streams should have high-throughput-tuned config.

        The default (threshold=5, recovery=60s) was too aggressive for the
        Redis Streams pipeline — a transient blip triggered 5 failures in <2s,
        causing a 60-second blackout that dropped ~256 events.  The custom
        config raises the threshold to 20 (each failure already survived 3
        retries) and cuts recovery to 15s.
        """
        registry = CircuitBreakerRegistry()
        breaker = await registry.get("data_sink:redis_streams")

        assert breaker.config.failure_threshold == 20
        assert breaker.config.recovery_timeout == 15
        assert breaker.config.success_threshold == 2

    @pytest.mark.asyncio
    async def test_get_all_status(self) -> None:
        """Registry should return status of all breakers."""
        registry = CircuitBreakerRegistry()
        await registry.get("comp1")
        await registry.get("comp2")

        statuses = registry.get_all_status()
        assert len(statuses) == 2
        assert any(s["name"] == "comp1" for s in statuses)
        assert any(s["name"] == "comp2" for s in statuses)


class TestCircuitBreakerStatus:
    """Tests for status reporting."""

    def test_get_status(self, breaker: CircuitBreaker) -> None:
        """Status should include all relevant fields."""
        status = breaker.get_status()

        assert status["name"] == "test"
        assert status["state"] == "closed"
        assert status["failure_count"] == 0
        assert status["success_count"] == 0
        assert "config" in status
        assert status["config"]["failure_threshold"] == 3


class TestGlobalRegistry:
    """Tests for global registry accessor."""

    @pytest.mark.asyncio
    async def test_get_circuit_breaker_returns_breaker(self) -> None:
        """get_circuit_breaker should return a breaker."""
        breaker = await get_circuit_breaker("global_test")
        assert breaker.name == "global_test"
