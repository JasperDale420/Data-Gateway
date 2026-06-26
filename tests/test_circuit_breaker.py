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


class TestHalfOpenConcurrencyGuard:
    """Tests that HALF_OPEN admits exactly one probe at a time."""

    @pytest.mark.asyncio
    async def test_half_open_rejects_concurrent_probes(self, breaker: CircuitBreaker) -> None:
        """While a HALF_OPEN probe is in flight, additional callers must be
        rejected with CircuitOpenError instead of executing in parallel.

        Regression — codex caught: the `_half_open_in_progress` guard was only
        consulted inside the `state == OPEN` branch. Once the first probe
        flipped OPEN → HALF_OPEN, every subsequent caller saw `state ==
        HALF_OPEN` and fell through `_check_state` without raising — so the
        breaker amplified concurrent load by N against an upstream that was
        only tentatively recovered.
        """

        async def failing_func() -> None:
            raise ValueError("Upstream error")

        # Open the circuit (failure_threshold=3 from the fixture).
        for _ in range(3):
            with pytest.raises(ValueError):
                await breaker.call(failing_func)
        assert breaker.state == CircuitState.OPEN

        # Wait past the recovery window so the next call probes.
        await asyncio.sleep(0.15)

        probe_in_flight = asyncio.Event()
        probe_release = asyncio.Event()
        probe_started_count = 0

        async def slow_probe() -> str:
            nonlocal probe_started_count
            probe_started_count += 1
            probe_in_flight.set()
            await probe_release.wait()
            return "ok"

        # First caller becomes the probe, flips OPEN → HALF_OPEN.
        first = asyncio.create_task(breaker.call(slow_probe))
        await asyncio.wait_for(probe_in_flight.wait(), timeout=1.0)
        assert breaker.state == CircuitState.HALF_OPEN
        assert probe_started_count == 1

        # Second caller arrives WHILE the probe is in flight — must be
        # rejected with CircuitOpenError, NOT executed in parallel.
        with pytest.raises(CircuitOpenError):
            await breaker.call(slow_probe)
        assert probe_started_count == 1, "second probe must not have started"

        # Third concurrent attempt: also rejected.
        with pytest.raises(CircuitOpenError):
            await breaker.call(slow_probe)
        assert probe_started_count == 1

        # Let the first probe finish; the breaker semantics from here on are
        # already covered by other tests in this file.
        probe_release.set()
        result = await asyncio.wait_for(first, timeout=1.0)
        assert result == "ok"


class TestHalfOpenProbeCancellation:
    """Tests that a cancelled HALF_OPEN probe releases the in-progress slot.

    Regression — codex caught: the concurrent-probe guard reserves
    `_half_open_in_progress = True` in `_check_state` and the success/failure
    handlers release it.  But the in-flight `await func(...)` can be cancelled
    by the caller (shutdown, asyncio.wait_for timeout, etc.), bypassing
    `except Exception` because `CancelledError` is a BaseException.  Without
    explicit handling the slot would stay reserved forever — every later
    caller would hit `CircuitOpenError` until a manual reset.
    """

    @pytest.mark.asyncio
    async def test_cancelled_half_open_probe_releases_slot(self, breaker: CircuitBreaker) -> None:
        async def failing_func() -> None:
            raise ValueError("Upstream error")

        # Open the circuit.
        for _ in range(3):
            with pytest.raises(ValueError):
                await breaker.call(failing_func)
        assert breaker.state == CircuitState.OPEN

        # Wait past recovery so the next call probes.
        await asyncio.sleep(0.15)

        probe_in_flight = asyncio.Event()

        async def slow_probe() -> str:
            probe_in_flight.set()
            await asyncio.sleep(10.0)
            return "should-never-return"

        # Start the probe; it'll flip OPEN -> HALF_OPEN and reserve the slot.
        probe = asyncio.create_task(breaker.call(slow_probe))
        await asyncio.wait_for(probe_in_flight.wait(), timeout=1.0)
        assert breaker.state == CircuitState.HALF_OPEN
        assert breaker._half_open_in_progress is True

        # Cancel the probe — without the fix, the slot would stay True
        # forever.
        probe.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(probe, timeout=1.0)

        # The slot must have been released so a follow-up caller can probe.
        assert breaker._half_open_in_progress is False

        # And the next call must NOT hit "another probe is already running":
        # it should be admitted as the new probe.
        async def success_func() -> str:
            return "ok"

        result = await breaker.call(success_func)
        assert result == "ok"


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


def test_circuit_breaker_exports_state_gauge() -> None:
    """Breaker state is exported as a gauge so a sink trip is alertable before
    the failed-event buffer starts evicting (a lagging signal)."""
    from gateway.core.circuit_breaker import CircuitBreaker, CircuitState
    from gateway.core.metrics import CIRCUIT_BREAKER_STATE

    cb = CircuitBreaker(name="test_gauge_breaker")
    gauge = CIRCUIT_BREAKER_STATE.labels(name="test_gauge_breaker")

    assert gauge._value.get() == 0  # closed at init (__post_init__ baseline)
    cb._set_state(CircuitState.OPEN)
    assert gauge._value.get() == 2
    cb._set_state(CircuitState.HALF_OPEN)
    assert gauge._value.get() == 1
    cb._set_state(CircuitState.CLOSED)
    assert gauge._value.get() == 0
