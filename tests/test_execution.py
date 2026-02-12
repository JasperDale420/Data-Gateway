"""Tests for generic execution and failover logic."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from gateway.core.circuit_breaker import CircuitState, get_circuit_registry
from gateway.core.execution import execute_provider_failover
from gateway.core.provider import DataProvider


class MockProvider(DataProvider):
    def __init__(self, name: str):
        self._name = name
        self._capabilities = ["stock_bars"]

    @property
    def name(self) -> str:
        return self._name

    @property
    def capabilities(self) -> list[str]:
        return self._capabilities

    @property
    def supported_feeds(self) -> list[str]:
        return ["sip"]

    async def initialize(self, config: dict):
        pass

    async def shutdown(self):
        pass

    async def health_check(self):
        from gateway.core.provider import HealthStatus

        return HealthStatus(healthy=True)

    async def get_bars(self, **kwargs):
        pass


@pytest.fixture
def registry():
    reg = MagicMock()
    return reg


@pytest.fixture
async def circuit_registry():
    # Reset singleton for tests
    reg = get_circuit_registry()
    await reg.reset_all()
    # Ensure mocked providers have breakers
    await reg.get("provider1")
    await reg.get("provider2")
    return reg


@pytest.mark.asyncio
async def test_execution_success_primary(registry, circuit_registry):
    """Test successful execution with the first provider."""
    p1 = MockProvider("provider1")
    registry.get_ordered_providers.return_value = [p1]

    handler = AsyncMock(return_value="success")

    result = await execute_provider_failover(
        capability="stock_bars",
        handler=handler,
        registry=registry,
    )

    assert result == "success"
    handler.assert_called_once_with(p1)


@pytest.mark.asyncio
async def test_execution_failover_success(registry, circuit_registry):
    """Test failover to secondary provider when primary fails."""
    p1 = MockProvider("provider1")
    p2 = MockProvider("provider2")
    registry.get_ordered_providers.return_value = [p1, p2]

    # Handler fails for p1, succeeds for p2
    async def handler(provider):
        if provider.name == "provider1":
            raise Exception("P1 failure")
        return "success_p2"

    result = await execute_provider_failover(
        capability="stock_bars",
        handler=handler,
        registry=registry,
    )

    assert result == "success_p2"

    # Check circuit breaker interaction
    breaker1 = await circuit_registry.get("provider1")
    assert breaker1.failure_count == 1
    # P1 failure didn't trip circuit yet (threshold default 5/10) but count incremented


@pytest.mark.asyncio
async def test_execution_timeout_failover(registry, circuit_registry):
    """Test failover when primary times out."""
    p1 = MockProvider("provider1")
    p2 = MockProvider("provider2")
    registry.get_ordered_providers.return_value = [p1, p2]

    async def handler(provider):
        if provider.name == "provider1":
            await asyncio.sleep(0.2)  # Longer than timeout
            return "too_late"
        return "success_p2"

    result = await execute_provider_failover(
        capability="stock_bars",
        handler=handler,
        registry=registry,
        timeout=0.1,  # Short timeout
    )

    assert result == "success_p2"

    breaker1 = await circuit_registry.get("provider1")
    assert breaker1.failure_count == 1


@pytest.mark.asyncio
async def test_execution_all_fail(registry, circuit_registry):
    """Test 503 raised when all providers fail."""
    p1 = MockProvider("provider1")
    registry.get_ordered_providers.return_value = [p1]

    async def handler(provider):
        raise Exception("Failure")

    with pytest.raises(HTTPException) as exc:
        await execute_provider_failover(
            capability="stock_bars",
            handler=handler,
            registry=registry,
        )

    assert exc.value.status_code == 503
    assert exc.value.detail["code"] == "GW-E5004"


@pytest.mark.asyncio
async def test_execution_skips_open_circuit(registry, circuit_registry):
    """Test that open circuits are skipped."""
    p1 = MockProvider("provider1")
    p2 = MockProvider("provider2")
    registry.get_ordered_providers.return_value = [p1, p2]

    # Open P1 circuit
    breaker1 = await circuit_registry.get("provider1")
    breaker1.state = CircuitState.OPEN
    import time

    breaker1.last_failure_time = time.time()

    handler = AsyncMock(return_value="success_p2")

    result = await execute_provider_failover(
        capability="stock_bars",
        handler=handler,
        registry=registry,
    )

    assert result == "success_p2"
    # Handler should ONLY be called with p2
    handler.assert_called_once_with(p2)
