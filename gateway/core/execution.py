"""Generic execution strategies for provider calls."""

import asyncio
from collections.abc import Awaitable, Callable

import structlog
from fastapi import HTTPException

from gateway.core.circuit_breaker import get_circuit_registry
from gateway.core.provider import DataProvider
from gateway.core.registry import ProviderRegistry

logger = structlog.get_logger()

PROVIDER_NOT_AVAILABLE = "No healthy providers available"


async def execute_provider_failover[T](
    capability: str,
    handler: Callable[[DataProvider], Awaitable[T]],
    registry: ProviderRegistry,
    timeout: float = 10.0,
) -> T:
    """Execute a provider call with automatic failover and timeout enforcement.

    Args:
        capability: The capability required (e.g. "stock_bars").
        handler: Async function that takes a provider and returns result.
        registry: The provider registry to look up candidates.
        timeout: Max seconds to wait for each attempt.

    Returns:
        The result from the first successful provider.

    Raises:
        HTTPException: If all providers fail (503).
    """
    # 1. Get ordered candidates
    providers = registry.get_ordered_providers(capability)
    if not providers:
        logger.warning("no_providers_for_capability", capability=capability)
        raise HTTPException(
            status_code=503,
            detail=f"No providers configured for {capability}",
        )

    circuit_registry = get_circuit_registry()
    errors: list[str] = []

    # 2. Iterate through candidates
    for provider in providers:
        # Get circuit breaker for this provider
        # Note: Provider names must match circuit breaker names in config
        breaker = await circuit_registry.get(provider.name)

        try:
            # Define the operation with timeout enforcement
            async def _timed_execution(p=provider):
                return await asyncio.wait_for(handler(p), timeout=timeout)

            # Execute through circuit breaker
            # This handles state checks, success/failure counting, and recovery
            return await breaker.call(_timed_execution)

        except Exception as e:
            # Circuit breaker prevented execution — skip to next provider
            if type(e).__name__ == "CircuitOpenError":
                errors.append(f"{provider.name}:circuit_open")
                continue

            msg = f"{provider.name}:{type(e).__name__}:{e!s}"
            event = "provider_execution_timeout" if isinstance(e, asyncio.TimeoutError) else "provider_execution_failed"
            logger.warning(event, provider=provider.name, capability=capability, error=str(e))
            errors.append(msg)

    # 3. All failed
    logger.error("all_providers_failed", capability=capability, attempts=len(errors), errors=errors)
    raise HTTPException(
        status_code=503,
        detail={
            "code": "GW-E5004",
            "message": PROVIDER_NOT_AVAILABLE,
            "errors": errors,
        },
    )
