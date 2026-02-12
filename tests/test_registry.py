from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from gateway.core.provider import DataProvider, HealthStatus, ProviderCapabilities
from gateway.core.registry import ProviderRegistry


class _TestProvider(DataProvider):
    def __init__(self, name: str, delay_seconds: float, should_raise: bool = False) -> None:
        self._name = name
        self._delay_seconds = delay_seconds
        self._should_raise = should_raise

    @property
    def name(self) -> str:
        return self._name

    @property
    def supported_feeds(self) -> list[str]:
        return []

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities()

    async def initialize(self, config: dict[str, Any]) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    async def health_check(self) -> HealthStatus:
        await asyncio.sleep(self._delay_seconds)
        if self._should_raise:
            raise RuntimeError(f"{self._name}_failed")
        return HealthStatus(healthy=True)


@pytest.mark.asyncio
async def test_health_check_all_runs_provider_checks_concurrently() -> None:
    registry = ProviderRegistry()
    registry._providers = {
        "one": _TestProvider("one", delay_seconds=0.12),
        "two": _TestProvider("two", delay_seconds=0.12),
        "three": _TestProvider("three", delay_seconds=0.12),
    }

    start = time.perf_counter()
    results = await registry.health_check_all()
    elapsed = time.perf_counter() - start

    assert set(results.keys()) == {"one", "two", "three"}
    assert all(status.healthy for status in results.values())
    # Sequential would be ~0.36s; concurrent should stay near single-check latency.
    assert elapsed < 0.25


@pytest.mark.asyncio
async def test_health_check_all_captures_provider_exceptions() -> None:
    registry = ProviderRegistry()
    registry._providers = {
        "ok": _TestProvider("ok", delay_seconds=0.01),
        "bad": _TestProvider("bad", delay_seconds=0.01, should_raise=True),
    }

    results = await registry.health_check_all()

    assert results["ok"].healthy is True
    assert results["bad"].healthy is False
    assert results["bad"].error == "bad_failed"
