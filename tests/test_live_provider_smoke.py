"""Tests for scripts.live_provider_smoke helper flow."""

import asyncio
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

from gateway.core.provider import HealthStatus


def _load_live_provider_smoke_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "live_provider_smoke.py"
    spec = importlib.util.spec_from_file_location("live_provider_smoke_script", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load live_provider_smoke module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


live_provider_smoke = _load_live_provider_smoke_module()


@pytest.mark.asyncio
async def test_run_provider_smoke_check_missing_provider() -> None:
    result = await live_provider_smoke._run_provider_smoke_check(
        provider_name="alpaca",
        provider=None,
        credential_env="APCA_API_KEY_ID",
        credential_present=False,
        semaphore=asyncio.Semaphore(1),
    )

    assert result.loaded is False
    assert result.sample_status == "skipped"
    assert result.health_error == "provider not loaded"


@pytest.mark.asyncio
async def test_run_provider_smoke_check_handles_health_check_error() -> None:
    class FailingProvider:
        async def health_check(self) -> HealthStatus:
            raise RuntimeError("health endpoint unavailable")

    result = await live_provider_smoke._run_provider_smoke_check(
        provider_name="alpaca",
        provider=FailingProvider(),
        credential_env="APCA_API_KEY_ID",
        credential_present=True,
        semaphore=asyncio.Semaphore(1),
    )

    assert result.loaded is True
    assert result.health_ok is False
    assert result.sample_status == "skipped"
    assert "health endpoint unavailable" in (result.health_error or "")


@pytest.mark.asyncio
async def test_run_provider_smoke_check_respects_semaphore(monkeypatch: pytest.MonkeyPatch) -> None:
    active = 0
    max_active = 0

    class SlowProvider:
        async def health_check(self) -> HealthStatus:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.02)
            active -= 1
            return HealthStatus(healthy=True)

    async def _fake_sample_call(provider_name: str, provider: object) -> tuple[bool, str, str]:
        return True, "ok", f"{provider_name} sample ok"

    monkeypatch.setattr(live_provider_smoke, "_run_sample_call", _fake_sample_call)

    semaphore = asyncio.Semaphore(1)
    provider = SlowProvider()
    await asyncio.gather(
        live_provider_smoke._run_provider_smoke_check(
            provider_name="alpaca",
            provider=provider,
            credential_env="APCA_API_KEY_ID",
            credential_present=True,
            semaphore=semaphore,
        ),
        live_provider_smoke._run_provider_smoke_check(
            provider_name="finnhub",
            provider=provider,
            credential_env="FINNHUB_API_KEY",
            credential_present=True,
            semaphore=semaphore,
        ),
    )

    assert max_active == 1
