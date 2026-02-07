from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api import bulk, calendar, corporate
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


class _FakeBulkManager:
    def __init__(self, *, has_bars_fetcher: bool, has_options_fetcher: bool) -> None:
        self._has_bars_fetcher = has_bars_fetcher
        self._has_options_fetcher = has_options_fetcher
        self.set_bars_calls = 0
        self.set_options_calls = 0

    def has_bars_fetcher(self) -> bool:
        return self._has_bars_fetcher

    def set_bars_fetcher(self, _fetcher: Any) -> None:
        self.set_bars_calls += 1
        self._has_bars_fetcher = True

    def has_options_fetcher(self) -> bool:
        return self._has_options_fetcher

    def set_options_fetcher(self, _fetcher: Any) -> None:
        self.set_options_calls += 1
        self._has_options_fetcher = True

    async def create_bars_job(self, _request: Any, _client_id: str) -> Any:
        return SimpleNamespace(job_id="bulk-bars-job")

    async def create_options_job(self, _request: Any, _client_id: str) -> Any:
        return SimpleNamespace(job_id="bulk-options-job")


class _FakeEarningsCalendar:
    def __init__(self, *, has_fetcher: bool) -> None:
        self._has_fetcher = has_fetcher
        self.set_fetcher_calls = 0

    def has_fetcher(self) -> bool:
        return self._has_fetcher

    def set_fetcher(self, _fetcher: Any) -> None:
        self.set_fetcher_calls += 1
        self._has_fetcher = True

    async def fetch_earnings(self, _symbols: list[str], _start: Any, _end: Any) -> list[Any]:
        return []


class _FakeCorporateService:
    def __init__(self, *, has_fetcher: bool) -> None:
        self._has_fetcher = has_fetcher
        self.set_fetcher_calls = 0

    def has_fetcher(self) -> bool:
        return self._has_fetcher

    def set_fetcher(self, _fetcher: Any) -> None:
        self.set_fetcher_calls += 1
        self._has_fetcher = True


@pytest.mark.asyncio
async def test_bulk_bars_endpoint_skips_fetcher_rebind_when_already_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _FakeBulkManager(has_bars_fetcher=True, has_options_fetcher=False)
    registry = _FakeRegistry({"alpaca": object()})
    request = bulk.BulkBarsRequestModel(
        symbols=["AAPL"],
        start="2026-01-01",
        end="2026-01-02",
    )

    monkeypatch.setattr(bulk, "get_bulk_manager", lambda: manager)
    monkeypatch.setattr(bulk, "get_settings", lambda: SimpleNamespace(allow_stub_data=False))

    response = await bulk.create_bulk_bars_job(
        request=request,
        client=SimpleNamespace(id="test-client"),
        registry=cast(ProviderRegistry, registry),
    )

    assert response.job_id == "bulk-bars-job"
    assert manager.set_bars_calls == 0


@pytest.mark.asyncio
async def test_bulk_options_endpoint_skips_fetcher_rebind_when_already_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _FakeBulkManager(has_bars_fetcher=False, has_options_fetcher=True)
    registry = _FakeRegistry({"alpaca": object()})
    request = bulk.BulkOptionsRequestModel(
        underlyings=["AAPL"],
        date="2026-01-01",
    )

    monkeypatch.setattr(bulk, "get_bulk_manager", lambda: manager)
    monkeypatch.setattr(bulk, "get_settings", lambda: SimpleNamespace(allow_stub_data=False))

    response = await bulk.create_bulk_options_job(
        request=request,
        client=SimpleNamespace(id="test-client"),
        registry=cast(ProviderRegistry, registry),
    )

    assert response.job_id == "bulk-options-job"
    assert manager.set_options_calls == 0


@pytest.mark.asyncio
async def test_calendar_get_earnings_skips_fetcher_rebind_when_already_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    earnings_calendar = _FakeEarningsCalendar(has_fetcher=True)
    registry = _FakeRegistry({"finnhub": object()})

    monkeypatch.setattr(calendar, "get_earnings_calendar", lambda: earnings_calendar)
    monkeypatch.setattr(calendar, "get_settings", lambda: SimpleNamespace(allow_stub_data=False))

    response = await calendar.get_earnings(
        symbols="AAPL",
        start="2026-01-01",
        end="2026-01-02",
        client=SimpleNamespace(id="test-client"),
        registry=cast(ProviderRegistry, registry),
    )

    assert response.earnings == []
    assert earnings_calendar.set_fetcher_calls == 0


def test_corporate_fetcher_config_skips_rebind_when_already_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _FakeCorporateService(has_fetcher=True)
    registry = _FakeRegistry({"alpaca": object()})

    monkeypatch.setattr(corporate, "get_corporate_actions_service", lambda: service)

    corporate._configure_corporate_fetcher(registry=cast(ProviderRegistry, registry))

    assert service.set_fetcher_calls == 0
