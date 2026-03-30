from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from gateway.api import calendar as calendar_api
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self, provider: Any) -> None:
        self._provider = provider

    def get(self, name: str) -> Any:
        if name == "alpaca":
            return self._provider
        return None


class _FailingCalendarProvider:
    def __init__(self) -> None:
        self.calls = 0

    def get_calendar(self, *_args: Any, **_kwargs: Any) -> list[dict[str, str]]:
        self.calls += 1
        raise RuntimeError("upstream calendar unavailable")


def _reset_calendar_provider_degraded_state() -> None:
    calendar_api._CALENDAR_PROVIDER_DEGRADED_UNTIL.clear()
    calendar_api._CALENDAR_PROVIDER_LAST_LOG_AT.clear()


def test_calendar_provider_degraded_state_window() -> None:
    _reset_calendar_provider_degraded_state()

    calendar_api._mark_calendar_provider_failure("market_hours", RuntimeError("boom"), now_monotonic=100.0)

    assert calendar_api._calendar_provider_is_degraded("market_hours", now_monotonic=110.0) is True
    assert calendar_api._calendar_provider_is_degraded("market_hours", now_monotonic=131.0) is False

    calendar_api._mark_calendar_provider_success("market_hours")
    assert calendar_api._calendar_provider_is_degraded("market_hours", now_monotonic=101.0) is False


@pytest.mark.asyncio
async def test_market_hours_skips_provider_calls_while_degraded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_calendar_provider_degraded_state()
    provider = _FailingCalendarProvider()
    registry = cast(ProviderRegistry, _FakeRegistry(provider))

    async def _no_rate_limit(_provider_name: str) -> None:
        return None

    monkeypatch.setattr(calendar_api, "require_provider_rate_limit", _no_rate_limit)

    response_one = await calendar_api.get_market_hours(
        date_str="2024-01-16",
        client=SimpleNamespace(id="client-1"),
        registry=registry,
    )
    response_two = await calendar_api.get_market_hours(
        date_str="2024-01-16",
        client=SimpleNamespace(id="client-1"),
        registry=registry,
    )

    assert provider.calls == 1
    assert response_one.date == "2024-01-16"
    assert response_two.date == "2024-01-16"


@pytest.mark.asyncio
async def test_trading_days_retries_provider_after_degraded_window_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_calendar_provider_degraded_state()
    provider = _FailingCalendarProvider()
    registry = cast(ProviderRegistry, _FakeRegistry(provider))

    async def _no_rate_limit(_provider_name: str) -> None:
        return None

    monkeypatch.setattr(calendar_api, "require_provider_rate_limit", _no_rate_limit)

    response_one = await calendar_api.get_trading_days(
        start="2024-01-01",
        end="2024-01-05",
        client=SimpleNamespace(id="client-1"),
        registry=registry,
    )
    calendar_api._CALENDAR_PROVIDER_DEGRADED_UNTIL["trading_days"] = 0.0
    response_two = await calendar_api.get_trading_days(
        start="2024-01-01",
        end="2024-01-05",
        client=SimpleNamespace(id="client-1"),
        registry=registry,
    )

    assert provider.calls == 2
    assert isinstance(response_one.trading_days, list)
    assert isinstance(response_two.trading_days, list)
