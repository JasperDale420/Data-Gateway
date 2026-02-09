from __future__ import annotations

from typing import Any

import pytest

from gateway.api.alphavantage import timeseries


class _FakeProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def get_intraday(self, symbol: str, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("intraday", {"symbol": symbol, **kwargs}))
        return []

    async def get_daily(self, symbol: str, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("daily", {"symbol": symbol, **kwargs}))
        return []


@pytest.mark.asyncio
async def test_intraday_route_forwards_max_points_and_keys_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    seen: dict[str, Any] = {}

    async def _fake_execute_av_cached(*, cache_key_value: str, fetcher, **kwargs: Any):
        seen["cache_key"] = cache_key_value
        seen["cache_mode"] = kwargs["cache_mode"]
        result = await fetcher(provider)
        return {"success": True, "data": result, "meta": {"cached": False}}

    monkeypatch.setattr(timeseries, "execute_av_cached", _fake_execute_av_cached)

    response = await timeseries.get_intraday(
        symbol="AAPL",
        interval="5min",
        outputsize="full",
        max_points=250,
        client=object(),
        registry=object(),
        cache=object(),
    )

    assert response["success"] is True
    assert seen["cache_key"].endswith(":250")
    assert seen["cache_mode"] == "full"
    assert provider.calls == [
        (
            "intraday",
            {"symbol": "AAPL", "interval": "5min", "outputsize": "full", "max_points": 250},
        )
    ]


@pytest.mark.asyncio
async def test_daily_route_forwards_max_points_and_keys_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    seen: dict[str, Any] = {}

    async def _fake_execute_av_cached(*, cache_key_value: str, fetcher, **kwargs: Any):
        seen["cache_key"] = cache_key_value
        seen["cache_mode"] = kwargs["cache_mode"]
        result = await fetcher(provider)
        return {"success": True, "data": result, "meta": {"cached": False}}

    monkeypatch.setattr(timeseries, "execute_av_cached", _fake_execute_av_cached)

    response = await timeseries.get_daily(
        symbol="MSFT",
        outputsize="compact",
        adjusted=True,
        max_points=75,
        client=object(),
        registry=object(),
        cache=object(),
    )

    assert response["success"] is True
    assert seen["cache_key"].endswith(":75")
    assert seen["cache_mode"] == "compact"
    assert provider.calls == [
        (
            "daily",
            {"symbol": "MSFT", "outputsize": "compact", "adjusted": True, "max_points": 75},
        )
    ]
