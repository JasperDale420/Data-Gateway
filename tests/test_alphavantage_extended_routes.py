from __future__ import annotations

from typing import Any

import pytest

from gateway.api.alphavantage import crypto, forex, indicators


class _FakeProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def get_technical_indicator(
        self,
        symbol: str,
        indicator: str,
        interval: str,
        time_period: int,
        series_type: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "indicator",
                {
                    "symbol": symbol,
                    "indicator": indicator,
                    "interval": interval,
                    "time_period": time_period,
                    "series_type": series_type,
                    **kwargs,
                },
            )
        )
        return {"data": []}

    async def get_forex_daily(self, from_symbol: str, to_symbol: str, **kwargs: Any) -> list[Any]:
        self.calls.append(
            ("forex_daily", {"from_symbol": from_symbol, "to_symbol": to_symbol, **kwargs})
        )
        return []

    async def get_crypto_daily(self, symbol: str, market: str, **kwargs: Any) -> list[Any]:
        self.calls.append(("crypto_daily", {"symbol": symbol, "market": market, **kwargs}))
        return []


@pytest.mark.asyncio
async def test_indicator_route_forwards_max_points_and_keys_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    seen: dict[str, Any] = {}

    async def _fake_execute_av_cached(*, cache_key_value: str, fetcher, **kwargs: Any):
        seen["cache_key"] = cache_key_value
        seen["cache_mode"] = kwargs["cache_mode"]
        result = await fetcher(provider)
        return {"success": True, "data": result, "meta": {"cached": False}}

    monkeypatch.setattr(indicators, "execute_av_cached", _fake_execute_av_cached)

    response = await indicators.get_technical_indicator(
        indicator="RSI",
        symbol="AAPL",
        interval="daily",
        time_period=14,
        series_type="close",
        max_points=200,
        client=object(),
        registry=object(),
        cache=object(),
    )

    assert response["success"] is True
    assert seen["cache_key"].endswith(":200")
    assert provider.calls == [
        (
            "indicator",
            {
                "symbol": "AAPL",
                "indicator": "RSI",
                "interval": "daily",
                "time_period": 14,
                "series_type": "close",
                "max_points": 200,
            },
        )
    ]


@pytest.mark.asyncio
async def test_forex_daily_route_forwards_max_points_and_keys_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    seen: dict[str, Any] = {}

    async def _fake_execute_av_cached(*, cache_key_value: str, fetcher, **kwargs: Any):
        seen["cache_key"] = cache_key_value
        result = await fetcher(provider)
        return {"success": True, "data": result, "meta": {"cached": False}}

    monkeypatch.setattr(forex, "execute_av_cached", _fake_execute_av_cached)

    response = await forex.get_forex_daily(
        from_symbol="EUR",
        to_symbol="USD",
        max_points=150,
        client=object(),
        registry=object(),
        cache=object(),
    )

    assert response["success"] is True
    assert seen["cache_key"].endswith(":150")
    assert provider.calls == [
        (
            "forex_daily",
            {"from_symbol": "EUR", "to_symbol": "USD", "max_points": 150},
        )
    ]


@pytest.mark.asyncio
async def test_crypto_daily_route_forwards_max_points_and_keys_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeProvider()
    seen: dict[str, Any] = {}

    async def _fake_execute_av_cached(*, cache_key_value: str, fetcher, **kwargs: Any):
        seen["cache_key"] = cache_key_value
        result = await fetcher(provider)
        return {"success": True, "data": result, "meta": {"cached": False}}

    monkeypatch.setattr(crypto, "execute_av_cached", _fake_execute_av_cached)

    response = await crypto.get_crypto_daily(
        symbol="BTC",
        market="USD",
        max_points=120,
        client=object(),
        registry=object(),
        cache=object(),
    )

    assert response["success"] is True
    assert seen["cache_key"].endswith(":120")
    assert provider.calls == [
        ("crypto_daily", {"symbol": "BTC", "market": "USD", "max_points": 120})
    ]
