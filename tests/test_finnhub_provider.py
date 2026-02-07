from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from gateway.providers.finnhub import FinnhubProvider


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    def __init__(self, payload: dict):
        self.payload = payload

    async def get(self, *_args, **_kwargs) -> _FakeResponse:
        return _FakeResponse(self.payload)


@pytest.mark.asyncio
async def test_get_bars_maps_rows_with_zip_iteration() -> None:
    provider = FinnhubProvider()
    provider._client = _FakeClient(
        {
            "s": "ok",
            "t": [1700000000, 1700000060],
            "o": [100.0, 101.0],
            "h": [101.0, 102.0],
            "l": [99.0, 100.0],
            "c": [100.5, 101.5],
            "v": [1000, 2000],
        }
    )

    bars = await provider.get_bars("aapl", resolution="1")

    assert len(bars) == 2
    assert bars[0].symbol == "AAPL"
    assert bars[0].timeframe == "1Min"
    assert bars[0].timestamp == datetime.fromtimestamp(1700000000, tz=UTC)
    assert bars[0].open == Decimal("100.0")
    assert bars[1].close == Decimal("101.5")


@pytest.mark.asyncio
async def test_get_bars_handles_mismatched_source_arrays_without_index_error() -> None:
    provider = FinnhubProvider()
    provider._client = _FakeClient(
        {
            "s": "ok",
            "t": [1700000000, 1700000060, 1700000120],
            "o": [100.0, 101.0],
            "h": [101.0, 102.0],
            "l": [99.0, 100.0],
            "c": [100.5, 101.5],
            "v": [1000, 2000],
        }
    )

    bars = await provider.get_bars("aapl", resolution="D")

    assert len(bars) == 2
    assert all(bar.timeframe == "1Day" for bar in bars)
