from __future__ import annotations

import asyncio
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


@pytest.mark.asyncio
async def test_get_quotes_uses_bounded_concurrency(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = FinnhubProvider()
    provider._quotes_max_concurrency = 2

    inflight = 0
    max_inflight = 0

    async def _fake_get_quote(symbol: str):
        nonlocal inflight
        nonlocal max_inflight
        inflight += 1
        max_inflight = max(max_inflight, inflight)
        await asyncio.sleep(0.01)
        inflight -= 1
        return {"symbol": symbol}

    monkeypatch.setattr(provider, "get_quote", _fake_get_quote)

    quotes = await provider.get_quotes(["AAPL", "MSFT", "NVDA", "TSLA"])

    assert len(quotes) == 4
    assert max_inflight <= 2


@pytest.mark.asyncio
async def test_get_quotes_skips_errors_without_failing_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = FinnhubProvider()
    provider._quotes_max_concurrency = 3

    async def _fake_get_quote(symbol: str):
        if symbol == "FAIL":
            raise RuntimeError("boom")
        await asyncio.sleep(0)
        return {"symbol": symbol}

    monkeypatch.setattr(provider, "get_quote", _fake_get_quote)

    quotes = await provider.get_quotes(["AAPL", "FAIL", "MSFT"])

    assert quotes == [{"symbol": "AAPL"}, {"symbol": "MSFT"}]


@pytest.mark.asyncio
async def test_get_quotes_records_requested_batch_size(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = FinnhubProvider()
    provider._quotes_max_concurrency = 2
    recorded: list[tuple[str, int]] = []

    async def _fake_get_quote(symbol: str):
        await asyncio.sleep(0)
        return {"symbol": symbol}

    monkeypatch.setattr(provider, "get_quote", _fake_get_quote)
    monkeypatch.setattr(
        "gateway.providers.finnhub.record_provider_quote_batch_size",
        lambda provider_name, batch_size: recorded.append((provider_name, batch_size)),
    )

    quotes = await provider.get_quotes(["AAPL", "MSFT", "NVDA"])

    assert len(quotes) == 3
    assert recorded == [("finnhub", 3)]
