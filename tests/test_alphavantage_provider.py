from __future__ import annotations

import asyncio

import pytest

from gateway.providers.alphavantage import (
    DEFAULT_QUOTES_MAX_CONCURRENCY,
    AlphaVantageProvider,
)


def test_parse_csv_response_handles_quoted_commas() -> None:
    provider = AlphaVantageProvider()
    payload = (
        "symbol,name,reportDate\n"
        'AAPL,"Apple, Inc.",2026-02-01\n'
        'MSFT,"Microsoft Corporation",2026-02-02\n'
    )

    rows = provider._parse_csv_response(payload)

    assert rows == [
        {"symbol": "AAPL", "name": "Apple, Inc.", "reportDate": "2026-02-01"},
        {"symbol": "MSFT", "name": "Microsoft Corporation", "reportDate": "2026-02-02"},
    ]


@pytest.mark.asyncio
async def test_initialize_reads_quotes_max_concurrency_without_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ALPHAVANTAGE_API_KEY", raising=False)
    provider = AlphaVantageProvider()

    await provider.initialize({"quotes_max_concurrency": "4"})

    assert provider._quotes_max_concurrency == 4


@pytest.mark.asyncio
async def test_initialize_invalid_quotes_max_concurrency_uses_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ALPHAVANTAGE_API_KEY", raising=False)
    provider = AlphaVantageProvider()

    await provider.initialize({"quotes_max_concurrency": "invalid"})

    assert provider._quotes_max_concurrency == DEFAULT_QUOTES_MAX_CONCURRENCY


@pytest.mark.asyncio
async def test_get_quotes_respects_bounded_concurrency(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = AlphaVantageProvider()
    provider._quotes_max_concurrency = 2

    state = {"inflight": 0, "max_inflight": 0}

    async def _fake_get_quote(symbol: str):
        state["inflight"] += 1
        if state["inflight"] > state["max_inflight"]:
            state["max_inflight"] = state["inflight"]
        await asyncio.sleep(0.01)
        state["inflight"] -= 1
        return symbol

    monkeypatch.setattr(provider, "get_quote", _fake_get_quote)
    results = await provider.get_quotes(["AAPL", "MSFT", "GOOG", "TSLA"])

    assert results == ["AAPL", "MSFT", "GOOG", "TSLA"]
    assert state["max_inflight"] == 2
    assert state["inflight"] == 0
