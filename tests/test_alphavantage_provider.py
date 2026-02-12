from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from gateway.providers.alphavantage import (
    DEFAULT_QUOTES_MAX_CONCURRENCY,
    AlphaVantageProvider,
)


class _FakeHTTPResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return

    def json(self) -> dict[str, object]:
        return self._payload


class _FakeHTTPClient:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload
        self.calls: list[dict[str, object]] = []

    async def get(self, _url: str, params: dict[str, object]) -> _FakeHTTPResponse:
        self.calls.append(params)
        return _FakeHTTPResponse(self._payload)


def test_parse_csv_response_handles_quoted_commas() -> None:
    provider = AlphaVantageProvider()
    payload = 'symbol,name,reportDate\nAAPL,"Apple, Inc.",2026-02-01\nMSFT,"Microsoft Corporation",2026-02-02\n'

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


@pytest.mark.asyncio
async def test_fetch_json_injects_api_key_and_returns_payload() -> None:
    provider = AlphaVantageProvider()
    provider._api_key = "demo"  # pragma: allowlist secret
    provider._client = cast(Any, _FakeHTTPClient({"Global Quote": {"05. price": "10.0"}}))

    payload = await provider._fetch_json({"function": "GLOBAL_QUOTE", "symbol": "IBM"})

    assert payload == {"Global Quote": {"05. price": "10.0"}}
    assert provider._client.calls[0]["apikey"] == "demo"  # pragma: allowlist secret


@pytest.mark.asyncio
async def test_fetch_json_raises_on_rate_limit_note() -> None:
    provider = AlphaVantageProvider()
    provider._api_key = "demo"  # pragma: allowlist secret
    provider._client = cast(Any, _FakeHTTPClient({"Note": "Thank you for using Alpha Vantage!"}))

    with pytest.raises(RuntimeError, match="Rate limit exceeded"):
        await provider._fetch_json({"function": "GLOBAL_QUOTE", "symbol": "IBM"})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected_error"),
    [
        (
            {
                "Information": (
                    "Thank you for using Alpha Vantage! This is a premium endpoint. Please subscribe to unlock."
                )
            },
            "Premium endpoint",
        ),
        (
            {
                "Information": (
                    "Thank you for using Alpha Vantage! Please consider spreading out your free API requests "
                    "more sparingly (1 request per second)."
                )
            },
            "Rate limit exceeded",
        ),
        (
            {
                "Information": (
                    "Thank you for using Alpha Vantage! Please consider spreading out your free API requests "
                    "more sparingly (1 request per second). You may subscribe to premium plans to lift limits."
                )
            },
            "Rate limit exceeded",
        ),
        (
            {
                "Error Message": "Invalid API call. Please retry or visit the documentation.",
            },
            "Alpha Vantage error",
        ),
    ],
)
async def test_fetch_json_raises_on_information_and_error_payloads(
    payload: dict[str, str], expected_error: str
) -> None:
    provider = AlphaVantageProvider()
    provider._api_key = "demo"  # pragma: allowlist secret
    provider._client = cast(Any, _FakeHTTPClient(payload))

    with pytest.raises(RuntimeError, match=expected_error):
        await provider._fetch_json({"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": "AAPL"})


@pytest.mark.asyncio
async def test_get_daily_respects_max_points_window(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = AlphaVantageProvider()

    async def _fake_fetch_json(_params: dict[str, object]) -> dict[str, object]:
        return {
            "Time Series (Daily)": {
                "2026-02-03": {
                    "1. open": "100",
                    "2. high": "101",
                    "3. low": "99",
                    "4. close": "100.5",
                    "6. volume": "10",
                },
                "2026-02-02": {
                    "1. open": "99",
                    "2. high": "100",
                    "3. low": "98",
                    "4. close": "99.5",
                    "6. volume": "20",
                },
            }
        }

    monkeypatch.setattr(provider, "_fetch_json", _fake_fetch_json)

    bars = await provider.get_daily("AAPL", max_points=1)

    assert len(bars) == 1
    assert bars[0].symbol == "AAPL"
    assert bars[0].timestamp.isoformat() == "2026-02-03T00:00:00"


def test_top_time_series_items_fast_path_keeps_head_order() -> None:
    provider = AlphaVantageProvider()
    series = {
        "2026-02-03": {"v": 3},
        "2026-02-02": {"v": 2},
        "2026-02-01": {"v": 1},
    }

    top_items = provider._top_time_series_items(series, limit=2)

    assert [key for key, _ in top_items] == ["2026-02-03", "2026-02-02"]


def test_top_time_series_items_falls_back_to_sorted_for_unordered_input() -> None:
    provider = AlphaVantageProvider()
    series = {
        "2026-02-01": {"v": 1},
        "2026-02-03": {"v": 3},
        "2026-02-02": {"v": 2},
    }

    top_items = provider._top_time_series_items(series, limit=2)

    assert [key for key, _ in top_items] == ["2026-02-03", "2026-02-02"]
