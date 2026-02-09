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


@pytest.mark.asyncio
async def test_get_quotes_records_requested_batch_size(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = AlphaVantageProvider()
    provider._quotes_max_concurrency = 2
    recorded: list[tuple[str, int]] = []

    async def _fake_get_quote(symbol: str):
        await asyncio.sleep(0)
        return symbol

    monkeypatch.setattr(provider, "get_quote", _fake_get_quote)
    monkeypatch.setattr(
        "gateway.providers.alphavantage.record_provider_quote_batch_size",
        lambda provider_name, batch_size: recorded.append((provider_name, batch_size)),
    )

    results = await provider.get_quotes(["AAPL", "MSFT", "GOOG"])

    assert results == ["AAPL", "MSFT", "GOOG"]
    assert recorded == [("alphavantage", 3)]


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
async def test_get_intraday_honors_max_points(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = AlphaVantageProvider()

    async def _fake_fetch_json(_params: dict[str, object]) -> dict[str, object]:
        return {
            "Time Series (5min)": {
                "2026-02-09 09:35:00": {
                    "1. open": "101.0",
                    "2. high": "102.0",
                    "3. low": "100.5",
                    "4. close": "101.5",
                    "5. volume": "1200",
                },
                "2026-02-09 09:30:00": {
                    "1. open": "100.0",
                    "2. high": "101.0",
                    "3. low": "99.5",
                    "4. close": "100.5",
                    "5. volume": "1100",
                },
                "2026-02-09 09:25:00": {
                    "1. open": "99.0",
                    "2. high": "100.0",
                    "3. low": "98.5",
                    "4. close": "99.5",
                    "5. volume": "1000",
                },
            }
        }

    monkeypatch.setattr(provider, "_fetch_json", _fake_fetch_json)

    bars = await provider.get_intraday("AAPL", interval="5min", outputsize="full", max_points=2)

    assert len(bars) == 2
    assert [bar.timestamp.isoformat() for bar in bars] == [
        "2026-02-09T09:35:00",
        "2026-02-09T09:30:00",
    ]


@pytest.mark.asyncio
async def test_get_monthly_honors_max_points_and_keeps_ascending_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = AlphaVantageProvider()

    async def _fake_fetch_json(_params: dict[str, object]) -> dict[str, object]:
        return {
            "Monthly Adjusted Time Series": {
                "2026-03-01": {
                    "1. open": "103.0",
                    "2. high": "104.0",
                    "3. low": "102.0",
                    "4. close": "103.5",
                    "5. adjusted close": "103.5",
                    "6. volume": "10000",
                },
                "2026-02-01": {
                    "1. open": "102.0",
                    "2. high": "103.0",
                    "3. low": "101.0",
                    "4. close": "102.5",
                    "5. adjusted close": "102.5",
                    "6. volume": "9000",
                },
                "2026-01-01": {
                    "1. open": "101.0",
                    "2. high": "102.0",
                    "3. low": "100.0",
                    "4. close": "101.5",
                    "5. adjusted close": "101.5",
                    "6. volume": "8000",
                },
            }
        }

    monkeypatch.setattr(provider, "_fetch_json", _fake_fetch_json)

    bars = await provider.get_monthly("AAPL", adjusted=True, max_points=2)

    assert len(bars) == 2
    assert [bar.timestamp.date().isoformat() for bar in bars] == ["2026-02-01", "2026-03-01"]


@pytest.mark.asyncio
async def test_get_technical_indicator_honors_max_points(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = AlphaVantageProvider()

    async def _fake_fetch_json(_params: dict[str, object]) -> dict[str, object]:
        return {
            "Technical Analysis: RSI": {
                "2026-02-09": {"RSI": "62.1"},
                "2026-02-08": {"RSI": "60.8"},
                "2026-02-07": {"RSI": "58.3"},
            },
            "Meta Data": {"1: Symbol": "AAPL"},
        }

    monkeypatch.setattr(provider, "_fetch_json", _fake_fetch_json)

    data = await provider.get_technical_indicator("AAPL", "RSI", max_points=2)

    assert [point["date"] for point in data["data"]] == ["2026-02-09", "2026-02-08"]


@pytest.mark.asyncio
async def test_get_forex_daily_honors_max_points(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = AlphaVantageProvider()

    async def _fake_fetch_json(_params: dict[str, object]) -> dict[str, object]:
        return {
            "Time Series FX (Daily)": {
                "2026-02-09": {
                    "1. open": "1.1",
                    "2. high": "1.2",
                    "3. low": "1.0",
                    "4. close": "1.15",
                },
                "2026-02-08": {
                    "1. open": "1.0",
                    "2. high": "1.1",
                    "3. low": "0.9",
                    "4. close": "1.05",
                },
                "2026-02-07": {
                    "1. open": "0.9",
                    "2. high": "1.0",
                    "3. low": "0.8",
                    "4. close": "0.95",
                },
            }
        }

    monkeypatch.setattr(provider, "_fetch_json", _fake_fetch_json)

    points = await provider.get_forex_daily("EUR", "USD", max_points=2)

    assert [point["date"] for point in points] == ["2026-02-09", "2026-02-08"]


@pytest.mark.asyncio
async def test_get_crypto_daily_honors_max_points(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = AlphaVantageProvider()

    async def _fake_fetch_json(_params: dict[str, object]) -> dict[str, object]:
        return {
            "Time Series (Digital Currency Daily)": {
                "2026-02-09": {
                    "1a. open (USD)": "50000",
                    "2a. high (USD)": "51000",
                    "3a. low (USD)": "49500",
                    "4a. close (USD)": "50500",
                    "5. volume": "1000",
                },
                "2026-02-08": {
                    "1a. open (USD)": "49000",
                    "2a. high (USD)": "50000",
                    "3a. low (USD)": "48500",
                    "4a. close (USD)": "49500",
                    "5. volume": "1200",
                },
                "2026-02-07": {
                    "1a. open (USD)": "48000",
                    "2a. high (USD)": "49000",
                    "3a. low (USD)": "47500",
                    "4a. close (USD)": "48500",
                    "5. volume": "900",
                },
            }
        }

    monkeypatch.setattr(provider, "_fetch_json", _fake_fetch_json)

    points = await provider.get_crypto_daily("BTC", market="USD", max_points=2)

    assert [point["date"] for point in points] == ["2026-02-09", "2026-02-08"]


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
