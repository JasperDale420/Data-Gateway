from __future__ import annotations

import asyncio
from decimal import Decimal
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
async def test_get_quotes_skips_per_symbol_failures_and_returns_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contract: a failing symbol is skipped; remaining symbols are returned."""
    provider = AlphaVantageProvider()
    provider._quotes_max_concurrency = 3

    async def _fake_get_quote(symbol: str):
        if symbol == "FAIL":
            raise RuntimeError("boom")
        return symbol

    monkeypatch.setattr(provider, "get_quote", _fake_get_quote)

    results = await provider.get_quotes(["AAPL", "FAIL", "MSFT"])

    assert results == ["AAPL", "MSFT"]


@pytest.mark.asyncio
async def test_get_quotes_total_failure_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Contract: when every symbol fails, return an empty list (no raise)."""
    provider = AlphaVantageProvider()
    provider._quotes_max_concurrency = 3

    async def _fake_get_quote(symbol: str):
        raise RuntimeError("boom")

    monkeypatch.setattr(provider, "get_quote", _fake_get_quote)

    results = await provider.get_quotes(["AAPL", "MSFT"])

    assert results == []


@pytest.mark.asyncio
async def test_get_quotes_empty_input_returns_empty() -> None:
    """Contract: empty symbols list returns an empty list."""
    provider = AlphaVantageProvider()

    assert await provider.get_quotes([]) == []


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
    assert bars[0].timestamp.isoformat() == "2026-02-03T00:00:00+00:00"


@pytest.mark.asyncio
async def test_get_daily_falls_back_to_unadjusted_on_premium_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = AlphaVantageProvider()
    calls: list[str] = []

    async def _fake_fetch_json(params: dict[str, object]) -> dict[str, object]:
        function = str(params["function"])
        calls.append(function)
        if function == "TIME_SERIES_DAILY_ADJUSTED":
            raise RuntimeError("Premium endpoint requires Alpha Vantage subscription")
        return {
            "Time Series (Daily)": {
                "2026-02-03": {
                    "1. open": "100",
                    "2. high": "101",
                    "3. low": "99",
                    "4. close": "100.5",
                    "5. volume": "10",
                },
            }
        }

    monkeypatch.setattr(provider, "_fetch_json", _fake_fetch_json)

    bars = await provider.get_daily("AAPL", adjusted=True, max_points=1)

    assert calls == ["TIME_SERIES_DAILY_ADJUSTED", "TIME_SERIES_DAILY"]
    assert len(bars) == 1
    assert bars[0].close == Decimal("100.5")


@pytest.mark.asyncio
async def test_get_weekly_falls_back_to_unadjusted_on_premium_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = AlphaVantageProvider()
    calls: list[str] = []

    async def _fake_fetch_json(params: dict[str, object]) -> dict[str, object]:
        function = str(params["function"])
        calls.append(function)
        if function == "TIME_SERIES_WEEKLY_ADJUSTED":
            raise RuntimeError("Premium endpoint requires Alpha Vantage subscription")
        return {
            "Weekly Time Series": {
                "2026-02-06": {
                    "1. open": "100",
                    "2. high": "101",
                    "3. low": "99",
                    "4. close": "100.5",
                    "5. volume": "10",
                },
            }
        }

    monkeypatch.setattr(provider, "_fetch_json", _fake_fetch_json)

    bars = await provider.get_weekly("AAPL", adjusted=True, max_points=1)

    assert calls == ["TIME_SERIES_WEEKLY_ADJUSTED", "TIME_SERIES_WEEKLY"]
    assert len(bars) == 1
    assert bars[0].close == Decimal("100.5")


@pytest.mark.asyncio
async def test_get_monthly_falls_back_to_unadjusted_on_premium_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = AlphaVantageProvider()
    calls: list[str] = []

    async def _fake_fetch_json(params: dict[str, object]) -> dict[str, object]:
        function = str(params["function"])
        calls.append(function)
        if function == "TIME_SERIES_MONTHLY_ADJUSTED":
            raise RuntimeError("Premium endpoint requires Alpha Vantage subscription")
        return {
            "Monthly Time Series": {
                "2026-02-01": {
                    "1. open": "100",
                    "2. high": "101",
                    "3. low": "99",
                    "4. close": "100.5",
                    "5. volume": "10",
                },
            }
        }

    monkeypatch.setattr(provider, "_fetch_json", _fake_fetch_json)

    bars = await provider.get_monthly("AAPL", adjusted=True, max_points=1)

    assert calls == ["TIME_SERIES_MONTHLY_ADJUSTED", "TIME_SERIES_MONTHLY"]
    assert len(bars) == 1
    assert bars[0].close == Decimal("100.5")
    assert bars[0].volume == 10


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


# ─────────────────────────────────────────────────────────────────────
# BLOCKER 5: canonical get_bars dispatcher
# ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_bars_dispatches_daily(monkeypatch: pytest.MonkeyPatch) -> None:
    """`get_bars(..., "1Day", ...)` must route to `get_daily`."""
    provider = AlphaVantageProvider()
    seen: list[tuple[str, dict[str, Any]]] = []

    from gateway.schemas import NormalizedBar  # noqa: PLC0415

    aware_ts = __import__("datetime").datetime(2026, 2, 3, tzinfo=__import__("datetime").UTC)

    async def _fake_daily(symbol: str, **kw: Any) -> list[NormalizedBar]:
        seen.append((symbol, kw))
        return [
            NormalizedBar(
                symbol=symbol,
                timestamp=aware_ts,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.5"),
                volume=Decimal("10"),
                provider="alphavantage",
                timeframe="1Day",
            )
        ]

    monkeypatch.setattr(provider, "get_daily", _fake_daily)
    bars = await provider.get_bars(
        symbols=["AAPL"],
        timeframe="1Day",
        start=__import__("datetime").datetime(2026, 2, 1, tzinfo=__import__("datetime").UTC),
        end=__import__("datetime").datetime(2026, 2, 5, tzinfo=__import__("datetime").UTC),
    )
    assert [b.symbol for b in bars] == ["AAPL"]
    assert seen and seen[0][0] == "AAPL"


@pytest.mark.asyncio
async def test_get_bars_dispatches_intraday(monkeypatch: pytest.MonkeyPatch) -> None:
    """`get_bars(..., "5Min", ...)` must route to `get_intraday` with `5min`."""
    from datetime import UTC, datetime  # noqa: PLC0415

    from gateway.schemas import NormalizedBar  # noqa: PLC0415

    provider = AlphaVantageProvider()
    captured: list[dict[str, Any]] = []

    async def _fake_intraday(symbol: str, **kw: Any) -> list[NormalizedBar]:
        captured.append({"symbol": symbol, **kw})
        return [
            NormalizedBar(
                symbol=symbol,
                timestamp=datetime(2026, 2, 3, 14, 30, tzinfo=UTC),
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.5"),
                volume=Decimal("10"),
                provider="alphavantage",
                timeframe="5Min",
            )
        ]

    monkeypatch.setattr(provider, "get_intraday", _fake_intraday)
    bars = await provider.get_bars(
        symbols=["AAPL"],
        timeframe="5Min",
        start=datetime(2026, 2, 1, tzinfo=UTC),
        end=datetime(2026, 2, 5, tzinfo=UTC),
    )
    assert len(bars) == 1
    assert captured[0]["interval"] == "5min"


@pytest.mark.asyncio
async def test_get_bars_dispatches_weekly_and_monthly(monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import UTC, datetime  # noqa: PLC0415

    from gateway.schemas import NormalizedBar  # noqa: PLC0415

    provider = AlphaVantageProvider()
    call_log: list[str] = []

    def _make_fake(name: str, ts: datetime) -> Any:
        async def _fake(symbol: str, **_kw: Any) -> list[NormalizedBar]:
            call_log.append(name)
            return [
                NormalizedBar(
                    symbol=symbol,
                    timestamp=ts,
                    open=Decimal("1"),
                    high=Decimal("2"),
                    low=Decimal("1"),
                    close=Decimal("1"),
                    volume=Decimal("0"),
                    provider="alphavantage",
                )
            ]

        return _fake

    monkeypatch.setattr(provider, "get_weekly", _make_fake("weekly", datetime(2026, 2, 6, tzinfo=UTC)))
    monkeypatch.setattr(provider, "get_monthly", _make_fake("monthly", datetime(2026, 2, 1, tzinfo=UTC)))

    await provider.get_bars(["AAPL"], "1Week", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 12, 31, tzinfo=UTC))
    await provider.get_bars(["AAPL"], "1Month", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 12, 31, tzinfo=UTC))
    assert call_log == ["weekly", "monthly"]


@pytest.mark.asyncio
async def test_get_bars_filters_by_start_end_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bars outside [start, end] are dropped client-side."""
    from datetime import UTC, datetime  # noqa: PLC0415

    from gateway.schemas import NormalizedBar  # noqa: PLC0415

    provider = AlphaVantageProvider()

    async def _fake_daily(symbol: str, **_kw: Any) -> list[NormalizedBar]:
        return [
            NormalizedBar(
                symbol=symbol,
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                open=Decimal("1"),
                high=Decimal("1"),
                low=Decimal("1"),
                close=Decimal("1"),
                volume=Decimal("0"),
                provider="alphavantage",
            ),
            NormalizedBar(
                symbol=symbol,
                timestamp=datetime(2026, 6, 1, tzinfo=UTC),
                open=Decimal("1"),
                high=Decimal("1"),
                low=Decimal("1"),
                close=Decimal("1"),
                volume=Decimal("0"),
                provider="alphavantage",
            ),
        ]

    monkeypatch.setattr(provider, "get_daily", _fake_daily)
    bars = await provider.get_bars(["AAPL"], "1Day", datetime(2026, 5, 1, tzinfo=UTC), datetime(2026, 7, 1, tzinfo=UTC))
    assert [b.timestamp.month for b in bars] == [6]


@pytest.mark.asyncio
async def test_get_bars_unknown_timeframe_raises() -> None:
    from datetime import UTC, datetime  # noqa: PLC0415

    provider = AlphaVantageProvider()
    with pytest.raises(ValueError, match="unsupported timeframe"):
        await provider.get_bars(["AAPL"], "1Tick", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC))
