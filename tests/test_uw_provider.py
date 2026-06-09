"""Tests for Unusual Whales provider concurrency guardrails."""

import asyncio
import time

import httpx
import pytest

from gateway.providers.uw import DEFAULT_UW_MAX_INFLIGHT_CALLS, UnusualWhalesProvider


@pytest.mark.asyncio
async def test_initialize_reads_max_inflight_calls_without_api_key(monkeypatch):
    """Initialization should apply concurrency config even without API key."""
    monkeypatch.delenv("UNUSUAL_WHALES_API_KEY", raising=False)
    provider = UnusualWhalesProvider()

    await provider.initialize({"max_inflight_calls": "7"})

    assert provider._max_inflight_calls == 7


@pytest.mark.asyncio
async def test_initialize_invalid_max_inflight_uses_default(monkeypatch):
    """Invalid max_inflight_calls should fall back to safe default."""
    monkeypatch.delenv("UNUSUAL_WHALES_API_KEY", raising=False)
    provider = UnusualWhalesProvider()

    await provider.initialize({"max_inflight_calls": "invalid"})

    assert provider._max_inflight_calls == DEFAULT_UW_MAX_INFLIGHT_CALLS


@pytest.mark.asyncio
async def test_call_sync_uses_semaphore_and_records_metrics(monkeypatch):
    """_call_sync should enforce bounded concurrency and emit timing metrics."""
    provider = UnusualWhalesProvider()
    provider._call_sync_semaphore = asyncio.Semaphore(1)

    waits: list[float] = []
    execs: list[float] = []
    state = {"inflight": 0, "max_inflight": 0}

    def _record_wait(_provider: str, duration: float) -> None:
        waits.append(duration)

    def _record_exec(_provider: str, duration: float) -> None:
        execs.append(duration)

    def _inc_inflight(_provider: str) -> None:
        state["inflight"] += 1
        if state["inflight"] > state["max_inflight"]:
            state["max_inflight"] = state["inflight"]

    def _dec_inflight(_provider: str) -> None:
        state["inflight"] -= 1

    monkeypatch.setattr("gateway.providers.uw._base.record_provider_sync_call_wait", _record_wait)
    monkeypatch.setattr("gateway.providers.uw._base.record_provider_sync_call_exec", _record_exec)
    monkeypatch.setattr("gateway.providers.uw._base.inc_provider_sync_call_inflight", _inc_inflight)
    monkeypatch.setattr("gateway.providers.uw._base.dec_provider_sync_call_inflight", _dec_inflight)

    def _blocking_call() -> int:
        time.sleep(0.05)
        return 1

    results = await asyncio.gather(
        provider._call_sync(_blocking_call),
        provider._call_sync(_blocking_call),
    )

    assert results == [1, 1]
    assert len(waits) == 2
    assert len(execs) == 2
    assert max(waits) > 0.01
    assert state["max_inflight"] == 1
    assert state["inflight"] == 0


class _FakeResponse:
    def __init__(self, data):
        self.additional_properties = {"data": data}


@pytest.mark.asyncio
async def test_get_flow_alerts_uses_native_offset_when_supported(monkeypatch):
    """When SDK supports offset, provider should use it directly."""
    provider = UnusualWhalesProvider()
    provider._client = object()

    calls: list[dict[str, int]] = []

    async def _fake_call_sync(_func, *args, **kwargs):
        del args
        calls.append(kwargs)
        return _FakeResponse(
            [
                {"ticker": "AAPL", "strike": 100, "expiry": "2026-01-01", "total_premium": 1},
            ]
        )

    monkeypatch.setattr(provider, "_call_sync", _fake_call_sync)
    monkeypatch.setattr(provider, "_normalize_flow_alert", lambda item: item)

    results = await provider.get_flow_alerts(limit=3, offset=8)

    assert len(calls) == 1
    assert calls[0]["limit"] == 3
    assert calls[0]["offset"] == 8
    assert len(results) == 1


@pytest.mark.asyncio
async def test_get_flow_alerts_falls_back_to_local_offset_slicing(monkeypatch):
    """When SDK offset/page params are unsupported, provider should overfetch then slice."""
    provider = UnusualWhalesProvider()
    provider._client = object()

    calls: list[dict[str, int]] = []

    async def _fake_call_sync(_func, *args, **kwargs):
        del args
        calls.append(kwargs)
        if "offset" in kwargs or "page" in kwargs:
            raise TypeError("unsupported pagination parameter")
        limit = kwargs["limit"]
        return _FakeResponse(
            [{"ticker": "AAPL", "strike": 100, "expiry": "2026-01-01", "total_premium": idx} for idx in range(limit)]
        )

    monkeypatch.setattr(provider, "_call_sync", _fake_call_sync)
    monkeypatch.setattr(provider, "_normalize_flow_alert", lambda item: item)

    # `limit=3` here represents a precomputed page-size request (e.g. API route `limit+1`).
    results = await provider.get_flow_alerts(limit=3, offset=2)

    assert calls[0]["offset"] == 2
    assert calls[1]["page"] == 2
    assert calls[2]["limit"] == 5
    assert len(results) == 3
    assert results[0]["total_premium"] == 2


class _FakeHTTPResponse:
    def __init__(self, payload, *, status_error: Exception | None = None):
        self._payload = payload
        self._status_error = status_error

    def raise_for_status(self) -> None:
        if self._status_error is not None:
            raise self._status_error
        return None

    def json(self):
        return self._payload


class _FakeHTTPClient:
    def __init__(self, response: _FakeHTTPResponse | list[_FakeHTTPResponse]) -> None:
        self._responses = response if isinstance(response, list) else [response]
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    def get(self, path: str, params: dict[str, str] | None = None):
        self.calls.append((path, params))
        idx = min(len(self.calls) - 1, len(self._responses) - 1)
        return self._responses[idx]

    def request(self, method: str, url: str, params: dict[str, str] | None = None):
        assert method.lower() == "get"
        return self.get(url, params)


class _FakeUWClient:
    def __init__(self, http_client: _FakeHTTPClient) -> None:
        self._http_client = http_client

    def get_httpx_client(self) -> _FakeHTTPClient:
        return self._http_client


@pytest.mark.asyncio
async def test_get_iv_rank_parses_raw_http_payload_when_sdk_shape_is_incompatible(
    monkeypatch,
):
    """IV-rank should parse raw API payloads instead of relying on SDK response parsing."""
    provider = UnusualWhalesProvider()
    http_response = _FakeHTTPResponse(
        {
            "data": [
                {"date": "2026-02-11", "volatility": "0.1478", "iv_rank_1y": "11.6152"},
                {"date": "2026-02-12", "volatility": "0.1756", "iv_rank_1y": "20.2449"},
            ]
        }
    )
    http_client = _FakeHTTPClient(http_response)
    provider._client = _FakeUWClient(http_client)

    async def _fake_call_sync(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(provider, "_call_sync", _fake_call_sync)

    result = await provider.get_iv_rank("SPY", date_str="2026-02-12")

    assert result is not None
    assert result.symbol == "SPY"
    assert str(result.iv_rank) == "20.2449"
    assert str(result.current_iv) == "0.1756"
    assert http_client.calls == [("/api/stock/SPY/iv-rank", {"date": "2026-02-12"})]


@pytest.mark.asyncio
async def test_get_iv_rank_uses_latest_payload_without_forcing_date(monkeypatch):
    """IV-rank lookup without date should avoid the date filter and request latest data."""
    provider = UnusualWhalesProvider()
    http_response = _FakeHTTPResponse({"data": [{"iv_rank_1y": "12.34", "volatility": "0.2"}]})
    http_client = _FakeHTTPClient(http_response)
    provider._client = _FakeUWClient(http_client)

    async def _fake_call_sync(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(provider, "_call_sync", _fake_call_sync)

    result = await provider.get_iv_rank("SPY")

    assert result is not None
    assert str(result.iv_rank) == "12.34"
    assert http_client.calls == [("/api/stock/SPY/iv-rank", None)]


@pytest.mark.asyncio
async def test_get_iv_rank_retries_without_date_after_422(monkeypatch):
    """Date-filter 422 responses should retry once without date and return latest payload."""
    provider = UnusualWhalesProvider()
    request = httpx.Request("GET", "https://api.unusualwhales.com/api/stock/SPY/iv-rank")
    first_response = _FakeHTTPResponse(
        {},
        status_error=httpx.HTTPStatusError(
            "422 Unprocessable Entity",
            request=request,
            response=httpx.Response(status_code=422, request=request),
        ),
    )
    second_response = _FakeHTTPResponse({"data": [{"iv_rank_1y": "45.67", "volatility": "0.31"}]})
    http_client = _FakeHTTPClient([first_response, second_response])
    provider._client = _FakeUWClient(http_client)

    async def _fake_call_sync(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(provider, "_call_sync", _fake_call_sync)

    result = await provider.get_iv_rank("SPY", date_str="2026-02-13")

    assert result is not None
    assert str(result.iv_rank) == "45.67"
    assert http_client.calls == [
        ("/api/stock/SPY/iv-rank", {"date": "2026-02-13"}),
        ("/api/stock/SPY/iv-rank", None),
    ]


@pytest.mark.asyncio
async def test_get_congress_trades_parses_raw_payload_with_unknown_txn_type(monkeypatch):
    provider = UnusualWhalesProvider()
    raw_txn_type = (
        "Star Catcher Industries, Inc. [OI] S \n"
        "FILING STATUS: New SUBHOLDING OF: Investment Fund  "
        "DESCRIPTION: Energy infrastructure in space, Jacksonville, FL"
    )
    http_response = _FakeHTTPResponse(
        {
            "data": [
                {
                    "ticker": "OI",
                    "name": "Reporter",
                    "txn_type": raw_txn_type,
                    "amounts": "$1,001 - $15,000",
                    "transaction_date": "2026-05-20",
                    "filed_at_date": "2026-05-21",
                    "member_type": "house",
                }
            ]
        }
    )
    http_client = _FakeHTTPClient(http_response)
    provider._client = _FakeUWClient(http_client)

    async def _fake_call_sync(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(provider, "_call_sync", _fake_call_sync)

    trades = await provider.get_congress_trades(limit=200)

    assert trades == [
        {
            "ticker": "OI",
            "name": "Reporter",
            "txn_type": raw_txn_type,
            "amounts": "$1,001 - $15,000",
            "transaction_date": "2026-05-20",
            "filed_at_date": "2026-05-21",
            "member_type": "house",
            "politician_id": None,
            "reporter": None,
            "notes": None,
            "issuer": None,
            "is_active": None,
        }
    ]
    assert http_client.calls == [("/api/congress/recent-trades", {"limit": 200})]


@pytest.mark.asyncio
async def test_get_darkpool_recent_falls_back_to_raw_http_when_sdk_parse_fails(monkeypatch):
    """Darkpool recent should fall back to raw HTTP when SDK parser fails on bad payloads."""
    provider = UnusualWhalesProvider()
    http_response = _FakeHTTPResponse(
        {
            "data": [
                {"ticker": "AAPL", "price": 201.5, "size": 50, "executed_at": "2026-02-13T02:11:00Z"},
                {"ticker": "MSFT", "price": 430.1, "size": 25, "executed_at": "2026-02-13T02:11:05Z"},
            ]
        }
    )
    http_client = _FakeHTTPClient(http_response)
    provider._client = _FakeUWClient(http_client)

    async def _fake_call_sync(func, *args, **kwargs):
        if getattr(func, "__name__", "") != "get":
            raise ValueError("Expecting value: line 1 column 1 (char 0)")
        return func(*args, **kwargs)

    monkeypatch.setattr(provider, "_call_sync", _fake_call_sync)
    monkeypatch.setattr(provider, "_normalize_darkpool_trade", lambda item: item)

    result = await provider.get_darkpool_recent(limit=2, offset=1)

    assert len(result) == 2
    assert http_client.calls == [("/api/darkpool/recent", {"limit": "2", "offset": "1"})]


class _FakeSDKResponse:
    """Mimics an openapi-generated SDK typed response.

    The SDK puts parsed rows on the ``.data`` attribute and dumps any leftover
    top-level keys into ``additional_properties``. Tests use this to reproduce
    the exact response shapes returned by the live UW API.
    """

    def __init__(self, data=None, additional_properties=None):
        self.data = data
        self.additional_properties = additional_properties or {}


@pytest.mark.asyncio
async def test_get_darkpool_recent_reads_sdk_data_attribute(monkeypatch):
    """SDK ``get_trades_by_date`` returns trades on ``DarkpoolTradeResponse.data``.

    The provider must read ``.data`` (via ``_extract_data``), not
    ``additional_properties['data']`` — otherwise the primary SDK path returns
    [] on every call and trades are only ever captured by the raw-HTTP fallback
    (which fires only on an SDK exception).
    """
    provider = UnusualWhalesProvider()
    provider._client = object()

    sdk_response = _FakeSDKResponse(
        data=[
            {"ticker": "AAPL", "price": 201.5, "size": 50, "executed_at": "2026-06-08T13:30:00Z"},
            {"ticker": "MSFT", "price": 430.1, "size": 25, "executed_at": "2026-06-08T13:30:05Z"},
        ],
        additional_properties={},
    )

    async def _fake_call_sync(_func, *args, **kwargs):
        return sdk_response

    monkeypatch.setattr(provider, "_call_sync", _fake_call_sync)
    monkeypatch.setattr(provider, "_normalize_darkpool_trade", lambda item: item)

    result = await provider.get_darkpool_recent(limit=200)

    assert len(result) == 2


@pytest.mark.asyncio
async def test_get_insiders_uses_transactions_endpoint(monkeypatch):
    """get_insiders must call ``insider.get_transactions`` (/api/insider/transactions).

    The previous call, ``market.get_insider_trades`` (/api/market/insider-buy-sells),
    is a market-wide aggregate whose rows have no ticker/owner_name/transaction_code,
    so 100% of them are dropped by the null-field filter (0 published every day).
    """
    from unusualwhales.api import insider as insider_api

    provider = UnusualWhalesProvider()
    provider._client = object()

    captured: dict = {}

    async def _fake_call_sync(func, *args, **kwargs):
        captured["func"] = func
        return _FakeSDKResponse(
            data=[
                {
                    "ticker": "AAPL",
                    "owner_name": "Jane Doe",
                    "transaction_code": "P",
                    "amount": 100,
                    "price": "150.0",
                    "transaction_date": "2026-06-08",
                },
            ],
        )

    monkeypatch.setattr(provider, "_call_sync", _fake_call_sync)

    txns = await provider.get_insiders(limit=5)

    assert captured["func"] is insider_api.get_transactions.sync
    assert len(txns) == 1
    assert txns[0]["ticker"] == "AAPL"
    assert txns[0]["owner_name"] == "Jane Doe"
    assert txns[0]["transaction_code"] == "P"


@pytest.mark.asyncio
async def test_get_short_volume_reads_si_key(monkeypatch):
    """UW ``/shorts/{ticker}/volume-and-ratio`` returns rows under
    ``additional_properties['si']`` — not ``.data``/``['data']``.

    ``_extract_data`` only checks ``.data``/``['data']``, so the provider must
    read the ``si`` key or it returns [] on every call (0 published every day).
    """
    provider = UnusualWhalesProvider()
    provider._client = object()

    # Real /shorts/{ticker}/volume-and-ratio rows use keys market_date /
    # short_volume / short_volume_ratio (not date / short_ratio).
    sdk_response = _FakeSDKResponse(
        data=None,
        additional_properties={
            "si": [
                {"market_date": "2026-06-08", "short_volume": "1234567", "short_volume_ratio": "0.42"},
                {"market_date": "2026-06-05", "short_volume": "1111111", "short_volume_ratio": "0.39"},
            ]
        },
    )

    async def _fake_call_sync(_func, *args, **kwargs):
        return sdk_response

    monkeypatch.setattr(provider, "_call_sync", _fake_call_sync)

    result = await provider.get_short_volume("AAPL")

    assert len(result) == 2
    assert result[0].symbol == "AAPL"
    assert result[0].date == "2026-06-08"
    assert result[0].short_interest == 1234567
    assert str(result[0].short_percent_float) == "0.42"
