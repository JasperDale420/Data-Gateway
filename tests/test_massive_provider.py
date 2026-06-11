from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from gateway.providers.massive import (
    MassiveError,
    MassivePaginationLimitError,
    MassiveProvider,
)

BASE = "https://api.massive.com"


def _bar(t_ms: int, close: float) -> dict:
    return {"t": t_ms, "o": 10.0, "h": 11.0, "l": 9.5, "c": close, "v": 1000, "vw": 10.5, "n": 42}


def _make_provider(handler, *, base_url: str = BASE, max_pages: int = 50) -> MassiveProvider:
    """Build a provider whose client is backed by an httpx.MockTransport."""
    provider = MassiveProvider()
    provider._api_key = "testkey"  # pragma: allowlist secret
    provider._base_url = base_url
    provider._max_pages = max_pages
    provider._client = httpx.AsyncClient(
        base_url=base_url,
        headers={"Authorization": "Bearer testkey"},
        transport=httpx.MockTransport(handler),
    )
    return provider


def _day(d: int) -> datetime:
    return datetime(2024, 1, d, tzinfo=UTC)


# ─────────────────────────── initialize / config ───────────────────────────


@pytest.mark.asyncio
async def test_initialize_without_key_does_not_create_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    provider = MassiveProvider()
    await provider.initialize({})
    assert provider.api_key_configured is False
    assert provider._client is None


@pytest.mark.asyncio
async def test_initialize_clamps_concurrency_and_parses_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    provider = MassiveProvider()
    await provider.initialize({"bars_max_concurrency": "bogus", "max_pages": 0, "min_request_interval_seconds": "2.5"})
    assert provider._bars_max_concurrency == 2  # default on bad input
    assert provider._max_pages == 1  # clamped up to minimum of 1
    assert provider._pacer._min == 2.5


# ─────────────────────────── input validation ───────────────────────────


@pytest.mark.asyncio
async def test_get_bars_unsupported_timeframe_raises() -> None:
    provider = _make_provider(lambda req: httpx.Response(200, json={"status": "OK", "results": []}))
    with pytest.raises(ValueError, match="Unsupported timeframe"):
        await provider.get_bars(["AAPL"], "3Min", _day(2), _day(3))


@pytest.mark.asyncio
async def test_get_bars_naive_datetime_raises() -> None:
    provider = _make_provider(lambda req: httpx.Response(200, json={"status": "OK", "results": []}))
    with pytest.raises(ValueError, match="timezone-aware"):
        await provider.get_bars(["AAPL"], "1Day", datetime(2024, 1, 2), datetime(2024, 1, 3))


@pytest.mark.asyncio
async def test_get_bars_invalid_ticker_raises_before_request() -> None:
    called = False

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(200, json={"status": "OK", "results": []})

    provider = _make_provider(handler)
    with pytest.raises(ValueError, match="Unsupported ticker"):
        await provider.get_bars(["AAPL/../x"], "1Day", _day(2), _day(3))
    assert called is False  # rejected before any HTTP request


# ─────────────────────────── happy path / contract ───────────────────────────


@pytest.mark.asyncio
async def test_get_bars_sends_auth_and_params_and_maps_fields() -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(200, json={"status": "OK", "results": [_bar(1_704_153_600_000, 10.0)]})

    provider = _make_provider(handler)
    bars = await provider.get_bars(["aapl"], "1Day", _day(2), _day(3))

    assert len(bars) == 1
    b = bars[0]
    assert b.symbol == "AAPL" and b.close == Decimal("10.0") and b.vwap == Decimal("10.5")
    assert b.trade_count == 42 and b.provider == "massive" and b.timeframe == "1Day"

    req = seen[0]
    assert req.headers["Authorization"] == "Bearer testkey"
    assert "/v2/aggs/ticker/AAPL/range/1/day/" in str(req.url)
    assert req.url.params["adjusted"] == "true"
    assert req.url.params["sort"] == "asc"
    assert req.url.params["limit"] == "50000"


@pytest.mark.asyncio
async def test_get_bars_follows_same_host_pagination() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if "next" in req.url.path:
            return httpx.Response(200, json={"status": "OK", "results": [_bar(1_704_240_000_000, 12.0)]})
        return httpx.Response(
            200,
            json={"status": "OK", "results": [_bar(1_704_153_600_000, 10.0)], "next_url": f"{BASE}/next/page2"},
        )

    provider = _make_provider(handler)
    bars = await provider.get_bars(["AAPL"], "1Day", _day(2), _day(3))
    assert [float(b.close) for b in bars] == [10.0, 12.0]


@pytest.mark.asyncio
async def test_get_bars_adjusted_false_sets_param() -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(200, json={"status": "OK", "results": []})

    provider = _make_provider(handler)
    await provider.get_bars(["AAPL"], "1Day", _day(2), _day(3), adjusted=False)
    assert seen[0].url.params["adjusted"] == "false"


# ─────────────────────────── fail-loud behaviors ───────────────────────────


@pytest.mark.asyncio
async def test_get_bars_propagates_auth_error_not_empty() -> None:
    """A 401 must raise, never be silently swallowed into an empty result."""
    provider = _make_provider(lambda req: httpx.Response(401, json={"status": "ERROR", "error": "Unknown API Key"}))
    with pytest.raises(httpx.HTTPStatusError):
        await provider.get_bars(["AAPL"], "1Day", _day(2), _day(3))


@pytest.mark.asyncio
async def test_get_bars_error_status_raises() -> None:
    provider = _make_provider(lambda req: httpx.Response(200, json={"status": "ERROR", "error": "bad request"}))
    with pytest.raises(MassiveError, match="status='ERROR'"):
        await provider.get_bars(["AAPL"], "1Day", _day(2), _day(3))


@pytest.mark.asyncio
async def test_get_bars_statusless_error_body_raises_not_empty() -> None:
    """A 200 with an error and no success status must raise, not return empty bars."""
    provider = _make_provider(lambda req: httpx.Response(200, json={"error": "Unknown API Key"}))
    with pytest.raises(MassiveError):
        await provider.get_bars(["AAPL"], "1Day", _day(2), _day(3))


@pytest.mark.asyncio
async def test_get_bars_non_list_results_raises() -> None:
    provider = _make_provider(lambda req: httpx.Response(200, json={"status": "OK", "results": "oops"}))
    with pytest.raises(MassiveError, match="non-list results"):
        await provider.get_bars(["AAPL"], "1Day", _day(2), _day(3))


@pytest.mark.asyncio
async def test_get_bars_malformed_row_raises() -> None:
    bad = {"t": 1_704_153_600_000, "o": 1, "h": 2, "l": 0.5, "v": 10}  # missing "c"
    provider = _make_provider(lambda req: httpx.Response(200, json={"status": "OK", "results": [bad]}))
    with pytest.raises(MassiveError, match="malformed aggregate row"):
        await provider.get_bars(["AAPL"], "1Day", _day(2), _day(3))


@pytest.mark.asyncio
async def test_get_bars_pagination_cap_raises() -> None:
    # Every page points to a new next_url, so pagination never terminates → must raise at the cap.
    counter = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        return httpx.Response(200, json={"status": "OK", "results": [], "next_url": f"{BASE}/next/{counter['n']}"})

    provider = _make_provider(handler, max_pages=3)
    with pytest.raises(MassivePaginationLimitError, match="max_pages=3"):
        await provider.get_bars(["AAPL"], "1Day", _day(2), _day(3))


@pytest.mark.asyncio
async def test_get_bars_hostile_next_url_raises_and_does_not_request_it() -> None:
    hosts: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        hosts.append(req.url.host)
        return httpx.Response(200, json={"status": "OK", "results": [], "next_url": "https://evil.example/steal"})

    provider = _make_provider(handler)
    with pytest.raises(MassiveError, match="host mismatch"):
        await provider.get_bars(["AAPL"], "1Day", _day(2), _day(3))
    assert "evil.example" not in hosts  # bearer token never sent off-host


@pytest.mark.asyncio
async def test_get_bars_next_url_loop_raises() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "OK", "results": [], "next_url": f"{BASE}/loop"})

    provider = _make_provider(handler, max_pages=100)
    with pytest.raises(MassivePaginationLimitError, match="loop"):
        await provider.get_bars(["AAPL"], "1Day", _day(2), _day(3))


# ─────────────────────────── health check ───────────────────────────


@pytest.mark.asyncio
async def test_health_check_ok() -> None:
    provider = _make_provider(lambda req: httpx.Response(200, json={"status": "OK", "results": [{"c": 1}]}))
    status = await provider.health_check()
    assert status.healthy is True


@pytest.mark.asyncio
async def test_health_check_error_status_unhealthy() -> None:
    provider = _make_provider(lambda req: httpx.Response(200, json={"status": "ERROR", "error": "nope"}))
    status = await provider.health_check()
    assert status.healthy is False and "status=ERROR" in (status.error or "")


@pytest.mark.asyncio
async def test_health_check_http_error_unhealthy() -> None:
    provider = _make_provider(lambda req: httpx.Response(403, text="forbidden"))
    status = await provider.health_check()
    assert status.healthy is False and "403" in (status.error or "")
