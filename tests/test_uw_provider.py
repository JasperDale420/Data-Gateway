"""Tests for Unusual Whales provider concurrency guardrails."""

import asyncio
import time

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

    monkeypatch.setattr("gateway.providers.uw.record_provider_sync_call_wait", _record_wait)
    monkeypatch.setattr("gateway.providers.uw.record_provider_sync_call_exec", _record_exec)
    monkeypatch.setattr("gateway.providers.uw.inc_provider_sync_call_inflight", _inc_inflight)
    monkeypatch.setattr("gateway.providers.uw.dec_provider_sync_call_inflight", _dec_inflight)

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
            [
                {"ticker": "AAPL", "strike": 100, "expiry": "2026-01-01", "total_premium": idx}
                for idx in range(limit)
            ]
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
