from __future__ import annotations

from typing import Any, cast

import pytest
from fastapi import HTTPException

from gateway.api.alpaca import common
from gateway.core.registry import ProviderRegistry


class _FakeRegistry:
    def __init__(self, providers: dict[str, Any]) -> None:
        self._providers = providers

    def get(self, name: str) -> Any:
        return self._providers.get(name)


def _noop_provider_call(_provider: Any):
    async def _run() -> str:
        return "ok"

    return _run()


def test_parse_comma_values_trims_and_uppercases() -> None:
    values = common.parse_comma_values(" aapl, msft ", uppercase=True)
    assert values == ["AAPL", "MSFT"]


def test_parse_comma_values_preserves_empty_entries_by_default() -> None:
    values = common.parse_comma_values("AAPL,,MSFT", uppercase=True)
    assert values == ["AAPL", "", "MSFT"]


def test_parse_comma_values_can_drop_empty_entries() -> None:
    values = common.parse_comma_values("AAPL,,MSFT", uppercase=True, drop_empty=True)
    assert values == ["AAPL", "MSFT"]


@pytest.mark.asyncio
async def test_execute_alpaca_provider_call_runs_rate_limit_and_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _FakeRegistry({"alpaca": object()})
    calls: list[tuple[str, bool]] = []

    async def _fake_rate_limit(provider_name: str, block: bool = False) -> None:
        calls.append((provider_name, block))

    monkeypatch.setattr(common, "require_provider_rate_limit", _fake_rate_limit)

    result = await common.execute_alpaca_provider_call(
        registry=cast(ProviderRegistry, registry),
        block=True,
        provider_call=_noop_provider_call,
    )

    assert result == "ok"
    assert calls == [("alpaca", True)]


@pytest.mark.asyncio
async def test_execute_alpaca_provider_call_raises_503_when_provider_missing() -> None:
    registry = _FakeRegistry({})
    with pytest.raises(HTTPException) as exc:
        await common.execute_alpaca_provider_call(
            registry=cast(ProviderRegistry, registry),
            provider_call=_noop_provider_call,
        )
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_execute_alpaca_provider_call_wraps_provider_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _FakeRegistry({"alpaca": object()})

    async def _fake_rate_limit(_provider_name: str, block: bool = False) -> None:
        return None

    async def _failing_call(_provider: Any) -> str:
        raise RuntimeError("boom")

    monkeypatch.setattr(common, "require_provider_rate_limit", _fake_rate_limit)

    with pytest.raises(HTTPException) as exc:
        await common.execute_alpaca_provider_call(
            registry=cast(ProviderRegistry, registry),
            provider_call=_failing_call,
        )

    assert exc.value.status_code == 502
    assert exc.value.detail == "Provider error: boom"


@pytest.mark.asyncio
async def test_execute_alpaca_provider_call_passthrough_http_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _FakeRegistry({"alpaca": object()})

    async def _fake_rate_limit(_provider_name: str, block: bool = False) -> None:
        return None

    async def _http_error_call(_provider: Any) -> str:
        raise HTTPException(status_code=404, detail="missing")

    monkeypatch.setattr(common, "require_provider_rate_limit", _fake_rate_limit)

    with pytest.raises(HTTPException) as exc:
        await common.execute_alpaca_provider_call(
            registry=cast(ProviderRegistry, registry),
            provider_call=_http_error_call,
        )

    assert exc.value.status_code == 404
    assert exc.value.detail == "missing"
