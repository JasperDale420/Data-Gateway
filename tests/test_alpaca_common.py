from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Generator
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


class _FakeAsyncCache:
    def __init__(self, initial: dict[str, Any] | None = None) -> None:
        self._store: dict[str, Any] = initial or {}
        self.set_calls: list[tuple[str, Any, int]] = []

    async def get(self, key: str) -> Any | None:
        return self._store.get(key)

    async def set(self, key: str, value: Any, ttl: int) -> None:
        self._store[key] = value
        self.set_calls.append((key, value, ttl))


@pytest.fixture(autouse=True)
def _clear_cache_inflight() -> Generator[None, None, None]:
    common._ALPACA_CACHE_INFLIGHT.clear()
    yield
    common._ALPACA_CACHE_INFLIGHT.clear()


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
    assert exc.value.detail == "Upstream provider error"


@pytest.mark.asyncio
async def test_execute_alpaca_provider_call_logs_endpoint_on_unexpected_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = _FakeRegistry({"alpaca": object()})

    async def _fake_rate_limit(_provider_name: str, block: bool = False) -> None:
        return None

    async def _failing_call(_provider: Any) -> str:
        raise RuntimeError("boom")

    monkeypatch.setattr(common, "require_provider_rate_limit", _fake_rate_limit)

    with caplog.at_level(logging.ERROR, logger="data-gateway"), pytest.raises(HTTPException):
        await common.execute_alpaca_provider_call(
            registry=cast(ProviderRegistry, registry),
            provider_call=_failing_call,
        )

    matches = [json.loads(rec.getMessage()) for rec in caplog.records if rec.getMessage().startswith("{")]
    failures = [m for m in matches if m.get("message") == "provider_request_failed"]
    assert failures, "expected a provider_request_failed log record"
    endpoint = failures[-1].get("endpoint")
    assert isinstance(endpoint, str) and endpoint.endswith("_failing_call"), endpoint


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


@pytest.mark.asyncio
async def test_execute_alpaca_cached_call_returns_cached_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = _FakeAsyncCache(initial={"alpaca:test:key": {"cached": True}})
    cache_events: list[tuple[str, str, str]] = []

    async def _unexpected_execute(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        raise AssertionError("execute_alpaca_provider_call should not run on cache hit")

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(common, "execute_alpaca_provider_call", _unexpected_execute)
    monkeypatch.setattr(common, "record_route_cache", _record_route_cache)

    result = await common.execute_alpaca_cached_call(
        registry=cast(ProviderRegistry, _FakeRegistry({"alpaca": object()})),
        cache=cast(Any, cache),
        cache_key="alpaca:test:key",
        ttl=30,
        provider_call=_noop_provider_call,
        route_label="alpaca_meta_conditions",
    )

    assert result == {"cached": True}
    assert cache.set_calls == []
    assert cache_events == [("alpaca_meta_conditions", "hit", "alpaca")]


@pytest.mark.asyncio
async def test_execute_alpaca_cached_call_caches_miss_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = _FakeAsyncCache()
    calls = 0
    cache_events: list[tuple[str, str, str]] = []
    registry = _FakeRegistry({"alpaca": object()})

    async def _fake_execute(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        nonlocal calls
        calls += 1
        provider = registry.get("alpaca")
        return await provider_call(provider)

    async def _provider_call(_provider: Any) -> dict[str, int]:
        return {"value": 1}

    def _record_route_cache(route: str, status: str, cache_mode: str = "default") -> None:
        cache_events.append((route, status, cache_mode))

    monkeypatch.setattr(common, "execute_alpaca_provider_call", _fake_execute)
    monkeypatch.setattr(common, "record_route_cache", _record_route_cache)

    first = await common.execute_alpaca_cached_call(
        registry=cast(ProviderRegistry, registry),
        cache=cast(Any, cache),
        cache_key="alpaca:test:key",
        ttl=30,
        provider_call=_provider_call,
        route_label="alpaca_meta_conditions",
    )
    second = await common.execute_alpaca_cached_call(
        registry=cast(ProviderRegistry, registry),
        cache=cast(Any, cache),
        cache_key="alpaca:test:key",
        ttl=30,
        provider_call=_provider_call,
        route_label="alpaca_meta_conditions",
    )

    assert first == {"value": 1}
    assert second == {"value": 1}
    assert calls == 1
    assert cache.set_calls == [("alpaca:test:key", {"value": 1}, 30)]
    assert cache_events == [
        ("alpaca_meta_conditions", "miss", "alpaca"),
        ("alpaca_meta_conditions", "hit", "alpaca"),
    ]


@pytest.mark.asyncio
async def test_execute_alpaca_cached_call_dedupes_concurrent_miss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = _FakeAsyncCache()
    calls = 0
    registry = _FakeRegistry({"alpaca": object()})

    async def _fake_execute(*, registry: ProviderRegistry, provider_call: Any, block: bool = False):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        provider = registry.get("alpaca")
        return await provider_call(provider)

    async def _provider_call(_provider: Any) -> dict[str, int]:
        return {"value": 42}

    monkeypatch.setattr(common, "execute_alpaca_provider_call", _fake_execute)

    first, second = await asyncio.gather(
        common.execute_alpaca_cached_call(
            registry=cast(ProviderRegistry, registry),
            cache=cast(Any, cache),
            cache_key="alpaca:test:key",
            ttl=30,
            provider_call=_provider_call,
            route_label="alpaca_meta_conditions",
        ),
        common.execute_alpaca_cached_call(
            registry=cast(ProviderRegistry, registry),
            cache=cast(Any, cache),
            cache_key="alpaca:test:key",
            ttl=30,
            provider_call=_provider_call,
            route_label="alpaca_meta_conditions",
        ),
    )

    assert first == {"value": 42}
    assert second == {"value": 42}
    assert calls == 1
    assert cache.set_calls == [("alpaca:test:key", {"value": 42}, 30)]
