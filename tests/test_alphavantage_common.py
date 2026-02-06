from __future__ import annotations

from typing import Any

import pytest
from fastapi import HTTPException

from gateway.api.alphavantage import common


class _FakeCache:
    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    async def get(self, key: str) -> Any:
        return self._store.get(key)

    async def set(self, key: str, value: Any, ttl: int) -> None:
        assert ttl > 0
        self._store[key] = value


class _FakeRegistry:
    def __init__(self, provider: Any) -> None:
        self._provider = provider
        self.calls = 0

    def get(self, name: str) -> Any:
        self.calls += 1
        assert name == "alphavantage"
        return self._provider


@pytest.mark.asyncio
async def test_execute_av_cached_returns_cached_without_registry_lookup() -> None:
    cache = _FakeCache()
    cache._store["av:test"] = {"price": "1.23"}
    registry = _FakeRegistry(provider=object())

    async def _fetcher(_provider: Any) -> Any:
        raise RuntimeError("should not fetch")

    response = await common.execute_av_cached(
        cache=cache,  # type: ignore[arg-type]
        cache_key_value="av:test",
        registry=registry,  # type: ignore[arg-type]
        ttl=60,
        fetcher=_fetcher,
        cache_transform=lambda payload: payload,
    )

    assert response == {
        "success": True,
        "data": {"price": "1.23"},
        "meta": {"cached": True, "provider": "alphavantage"},
    }
    assert registry.calls == 0


@pytest.mark.asyncio
async def test_execute_av_cached_cache_miss_fetches_and_stores(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = _FakeCache()
    provider = object()
    registry = _FakeRegistry(provider=provider)
    calls = {"rate_limit": 0, "fetch": 0}

    async def _fake_rate_limit(provider_name: str) -> None:
        assert provider_name == "alphavantage"
        calls["rate_limit"] += 1

    async def _fetcher(p: Any) -> list[int]:
        assert p is provider
        calls["fetch"] += 1
        return [1, 2]

    monkeypatch.setattr(common, "require_provider_rate_limit", _fake_rate_limit)

    response = await common.execute_av_cached(
        cache=cache,  # type: ignore[arg-type]
        cache_key_value="av:miss",
        registry=registry,  # type: ignore[arg-type]
        ttl=120,
        fetcher=_fetcher,
        cache_transform=lambda values: {"values": values},
        miss_meta_builder=lambda _result, data: {"count": len(data["values"])},
    )

    assert calls == {"rate_limit": 1, "fetch": 1}
    assert cache._store["av:miss"] == {"values": [1, 2]}
    assert response == {
        "success": True,
        "data": {"values": [1, 2]},
        "meta": {"cached": False, "provider": "alphavantage", "count": 2},
    }


def test_get_alphavantage_provider_raises_when_missing() -> None:
    registry = _FakeRegistry(provider=None)
    with pytest.raises(HTTPException) as exc:
        common.get_alphavantage_provider(registry)  # type: ignore[arg-type]
    assert exc.value.status_code == 503
