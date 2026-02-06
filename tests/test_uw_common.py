from __future__ import annotations

import base64
from typing import Any

import pytest

from gateway.api.uw import common


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

    def get(self, name: str) -> Any:
        assert name == "unusual_whales"
        return self._provider


class _ModelLike:
    def __init__(self, value: int) -> None:
        self.value = value

    def model_dump(self, mode: str = "json") -> dict[str, int]:
        assert mode == "json"
        return {"value": self.value}


def _encode_cursor(offset: int) -> str:
    return base64.b64encode(str(offset).encode()).decode()


def test_decode_cursor_clamps_to_max_offset() -> None:
    cursor = _encode_cursor(999_999)
    decoded = common.decode_cursor(cursor, max_offset=100)
    assert decoded == 100


def test_paginate_response_serializes_model_like_items() -> None:
    data = [_ModelLike(1), _ModelLike(2), {"value": 3}]
    response = common.paginate_response(data, limit=2, cursor=None)
    assert response["success"] is True
    assert response["pagination"]["total_count"] == 3
    assert response["data"] == [{"value": 1}, {"value": 2}]


@pytest.mark.asyncio
async def test_execute_uw_cached_uses_cache_and_rate_limit_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = _FakeCache()
    provider = object()
    registry = _FakeRegistry(provider=provider)
    calls: dict[str, int] = {"rate": 0, "fetch": 0}

    async def _fake_rate_limit(provider_name: str) -> None:
        assert provider_name == "unusual_whales"
        calls["rate"] += 1

    async def _fetcher(p: Any) -> list[int]:
        assert p is provider
        calls["fetch"] += 1
        return [1, 2, 3]

    def _build_response(payload: list[int]) -> dict[str, Any]:
        return {"success": True, "data": payload}

    monkeypatch.setattr(common, "require_provider_rate_limit", _fake_rate_limit)

    first = await common.execute_uw_cached(
        cache=cache,  # type: ignore[arg-type]
        cache_key="uw:test:key",
        registry=registry,  # type: ignore[arg-type]
        ttl=30,
        fetcher=_fetcher,
        build_response=_build_response,
    )
    second = await common.execute_uw_cached(
        cache=cache,  # type: ignore[arg-type]
        cache_key="uw:test:key",
        registry=registry,  # type: ignore[arg-type]
        ttl=30,
        fetcher=_fetcher,
        build_response=_build_response,
    )

    assert first == second
    assert calls == {"rate": 1, "fetch": 1}
