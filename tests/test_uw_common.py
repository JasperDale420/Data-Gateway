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


def test_cursor_page_limit_uses_limit_plus_one() -> None:
    cursor = _encode_cursor(7)
    offset, page_limit = common.cursor_page_limit(limit=25, cursor=cursor)
    assert offset == 7
    assert page_limit == 26


def test_paginate_offset_response_uses_prefetched_window() -> None:
    data = [_ModelLike(1), _ModelLike(2), _ModelLike(3)]
    response = common.paginate_offset_response(data, limit=2, offset=4)
    assert response == {
        "success": True,
        "data": [{"value": 1}, {"value": 2}],
        "pagination": {
            "next_cursor": _encode_cursor(6),
            "has_more": True,
            "total_count": 7,
        },
    }


def test_make_response_includes_extra_meta_and_serializes_data() -> None:
    response = common.make_response(
        data=[_ModelLike(5)],
        symbol="AAPL",
        count=1,
        extra_meta={"timeframe": "1d", "nullable": None},
    )
    assert response == {
        "success": True,
        "data": [{"value": 5}],
        "meta": {
            "provider": "unusual_whales",
            "symbol": "AAPL",
            "count": 1,
            "timeframe": "1d",
        },
    }


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

    from gateway.api import deps

    monkeypatch.setattr(deps, "require_provider_rate_limit", _fake_rate_limit)

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
