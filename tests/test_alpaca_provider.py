from __future__ import annotations

from typing import Any, cast

import pytest

from gateway.providers.alpaca import AlpacaProvider


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeClient:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload
        self.last_params: dict[str, Any] | None = None
        self.last_path: str | None = None

    async def get(self, path: str, params: dict[str, Any]) -> _FakeResponse:
        self.last_path = path
        self.last_params = params
        return _FakeResponse(self._payload)


def _option_chain_payload() -> dict[str, Any]:
    return {
        "snapshots": {
            "AAPL250117C00200000": {
                "expiration_date": "2025-01-17",
                "strike_price": 200.0,
                "type": "call",
                "open_interest": 42,
                "latestQuote": {"bp": 1.2, "ap": 1.3},
                "latestTrade": {"p": 1.25},
            }
        }
    }


@pytest.mark.asyncio
async def test_get_option_chain_uses_default_limit_when_not_provided() -> None:
    provider = AlpacaProvider()
    fake_client = _FakeClient(_option_chain_payload())
    provider._client = cast(Any, fake_client)

    contracts = await provider.get_option_chain("aapl")

    assert len(contracts) == 1
    assert fake_client.last_path == "/v1beta1/options/snapshots"
    assert fake_client.last_params is not None
    assert fake_client.last_params["limit"] == 1000


@pytest.mark.asyncio
async def test_get_option_chain_applies_custom_limit_bounds() -> None:
    provider = AlpacaProvider()
    fake_client = _FakeClient(_option_chain_payload())
    provider._client = cast(Any, fake_client)

    await provider.get_option_chain("aapl", limit=100)
    assert fake_client.last_params is not None
    assert fake_client.last_params["limit"] == 100

    await provider.get_option_chain("aapl", limit=5000)
    assert fake_client.last_params is not None
    assert fake_client.last_params["limit"] == 1000

    await provider.get_option_chain("aapl", limit=0)
    assert fake_client.last_params is not None
    assert fake_client.last_params["limit"] == 1
