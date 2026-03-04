"""Tests for Alpaca route error mapping and crypto input validation."""

from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from alpaca.common.exceptions import APIError

from gateway.api.deps import require_api_key
from gateway.core.auth import Client, ClientPermissions
from gateway.main import app


def _build_alpaca_api_error(code: int, message: str) -> APIError:
    payload = f'{{"code":{code},"message":"{message}"}}'
    return APIError(payload)


@contextmanager
def _trader_auth_override() -> Iterator[None]:
    original = app.dependency_overrides.get(require_api_key)

    def _fake_trader_client() -> Client:
        return Client(
            id="test-trader",
            role="trader",
            permissions=ClientPermissions(
                providers=["alpaca"],
                feeds=["bars", "quotes", "trades", "news"],
                max_symbols=1000,
                rate_limit=600,
            ),
            enabled=True,
        )

    app.dependency_overrides[require_api_key] = _fake_trader_client
    try:
        yield
    finally:
        if original is None:
            app.dependency_overrides.pop(require_api_key, None)
        else:
            app.dependency_overrides[require_api_key] = original


def test_create_order_maps_alpaca_403_to_403(client, auth_headers, test_registry) -> None:
    """Alpaca permission/account rejects should remain 403, not 502."""
    provider = SimpleNamespace()
    provider.create_order = MagicMock(
        side_effect=_build_alpaca_api_error(
            code=40310000,
            message="account not eligible to trade uncovered option contracts",
        )
    )
    test_registry.get.return_value = provider

    with _trader_auth_override():
        response = client.post(
            "/api/v1/alpaca/orders",
            params={
                "symbol": "SPY260304C00663000",
                "qty": 1,
                "side": "sell",
                "order_type": "market",
                "time_in_force": "day",
            },
            headers=auth_headers,
        )

    assert response.status_code == 403
    body = response.json()
    assert body["success"] is False
    assert "not eligible" in body["error"]["message"]


def test_cancel_order_maps_malformed_id_to_400(client, auth_headers, test_registry) -> None:
    """Malformed non-UUID order IDs should be a client error (400)."""
    provider = SimpleNamespace()
    provider.cancel_order = MagicMock(
        side_effect=ValueError("badly formed hexadecimal UUID string")
    )
    test_registry.get.return_value = provider

    with _trader_auth_override():
        response = client.delete("/api/v1/alpaca/orders/sim-06a55d69", headers=auth_headers)

    assert response.status_code == 400
    body = response.json()
    assert body["success"] is False
    assert "UUID" in body["error"]["message"]


def test_crypto_bars_rejects_stock_symbol_before_provider_call(
    client, auth_headers, test_registry
) -> None:
    """Stock symbols sent to crypto endpoint should fail fast with 400."""
    provider = SimpleNamespace()
    provider.get_crypto_bars = AsyncMock(return_value=[])
    test_registry.get.return_value = provider

    response = client.get(
        "/api/v1/alpaca/crypto/AAPL/bars",
        params={"timeframe": "1Day"},
        headers=auth_headers,
    )

    assert response.status_code == 400
    body = response.json()
    assert body["success"] is False
    assert body["error"]["code"] == "GW-E8001"
    provider.get_crypto_bars.assert_not_called()
