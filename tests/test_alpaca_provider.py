from __future__ import annotations

from datetime import UTC, datetime
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
                "volume": 17,
                "open_interest": 42,
                "underlyingPrice": 198.45,
                "latestQuote": {"bp": 1.2, "ap": 1.3},
                "latestTrade": {"p": 1.25},
                "greeks": {"delta": 0.51, "gamma": 0.02},
                "impliedVolatility": 0.31,
            }
        }
    }


def _quotes_payload() -> dict[str, Any]:
    return {
        "quotes": {
            "AAPL": {
                "t": datetime(2026, 2, 9, tzinfo=UTC).isoformat(),
                "bp": 100.0,
                "ap": 100.1,
                "bs": 10,
                "as": 12,
            }
        }
    }


def _option_quotes_payload_with_string_conditions() -> dict[str, Any]:
    return {
        "quotes": {
            "SPY260618C00700000": {
                "t": datetime(2026, 2, 12, 21, 34, tzinfo=UTC).isoformat(),
                "bp": 12.1,
                "ap": 12.3,
                "bs": 5,
                "as": 7,
                "c": " ",
            }
        }
    }


def _option_trades_payload() -> dict[str, Any]:
    return {
        "trades": {
            "SPY260618C00700000": [
                {
                    "t": datetime(2026, 2, 12, 21, 35, tzinfo=UTC).isoformat(),
                    "p": 12.2,
                    "s": 3,
                    "x": "OPRA",
                    "c": " ",
                }
            ]
        }
    }


@pytest.mark.asyncio
async def test_get_option_chain_uses_default_limit_when_not_provided() -> None:
    provider = AlpacaProvider()
    fake_client = _FakeClient(_option_chain_payload())
    provider._client = cast(Any, fake_client)

    contracts = await provider.get_option_chain("aapl")

    assert len(contracts) == 1
    assert fake_client.last_path == "/v1beta1/options/snapshots/AAPL"
    assert fake_client.last_params is not None
    assert fake_client.last_params["feed"] == "opra"
    assert fake_client.last_params["limit"] == 1000
    assert "underlying_symbols" not in fake_client.last_params


@pytest.mark.asyncio
async def test_get_option_chain_parses_occ_components_without_snapshot_contract_fields() -> None:
    provider = AlpacaProvider()
    fake_client = _FakeClient(_option_chain_payload())
    provider._client = cast(Any, fake_client)

    contracts = await provider.get_option_chain("aapl")

    assert len(contracts) == 1
    contract = contracts[0]
    assert contract.contract_symbol == "AAPL250117C00200000"
    assert contract.underlying == "AAPL"
    assert contract.expiration == "2025-01-17"
    assert float(contract.strike) == pytest.approx(200.0)
    assert contract.option_type == "call"


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


@pytest.mark.asyncio
async def test_get_option_snapshot_contracts_normalizes_full_snapshot_without_limit() -> None:
    provider = AlpacaProvider()
    fake_client = _FakeClient(_option_chain_payload())
    provider._client = cast(Any, fake_client)

    contracts = await provider.get_option_snapshot_contracts("aapl")

    assert len(contracts) == 1
    assert fake_client.last_path == "/v1beta1/options/snapshots/AAPL"
    assert fake_client.last_params == {"feed": "opra", "limit": 1000}
    assert contracts[0].contract_symbol == "AAPL250117C00200000"
    assert contracts[0].underlying == "AAPL"


@pytest.mark.asyncio
async def test_get_option_snapshot_contracts_maps_volume_open_interest_and_underlying_price() -> None:
    provider = AlpacaProvider()
    fake_client = _FakeClient(_option_chain_payload())
    provider._client = cast(Any, fake_client)

    contracts = await provider.get_option_snapshot_contracts("aapl")

    assert len(contracts) == 1
    contract = contracts[0]
    assert contract.volume == 17
    assert contract.open_interest == 42
    assert float(contract.underlying_price) == pytest.approx(198.45)


@pytest.mark.asyncio
async def test_get_trades_applies_limit_bounds() -> None:
    provider = AlpacaProvider()
    fake_client = _FakeClient({"trades": {}})
    provider._client = cast(Any, fake_client)

    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, tzinfo=UTC)

    await provider.get_trades(["AAPL"], start=start, end=end, limit=250)
    assert fake_client.last_params is not None
    assert fake_client.last_params["limit"] == 250

    await provider.get_trades(["AAPL"], start=start, end=end, limit=0)
    assert fake_client.last_params is not None
    assert fake_client.last_params["limit"] == 1

    await provider.get_trades(["AAPL"], start=start, end=end, limit=20_000)
    assert fake_client.last_params is not None
    assert fake_client.last_params["limit"] == 10_000


def test_parse_timestamp_accepts_z_suffix() -> None:
    provider = AlpacaProvider()
    parsed = provider._parse_timestamp("2026-01-01T12:00:00Z")
    assert parsed == datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def test_parse_timestamp_returns_datetime_input_unchanged() -> None:
    provider = AlpacaProvider()
    source = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    parsed = provider._parse_timestamp(source)
    assert parsed is source


@pytest.mark.asyncio
async def test_get_quotes_records_requested_batch_size(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = AlpacaProvider()
    provider._client = cast(Any, _FakeClient(_quotes_payload()))
    recorded: list[tuple[str, int]] = []

    monkeypatch.setattr(
        "gateway.providers.alpaca.market.record_provider_quote_batch_size",
        lambda provider_name, batch_size: recorded.append((provider_name, batch_size)),
    )

    quotes = await provider.get_quotes(["AAPL", "MSFT"])

    assert len(quotes) == 1
    assert recorded == [("alpaca", 2)]


@pytest.mark.asyncio
async def test_get_option_quotes_coerces_string_conditions_to_list() -> None:
    provider = AlpacaProvider()
    fake_client = _FakeClient(_option_quotes_payload_with_string_conditions())
    provider._client = cast(Any, fake_client)

    quotes = await provider.get_option_quotes(["SPY260618C00700000"])

    assert len(quotes) == 1
    assert fake_client.last_path == "/v1beta1/options/quotes/latest"
    assert fake_client.last_params == {"symbols": "SPY260618C00700000", "feed": "opra"}
    assert quotes[0].symbol == "SPY260618C00700000"
    assert quotes[0].conditions == []


@pytest.mark.asyncio
async def test_get_option_trades_uses_opra_feed_by_default() -> None:
    provider = AlpacaProvider()
    fake_client = _FakeClient(_option_trades_payload())
    provider._client = cast(Any, fake_client)

    trades = await provider.get_option_trades(["SPY260618C00700000"])

    assert len(trades) == 1
    assert fake_client.last_path == "/v1beta1/options/trades"
    assert fake_client.last_params == {"symbols": "SPY260618C00700000", "feed": "opra", "limit": 1000}


@pytest.mark.asyncio
async def test_initialize_allows_explicit_option_feed_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APCA_API_KEY_ID", "dummy-id")  # pragma: allowlist secret
    monkeypatch.setenv("APCA_API_SECRET_KEY", "dummy-passphrase")  # pragma: allowlist secret

    provider = AlpacaProvider()

    await provider.initialize(
        {
            "api_key_env": "APCA_API_KEY_ID",  # pragma: allowlist secret
            "secret_key_env": "APCA_API_SECRET_KEY",  # pragma: allowlist secret
            "options_feed": "indicative",
        }
    )

    assert provider._options_feed == "indicative"

    await provider.shutdown()
