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


class _RecordingLogger:
    """Captures (level, event) pairs so tests can assert log severity."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def _record(self, level: str, event: str, **_: Any) -> None:
        self.calls.append((level, event))

    def info(self, event: str, **kw: Any) -> None:
        self._record("info", event, **kw)

    def warning(self, event: str, **kw: Any) -> None:
        self._record("warning", event, **kw)

    def error(self, event: str, **kw: Any) -> None:
        self._record("error", event, **kw)

    def debug(self, event: str, **kw: Any) -> None:
        self._record("debug", event, **kw)


class _HTTPErrorResponse:
    def __init__(self, status: int, url: str) -> None:
        self._status = status
        self._url = url

    def raise_for_status(self) -> None:
        import httpx

        request = httpx.Request("GET", self._url)
        response = httpx.Response(status_code=self._status, request=request)
        raise httpx.HTTPStatusError(f"{self._status}", request=request, response=response)

    def json(self) -> dict[str, Any]:
        return {}


class _HTTPErrorClient:
    def __init__(self, status: int) -> None:
        self._status = status

    async def get(self, path: str, params: dict[str, Any]) -> _HTTPErrorResponse:
        return _HTTPErrorResponse(self._status, "https://data.alpaca.markets/v2/stocks/quotes/latest")


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "expected_level"), [(400, "warning"), (500, "error")])
async def test_get_quotes_logs_4xx_as_warning_5xx_as_error(
    monkeypatch: pytest.MonkeyPatch, status: int, expected_level: str
) -> None:
    """Client-caused 4xx (e.g. index symbol SPX → 400) must not flood the ERROR log."""
    import httpx

    provider = AlpacaProvider()
    provider._client = cast(Any, _HTTPErrorClient(status))
    rec = _RecordingLogger()
    monkeypatch.setattr("gateway.providers.alpaca.market.logger", rec)

    with pytest.raises(httpx.HTTPStatusError):
        await provider.get_quotes(["SPX"])

    levels = {lvl for lvl, event in rec.calls if event == "alpaca_quotes_error"}
    assert levels == {expected_level}


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "expected_level"), [(400, "warning"), (500, "error")])
async def test_get_bars_logs_4xx_as_warning_5xx_as_error(
    monkeypatch: pytest.MonkeyPatch, status: int, expected_level: str
) -> None:
    """Same severity-by-status convention as the API layer (common.py)."""
    import httpx

    provider = AlpacaProvider()
    provider._client = cast(Any, _FakeClient({}))

    async def _raise(*_args: Any, **_kwargs: Any) -> Any:
        request = httpx.Request("GET", "https://data.alpaca.markets/v2/stocks/bars")
        response = httpx.Response(status_code=status, request=request)
        raise httpx.HTTPStatusError(f"{status}", request=request, response=response)

    monkeypatch.setattr(provider, "_paginate", _raise)
    rec = _RecordingLogger()
    monkeypatch.setattr("gateway.providers.alpaca.market.logger", rec)

    with pytest.raises(httpx.HTTPStatusError):
        await provider.get_bars(
            ["SPX"],
            "1Day",
            datetime(2026, 1, 14, tzinfo=UTC),
            datetime(2026, 3, 5, tzinfo=UTC),
        )

    levels = {lvl for lvl, event in rec.calls if event == "alpaca_bars_error"}
    assert levels == {expected_level}


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


# ---------------------------------------------------------------------------
# close_position naked-call regression — must default to 100% when neither
# qty nor percentage is supplied. The prior code passed both as None and
# Alpaca's ClosePositionRequest raised ValidationError, which surfaced as a
# 502 to callers and left their positions open at the broker.
# ---------------------------------------------------------------------------


class _FakeOrder:
    def __init__(self, **fields: Any) -> None:
        for k, v in fields.items():
            setattr(self, k, v)


class _FakeTradingClient:
    """Records the arguments passed to close_position so the test can assert
    on the ClosePositionRequest fields that were sent to the SDK."""

    def __init__(self) -> None:
        self.close_position_calls: list[dict[str, Any]] = []

    def close_position(self, symbol: str, close_options: Any) -> _FakeOrder:
        self.close_position_calls.append(
            {
                "symbol": symbol,
                "qty": close_options.qty,
                "percentage": close_options.percentage,
            }
        )
        return _FakeOrder(id="closing-order-1", symbol=symbol, status="accepted")


def test_close_position_defaults_to_full_close_when_qty_and_percentage_omitted() -> None:
    """Bug fix: a naked DELETE /positions/<symbol> must close 100% rather
    than triggering ValidationError → 502 with the position still open."""
    provider = AlpacaProvider()
    fake_client = _FakeTradingClient()
    provider._trading_client = cast(Any, fake_client)

    data = provider.close_position("aapl")

    assert data["id"] == "closing-order-1"
    assert len(fake_client.close_position_calls) == 1
    call = fake_client.close_position_calls[0]
    assert call["symbol"] == "AAPL"
    # Naked call → percentage defaults to 100, qty stays None.
    assert call["qty"] is None
    assert call["percentage"] == "100.0"


def test_close_position_explicit_qty_is_forwarded() -> None:
    provider = AlpacaProvider()
    fake_client = _FakeTradingClient()
    provider._trading_client = cast(Any, fake_client)

    provider.close_position("AAPL", qty=5.0)

    call = fake_client.close_position_calls[0]
    assert call["qty"] == "5.0"
    assert call["percentage"] is None


def test_close_position_explicit_percentage_is_forwarded() -> None:
    provider = AlpacaProvider()
    fake_client = _FakeTradingClient()
    provider._trading_client = cast(Any, fake_client)

    provider.close_position("AAPL", percentage=50.0)

    call = fake_client.close_position_calls[0]
    assert call["qty"] is None
    assert call["percentage"] == "50.0"


def test_close_position_qty_zero_is_forwarded_not_silently_defaulted() -> None:
    """Edge case: qty=0 is falsy in Python — both the naked-default guard
    and the ClosePositionRequest builder must use ``is None`` rather than
    truthiness, otherwise qty=0 either silently becomes "close 100% of the
    position" (the original bug under a different trigger) or gets
    rewritten back to None and triggers the same ValidationError → 502
    the naked-default guard was added to prevent.

    Contract: qty=0 is forwarded verbatim ("0") to Alpaca, which is then
    free to reject it remotely. The gateway must not rewrite the caller's
    explicit intent."""
    provider = AlpacaProvider()
    fake_client = _FakeTradingClient()
    provider._trading_client = cast(Any, fake_client)

    provider.close_position("AAPL", qty=0)

    call = fake_client.close_position_calls[0]
    # qty=0 is forwarded as a literal "0" — NOT rewritten to None (which
    # would re-trigger the original ValidationError) and NOT
    # silently-defaulted to "percentage=100" by the naked-call guard.
    assert call["qty"] == "0"
    assert call["percentage"] is None


class _OrdersRecordingTradingClient:
    """Records the GetOrdersRequest passed to get_orders for assertion."""

    def __init__(self) -> None:
        self.requests: list[Any] = []

    def get_orders(self, request: Any) -> list[_FakeOrder]:
        self.requests.append(request)
        return [_FakeOrder(id="o-1", client_order_id="orion_abc", filled_at="2026-06-10T15:30:00Z")]


def test_get_orders_threads_after_until_to_sdk_request() -> None:
    """O2b: after/until date params reach the SDK GetOrdersRequest so Orion can
    page the submitted_at window for same-day fill coverage."""
    provider = AlpacaProvider()
    fake_client = _OrdersRecordingTradingClient()
    provider._trading_client = cast(Any, fake_client)

    after = datetime(2026, 6, 10, 0, 0, tzinfo=UTC)
    until = datetime(2026, 6, 11, 0, 0, tzinfo=UTC)
    result = provider.get_orders(status="all", limit=500, after=after, until=until)

    assert len(fake_client.requests) == 1
    req = fake_client.requests[0]
    assert req.after == after
    assert req.until == until
    # The returned order carries the fields Orion's reconciliation maps on.
    assert result[0]["client_order_id"] == "orion_abc"


def test_get_orders_after_until_default_to_none() -> None:
    """O2b: omitting after/until leaves them unset (back-compat for existing callers)."""
    provider = AlpacaProvider()
    fake_client = _OrdersRecordingTradingClient()
    provider._trading_client = cast(Any, fake_client)

    provider.get_orders(status="open")

    req = fake_client.requests[0]
    assert req.after is None
    assert req.until is None


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
