"""OptionCaptureService should optionally emit per-contract trade envelopes.

The service already fetches the full chain snapshot every cycle. Adding a
flag to ALSO emit ``feed=option_trades`` envelopes for contracts with a
recent trade lets us populate the ``alpaca/option_trades`` Bronze feed
with zero additional API calls.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest

from gateway.core.option_capture import OptionCaptureService


def _fake_contract(
    *,
    occ_symbol: str,
    underlying: str,
    strike: float,
    expiration: str,
    option_type: str,
    last: float = 1.50,
    last_size: int = 5,
    timestamp: str = "2026-04-29T15:30:00Z",
) -> dict[str, Any]:
    return {
        "symbol": occ_symbol,
        "underlying": underlying,
        "strike": strike,
        "expiration": expiration,
        "option_type": option_type,
        "last": last,
        "last_trade_size": last_size,
        "timestamp": timestamp,
        "volume": 100,
        "open_interest": 500,
        "bid": last - 0.05,
        "ask": last + 0.05,
        "iv": 0.3,
        "delta": 0.5,
    }


@pytest.fixture
def mock_alpaca():
    provider = AsyncMock()
    provider.get_option_snapshot_contracts = AsyncMock(
        return_value=[
            _fake_contract(
                occ_symbol="SPY260516C00500000",
                underlying="SPY",
                strike=500.0,
                expiration="2026-05-16",
                option_type="call",
            ),
            _fake_contract(
                occ_symbol="SPY260516P00500000",
                underlying="SPY",
                strike=500.0,
                expiration="2026-05-16",
                option_type="put",
                last=2.10,
            ),
        ]
    )
    return provider


class _FixedTimeCalendar:
    def is_market_open(self, _ts=None):
        return True


@pytest.fixture
def sink():
    s = AsyncMock()
    s.publish_all = AsyncMock()
    s.publish_all_batch = AsyncMock(side_effect=lambda msgs: len(msgs))
    return s


@pytest.fixture
def service_with_trades(mock_alpaca, sink):
    return OptionCaptureService(
        alpaca_provider=mock_alpaca,
        multiplexer=None,
        sink_registry=sink,
        symbols=["SPY"],
        interval_seconds=60,
        market_hours_only=True,
        snapshot_timeout_seconds=10.0,
        websocket_enabled=False,
        option_ws_contract_limit_per_symbol=10,
        calendar=_FixedTimeCalendar(),
        now_fn=lambda: datetime(2026, 4, 29, 15, 30, tzinfo=UTC),
        publish_per_contract_trades=True,
    )


@pytest.fixture
def service_without_trades(mock_alpaca, sink):
    return OptionCaptureService(
        alpaca_provider=mock_alpaca,
        multiplexer=None,
        sink_registry=sink,
        symbols=["SPY"],
        interval_seconds=60,
        market_hours_only=True,
        snapshot_timeout_seconds=10.0,
        websocket_enabled=False,
        option_ws_contract_limit_per_symbol=10,
        calendar=_FixedTimeCalendar(),
        now_fn=lambda: datetime(2026, 4, 29, 15, 30, tzinfo=UTC),
        publish_per_contract_trades=False,
    )


@pytest.mark.asyncio
async def test_per_contract_trades_disabled_emits_only_snapshot(service_without_trades, sink):
    await service_without_trades.run_cycle()

    feeds = []
    for call in sink.publish_all_batch.call_args_list:
        for _topic, env in call[0][0]:
            feeds.append(env["feed"])
    assert feeds == ["option_chain_snapshot"]


@pytest.mark.asyncio
async def test_per_contract_trades_enabled_emits_trade_envelopes(service_with_trades, sink):
    await service_with_trades.run_cycle()

    all_envelopes: list[dict[str, Any]] = []
    for call in sink.publish_all_batch.call_args_list:
        for _topic, env in call[0][0]:
            all_envelopes.append(env)

    snapshot_envs = [e for e in all_envelopes if e["feed"] == "option_chain_snapshot"]
    trade_envs = [e for e in all_envelopes if e["feed"] == "option_trades"]

    assert len(snapshot_envs) == 1, "expected 1 snapshot envelope per underlying"
    assert len(trade_envs) == 2, "expected per-contract trade envelopes for both contracts"

    for env in trade_envs:
        assert env["provider"] == "alpaca"
        assert env["source"] == "rest"
        assert env["instrument_type"] == "option"
        assert env["instrument_key"].startswith("option:OCC:SPY")
        assert env["payload"]["price"] in (1.5, 2.1)
        assert "size" in env["payload"]


@pytest.mark.asyncio
async def test_per_contract_trades_skips_contracts_without_last_price(mock_alpaca, sink):
    # One contract has no last_price (never traded today) — should be skipped.
    mock_alpaca.get_option_snapshot_contracts = AsyncMock(
        return_value=[
            _fake_contract(
                occ_symbol="SPY260516C00500000",
                underlying="SPY",
                strike=500.0,
                expiration="2026-05-16",
                option_type="call",
                last=0,  # zero last → no trade
            ),
            _fake_contract(
                occ_symbol="SPY260516P00500000",
                underlying="SPY",
                strike=500.0,
                expiration="2026-05-16",
                option_type="put",
                last=2.10,
            ),
        ]
    )
    service = OptionCaptureService(
        alpaca_provider=mock_alpaca,
        multiplexer=None,
        sink_registry=sink,
        symbols=["SPY"],
        interval_seconds=60,
        market_hours_only=True,
        snapshot_timeout_seconds=10.0,
        websocket_enabled=False,
        option_ws_contract_limit_per_symbol=10,
        calendar=_FixedTimeCalendar(),
        now_fn=lambda: datetime(2026, 4, 29, 15, 30, tzinfo=UTC),
        publish_per_contract_trades=True,
    )
    await service.run_cycle()

    trade_envs = []
    for call in sink.publish_all_batch.call_args_list:
        for _topic, env in call[0][0]:
            if env["feed"] == "option_trades":
                trade_envs.append(env)
    assert len(trade_envs) == 1


def test_option_trade_size_uses_last_trade_size_not_volume(service_with_trades):
    """A missing/zero last_trade_size must yield size 0, never the whole-day
    volume (which would mint one fake trade of size = full-day volume and
    double-count flow / corrupt VWAP)."""
    contract = _fake_contract(
        occ_symbol="SPY260516C00500000",
        underlying="SPY",
        strike=500.0,
        expiration="2026-05-16",
        option_type="call",
        last_size=0,  # no last-trade size reported
    )
    contract["volume"] = 9999  # whole-day volume must NOT leak into trade size
    payload = {"chain_json": {"data": {"contracts": [contract]}}}

    envs = service_with_trades._build_per_contract_trade_envelopes(payload, datetime(2026, 4, 29, tzinfo=UTC))

    assert len(envs) == 1
    assert envs[0]["payload"]["size"] == 0  # not 9999
