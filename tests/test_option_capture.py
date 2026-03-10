from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from gateway.config import Settings
from gateway.core.option_capture import OptionCaptureService


@dataclass
class _FakeContract:
    contract_symbol: str
    underlying: str
    expiration: str
    strike: float
    option_type: str
    bid: float
    ask: float
    last: float
    volume: int
    open_interest: int
    delta: float | None
    gamma: float | None
    theta: float | None
    vega: float | None
    iv: float | None
    timestamp: datetime
    provider: str = "alpaca"

    def model_dump(self, mode: str = "json") -> dict[str, Any]:
        return {
            "contract_symbol": self.contract_symbol,
            "underlying": self.underlying,
            "expiration": self.expiration,
            "strike": self.strike,
            "option_type": self.option_type,
            "bid": self.bid,
            "ask": self.ask,
            "last": self.last,
            "volume": self.volume,
            "open_interest": self.open_interest,
            "delta": self.delta,
            "gamma": self.gamma,
            "theta": self.theta,
            "vega": self.vega,
            "iv": self.iv,
            "timestamp": self.timestamp.isoformat(),
            "provider": self.provider,
        }


class _FakeAlpacaProvider:
    def __init__(self, responses: dict[str, list[_FakeContract] | Exception]) -> None:
        self._responses = responses
        self.calls: list[str] = []

    async def get_option_snapshot_contracts(self, underlying: str) -> list[_FakeContract]:
        self.calls.append(underlying)
        response = self._responses[underlying]
        if isinstance(response, Exception):
            raise response
        return response


class _FakeSinkRegistry:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def publish_all(self, topic: str, envelope: dict[str, Any]) -> None:
        self.published.append((topic, envelope))


class _FakeMultiplexer:
    def __init__(self) -> None:
        self.subscribe_calls: list[dict[str, Any]] = []
        self.unsubscribe_calls: list[dict[str, Any]] = []

    async def client_subscribe(
        self,
        client_id: str,
        stream_type: Any,
        bars: list[str] | None = None,
        quotes: list[str] | None = None,
        trades: list[str] | None = None,
        news: list[str] | None = None,
    ) -> dict[str, Any]:
        self.subscribe_calls.append(
            {
                "client_id": client_id,
                "stream_type": stream_type,
                "bars": sorted(bars or []),
                "quotes": sorted(quotes or []),
                "trades": sorted(trades or []),
                "news": sorted(news or []),
            }
        )
        return {
            "status": "ok",
            "subscribed": sorted(set((bars or []) + (quotes or []) + (trades or []) + (news or []))),
        }

    async def client_unsubscribe(
        self,
        client_id: str,
        stream_type: Any,
        bars: list[str] | None = None,
        quotes: list[str] | None = None,
        trades: list[str] | None = None,
        news: list[str] | None = None,
    ) -> dict[str, Any]:
        self.unsubscribe_calls.append(
            {
                "client_id": client_id,
                "stream_type": stream_type,
                "bars": sorted(bars or []),
                "quotes": sorted(quotes or []),
                "trades": sorted(trades or []),
                "news": sorted(news or []),
            }
        )
        return {
            "status": "ok",
            "unsubscribed": sorted(set((bars or []) + (quotes or []) + (trades or []) + (news or []))),
        }


class _FakeCalendar:
    def __init__(self, open_now: bool) -> None:
        self.open_now = open_now
        self.calls: list[datetime] = []

    def is_market_open(self, dt: datetime | None = None) -> bool:
        if dt is not None:
            self.calls.append(dt)
        return self.open_now


def _contract(
    symbol: str, *, underlying: str = "SPY", strike: float = 500.0, option_type: str = "call"
) -> _FakeContract:
    return _FakeContract(
        contract_symbol=symbol,
        underlying=underlying,
        expiration="2026-03-10",
        strike=strike,
        option_type=option_type,
        bid=2.0,
        ask=2.1,
        last=2.05,
        volume=1000,
        open_interest=2500,
        delta=0.51 if option_type == "call" else -0.49,
        gamma=0.09,
        theta=-0.02,
        vega=0.04,
        iv=0.24,
        timestamp=datetime(2026, 3, 10, 14, 31, tzinfo=UTC),
    )


def test_option_capture_settings_parse_symbol_csv() -> None:
    settings = Settings(option_capture_symbols=" spy, qqq ,, iwm ")

    assert settings.option_capture_symbol_list == ["SPY", "QQQ", "IWM"]


@pytest.mark.asyncio
async def test_run_cycle_skips_when_market_is_closed() -> None:
    provider = _FakeAlpacaProvider({"SPY": [_contract("SPY260310C00500000")]})
    sink_registry = _FakeSinkRegistry()
    service = OptionCaptureService(
        alpaca_provider=provider,
        multiplexer=_FakeMultiplexer(),
        sink_registry=sink_registry,
        symbols=["SPY"],
        calendar=_FakeCalendar(open_now=False),
    )

    summary = await service.run_cycle(now=datetime(2026, 3, 10, 14, 31, tzinfo=UTC))

    assert summary["published"] == 0
    assert summary["skipped_market_closed"] is True
    assert provider.calls == []
    assert sink_registry.published == []


@pytest.mark.asyncio
async def test_run_cycle_publishes_one_snapshot_envelope_per_symbol() -> None:
    provider = _FakeAlpacaProvider(
        {
            "SPY": [
                _contract("SPY260310C00500000", underlying="SPY"),
                _contract("SPY260310P00500000", underlying="SPY", option_type="put"),
            ],
            "QQQ": [_contract("QQQ260310C00450000", underlying="QQQ", strike=450.0)],
        }
    )
    sink_registry = _FakeSinkRegistry()
    multiplexer = _FakeMultiplexer()
    service = OptionCaptureService(
        alpaca_provider=provider,
        multiplexer=multiplexer,
        sink_registry=sink_registry,
        symbols=["SPY", "QQQ"],
        calendar=_FakeCalendar(open_now=True),
    )

    summary = await service.run_cycle(now=datetime(2026, 3, 10, 14, 31, tzinfo=UTC))

    assert summary["published"] == 2
    assert summary["failed_symbols"] == []
    assert [topic for topic, _ in sink_registry.published] == ["heber:events", "heber:events"]
    published_by_symbol = {envelope["symbol"]: envelope for _, envelope in sink_registry.published}
    assert set(published_by_symbol) == {"SPY", "QQQ"}

    spy_envelope = published_by_symbol["SPY"]
    assert spy_envelope["provider"] == "alpaca"
    assert spy_envelope["feed"] == "option_chain_snapshot"
    assert spy_envelope["source"] == "rest"
    assert spy_envelope["instrument_type"] == "equity"
    assert spy_envelope["instrument_key"] == "equity:SPY"
    assert spy_envelope["payload"]["underlying"] == "SPY"
    assert spy_envelope["payload"]["total_call_volume"] == pytest.approx(1000.0)
    assert spy_envelope["payload"]["total_put_volume"] == pytest.approx(1000.0)
    assert spy_envelope["payload"]["total_call_oi"] == pytest.approx(2500.0)
    assert spy_envelope["payload"]["total_put_oi"] == pytest.approx(2500.0)
    assert spy_envelope["payload"]["atm_iv"] == pytest.approx(0.24)
    assert spy_envelope["payload"]["chain_json"]["data"]["contracts"][0]["contract_symbol"] == "SPY260310C00500000"

    assert len(multiplexer.subscribe_calls) == 1
    subscribe_call = multiplexer.subscribe_calls[0]
    assert subscribe_call["bars"] == ["QQQ260310C00450000", "SPY260310C00500000", "SPY260310P00500000"]
    assert subscribe_call["quotes"] == subscribe_call["bars"]
    assert subscribe_call["trades"] == subscribe_call["bars"]


@pytest.mark.asyncio
async def test_run_cycle_continues_when_one_symbol_snapshot_fails() -> None:
    provider = _FakeAlpacaProvider(
        {
            "SPY": [_contract("SPY260310C00500000", underlying="SPY")],
            "QQQ": RuntimeError("boom"),
        }
    )
    sink_registry = _FakeSinkRegistry()
    service = OptionCaptureService(
        alpaca_provider=provider,
        multiplexer=_FakeMultiplexer(),
        sink_registry=sink_registry,
        symbols=["SPY", "QQQ"],
        calendar=_FakeCalendar(open_now=True),
    )

    summary = await service.run_cycle(now=datetime(2026, 3, 10, 14, 31, tzinfo=UTC))

    assert summary["published"] == 1
    assert summary["failed_symbols"] == ["QQQ"]
    assert [envelope["symbol"] for _, envelope in sink_registry.published] == ["SPY"]


@pytest.mark.asyncio
async def test_run_cycle_reconciles_websocket_contract_subscriptions() -> None:
    provider = _FakeAlpacaProvider(
        {
            "SPY": [_contract("SPY260310C00500000"), _contract("SPY260310P00500000", option_type="put")],
        }
    )
    sink_registry = _FakeSinkRegistry()
    multiplexer = _FakeMultiplexer()
    service = OptionCaptureService(
        alpaca_provider=provider,
        multiplexer=multiplexer,
        sink_registry=sink_registry,
        symbols=["SPY"],
        calendar=_FakeCalendar(open_now=True),
    )

    await service.run_cycle(now=datetime(2026, 3, 10, 14, 31, tzinfo=UTC))

    provider._responses["SPY"] = [
        _contract("SPY260310P00500000", option_type="put"),
        _contract("SPY260310C00505000", strike=505.0),
    ]
    summary = await service.run_cycle(now=datetime(2026, 3, 10, 14, 32, tzinfo=UTC))

    assert summary["published"] == 1
    assert len(multiplexer.subscribe_calls) == 2
    assert multiplexer.subscribe_calls[-1]["bars"] == ["SPY260310C00505000"]
    assert len(multiplexer.unsubscribe_calls) == 1
    assert multiplexer.unsubscribe_calls[0]["bars"] == ["SPY260310C00500000"]
