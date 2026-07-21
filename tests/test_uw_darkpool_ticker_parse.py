"""get_darkpool_ticker must parse the SDK's typed response shape.

The unusualwhales SDK returns ``DarkpoolTradeResponse`` with the rows in
``.data`` and an EMPTY ``additional_properties`` dict. The old parser read
only ``additional_properties["data"]``, so every per-ticker darkpool fetch
returned 0 rows with no error — which made date-targeted darkpool recovery
backfills silently publish nothing.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from gateway.providers.uw import UnusualWhalesProvider


def _typed_response(items: list) -> SimpleNamespace:
    # Mirrors DarkpoolTradeResponse: rows in .data, additional_properties empty.
    return SimpleNamespace(data=items, additional_properties={})


def _trade(ts: str, price: str, size: int) -> SimpleNamespace:
    return SimpleNamespace(
        ticker="AAPL",
        executed_at=ts,
        price=price,
        size=size,
        premium=None,
        market_center="L",
        tracking_id=12345,
        nbbo_bid=None,
        nbbo_ask=None,
        nbbo_bid_quantity=None,
        nbbo_ask_quantity=None,
        ext_hour_sold_codes=None,
        sale_cond_codes=None,
        timestamp=None,
        volume=None,
        notional=None,
        symbol=None,
        exchange=None,
        venue=None,
    )


@pytest.fixture
async def provider() -> UnusualWhalesProvider:
    p = UnusualWhalesProvider()
    p._client = object()  # bypass initialize(); the SDK call is patched in tests
    return p


async def test_typed_data_response_yields_trades(provider, monkeypatch):
    from unusualwhales.api import darkpool

    resp = _typed_response(
        [_trade("2026-07-16T14:30:00Z", "211.50", 1200), _trade("2026-07-16T15:00:00Z", "212.10", 800)]
    )
    monkeypatch.setattr(darkpool.get_trades_by_ticker, "sync", lambda **kw: resp)

    trades = await provider.get_darkpool_ticker("AAPL", date_str="2026-07-16", limit=500)

    assert len(trades) == 2
    assert trades[0].symbol == "AAPL"
    assert trades[0].size == 1200


async def test_legacy_additional_properties_response_still_parses(provider, monkeypatch):
    from unusualwhales.api import darkpool

    legacy = SimpleNamespace(
        additional_properties={
            "data": [{"ticker": "AAPL", "executed_at": "2026-07-16T14:30:00Z", "price": "211.50", "size": 500}]
        }
    )
    monkeypatch.setattr(darkpool.get_trades_by_ticker, "sync", lambda **kw: legacy)

    trades = await provider.get_darkpool_ticker("AAPL")

    assert len(trades) == 1
    assert trades[0].size == 500
