"""Envelope -> Heber instrument_key contract test.

This is the regression net for the three prior option-key DLQ incidents
(streaming OPRA quotes, UW flow alerts, and per-underlying analytics all once
emitted malformed `option:{symbol}` keys with no `OCC:` infix, which Heber's
writer-side validator rejects -- dropping 100% of those records on the
Bronze -> Silver hop).

Heber's writer-side validator (heber/models/envelope.py) accepts instrument
keys matching EXACTLY the regexes mirrored below, one per instrument_type.
This test compiles those regexes locally and asserts that the instrument_key
produced by ``wrap_event`` AND ``fast_wrap_streaming_event`` validates for
representative payloads of every feed in ``FEED_UNIQUE_FIELDS`` and every
instrument_type (equity / option / crypto / forex / macro).

A negative control proves the test has teeth: an option key built WITHOUT the
OCC contract (the historical bug) must FAIL validation.

The representative payloads and override kwargs mirror the real gateway call
sites (e.g. UW flow passes the OCC contract via ``option_chain``; per-underlying
analytics with an expiry pass ``instrument_type_override="equity"``; the
treasury poller passes ``instrument_type_override="macro"``). The contract is
only meaningful if it tests what the gateway actually emits in production.
"""

import re
from datetime import UTC, datetime

import pytest

from gateway.core.envelope import (
    FEED_UNIQUE_FIELDS,
    fast_wrap_streaming_event,
    wrap_event,
)

# Heber writer-side instrument_key validators (heber/models/envelope.py),
# mirrored verbatim so the gateway fails fast on any key shape Heber rejects.
HEBER_KEY_PATTERNS: dict[str, re.Pattern[str]] = {
    "equity": re.compile(r"^equity:[A-Z0-9]+(?:[.-][A-Z0-9]+)*$"),
    "crypto": re.compile(r"^crypto:[A-Z]{2,10}-[A-Z]{2,10}$"),
    "forex": re.compile(r"^forex:[A-Z]{3}-[A-Z]{3}$"),
    "option": re.compile(r"^option:OCC:[A-Z]{1,6}\d{6}[CP]\d{8}$"),
    "macro": re.compile(r"^macro:[a-z][a-z0-9_]*(?::[a-z0-9_]+)*$"),
}


def _assert_key_valid(envelope: dict) -> None:
    """Assert the envelope's instrument_key matches Heber's validator for its type."""
    inst_type = envelope["instrument_type"]
    inst_key = envelope["instrument_key"]
    pattern = HEBER_KEY_PATTERNS.get(inst_type)
    assert pattern is not None, f"unknown instrument_type {inst_type!r} (key={inst_key!r})"
    assert pattern.match(inst_key), (
        f"instrument_key {inst_key!r} (type={inst_type!r}) would be rejected by Heber validator {pattern.pattern!r}"
    )


_TS = datetime(2025, 1, 2, 14, 30, tzinfo=UTC).isoformat()

# Representative (event, kwargs) per feed in FEED_UNIQUE_FIELDS. The payloads and
# overrides mirror the real gateway call sites; each entry is what the gateway
# actually emits to Heber for that feed.
REST_FEED_CASES: dict[str, tuple[dict, dict]] = {
    "trades": ({"symbol": "AAPL", "price": 150.0, "trade_id": "t1", "timestamp": _TS}, {}),
    "bars": ({"symbol": "AAPL", "timeframe": "1Min", "o": 1, "h": 2, "l": 1, "c": 2, "timestamp": _TS}, {}),
    "quotes": ({"symbol": "AAPL", "bp": 1, "ap": 2, "bs": 1, "as": 1, "timestamp": _TS}, {}),
    # UW options flow: OCC contract carried in `option_chain` -> option:OCC:...
    "flow": (
        {
            "underlying": "AAPL",
            "option_chain": "AAPL250117C00200000",
            "expiry": "2025-01-17",
            "strike": 200,
            "put_call": "call",
            "premium": 1000,
            "volume": 5,
            "timestamp": _TS,
        },
        {},
    ),
    "flow_alerts": (
        {
            "ticker": "TSLA",
            "option_chain": "TSLA250620P00250000",
            "expiry": "2025-06-20",
            "strike": 250,
            "put_call": "put",
            "premium": 500,
            "volume": 2,
            "timestamp": _TS,
        },
        {},
    ),
    "darkpool": (
        {"symbol": "MSFT", "tracking_id": "d1", "price": 300, "size": 100, "notional": 30000, "timestamp": _TS},
        {},
    ),
    "news": ({"symbol": "AAPL", "article_id": "n1", "timestamp": _TS}, {}),
    "etf": ({"etf_symbol": "SPY", "symbol": "AAPL", "date": "2025-01-02", "timestamp": _TS}, {}),
    "shorts": ({"symbol": "GME", "date": "2025-01-02", "quantity": 1000, "timestamp": _TS}, {}),
    "screener": ({"symbol": "NVDA", "screen_type": "gappers", "position": 1, "timestamp": _TS}, {}),
    # Market-wide feeds: no symbol -> placeholder MARKET (equity:MARKET)
    "market_tide": (
        {"date": "2025-01-02", "net_call_premium": 100, "net_put_premium": 50, "timestamp": _TS},
        {},
    ),
    "sector_tide": (
        {"date": "2025-01-02", "sector": "Tech", "net_call_premium": 100, "net_put_premium": 50, "timestamp": _TS},
        {},
    ),
    "insiders": (
        {"symbol": "AAPL", "id": "i1", "filing_date": "2025-01-02", "insider_name": "X", "timestamp": _TS},
        {},
    ),
    "institutions": ({"symbol": "AAPL", "id": "x1", "filing_date": "2025-01-02", "timestamp": _TS}, {}),
    "politicians": ({"symbol": "AAPL", "id": "p1", "filing_date": "2025-01-02", "timestamp": _TS}, {}),
    # Per-underlying analytics carry an expiry -> would mis-infer as option.
    # The real poller passes equity overrides (the iv_term_structure fix).
    "analytics": (
        {"symbol": "NVDA", "expiry": "2025-03-21", "metric_type": "iv_term_structure", "timestamp": _TS},
        {"instrument_type_override": "equity", "instrument_key_override": "equity:NVDA"},
    ),
    "forex": (
        {"pair": "EUR-USD", "symbol": "EURUSD", "bid": 1, "ask": 2, "timestamp": _TS},
        {"instrument_type_override": "forex"},
    ),
    "fundamentals": ({"symbol": "AAPL", "market_cap": 1_000_000, "timestamp": _TS}, {}),
    "greek_exposure": ({"symbol": "AAPL", "call_gamma": 0.5, "timestamp": _TS}, {}),
    "iv_rank": ({"symbol": "AAPL", "iv_rank": 0.4, "timestamp": _TS}, {}),
    "oi_change": ({"symbol": "AAPL", "date": "2025-01-02", "call_oi_change": 100, "timestamp": _TS}, {}),
    # Carries an expiry -> equity overrides (same fix as analytics).
    "historic_option_volume": (
        {"symbol": "SPY", "date": "2025-01-02", "expiry": "2025-01-17", "timestamp": _TS},
        {"instrument_type_override": "equity", "instrument_key_override": "equity:SPY"},
    ),
    "short_interest": ({"symbol": "AAPL", "date": "2025-01-02", "short_interest": 1000, "timestamp": _TS}, {}),
    "short_volume": ({"symbol": "AAPL", "date": "2025-01-02", "short_interest": 1000, "timestamp": _TS}, {}),
    "ftds": ({"symbol": "AAPL", "date": "2025-01-02", "quantity": 500, "timestamp": _TS}, {}),
    "congress_trades": ({"ticker": "BRK.B", "name": "X", "transaction_date": "2025-01-02", "timestamp": _TS}, {}),
    "insider_trades": ({"ticker": "AAPL", "owner_name": "X", "transaction_date": "2025-01-02", "timestamp": _TS}, {}),
    # Treasury yields are macro data; the poller passes macro overrides.
    "treasury_yields": (
        {"date": "2025-01-02", "maturity": "10year", "yield_pct": 4.2, "timestamp": _TS},
        {
            "instrument_type_override": "macro",
            "instrument_key_override": "macro:treasury_yield:10year",
            "symbol_override": "TREASURY_10YEAR",
        },
    ),
}


def test_all_feeds_have_a_contract_case() -> None:
    """Every feed in FEED_UNIQUE_FIELDS is exercised -- new feeds must be added here."""
    missing = set(FEED_UNIQUE_FIELDS) - set(REST_FEED_CASES)
    assert not missing, f"feeds in FEED_UNIQUE_FIELDS missing a contract case: {sorted(missing)}"


@pytest.mark.parametrize("feed", sorted(REST_FEED_CASES))
def test_wrap_event_key_passes_heber_validator(feed: str) -> None:
    """wrap_event emits a Heber-valid instrument_key for every feed."""
    event, kwargs = REST_FEED_CASES[feed]
    envelope = wrap_event(event=event, provider="unusual_whales", feed=feed, source="rest", **kwargs)
    assert envelope["feed"] == feed
    _assert_key_valid(envelope)


# fast_wrap_streaming_event is the OPRA / Alpaca streaming hot path. It takes a
# pre-resolved instrument_type and (for options) the OCC contract as the symbol.
STREAMING_CASES: dict[str, tuple[dict, str, str]] = {
    # name: (event, feed, instrument_type)
    "equity_trade": ({"S": "AAPL", "t": _TS, "p": 150, "i": "t1"}, "trades", "equity"),
    "equity_quote": ({"S": "AAPL", "t": _TS, "bp": 1, "ap": 2, "bs": 1, "as": 1}, "quotes", "equity"),
    # OPRA sends the OCC contract as the symbol -> option:OCC:...
    "option_quote": ({"S": "QQQ260609C00690000", "t": _TS, "bp": 1, "ap": 2}, "quotes", "option"),
    "option_trade": ({"S": "QQQ260609C00690000", "t": _TS, "p": 1, "i": "x1"}, "trades", "option"),
    "crypto_trade": ({"S": "BTC/USD", "t": _TS, "p": 50000, "i": "c1"}, "trades", "crypto"),
}


@pytest.mark.parametrize("name", sorted(STREAMING_CASES))
def test_fast_wrap_streaming_key_passes_heber_validator(name: str) -> None:
    """fast_wrap_streaming_event emits a Heber-valid instrument_key for every type."""
    event, feed, inst_type = STREAMING_CASES[name]
    envelope = fast_wrap_streaming_event(
        event=event,
        provider="alpaca",
        feed=feed,
        instrument_type=inst_type,
        ts_ingest=datetime.now(UTC),
    )
    assert envelope["instrument_type"] == inst_type
    _assert_key_valid(envelope)


def test_negative_control_option_key_without_occ_fails() -> None:
    """Negative control: the historical bug (option key lacking the OCC infix)
    MUST fail Heber's option validator. This proves the contract has teeth -- if
    this passed, the test could never catch the DLQ-flood regression."""
    bug_key = "option:QQQ260609C00690000"  # no `OCC:` infix -- the historical bug
    assert not HEBER_KEY_PATTERNS["option"].match(bug_key)
    # And the correctly-formed key it should have been MUST pass.
    assert HEBER_KEY_PATTERNS["option"].match("option:OCC:QQQ260609C00690000")


@pytest.mark.parametrize(
    "feed, base, field, val_a, val_b",
    [
        (
            "insider_trades",
            {"ticker": "AAPL", "owner_name": "Jane Doe", "transaction_date": "2026-06-25", "timestamp": _TS},
            "id",
            "txn-1",
            "txn-2",
        ),
        (
            "congress_trades",
            {"ticker": "AAPL", "name": "Sen X", "transaction_date": "2026-06-25", "timestamp": _TS},
            "txn_type",
            "Purchase",
            "Sale",
        ),
        (
            "oi_change",
            {"symbol": "AAPL", "date": "2026-06-25", "call_oi_change": 0, "timestamp": _TS},
            "option_symbol",
            "AAPL250117C00200000",
            "AAPL250117P00200000",
        ),
    ],
)
def test_distinct_rows_get_distinct_event_ids(feed, base, field, val_a, val_b) -> None:
    """Multi-row feeds must not collapse distinct same-day rows to one event_id.

    Regression guard for the insider/congress/oi_change collision class (the
    insider_trades 200→~2 Bronze loss): two rows differing only in the row's
    discriminating field must get distinct event_ids; identical rows still
    collapse (true duplicate).
    """
    a = wrap_event(event={**base, field: val_a}, provider="unusual_whales", feed=feed, source="rest")
    b = wrap_event(event={**base, field: val_b}, provider="unusual_whales", feed=feed, source="rest")
    assert a["event_id"] != b["event_id"]  # distinct rows → distinct ids

    c = wrap_event(event={**base, field: val_a}, provider="unusual_whales", feed=feed, source="rest")
    assert a["event_id"] == c["event_id"]  # true duplicate still collapses
