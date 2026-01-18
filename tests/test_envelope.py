"""Unit tests for EventEnvelope module.

Tests:
- make_instrument_key determinism for equities/crypto/options
- compute_event_id stability across identical inputs
- wrap_event serialization for bars, quotes, trades, flow alerts
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from gateway.core.envelope import (
    SCHEMA_VERSION,
    EventEnvelope,
    compute_event_id,
    make_instrument_key,
    wrap_event,
)


class TestMakeInstrumentKey:
    """Tests for make_instrument_key determinism."""

    def test_equity_key(self):
        """Equities use equity:SYMBOL format."""
        assert make_instrument_key("AAPL", "equity") == "equity:AAPL"
        assert make_instrument_key("aapl", "equity") == "equity:AAPL"  # Case normalized
        assert make_instrument_key("  AAPL  ", "equity") == "equity:AAPL"  # Trimmed

    def test_crypto_key_with_dash(self):
        """Crypto with dash separator."""
        assert make_instrument_key("BTC-USD", "crypto") == "crypto:BTC-USD"
        assert make_instrument_key("ETH-USDT", "crypto") == "crypto:ETH-USDT"

    def test_crypto_key_with_slash(self):
        """Crypto with slash separator is normalized to dash."""
        assert make_instrument_key("BTC/USD", "crypto") == "crypto:BTC-USD"
        assert make_instrument_key("ETH/USDT", "crypto") == "crypto:ETH-USDT"

    def test_crypto_key_no_separator(self):
        """Crypto without separator is split (assume 3-char base)."""
        assert make_instrument_key("BTCUSD", "crypto") == "crypto:BTC-USD"
        assert make_instrument_key("ETHUSD", "crypto") == "crypto:ETH-USD"

    def test_option_key_with_contract(self):
        """Options use OCC contract symbol when available."""
        assert (
            make_instrument_key("AAPL", "option", contract_symbol="AAPL250117C00200000")
            == "option:OCC:AAPL250117C00200000"
        )

    def test_option_key_fallback(self):
        """Options fallback to symbol when no contract."""
        assert make_instrument_key("AAPL", "option") == "option:AAPL"

    def test_forex_key(self):
        """Forex pairs normalized to BASE-QUOTE."""
        assert make_instrument_key("EURUSD", "forex") == "forex:EUR-USD"
        assert make_instrument_key("EUR/USD", "forex") == "forex:EUR-USD"

    def test_determinism(self):
        """Same inputs always produce same key."""
        for _ in range(100):
            assert make_instrument_key("AAPL", "equity") == "equity:AAPL"
            assert make_instrument_key("BTC-USD", "crypto") == "crypto:BTC-USD"


class TestComputeEventId:
    """Tests for compute_event_id stability."""

    def test_same_inputs_same_id(self):
        """Identical inputs produce identical event IDs."""
        ts = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)

        id1 = compute_event_id("alpaca", "bars", "equity:AAPL", ts, ["1Min"])
        id2 = compute_event_id("alpaca", "bars", "equity:AAPL", ts, ["1Min"])

        assert id1 == id2
        assert len(id1) == 32  # SHA256 truncated to 32 chars

    def test_different_timestamps_different_ids(self):
        """Different timestamps produce different event IDs."""
        ts1 = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2026, 1, 15, 12, 0, 1, tzinfo=UTC)

        id1 = compute_event_id("alpaca", "bars", "equity:AAPL", ts1, ["1Min"])
        id2 = compute_event_id("alpaca", "bars", "equity:AAPL", ts2, ["1Min"])

        assert id1 != id2

    def test_different_providers_different_ids(self):
        """Different providers produce different event IDs."""
        ts = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)

        id1 = compute_event_id("alpaca", "bars", "equity:AAPL", ts, ["1Min"])
        id2 = compute_event_id("finnhub", "bars", "equity:AAPL", ts, ["1Min"])

        assert id1 != id2

    def test_decimal_unique_fields(self):
        """Handles Decimal values in unique fields."""
        ts = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)

        id1 = compute_event_id(
            "alpaca", "quotes", "equity:AAPL", ts, [Decimal("150.25"), Decimal("150.26"), 100, 200]
        )
        id2 = compute_event_id(
            "alpaca", "quotes", "equity:AAPL", ts, [Decimal("150.25"), Decimal("150.26"), 100, 200]
        )

        assert id1 == id2


class TestWrapEvent:
    """Tests for wrap_event envelope creation."""

    def test_equity_bar_envelope(self):
        """Wraps equity bar event correctly."""
        bar = {
            "T": "b",
            "S": "AAPL",
            "t": "2026-01-15T12:00:00Z",
            "o": 150.0,
            "h": 151.0,
            "l": 149.0,
            "c": 150.5,
            "v": 100000,
            "x": "1Min",
        }

        envelope = wrap_event(bar, provider="alpaca", feed="bars", source="websocket")

        assert envelope["provider"] == "alpaca"
        assert envelope["feed"] == "bars"
        assert envelope["source"] == "websocket"
        assert envelope["instrument_type"] == "equity"
        assert envelope["instrument_key"] == "equity:AAPL"
        assert envelope["symbol"] == "AAPL"
        assert envelope["schema_version"] == SCHEMA_VERSION
        assert "event_id" in envelope
        assert len(envelope["event_id"]) == 32
        assert envelope["payload"] == bar

    def test_equity_quote_envelope(self):
        """Wraps equity quote event correctly."""
        quote = {
            "T": "q",
            "S": "AAPL",
            "t": "2026-01-15T12:00:00Z",
            "bp": 150.25,
            "bs": 100,
            "ap": 150.26,
            "as": 200,
        }

        envelope = wrap_event(quote, provider="alpaca", feed="quotes", source="websocket")

        assert envelope["feed"] == "quotes"
        assert envelope["instrument_key"] == "equity:AAPL"
        assert "event_id" in envelope

    def test_options_flow_envelope(self):
        """Wraps options flow alert correctly."""
        flow = {
            "symbol": "AAPL",
            "timestamp": "2026-01-15T12:00:00Z",
            "strike": 200.0,
            "expiry": "2026-01-17",
            "put_call": "call",
            "premium": 1500000,
            "volume": 5000,
            "open_interest": 10000,
        }

        envelope = wrap_event(flow, provider="unusual_whales", feed="flow", source="rest")

        assert envelope["provider"] == "unusual_whales"
        assert envelope["feed"] == "flow"
        assert envelope["source"] == "rest"
        assert envelope["instrument_type"] == "option"
        assert "cached" in envelope["quality_flags"]  # REST source adds cached flag

    def test_envelope_serializable(self):
        """Envelope can be JSON serialized."""
        import json

        bar = {"S": "AAPL", "t": "2026-01-15T12:00:00Z", "c": 150.5}
        envelope = wrap_event(bar, provider="alpaca", feed="bars", source="websocket")

        # Should not raise
        json_str = json.dumps(envelope)
        parsed = json.loads(json_str)

        assert parsed["symbol"] == "AAPL"
        assert parsed["event_id"] == envelope["event_id"]

    def test_event_id_stable_across_calls(self):
        """Same event wrapped multiple times gets same event_id."""
        bar = {
            "S": "AAPL",
            "t": "2026-01-15T12:00:00Z",
            "x": "1Min",
        }

        envelope1 = wrap_event(bar, provider="alpaca", feed="bars", source="websocket")
        envelope2 = wrap_event(bar, provider="alpaca", feed="bars", source="websocket")

        assert envelope1["event_id"] == envelope2["event_id"]


class TestEventEnvelopeModel:
    """Tests for EventEnvelope Pydantic model."""

    def test_model_validation(self):
        """Model validates all required fields."""
        envelope = EventEnvelope(
            event_id="abc123",
            provider="alpaca",
            feed="bars",
            source="websocket",
            instrument_type="equity",
            instrument_key="equity:AAPL",
            symbol="AAPL",
            ts_event=datetime.now(UTC),
            ts_ingest=datetime.now(UTC),
            payload={"test": "data"},
        )

        assert envelope.event_id == "abc123"
        assert envelope.schema_version == SCHEMA_VERSION

    def test_model_defaults(self):
        """Model has correct defaults."""
        envelope = EventEnvelope(
            event_id="abc123",
            provider="alpaca",
            feed="bars",
            source="websocket",
            instrument_type="equity",
            instrument_key="equity:AAPL",
            symbol="AAPL",
            ts_event=datetime.now(UTC),
            ts_ingest=datetime.now(UTC),
            payload={},
        )

        assert envelope.lineage == {}
        assert envelope.quality_flags == []
        assert envelope.schema_version == SCHEMA_VERSION


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
