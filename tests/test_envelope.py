"""Unit tests for EventEnvelope module.

Tests:
- make_instrument_key determinism for equities/crypto/options
- compute_event_id stability across identical inputs
- wrap_event serialization for bars, quotes, trades, flow alerts
"""

import re
from datetime import UTC, datetime
from decimal import Decimal
from unittest import mock

import pytest
from pydantic import BaseModel

from gateway.core.envelope import (
    SCHEMA_VERSION,
    EnvelopeWrapError,
    EventEnvelope,
    compute_event_id,
    fast_wrap_streaming_event,
    make_instrument_key,
    wrap_event,
)

# Heber's writer-side option instrument_key validator (heber/models/envelope.py).
# Mirrored here so the gateway fails fast on any key shape Heber would reject.
HEBER_OPTION_KEY = re.compile(r"^option:OCC:[A-Z]{1,6}\d{6}[CP]\d{8}$")


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

    def test_event_id_is_pinned_and_decimal_precision_sensitive(self):
        """Pin the gateway's local compute_event_id against silent divergence.

        empire-schemas ships a *divergent* duplicate compute_event_id that
        stringifies Decimals as ``str(float(d))`` ("150.5") instead of this
        module's ``str(d)`` ("150.50"). The gateway uses ONLY this local copy
        today, so the duplication is inert — but any future "dedupe the
        duplication" refactor that swaps to the shared impl would silently
        change EVERY event_id and reset Heber's dedup bloom filter, flooding
        duplicate Bronze rows. This pin (and the precision check below) makes
        that swap fail loudly here instead. See project_gateway_frozen_contracts.
        """
        ts = datetime(2026, 6, 25, 14, 30, 0, tzinfo=UTC)
        fields = [Decimal("150.50"), Decimal("150.60"), 100, 200]

        pinned = compute_event_id("alpaca", "quotes", "equity:AAPL", ts, fields)
        assert pinned == "1f2499f93a1c902305db9d07fcd9244a"  # pragma: allowlist secret

        # The float-rounded form (str(150.5)) must NOT collide with the
        # Decimal form (str(Decimal("150.50"))="150.50") — proves the hash is
        # sensitive to the exact divergence the empire-schemas copy introduces.
        float_form = compute_event_id(
            "alpaca", "quotes", "equity:AAPL", ts, [float(f) if isinstance(f, Decimal) else f for f in fields]
        )
        assert pinned != float_form

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

        id1 = compute_event_id("alpaca", "quotes", "equity:AAPL", ts, [Decimal("150.25"), Decimal("150.26"), 100, 200])
        id2 = compute_event_id("alpaca", "quotes", "equity:AAPL", ts, [Decimal("150.25"), Decimal("150.26"), 100, 200])

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
            "option_chain": "AAPL260117C00200000",
        }

        envelope = wrap_event(flow, provider="unusual_whales", feed="flow", source="rest")

        assert envelope["provider"] == "unusual_whales"
        assert envelope["feed"] == "flow"
        assert envelope["source"] == "rest"
        assert envelope["instrument_type"] == "option"
        assert "validated" in envelope["quality_flags"]

    def test_options_flow_envelope_uses_option_chain_for_occ_key(self):
        """UW flow alerts carry the OCC contract in `option_chain`.

        The envelope must build a valid `option:OCC:...` key from it. Without
        this the key is `option:{symbol}` (e.g. `option:SPX`), which Heber's
        writer-side validator rejects — its regex requires
        `option:OCC:[A-Z]{1,6}\\d{6}[CP]\\d{8}` — so 100% of flow alerts are
        dropped before reaching Bronze.
        """
        flow = {
            "symbol": "SPX",
            "timestamp": "2026-06-08T20:58:59Z",
            "strike": 7500.0,
            "expiry": "2026-09-18",
            "put_call": "put",
            "premium": 1500000,
            "volume": 5000,
            "option_chain": "SPX260918P07500000",
        }

        envelope = wrap_event(flow, provider="unusual_whales", feed="flow_alerts", source="rest")

        assert envelope["instrument_type"] == "option"
        assert envelope["instrument_key"] == "option:OCC:SPX260918P07500000"

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

    def test_fast_path_envelope_fields_and_iso_timestamps(self):
        """Fast-path assembly should preserve schema fields and JSON-ready timestamps."""
        ingest_ts = datetime(2026, 1, 15, 12, 0, 5, tzinfo=UTC)
        quote = {
            "S": "AAPL",
            "t": datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC),
            "bp": 150.25,
            "ap": 150.26,
            "x": 123,
        }

        envelope = wrap_event(
            quote,
            provider="alpaca",
            feed="quotes",
            source="websocket",
            stream_type="stock",
            ts_ingest=ingest_ts,
        )

        assert envelope["schema_version"] == SCHEMA_VERSION
        assert isinstance(envelope["ts_event"], str)
        assert isinstance(envelope["ts_ingest"], str)
        assert envelope["ts_event"] == "2026-01-15T12:00:00+00:00"
        assert envelope["ts_ingest"] == "2026-01-15T12:00:05+00:00"
        assert envelope["lineage"]["stream_type"] == "stock"
        assert envelope["lineage"]["sequence"] == "123"

    @pytest.mark.parametrize(
        "occ_symbol",
        [
            "QQQ260609C00690000",  # 3-char root, call (the flooding 0DTE shape)
            "SPXW260918P07500000",  # 4-char index weekly, put (incident's index options)
            "ABCDEF260116C00200000",  # 6-char root — regex upper boundary
            "F260116P00012000",  # 1-char root, put — regex lower boundary
        ],
    )
    def test_fast_path_option_quote_builds_occ_key(self, occ_symbol):
        """OPRA option quotes arrive with the OCC contract as the symbol (`S`).

        The streaming fast-path must emit a valid `option:OCC:...` key. The old
        path produced `option:{symbol}` (e.g. `option:QQQ260609C00690000`, no
        `OCC:` infix), which Heber's writer-side validator rejects — DLQ-ing
        100% of option quotes and stalling the consumer. Covers a range of OCC
        root widths since the bug was visible across index weeklies (SPXW) too.
        """
        envelope = fast_wrap_streaming_event(
            event={"S": occ_symbol, "t": "2026-06-09T20:02:11Z", "bp": 1.23, "ap": 1.25},
            provider="alpaca",
            feed="quotes",
            instrument_type="option",
            ts_ingest=datetime(2026, 6, 9, 20, 2, 11, tzinfo=UTC),
        )

        assert envelope["instrument_type"] == "option"
        assert envelope["instrument_key"] == f"option:OCC:{occ_symbol}"
        assert HEBER_OPTION_KEY.match(envelope["instrument_key"])

    def test_wrap_event_accepts_pydantic_event_models(self):
        """Pydantic event payloads should still be converted to dict payloads."""

        class _EventModel(BaseModel):
            S: str
            t: str
            value: int

        event = _EventModel(S="MSFT", t="2026-01-15T12:00:00Z", value=42)
        envelope = wrap_event(event, provider="alpaca", feed="bars")

        assert envelope["symbol"] == "MSFT"
        assert envelope["payload"]["value"] == 42

    def test_wrap_event_symbol_keyed_dict_does_not_crash(self):
        """wrap_event handles payloads where a key like 'S' maps to a dict (snapshot data)."""
        # Simulates a snapshot payload like {"S": {"latestTrade": {...}}, "AAPL": {...}}
        # where "S" is the SentinelOne ticker and its value is a nested dict
        snapshot_payload = {
            "S": {"latestTrade": {"p": 10.5}, "latestQuote": {"bp": 10.4}},
            "AAPL": {"latestTrade": {"p": 150.0}, "latestQuote": {"bp": 149.9}},
        }
        # Should not raise 'dict' object has no attribute 'upper'
        envelope = wrap_event(snapshot_payload, provider="alpaca", feed="snapshots", source="rest")
        assert isinstance(envelope["symbol"], str)
        assert envelope["provider"] == "alpaca"


class TestWrapEventFailure:
    """Wrap failures must be loud: never emit a malformed key, always count."""

    def _failing_bar(self) -> dict:
        return {"symbol": "AAPL", "t": "2026-01-15T12:00:00Z", "o": 150, "c": 151}

    def test_failure_raises_envelope_wrap_error_lenient(self):
        """Lenient mode (default) raises a tagged EnvelopeWrapError, never returns unknown:."""
        with (
            mock.patch(
                "gateway.core.envelope.record_envelope_created",
                side_effect=RuntimeError("boom"),
            ),
            pytest.raises(EnvelopeWrapError),
        ):
            wrap_event(self._failing_bar(), provider="alpaca", feed="bars", source="websocket")

    def test_failure_raises_original_in_strict_mode(self):
        """Strict mode propagates the original exception so middleware can 500."""
        settings = mock.Mock()
        settings.strict_envelopes = True
        with (
            mock.patch(
                "gateway.core.envelope.record_envelope_created",
                side_effect=RuntimeError("boom"),
            ),
            mock.patch("gateway.config.get_settings", return_value=settings),
            pytest.raises(RuntimeError, match="boom"),
        ):
            wrap_event(self._failing_bar(), provider="alpaca", feed="bars", source="websocket")

    def test_failure_increments_alerting_counter(self):
        """A wrap failure always increments the dropped-message alerting counter."""
        with (
            mock.patch(
                "gateway.core.envelope.record_envelope_created",
                side_effect=RuntimeError("boom"),
            ),
            mock.patch("gateway.core.envelope.record_message_dropped") as counter,
            pytest.raises(EnvelopeWrapError),
        ):
            wrap_event(self._failing_bar(), provider="alpaca", feed="bars", source="websocket")
        counter.assert_called_once_with(reason="envelope_wrap_error", feed="bars")

    def test_failure_never_returns_unknown_key(self):
        """The fallback never silently ships an unknown:/malformed instrument key."""
        with mock.patch(
            "gateway.core.envelope.record_envelope_created",
            side_effect=RuntimeError("boom"),
        ):
            try:
                result = wrap_event(self._failing_bar(), provider="alpaca", feed="bars")
            except EnvelopeWrapError:
                result = None
            assert result is None, "wrap failure must raise, not return a degraded envelope"

    def test_failure_raises_when_settings_unavailable(self):
        """If settings cannot load, prefer raising loudly over a silent malformed key."""
        with (
            mock.patch(
                "gateway.core.envelope.record_envelope_created",
                side_effect=RuntimeError("boom"),
            ),
            mock.patch("gateway.config.get_settings", side_effect=RuntimeError("no settings")),
            pytest.raises(EnvelopeWrapError, match="settings unavailable"),
        ):
            wrap_event(self._failing_bar(), provider="alpaca", feed="bars")

    def test_option_payload_without_occ_contract_raises_and_counts(self):
        """Option-shaped events must not emit malformed option:{symbol} keys."""
        payload = {
            "symbol": "AAPL",
            "timestamp": "2026-01-15T12:00:00Z",
            "strike": 200.0,
            "expiry": "2026-01-17",
            "put_call": "call",
        }

        with (
            mock.patch("gateway.core.envelope.record_message_dropped") as counter,
            pytest.raises(EnvelopeWrapError, match="invalid instrument_key"),
        ):
            wrap_event(payload, provider="unusual_whales", feed="flow_alerts", source="rest")

        counter.assert_called_once_with(reason="envelope_wrap_error", feed="flow_alerts")


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


class TestIsSymbolKeyedDict:
    """Tests for EventEnvelopeMiddleware._is_symbol_keyed_dict."""

    def _make_middleware(self):
        from gateway.api.middleware import EventEnvelopeMiddleware

        return EventEnvelopeMiddleware(app=None)

    def test_snapshot_dict_detected(self):
        """Symbol-keyed dicts like snapshots are correctly identified."""
        mw = self._make_middleware()
        payload = {
            "AAPL": {"latestTrade": {"p": 150.0}},
            "MSFT": {"latestTrade": {"p": 420.0}},
        }
        assert mw._is_symbol_keyed_dict(payload) is True

    def test_normal_event_dict_not_detected(self):
        """Normal event dicts with metadata keys are not false-positived."""
        mw = self._make_middleware()
        payload = {
            "symbol": "AAPL",
            "timeframe": "1Day",
            "bars": [{"o": 1, "c": 2}],
        }
        assert mw._is_symbol_keyed_dict(payload) is False

    def test_empty_dict_not_detected(self):
        """Empty dicts return False."""
        mw = self._make_middleware()
        assert mw._is_symbol_keyed_dict({}) is False

    def test_non_dict_values_not_detected(self):
        """Dicts with non-dict values (like auction lists) are not matched."""
        mw = self._make_middleware()
        payload = {
            "AAPL": [{"auction_price": 150.0}],
            "MSFT": [{"auction_price": 420.0}],
        }
        assert mw._is_symbol_keyed_dict(payload) is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def test_darkpool_distinct_trades_get_distinct_event_ids():
    """200 darkpool prints with distinct tracking_ids → 200 distinct event_ids.

    Guards FEED_UNIQUE_FIELDS['darkpool']: if tracking_id were dropped from the
    dedup key, distinct prints differing only by tracking_id would collide and be
    silently deduped — exactly the latent darkpool silent-loss the runtime hinted
    at (published:0 / duplicates:200 every cycle). A true duplicate still collapses.
    """
    base = {
        "symbol": "MSFT",
        "price": 300,
        "size": 100,
        "notional": 30000,
        "timestamp": "2026-06-25T15:00:00Z",
    }
    ids = set()
    for i in range(200):
        env = wrap_event(
            event={**base, "tracking_id": f"d{i}"}, provider="unusual_whales", feed="darkpool", source="rest"
        )
        ids.add(env["event_id"])
    assert len(ids) == 200  # every distinct print kept

    e1 = wrap_event(event={**base, "tracking_id": "dup"}, provider="unusual_whales", feed="darkpool", source="rest")
    e2 = wrap_event(event={**base, "tracking_id": "dup"}, provider="unusual_whales", feed="darkpool", source="rest")
    assert e1["event_id"] == e2["event_id"]  # true duplicate collapses to one id


def test_oi_change_distinct_contracts_get_distinct_event_ids():
    """Distinct OCC contracts on one underlying/date must not collapse to one event_id.

    Mirrors the darkpool tracking_id guard. FEED_UNIQUE_FIELDS['oi_change'] leads with
    ``option_symbol``, but NormalizedOIChange used to omit that field, so Pydantic
    (extra="ignore") dropped it before the payload reached compute_event_id. Two
    contracts sharing ``call_oi_change`` (often 0) then hashed identically and all but
    one dropped at Heber's Bronze dedup. Building the payload via ``model_dump`` makes
    this test fail if the schema ever stops carrying ``option_symbol``.
    """
    from gateway.schemas import NormalizedOIChange

    def eid(option_symbol: str) -> str:
        row = NormalizedOIChange(
            symbol="AAPL",
            date="2025-01-02",
            call_oi=0,
            put_oi=0,
            call_oi_change=0,
            put_oi_change=0,
            option_symbol=option_symbol,
            provider="unusual_whales",
        )
        env = wrap_event(row.model_dump(mode="json"), provider="unusual_whales", feed="oi_change", source="rest")
        return env["event_id"]

    a = eid("AAPL250117C00200000")
    b = eid("AAPL250117C00210000")
    assert a != b  # distinct contracts kept
    assert eid("AAPL250117C00200000") == a  # true duplicate still collapses to one id
