"""Tests for SequenceTracker — per-symbol, per-feed gap detection (PRD §Sequence Numbers)."""

from unittest.mock import MagicMock, patch

import pytest

from gateway.core.stream import SequenceTracker

# ---------- Unit tests: SequenceTracker ----------


class TestSequenceTrackerBasic:
    """Core counter behaviour."""

    def test_first_call_returns_one(self):
        tracker = SequenceTracker()
        assert tracker.next_seq("AAPL", "bars") == 1

    def test_sequential_increments(self):
        tracker = SequenceTracker()
        results = [tracker.next_seq("AAPL", "bars") for _ in range(5)]
        assert results == [1, 2, 3, 4, 5]

    def test_independent_per_symbol(self):
        tracker = SequenceTracker()
        tracker.next_seq("AAPL", "bars")
        tracker.next_seq("AAPL", "bars")
        assert tracker.next_seq("TSLA", "bars") == 1
        assert tracker.next_seq("AAPL", "bars") == 3

    def test_independent_per_feed(self):
        tracker = SequenceTracker()
        tracker.next_seq("AAPL", "bars")
        tracker.next_seq("AAPL", "bars")
        assert tracker.next_seq("AAPL", "quotes") == 1
        assert tracker.next_seq("AAPL", "trades") == 1
        assert tracker.next_seq("AAPL", "bars") == 3

    def test_combined_independence(self):
        """Different (symbol, feed) pairs are fully independent."""
        tracker = SequenceTracker()
        tracker.next_seq("AAPL", "bars")
        tracker.next_seq("TSLA", "quotes")
        tracker.next_seq("AAPL", "bars")
        tracker.next_seq("TSLA", "quotes")

        assert tracker.next_seq("AAPL", "bars") == 3
        assert tracker.next_seq("TSLA", "quotes") == 3
        assert tracker.next_seq("AAPL", "quotes") == 1
        assert tracker.next_seq("TSLA", "bars") == 1


class TestSequenceTrackerReset:
    """Reset clears all counters per PRD (reset to 0 on gateway restart)."""

    def test_reset_clears_all(self):
        tracker = SequenceTracker()
        tracker.next_seq("AAPL", "bars")
        tracker.next_seq("TSLA", "quotes")
        tracker.reset()
        assert tracker.next_seq("AAPL", "bars") == 1
        assert tracker.next_seq("TSLA", "quotes") == 1

    def test_reset_idempotent(self):
        tracker = SequenceTracker()
        tracker.reset()
        tracker.reset()
        assert tracker.next_seq("AAPL", "bars") == 1


class TestSequenceTrackerStatus:
    """Diagnostic status reporting."""

    def test_empty_status(self):
        tracker = SequenceTracker()
        status = tracker.get_status()
        assert status["tracked_keys"] == 0
        assert status["total_messages"] == 0

    def test_status_after_activity(self):
        tracker = SequenceTracker()
        for _ in range(10):
            tracker.next_seq("AAPL", "bars")
        for _ in range(5):
            tracker.next_seq("TSLA", "quotes")

        status = tracker.get_status()
        assert status["tracked_keys"] == 2
        assert status["total_messages"] == 15

    def test_status_after_reset(self):
        tracker = SequenceTracker()
        tracker.next_seq("AAPL", "bars")
        tracker.reset()
        status = tracker.get_status()
        assert status["tracked_keys"] == 0
        assert status["total_messages"] == 0


# ---------- Integration: Multiplexer injects seq into envelope ----------


class TestSequenceInMultiplexer:
    """Verify StreamMultiplexer injects seq into envelopes during fanout."""

    @pytest.fixture
    def captured_envelopes(self):
        """Capture envelopes delivered by the multiplexer."""
        return []

    @pytest.fixture
    def multiplexer(self, captured_envelopes):
        """Create a StreamMultiplexer with a capturing on_data callback."""
        from gateway.core.stream import StreamMultiplexer

        async def capture(client_id, data_type, envelope):
            captured_envelopes.append((client_id, data_type, dict(envelope)))

        mux = StreamMultiplexer(
            api_key="test",  # pragma: allowlist secret
            api_secret="test",  # pragma: allowlist secret
            on_data=capture,
            lazy_connect=True,
        )
        return mux

    @pytest.mark.asyncio
    async def test_seq_injected_into_envelope(self, multiplexer, captured_envelopes):
        """Messages fanned out to clients should contain seq in lineage."""
        # Subscribe a client
        conn = list(multiplexer._connections.values())[0]
        conn.subscriptions.subscribe("client1", bars=["AAPL"])

        # Simulate incoming bar message
        stream_type = list(multiplexer._connections.keys())[0]
        msg = {"T": "b", "S": "AAPL", "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1000}

        with patch("gateway.core.stream.get_validator") as mock_validator:
            mock_result = MagicMock()
            mock_result.valid = True
            mock_validator.return_value.validate_bar.return_value = mock_result

            await multiplexer._handle_message(stream_type, msg)

        assert len(captured_envelopes) == 1
        _, _, envelope = captured_envelopes[0]
        assert "seq" in envelope, "Top-level seq field missing from envelope"
        assert envelope["seq"] == 1
        assert envelope["lineage"]["seq"] == 1

    @pytest.mark.asyncio
    async def test_seq_increments_per_symbol(self, multiplexer, captured_envelopes):
        """Multiple messages for the same symbol increment seq."""
        conn = list(multiplexer._connections.values())[0]
        conn.subscriptions.subscribe("client1", bars=["AAPL"])

        stream_type = list(multiplexer._connections.keys())[0]
        msg = {"T": "b", "S": "AAPL", "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1000}

        with patch("gateway.core.stream.get_validator") as mock_validator:
            mock_result = MagicMock()
            mock_result.valid = True
            mock_validator.return_value.validate_bar.return_value = mock_result

            await multiplexer._handle_message(stream_type, msg)
            await multiplexer._handle_message(stream_type, msg)
            await multiplexer._handle_message(stream_type, msg)

        assert len(captured_envelopes) == 3
        seqs = [env["seq"] for _, _, env in captured_envelopes]
        assert seqs == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_seq_independent_across_symbols(self, multiplexer, captured_envelopes):
        """Different symbols get independent sequences."""
        conn = list(multiplexer._connections.values())[0]
        conn.subscriptions.subscribe("client1", bars=["AAPL", "TSLA"])

        stream_type = list(multiplexer._connections.keys())[0]
        aapl_msg = {"T": "b", "S": "AAPL", "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1000}
        tsla_msg = {"T": "b", "S": "TSLA", "o": 200.0, "h": 201.0, "l": 199.0, "c": 200.5, "v": 500}

        with patch("gateway.core.stream.get_validator") as mock_validator:
            mock_result = MagicMock()
            mock_result.valid = True
            mock_validator.return_value.validate_bar.return_value = mock_result

            await multiplexer._handle_message(stream_type, aapl_msg)
            await multiplexer._handle_message(stream_type, tsla_msg)
            await multiplexer._handle_message(stream_type, aapl_msg)

        # AAPL msgs should have seq 1, 2; TSLA should have seq 1
        aapl_seqs = [env["seq"] for _, _, env in captured_envelopes if env["symbol"] == "AAPL"]
        tsla_seqs = [env["seq"] for _, _, env in captured_envelopes if env["symbol"] == "TSLA"]
        assert aapl_seqs == [1, 2]
        assert tsla_seqs == [1]

    @pytest.mark.asyncio
    async def test_seq_independent_across_feeds(self, multiplexer, captured_envelopes):
        """Different feed types for same symbol get independent sequences."""
        conn = list(multiplexer._connections.values())[0]
        conn.subscriptions.subscribe("client1", bars=["AAPL"], quotes=["AAPL"])

        stream_type = list(multiplexer._connections.keys())[0]
        bar_msg = {"T": "b", "S": "AAPL", "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1000}
        quote_msg = {"T": "q", "S": "AAPL", "bp": 100.0, "ap": 100.1, "bs": 100, "as": 200}

        with patch("gateway.core.stream.get_validator") as mock_validator:
            mock_result = MagicMock()
            mock_result.valid = True
            mock_validator.return_value.validate_bar.return_value = mock_result
            mock_validator.return_value.validate_quote.return_value = mock_result

            await multiplexer._handle_message(stream_type, bar_msg)
            await multiplexer._handle_message(stream_type, quote_msg)
            await multiplexer._handle_message(stream_type, bar_msg)

        bar_seqs = [env["seq"] for _, _, env in captured_envelopes if env["feed"] == "bars"]
        quote_seqs = [env["seq"] for _, _, env in captured_envelopes if env["feed"] == "quotes"]
        assert bar_seqs == [1, 2]
        assert quote_seqs == [1]
