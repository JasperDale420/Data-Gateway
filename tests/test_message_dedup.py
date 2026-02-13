"""Tests for Message Deduplicator."""

import datetime as dt

import pytest

from gateway.core.dedup import (
    MessageDeduplicator,
    get_message_deduplicator,
)


class TestMessageDeduplicator:
    """Test MessageDeduplicator functionality."""

    @pytest.fixture
    def dedup(self) -> MessageDeduplicator:
        return MessageDeduplicator(window_size=100)

    def test_first_message_not_duplicate(self, dedup):
        """First message for a symbol should not be duplicate."""
        message = {
            "timestamp": "2024-01-15T14:30:00Z",
            "data": {"close": 185.50, "volume": 1000},
        }

        assert dedup.is_duplicate("AAPL", message) is False

    def test_same_message_is_duplicate(self, dedup):
        """Same message sent twice should be duplicate."""
        message = {
            "timestamp": "2024-01-15T14:30:00Z",
            "data": {"close": 185.50, "volume": 1000},
        }

        assert dedup.is_duplicate("AAPL", message) is False
        assert dedup.is_duplicate("AAPL", message) is True

    def test_different_timestamp_not_duplicate(self, dedup):
        """Same data at different timestamp is not duplicate."""
        msg1 = {
            "timestamp": "2024-01-15T14:30:00Z",
            "data": {"close": 185.50},
        }
        msg2 = {
            "timestamp": "2024-01-15T14:31:00Z",
            "data": {"close": 185.50},
        }

        assert dedup.is_duplicate("AAPL", msg1) is False
        assert dedup.is_duplicate("AAPL", msg2) is False

    def test_different_data_not_duplicate(self, dedup):
        """Same timestamp with different data is not duplicate."""
        msg1 = {
            "timestamp": "2024-01-15T14:30:00Z",
            "data": {"close": 185.50},
        }
        msg2 = {
            "timestamp": "2024-01-15T14:30:00Z",
            "data": {"close": 185.75},
        }

        assert dedup.is_duplicate("AAPL", msg1) is False
        assert dedup.is_duplicate("AAPL", msg2) is False

    def test_different_symbol_not_duplicate(self, dedup):
        """Same message for different symbol is not duplicate."""
        message = {
            "timestamp": "2024-01-15T14:30:00Z",
            "data": {"close": 185.50},
        }

        assert dedup.is_duplicate("AAPL", message) is False
        assert dedup.is_duplicate("MSFT", message) is False

    def test_lru_window(self):
        """Old messages should be evicted from LRU window."""
        dedup = MessageDeduplicator(window_size=2)

        msg1 = {"timestamp": "t1", "data": {"v": 1}}
        msg2 = {"timestamp": "t2", "data": {"v": 2}}
        msg3 = {"timestamp": "t3", "data": {"v": 3}}

        # Fill window
        assert dedup.is_duplicate("AAPL", msg1) is False
        assert dedup.is_duplicate("AAPL", msg2) is False

        # This should evict msg1
        assert dedup.is_duplicate("AAPL", msg3) is False

        # msg1 should no longer be in cache
        assert dedup.is_duplicate("AAPL", msg1) is False  # Not duplicate anymore

    def test_clear_symbol(self, dedup):
        """clear_symbol should remove all messages for that symbol."""
        message = {"timestamp": "t1", "data": {"v": 1}}

        dedup.is_duplicate("AAPL", message)
        dedup.clear_symbol("AAPL")

        # Same message should not be duplicate after clear
        assert dedup.is_duplicate("AAPL", message) is False

    def test_clear_all(self, dedup):
        """clear_all should remove all messages."""
        msg = {"timestamp": "t1", "data": {"v": 1}}

        dedup.is_duplicate("AAPL", msg)
        dedup.is_duplicate("MSFT", msg)
        dedup.clear_all()

        assert dedup.is_duplicate("AAPL", msg) is False
        assert dedup.is_duplicate("MSFT", msg) is False

    def test_get_stats(self, dedup):
        """get_stats should return correct statistics."""
        msg = {"timestamp": "t1", "data": {"v": 1}}

        dedup.is_duplicate("AAPL", msg)
        dedup.is_duplicate("AAPL", msg)  # Duplicate
        dedup.is_duplicate("MSFT", msg)

        stats = dedup.get_stats()

        assert stats["total_messages"] == 3
        assert stats["duplicates_detected"] == 1
        assert stats["symbols_tracked"] == 2
        assert stats["duplicate_rate"] == pytest.approx(1 / 3)

    def test_handles_empty_data(self, dedup):
        """Empty data should be handled."""
        msg = {"timestamp": "t1", "data": {}}

        assert dedup.is_duplicate("AAPL", msg) is False
        assert dedup.is_duplicate("AAPL", msg) is True

    def test_handles_missing_keys(self, dedup):
        """Missing timestamp/data keys should be handled."""
        msg = {}

        assert dedup.is_duplicate("AAPL", msg) is False
        assert dedup.is_duplicate("AAPL", msg) is True

    def test_handles_non_json_data(self, dedup):
        """Non-JSON-serializable data should not crash."""
        msg = {"timestamp": "t1", "data": {"when": dt.datetime(2024, 1, 1, 12, 0, 0)}}

        assert dedup.is_duplicate("AAPL", msg) is False
        assert dedup.is_duplicate("AAPL", msg) is True

    def test_singleton(self):
        """get_message_deduplicator should return singleton."""
        d1 = get_message_deduplicator()
        d2 = get_message_deduplicator()
        assert d1 is d2
