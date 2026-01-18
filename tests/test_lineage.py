"""Tests for Data Lineage module."""

import time
from datetime import UTC, datetime, timedelta

import pytest

from gateway.core.lineage import (
    DataLineage,
    LatencyMetrics,
    LineageTracker,
    RESTResponse,
    RESTResponseMeta,
    WebSocketEnvelope,
    create_rest_meta,
    generate_request_id,
    get_lineage_tracker,
)


class TestLatencyMetrics:
    """Test LatencyMetrics dataclass."""

    def test_to_dict(self):
        """to_dict should produce correct structure."""
        latency = LatencyMetrics(
            provider_to_gateway=5,
            gateway_processing=2,
            total=7,
        )

        data = latency.to_dict()

        assert data["provider_to_gateway"] == 5
        assert data["gateway_processing"] == 2
        assert data["total"] == 7


class TestDataLineage:
    """Test DataLineage dataclass."""

    def test_to_dict_minimal(self):
        """Minimal lineage should produce correct structure."""
        lineage = DataLineage(provider="alpaca")

        data = lineage.to_dict()

        assert data["provider"] == "alpaca"
        assert data["cached"] is False
        assert data["normalized"] is False
        assert data["validated"] is False
        assert "sequence" not in data

    def test_to_dict_full(self):
        """Full lineage should include all fields."""
        now = datetime.now(UTC)
        lineage = DataLineage(
            provider="alpaca",
            provider_timestamp=now,
            gateway_received_at=now,
            gateway_processed_at=now,
            gateway_sent_at=now,
            latency_ms=LatencyMetrics(total=5),
            sequence=123,
            cached=True,
            cache_key="bars:AAPL:1Min",
            normalized=True,
            validated=True,
        )

        data = lineage.to_dict()

        assert data["provider"] == "alpaca"
        assert "provider_timestamp" in data
        assert "gateway_received_at" in data
        assert data["sequence"] == 123
        assert data["cached"] is True
        assert data["cache_key"] == "bars:AAPL:1Min"


class TestLineageTracker:
    """Test LineageTracker functionality."""

    @pytest.fixture
    def tracker(self) -> LineageTracker:
        return LineageTracker()

    def test_start_tracking(self, tracker):
        """start_tracking should create lineage with timestamps."""
        provider_ts = datetime.now(UTC) - timedelta(seconds=1)

        lineage = tracker.start_tracking("alpaca", provider_ts)

        assert lineage.provider == "alpaca"
        assert lineage.provider_timestamp == provider_ts
        assert lineage.gateway_received_at is not None
        assert lineage.sequence is not None

    def test_sequence_increments(self, tracker):
        """Each tracking should increment sequence."""
        l1 = tracker.start_tracking("alpaca")
        l2 = tracker.start_tracking("alpaca")

        assert l2.sequence == l1.sequence + 1

    def test_mark_processed(self, tracker):
        """mark_processed should set timestamp."""
        lineage = tracker.start_tracking("alpaca")

        lineage = tracker.mark_processed(lineage)

        assert lineage.gateway_processed_at is not None

    def test_mark_normalized(self, tracker):
        """mark_normalized should set flag."""
        lineage = tracker.start_tracking("alpaca")

        lineage = tracker.mark_normalized(lineage)

        assert lineage.normalized is True

    def test_mark_validated(self, tracker):
        """mark_validated should set flag."""
        lineage = tracker.start_tracking("alpaca")

        lineage = tracker.mark_validated(lineage)

        assert lineage.validated is True

    def test_mark_cached(self, tracker):
        """mark_cached should set flag and key."""
        lineage = tracker.start_tracking("alpaca")

        lineage = tracker.mark_cached(lineage, "bars:AAPL")

        assert lineage.cached is True
        assert lineage.cache_key == "bars:AAPL"

    def test_mark_sent(self, tracker):
        """mark_sent should set timestamp."""
        lineage = tracker.start_tracking("alpaca")

        lineage = tracker.mark_sent(lineage)

        assert lineage.gateway_sent_at is not None

    def test_calculate_latency(self, tracker):
        """calculate_latency should compute metrics."""
        provider_ts = datetime.now(UTC) - timedelta(milliseconds=10)

        lineage = tracker.start_tracking("alpaca", provider_ts)
        time.sleep(0.01)  # 10ms
        lineage = tracker.mark_sent(lineage)
        lineage = tracker.calculate_latency(lineage)

        assert lineage.latency_ms is not None
        assert lineage.latency_ms.provider_to_gateway is not None
        assert lineage.latency_ms.gateway_processing is not None
        assert lineage.latency_ms.total is not None

    def test_singleton(self):
        """get_lineage_tracker should return singleton."""
        t1 = get_lineage_tracker()
        t2 = get_lineage_tracker()
        assert t1 is t2


class TestWebSocketEnvelope:
    """Test WebSocketEnvelope dataclass."""

    def test_to_dict(self):
        """to_dict should produce PRD-compliant structure."""
        lineage = DataLineage(provider="alpaca", normalized=True)
        envelope = WebSocketEnvelope(
            msg_type="data",
            feed="stock_bars",
            symbol="AAPL",
            data={"close": 185.50, "volume": 1000},
            meta=lineage,
        )

        data = envelope.to_dict()

        assert data["type"] == "data"
        assert data["feed"] == "stock_bars"
        assert data["symbol"] == "AAPL"
        assert data["data"]["close"] == 185.50
        assert data["meta"]["provider"] == "alpaca"


class TestRESTResponseMeta:
    """Test RESTResponseMeta dataclass."""

    def test_to_dict(self):
        """to_dict should produce correct structure."""
        meta = RESTResponseMeta(
            request_id="req-abc123",
            provider="yfinance",
            cached=True,
            cache_age_ms=15000,
            cache_ttl_remaining_ms=285000,
            record_count=100,
        )

        data = meta.to_dict()

        assert data["request_id"] == "req-abc123"
        assert data["provider"] == "yfinance"
        assert data["cached"] is True
        assert data["cache_age_ms"] == 15000
        assert data["record_count"] == 100


class TestRESTResponse:
    """Test RESTResponse dataclass."""

    def test_success_response(self):
        """Success response should have data."""
        meta = RESTResponseMeta(request_id="req-123", provider="alpaca")
        response = RESTResponse(
            success=True,
            data=[{"symbol": "AAPL"}],
            meta=meta,
        )

        data = response.to_dict()

        assert data["success"] is True
        assert "data" in data
        assert "meta" in data

    def test_error_response(self):
        """Error response should have error details."""
        response = RESTResponse(
            success=False,
            data=None,
            error={"code": "GW-E8001", "message": "Invalid symbol"},
        )

        data = response.to_dict()

        assert data["success"] is False
        assert "error" in data
        assert data["error"]["code"] == "GW-E8001"


class TestHelpers:
    """Test helper functions."""

    def test_generate_request_id(self):
        """generate_request_id should produce unique IDs."""
        id1 = generate_request_id()
        id2 = generate_request_id()

        assert id1.startswith("req-")
        assert id2.startswith("req-")
        assert id1 != id2

    def test_create_rest_meta(self):
        """create_rest_meta should create complete metadata."""
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 15, tzinfo=UTC)

        meta = create_rest_meta(
            provider="alpaca",
            record_count=100,
            cached=True,
            cache_age_ms=5000,
            data_range=(start, end),
        )

        assert meta.provider == "alpaca"
        assert meta.record_count == 100
        assert meta.cached is True
        assert meta.data_range.start == start
