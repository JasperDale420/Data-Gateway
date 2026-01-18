"""Data Lineage and Provenance Tracking.

Implements metadata tracking for data traceability as specified in
PRD Data Lineage & Provenance section (lines 527-583).
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import structlog

logger = structlog.get_logger()


# ─────────────────────────────────────────────────────────────────────────────
# Latency Metrics
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class LatencyMetrics:
    """Latency tracking for data processing stages."""

    provider_to_gateway: int | None = None  # ms
    gateway_processing: int | None = None  # ms
    total: int | None = None  # ms

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider_to_gateway": self.provider_to_gateway,
            "gateway_processing": self.gateway_processing,
            "total": self.total,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Data Lineage (WebSocket envelope metadata)
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class DataLineage:
    """Metadata envelope for tracking data provenance.

    Used for WebSocket messages to track data origin and processing times.
    """

    provider: str
    provider_timestamp: datetime | None = None
    gateway_received_at: datetime | None = None
    gateway_processed_at: datetime | None = None
    gateway_sent_at: datetime | None = None
    latency_ms: LatencyMetrics | None = None
    sequence: int | None = None
    cached: bool = False
    cache_key: str | None = None
    normalized: bool = False
    validated: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
        result: dict[str, Any] = {
            "provider": self.provider,
            "cached": self.cached,
            "normalized": self.normalized,
            "validated": self.validated,
        }

        if self.provider_timestamp:
            result["provider_timestamp"] = self.provider_timestamp.isoformat()
        if self.gateway_received_at:
            result["gateway_received_at"] = self.gateway_received_at.isoformat()
        if self.gateway_processed_at:
            result["gateway_processed_at"] = self.gateway_processed_at.isoformat()
        if self.gateway_sent_at:
            result["gateway_sent_at"] = self.gateway_sent_at.isoformat()
        if self.latency_ms:
            result["latency_ms"] = self.latency_ms.to_dict()
        if self.sequence is not None:
            result["sequence"] = self.sequence
        if self.cache_key:
            result["cache_key"] = self.cache_key

        return result


# ─────────────────────────────────────────────────────────────────────────────
# REST Response Metadata
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class DataRange:
    """Date range for data in response."""

    start: datetime
    end: datetime

    def to_dict(self) -> dict[str, str]:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
        }


@dataclass
class RESTResponseMeta:
    """Metadata for REST API responses.

    Provides context about the data source, caching status, and data range.
    """

    request_id: str
    provider: str
    cached: bool = False
    cache_age_ms: int | None = None
    cache_ttl_remaining_ms: int | None = None
    record_count: int = 0
    data_range: DataRange | None = None
    processing_time_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
        result: dict[str, Any] = {
            "request_id": self.request_id,
            "provider": self.provider,
            "cached": self.cached,
            "record_count": self.record_count,
        }

        if self.cache_age_ms is not None:
            result["cache_age_ms"] = self.cache_age_ms
        if self.cache_ttl_remaining_ms is not None:
            result["cache_ttl_remaining_ms"] = self.cache_ttl_remaining_ms
        if self.data_range:
            result["data_range"] = self.data_range.to_dict()
        if self.processing_time_ms is not None:
            result["processing_time_ms"] = self.processing_time_ms

        return result


# ─────────────────────────────────────────────────────────────────────────────
# Lineage Tracker
# ─────────────────────────────────────────────────────────────────────────────


class LineageTracker:
    """Tracks data lineage through the processing pipeline.

    Example:
        tracker = LineageTracker()
        lineage = tracker.start_tracking("alpaca", provider_ts)

        # Process data...
        lineage = tracker.mark_processed(lineage)
        lineage = tracker.mark_validated(lineage)

        # Send to client
        lineage = tracker.mark_sent(lineage)
        lineage = tracker.calculate_latency(lineage)
    """

    def __init__(self) -> None:
        self._sequence_counter = 0

    def start_tracking(
        self,
        provider: str,
        provider_timestamp: datetime | None = None,
    ) -> DataLineage:
        """Start tracking lineage for incoming data.

        Args:
            provider: Name of the data provider
            provider_timestamp: Original timestamp from provider

        Returns:
            New DataLineage with received timestamp set
        """
        self._sequence_counter += 1

        return DataLineage(
            provider=provider,
            provider_timestamp=provider_timestamp,
            gateway_received_at=datetime.now(UTC),
            sequence=self._sequence_counter,
        )

    def mark_processed(self, lineage: DataLineage) -> DataLineage:
        """Mark data as processed."""
        lineage.gateway_processed_at = datetime.now(UTC)
        return lineage

    def mark_normalized(self, lineage: DataLineage) -> DataLineage:
        """Mark data as normalized."""
        lineage.normalized = True
        return lineage

    def mark_validated(self, lineage: DataLineage) -> DataLineage:
        """Mark data as validated."""
        lineage.validated = True
        return lineage

    def mark_cached(
        self,
        lineage: DataLineage,
        cache_key: str | None = None,
    ) -> DataLineage:
        """Mark data as coming from cache."""
        lineage.cached = True
        lineage.cache_key = cache_key
        return lineage

    def mark_sent(self, lineage: DataLineage) -> DataLineage:
        """Mark data as sent to client."""
        lineage.gateway_sent_at = datetime.now(UTC)
        return lineage

    def calculate_latency(self, lineage: DataLineage) -> DataLineage:
        """Calculate latency metrics from timestamps."""
        latency = LatencyMetrics()

        # Provider to gateway latency
        if lineage.provider_timestamp and lineage.gateway_received_at:
            delta = lineage.gateway_received_at - lineage.provider_timestamp
            latency.provider_to_gateway = int(delta.total_seconds() * 1000)

        # Gateway processing time
        if lineage.gateway_received_at and lineage.gateway_sent_at:
            delta = lineage.gateway_sent_at - lineage.gateway_received_at
            latency.gateway_processing = int(delta.total_seconds() * 1000)

        # Total latency
        if lineage.provider_timestamp and lineage.gateway_sent_at:
            delta = lineage.gateway_sent_at - lineage.provider_timestamp
            latency.total = int(delta.total_seconds() * 1000)

        lineage.latency_ms = latency
        return lineage


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket Message Envelope
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class WebSocketEnvelope:
    """Complete WebSocket message envelope with metadata.

    Structure per PRD:
    {
        "type": "data",
        "feed": "stock_bars",
        "symbol": "AAPL",
        "data": {...},
        "meta": {...}
    }
    """

    msg_type: str  # "data", "error", "status", etc.
    feed: str  # "stock_bars", "stock_quotes", etc.
    symbol: str
    data: dict[str, Any]
    meta: DataLineage | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
        result = {
            "type": self.msg_type,
            "feed": self.feed,
            "symbol": self.symbol,
            "data": self.data,
        }
        if self.meta:
            result["meta"] = self.meta.to_dict()
        return result


# ─────────────────────────────────────────────────────────────────────────────
# REST Response Wrapper
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class RESTResponse:
    """Complete REST response wrapper with metadata.

    Structure per PRD:
    {
        "success": true,
        "data": [...],
        "meta": {...}
    }
    """

    success: bool
    data: Any
    meta: RESTResponseMeta | None = None
    error: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
        result: dict[str, Any] = {
            "success": self.success,
        }

        if self.success:
            result["data"] = self.data
        else:
            result["error"] = self.error

        if self.meta:
            result["meta"] = self.meta.to_dict()

        return result


# ─────────────────────────────────────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────────────────────────────────────


def generate_request_id() -> str:
    """Generate a unique request ID."""
    return f"req-{uuid.uuid4().hex[:12]}"


def create_rest_meta(
    provider: str,
    record_count: int = 0,
    cached: bool = False,
    cache_age_ms: int | None = None,
    cache_ttl_remaining_ms: int | None = None,
    data_range: tuple[datetime, datetime] | None = None,
    processing_time_ms: int | None = None,
) -> RESTResponseMeta:
    """Helper to create REST response metadata."""
    dr = None
    if data_range:
        dr = DataRange(start=data_range[0], end=data_range[1])

    return RESTResponseMeta(
        request_id=generate_request_id(),
        provider=provider,
        cached=cached,
        cache_age_ms=cache_age_ms,
        cache_ttl_remaining_ms=cache_ttl_remaining_ms,
        record_count=record_count,
        data_range=dr,
        processing_time_ms=processing_time_ms,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

_tracker: LineageTracker | None = None


def get_lineage_tracker() -> LineageTracker:
    """Get singleton LineageTracker instance."""
    global _tracker
    if _tracker is None:
        _tracker = LineageTracker()
    return _tracker
