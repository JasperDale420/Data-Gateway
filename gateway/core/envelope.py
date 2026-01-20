"""EventEnvelope - Universal wrapper for all outbound gateway events.

Provides:
- EventEnvelope: Pydantic model wrapping all events for downstream routing/storage
- make_instrument_key: Canonical key generator for consistent instrument identification
- compute_event_id: SHA256 idempotency hash for dedupe across reconnects/retries
- wrap_event: Factory function to create envelopes from normalized events
"""

import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger()

# Schema version for envelope format
SCHEMA_VERSION = "v1"


class EventEnvelope(BaseModel):
    """Universal event wrapper for downstream consumers.

    All outbound events are wrapped in this envelope to enable:
    - Routing without special-casing feeds or providers
    - Storage with consistent schema
    - Querying by instrument, feed, provider, or time range
    - Idempotency and deduplication across reconnects
    """

    # Idempotency
    event_id: str = Field(description="SHA256 idempotency hash (32 chars)")

    # Source identification
    provider: str = Field(description="Data provider: alpaca, unusual_whales, finnhub, etc")
    feed: str = Field(description="Feed type: bars, quotes, trades, flow, darkpool, news")
    source: str = Field(description="Delivery method: websocket, rest")

    # Instrument identification
    instrument_type: str = Field(description="Asset class: equity, option, crypto, forex")
    instrument_key: str = Field(description="Canonical key: equity:AAPL, crypto:BTC-USD")
    symbol: str = Field(description="Human-readable symbol")

    # Timestamps
    ts_event: datetime = Field(description="Event time from provider")
    ts_ingest: datetime = Field(description="Gateway receive/process time")

    # Metadata
    schema_version: str = Field(default=SCHEMA_VERSION, description="Envelope schema version")
    lineage: dict = Field(default_factory=dict, description="Sequence numbers, stream IDs")
    quality_flags: list[str] = Field(default_factory=list, description="validated, deduped, cached")

    # Payload
    payload: dict = Field(description="Normalized event data")


def make_instrument_key(
    symbol: str,
    instrument_type: str,
    contract_symbol: str | None = None,
) -> str:
    """Generate a canonical, consistent instrument key.

    Args:
        symbol: The ticker symbol (e.g., AAPL, BTC/USD, BTC-USD)
        instrument_type: Asset class (equity, option, crypto, forex)
        contract_symbol: OCC contract symbol for options (e.g., AAPL250117C00200000)

    Returns:
        Canonical key like "equity:AAPL", "crypto:BTC-USD", "option:OCC:AAPL250117C00200000"
    """
    symbol = symbol.upper().strip()
    instrument_type = instrument_type.lower()

    if instrument_type == "option":
        if contract_symbol:
            return f"option:OCC:{contract_symbol.upper()}"
        # Fallback: use symbol as-is
        return f"option:{symbol}"

    elif instrument_type == "crypto":
        # Normalize various crypto formats to BASE-QUOTE
        # Handle: BTC/USD, BTCUSD, BTC-USD, BTC_USD
        normalized = symbol.replace("/", "-").replace("_", "-")
        if len(normalized) >= 6 and "-" not in normalized:
            # Try to split BTCUSD -> BTC-USD (assume 3-char base)
            base = normalized[:3]
            quote = normalized[3:]
            normalized = f"{base}-{quote}"
        return f"crypto:{normalized}"

    elif instrument_type == "forex":
        # Normalize forex pairs: EURUSD -> EUR-USD
        normalized = symbol.replace("/", "-").replace("_", "-")
        if len(normalized) == 6 and "-" not in normalized:
            base = normalized[:3]
            quote = normalized[3:]
            normalized = f"{base}-{quote}"
        return f"forex:{normalized}"

    else:
        # Default to equity
        return f"equity:{symbol}"


def compute_event_id(
    provider: str,
    feed: str,
    instrument_key: str,
    ts_event: datetime,
    unique_fields: list[Any],
) -> str:
    """Compute SHA256 idempotency hash for event deduplication.

    Args:
        provider: Data provider name
        feed: Feed type (bars, quotes, trades, etc)
        instrument_key: Canonical instrument key
        ts_event: Event timestamp
        unique_fields: Feed-specific unique values for disambiguation
            - trades: [trade_id]
            - bars: [timeframe, open_timestamp]
            - quotes: [bid_price, ask_price, bid_size, ask_size]
            - flow: [expiry, strike, put_call, premium, volume]

    Returns:
        32-character hex hash
    """
    parts = [
        provider,
        feed,
        instrument_key,
        ts_event.isoformat() if ts_event else "",
    ]

    # Add unique fields, converting to strings
    for field in unique_fields:
        if isinstance(field, Decimal):
            parts.append(str(float(field)))
        elif field is not None:
            parts.append(str(field))

    data = "|".join(parts)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()[:32]


def _extract_unique_fields(feed: str, payload: dict) -> list[Any]:
    """Extract feed-specific unique fields for event ID computation."""

    if feed == "trades":
        return [payload.get("trade_id", payload.get("i", ""))]

    elif feed == "bars":
        return [
            payload.get("timeframe", payload.get("x", "1Min")),
            payload.get("timestamp", payload.get("t", "")),
        ]

    elif feed == "quotes":
        return [
            payload.get("bid_price", payload.get("bp", 0)),
            payload.get("ask_price", payload.get("ap", 0)),
            payload.get("bid_size", payload.get("bs", 0)),
            payload.get("ask_size", payload.get("as", 0)),
        ]

    elif feed in ("flow", "darkpool"):
        return [
            payload.get("expiry", ""),
            payload.get("strike", 0),
            payload.get("put_call", ""),
            payload.get("premium", 0),
            payload.get("volume", 0),
        ]

    elif feed == "news":
        return [payload.get("article_id", payload.get("id", ""))]

    else:
        # Generic: use timestamp only
        return []


def _infer_instrument_type(feed: str, symbol: str, payload: dict) -> str:
    """Infer instrument type from feed and payload."""

    # Options indicators
    if feed == "flow" or payload.get("strike") or payload.get("expiry"):
        return "option"

    # Crypto indicators
    if "/" in symbol or any(crypto in symbol for crypto in ["BTC", "ETH", "USD", "USDT"]):
        if len(symbol) >= 6:
            return "crypto"

    # Default to equity
    return "equity"


def wrap_event(
    event: dict | BaseModel,
    provider: str,
    feed: str,
    source: str = "websocket",
    stream_type: str | None = None,
    ts_ingest: datetime | None = None,
) -> dict:
    """Wrap a normalized event in an EventEnvelope.

    Args:
        event: Normalized event (dict or Pydantic model)
        provider: Data provider name
        feed: Feed type (bars, quotes, trades, etc)
        source: Delivery method (websocket, rest)
        stream_type: Optional stream identifier for lineage
        ts_ingest: Gateway receive time (defaults to now)

    Returns:
        EventEnvelope as dict for JSON serialization
    """
    # Convert Pydantic model to dict if needed
    if isinstance(event, BaseModel):
        payload = event.model_dump(mode="json")
    else:
        payload = dict(event)

    # Extract symbol - handle various field names
    symbol = payload.get("symbol") or payload.get("S") or payload.get("underlying") or ""

    # Extract event timestamp
    ts_event_raw = (
        payload.get("timestamp")
        or payload.get("t")
        or payload.get("published_at")
        or payload.get("datetime")
    )

    if isinstance(ts_event_raw, datetime):
        ts_event = ts_event_raw
    elif isinstance(ts_event_raw, str):
        try:
            ts_event = datetime.fromisoformat(ts_event_raw.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            ts_event = datetime.now(UTC)
    else:
        ts_event = datetime.now(UTC)

    # Ingest time
    if ts_ingest is None:
        ts_ingest = datetime.now(UTC)

    # Infer instrument type
    instrument_type = _infer_instrument_type(feed, symbol, payload)

    # Generate instrument key
    contract_symbol = payload.get("contract_symbol") or payload.get("contract")
    instrument_key = make_instrument_key(symbol, instrument_type, contract_symbol)

    # Extract unique fields for event ID
    unique_fields = _extract_unique_fields(feed, payload)

    # Compute idempotency hash
    event_id = compute_event_id(
        provider=provider,
        feed=feed,
        instrument_key=instrument_key,
        ts_event=ts_event,
        unique_fields=unique_fields,
    )

    # Build lineage
    lineage = {}
    if stream_type:
        lineage["stream_type"] = str(stream_type)
    if "sequence" in payload or "x" in payload:
        lineage["sequence"] = payload.get("sequence", payload.get("x"))

    # Quality flags
    quality_flags = ["validated"]
    if source == "rest":
        quality_flags.append("cached")

    # Create envelope
    try:
        envelope = EventEnvelope(
            event_id=event_id,
            provider=provider,
            feed=feed,
            source=source,
            instrument_type=instrument_type,
            instrument_key=instrument_key,
            symbol=symbol,
            ts_event=ts_event,
            ts_ingest=ts_ingest,
            lineage=lineage,
            quality_flags=quality_flags,
            payload=payload,
        )

        logger.debug(
            "event_envelope_created",
            event_id=event_id,
            provider=provider,
            feed=feed,
            instrument_key=instrument_key,
            ts_event=ts_event.isoformat(),
        )

        # Optimize serialization: avoid deep traversal of payload
        # payload is already a dict/list and doesn't need Pydantic validation/conversion
        dump = envelope.model_dump(mode="json", exclude={"payload"})
        dump["payload"] = payload
        return dump

    except Exception as e:
        logger.error(
            "event_envelope_failed",
            provider=provider,
            feed=feed,
            symbol=symbol,
            error=str(e),
            exc_info=True,
        )
        # Return minimal fallback envelope
        return {
            "event_id": event_id,
            "provider": provider,
            "feed": feed,
            "source": source,
            "instrument_type": "unknown",
            "instrument_key": f"unknown:{symbol}",
            "symbol": symbol,
            "ts_event": ts_event.isoformat() if ts_event else None,
            "ts_ingest": ts_ingest.isoformat() if ts_ingest else None,
            "schema_version": SCHEMA_VERSION,
            "lineage": {},
            "quality_flags": ["error"],
            "payload": payload,
        }
