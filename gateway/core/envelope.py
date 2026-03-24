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

from gateway.core.metrics import record_envelope_created

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
        # Handle: BTC/USD, BTCUSD, BTC-USD, BTC_USD, DOGEUSD
        normalized = symbol.replace("/", "-").replace("_", "-")
        if len(normalized) >= 6 and "-" not in normalized:
            # Try to split by known quote currencies (3-char first, then 4-char)
            known_quotes_3 = ("USD", "EUR", "BTC", "ETH", "GBP")
            known_quotes_4 = ("USDT", "USDC", "BUSD")
            matched = False
            for quote_len, known in ((3, known_quotes_3), (4, known_quotes_4)):
                if len(normalized) > quote_len:
                    candidate_quote = normalized[-quote_len:]
                    if candidate_quote in known:
                        base = normalized[:-quote_len]
                        normalized = f"{base}-{candidate_quote}"
                        matched = True
                        break
            if not matched:
                # Fallback: assume 3-char base
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
    """Compute BLAKE2b idempotency hash for event deduplication.

    Uses BLAKE2b with a 16-byte digest (32 hex chars) for faster hashing
    compared to SHA256, while maintaining sufficient collision resistance
    for bounded event streams.

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
    return hashlib.blake2b(data.encode("utf-8"), digest_size=16, usedforsecurity=False).hexdigest()


# Feed-specific unique field extractors for event ID computation
# Format: feed -> list of (key, fallback_key, default_value)
FEED_UNIQUE_FIELDS: dict[str, list[tuple[str, str | None, Any]]] = {
    "trades": [("trade_id", "i", "")],
    "bars": [("timeframe", "x", "1Min"), ("timestamp", "t", "")],
    "quotes": [
        ("bid_price", "bp", 0),
        ("ask_price", "ap", 0),
        ("bid_size", "bs", 0),
        ("ask_size", "as", 0),
    ],
    "flow": [
        ("expiry", None, ""),
        ("strike", None, 0),
        ("put_call", None, ""),
        ("premium", None, 0),
        ("volume", None, 0),
    ],
    "flow_alerts": [
        ("expiry", None, ""),
        ("strike", None, 0),
        ("put_call", None, ""),
        ("premium", None, 0),
        ("volume", None, 0),
    ],
    "darkpool": [
        ("tracking_id", None, ""),
        ("price", None, 0),
        ("size", None, 0),
        ("notional", None, 0),
    ],
    "news": [("article_id", "id", "")],
    "etf": [("etf_symbol", None, ""), ("holding_symbol", "symbol", ""), ("date", None, "")],
    "shorts": [("date", None, ""), ("short_interest", "quantity", 0)],
    "screener": [("screen_type", None, ""), ("rank", "position", 0)],
    "market_tide": [
        ("date", None, ""),
        ("sector", None, ""),
        ("net_call_premium", None, 0),
        ("net_put_premium", None, 0),
    ],
    "sector_tide": [
        ("date", None, ""),
        ("sector", None, ""),
        ("net_call_premium", None, 0),
        ("net_put_premium", None, 0),
    ],
    "insiders": [
        ("transaction_id", "id", ""),
        ("filing_date", None, ""),
        ("insider_name", None, ""),
    ],
    "institutions": [("transaction_id", "id", ""), ("date", "filing_date", "")],
    "politicians": [("transaction_id", "id", ""), ("date", "filing_date", "")],
    "analytics": [("expiry", None, ""), ("metric_type", None, "")],
    "forex": [("pair", None, ""), ("bid", None, 0), ("ask", None, 0)],
    "fundamentals": [("symbol", None, ""), ("market_cap", None, 0)],
    # EOD per-ticker UW feeds
    "greek_exposure": [("symbol", None, ""), ("call_gamma", None, 0)],
    "iv_rank": [("symbol", None, ""), ("iv_rank", None, 0)],
    "oi_change": [("symbol", None, ""), ("date", None, ""), ("call_oi_change", None, 0)],
    "historic_option_volume": [
        ("symbol", None, ""),
        ("date", None, ""),
        ("expiry", None, ""),
    ],
    "short_interest": [("symbol", None, ""), ("date", None, ""), ("short_interest", None, 0)],
    "short_volume": [("symbol", None, ""), ("date", None, ""), ("short_interest", None, 0)],
    "ftds": [("symbol", None, ""), ("date", None, ""), ("quantity", None, 0)],
    "congress_trades": [
        ("ticker", None, ""),
        ("name", None, ""),
        ("transaction_date", None, ""),
    ],
    "insider_trades": [
        ("ticker", None, ""),
        ("owner_name", None, ""),
        ("transaction_date", None, ""),
    ],
}


def _extract_unique_fields(feed: str, payload: dict) -> list[Any]:
    """Extract feed-specific unique fields for event ID computation."""
    field_spec = FEED_UNIQUE_FIELDS.get(feed)
    if not field_spec:
        return []

    result = []
    for primary_key, fallback_key, default in field_spec:
        if fallback_key:
            value = payload.get(primary_key, payload.get(fallback_key, default))
        else:
            value = payload.get(primary_key, default)
        result.append(value)
    return result


def _infer_instrument_type(feed: str, symbol: str, payload: dict) -> str:
    """Infer instrument type from feed and payload."""

    # Options indicators
    if feed in {"flow", "flow_alerts"} or payload.get("strike") or payload.get("expiry"):
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
    instrument_type_override: str | None = None,
    instrument_key_override: str | None = None,
    symbol_override: str | None = None,
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
    symbol = (
        symbol_override
        or payload.get("symbol")
        or payload.get("S")
        or payload.get("underlying")
        or payload.get("ticker")
        or ""
    )
    if not isinstance(symbol, str):
        symbol = str(symbol) if not isinstance(symbol, dict) else ""

    # Market-wide feeds may not include a symbol; set a stable placeholder
    if not symbol and feed in {"market_tide", "sector_tide", "etf_tide", "market_tide_by_etf"}:
        symbol = payload.get("sector") or payload.get("etf") or "MARKET"

    # Extract event timestamp
    ts_event_raw = (
        payload.get("timestamp") or payload.get("t") or payload.get("published_at") or payload.get("datetime")
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
    instrument_type = instrument_type_override or _infer_instrument_type(feed, symbol, payload)

    # Generate instrument key
    contract_symbol = payload.get("contract_symbol") or payload.get("contract")
    instrument_key = instrument_key_override or make_instrument_key(symbol, instrument_type, contract_symbol)

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
        seq_val = payload.get("sequence", payload.get("x"))
        if seq_val is not None:
            lineage["sequence"] = str(seq_val)

    # Quality flags
    quality_flags = ["validated"]
    if source == "rest":
        quality_flags.append("cached")

    # Create envelope
    try:
        # Fast-path dict assembly avoids per-message Pydantic serialization overhead.
        envelope = {
            "event_id": event_id,
            "provider": provider,
            "feed": feed,
            "source": source,
            "instrument_type": instrument_type,
            "instrument_key": instrument_key,
            "symbol": symbol,
            "ts_event": ts_event.isoformat() if ts_event else None,
            "ts_ingest": ts_ingest.isoformat() if ts_ingest else None,
            "schema_version": SCHEMA_VERSION,
            "lineage": lineage,
            "quality_flags": quality_flags,
            "payload": payload,
        }

        logger.debug(
            "event_envelope_created",
            event_id=event_id,
            provider=provider,
            feed=feed,
            instrument_key=instrument_key,
            ts_event=ts_event.isoformat(),
        )

        # Record metrics
        record_envelope_created(provider=provider, feed=feed)

        return envelope

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


def fast_wrap_streaming_event(
    event: dict[str, Any],
    provider: str,
    feed: str,
    instrument_type: str,
    ts_ingest: datetime,
    sequence: int | None = None,
) -> dict[str, Any]:
    """Optimized event wrapper for high-frequency streaming.

    Skips:
    - Pydantic model validation (assumes dict)
    - Instrument type inference (passed in)
    - Content hashing (uses fast random ID)
    - Extensive unique field extraction

    Args:
        event: Raw event dict
        provider: Provider name (e.g. "alpaca")
        feed: Feed type (e.g. "trades")
        instrument_type: Known instrument type
        ts_ingest: Pre-computed ingest timestamp
        sequence: Optional sequence number

    Returns:
        Serialized envelope dict
    """
    # symbol - fast path
    symbol = event.get("S") or event.get("symbol") or ""
    if not isinstance(symbol, str):
        symbol = str(symbol)

    # timestamp - assume ISO string or let json.dumps handle format if it's already a string?
    # Actually, we need to standardize to ISO string for the envelope.
    # Alpaca sends 't' as RFC3339 string (usually).
    ts_event_str = event.get("t") or event.get("timestamp")
    if not ts_event_str:
        ts_event_str = ts_ingest.isoformat()
    elif not isinstance(ts_event_str, str):
        # Handle non-string timestamps: datetime, msgpack.Timestamp, epoch int
        try:
            ts_event_str = ts_event_str.isoformat()
        except AttributeError:
            # msgpack.Timestamp has to_datetime(), not isoformat()
            if hasattr(ts_event_str, "to_datetime"):
                ts_event_str = ts_event_str.to_datetime().isoformat()
            elif isinstance(ts_event_str, int | float):
                from datetime import datetime as _dt

                ts_event_str = _dt.fromtimestamp(ts_event_str, tz=UTC).isoformat()
            else:
                ts_event_str = ts_ingest.isoformat()

    # instrument_key - manual inline construction for speed
    # (Replicates make_instrument_key logic for common cases)
    if instrument_type == "option":
        # Fast path for options (symbol is key if no contract specific)
        # But we assume standard "option:SYMBOL" or "option:OCC:..." logic?
        # make_instrument_key handles crypto normalization too.
        # For speed, let's call make_instrument_key but optimize it later if needed.
        # It's dominated by hashing anyway.
        inst_key = make_instrument_key(symbol, instrument_type)
    else:
        # Equity is just equity:SYMBOL
        inst_key = f"equity:{symbol}" if instrument_type == "equity" else make_instrument_key(symbol, instrument_type)

    # event_id - fast random 32-char hex (skip BLAKE2b/SHA256)
    # Using urandom or simple string construction is faster than hashing content.
    # UUID4 is ~1-2us. Hashing 100 bytes is ~5-10us.
    # Let's use os.urandom(16).hex()
    import os

    event_id = os.urandom(16).hex()

    lineage = {}
    if sequence is not None:
        lineage["seq"] = sequence

    envelope = {
        "event_id": event_id,
        "provider": provider,
        "feed": feed,
        "source": "websocket",
        "instrument_type": instrument_type,
        "instrument_key": inst_key,
        "symbol": symbol,
        "ts_event": ts_event_str,
        "ts_ingest": ts_ingest.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "lineage": lineage,
        "quality_flags": ["streaming"],
        "payload": event,
    }
    if sequence is not None:
        envelope["seq"] = sequence
    return envelope
