"""EventEnvelope — universal wrapper for all gateway events.

Provides:
- EventEnvelope: Pydantic model wrapping all events for downstream routing/storage
- make_instrument_key: Canonical key generator for consistent instrument identification
- compute_event_id: BLAKE2b idempotency hash for dedupe across reconnects/retries
- FEED_UNIQUE_FIELDS: Feed-specific unique field extractors for event ID computation

NOTE: wrap_event() and fast_wrap_streaming_event() are NOT included here because they
depend on gateway-specific metrics. Those remain in Data-Gateway's gateway/core/envelope.py.
"""

import hashlib
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

SCHEMA_VERSION = "v1"


class EventEnvelope(BaseModel):
    """Universal event wrapper for downstream consumers.

    All outbound events are wrapped in this envelope to enable:
    - Routing without special-casing feeds or providers
    - Storage with consistent schema
    - Querying by instrument, feed, provider, or time range
    - Idempotency and deduplication across reconnects
    """

    event_id: str = Field(description="BLAKE2b idempotency hash (32 hex chars)")
    provider: str = Field(description="Data provider: alpaca, unusual_whales, finnhub, etc")
    feed: str = Field(description="Feed type: bars, quotes, trades, flow, darkpool, news")
    source: str = Field(description="Delivery method: websocket, rest")
    instrument_type: str = Field(description="Asset class: equity, option, crypto, forex")
    instrument_key: str = Field(description="Canonical key: equity:AAPL, crypto:BTC-USD")
    symbol: str = Field(description="Human-readable symbol")
    ts_event: datetime = Field(description="Event time from provider")
    ts_ingest: datetime = Field(description="Gateway receive/process time")
    schema_version: str = Field(default=SCHEMA_VERSION, description="Envelope schema version")
    lineage: dict = Field(default_factory=dict, description="Sequence numbers, stream IDs")
    quality_flags: list[str] = Field(default_factory=list, description="validated, deduped, cached")
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
        contract_symbol: OCC contract symbol for options

    Returns:
        Canonical key like "equity:AAPL", "crypto:BTC-USD", "option:OCC:AAPL250117C00200000"
    """
    symbol = symbol.upper().strip()
    instrument_type = instrument_type.lower()

    if instrument_type == "option":
        if contract_symbol:
            return f"option:OCC:{contract_symbol.upper()}"
        return f"option:{symbol}"

    elif instrument_type == "crypto":
        normalized = symbol.replace("/", "-").replace("_", "-")
        if len(normalized) >= 6 and "-" not in normalized:
            base = normalized[:3]
            quote = normalized[3:]
            normalized = f"{base}-{quote}"
        return f"crypto:{normalized}"

    elif instrument_type == "forex":
        normalized = symbol.replace("/", "-").replace("_", "-")
        if len(normalized) == 6 and "-" not in normalized:
            base = normalized[:3]
            quote = normalized[3:]
            normalized = f"{base}-{quote}"
        return f"forex:{normalized}"

    else:
        return f"equity:{symbol}"


def compute_event_id(
    provider: str,
    feed: str,
    instrument_key: str,
    ts_event: datetime,
    unique_fields: list[Any],
) -> str:
    """Compute BLAKE2b idempotency hash for event deduplication.

    Uses BLAKE2b with a 16-byte digest (32 hex chars) for faster hashing.

    Args:
        provider: Data provider name
        feed: Feed type (bars, quotes, trades, etc)
        instrument_key: Canonical instrument key
        ts_event: Event timestamp
        unique_fields: Feed-specific unique values for disambiguation

    Returns:
        32-character hex hash
    """
    parts = [
        provider,
        feed,
        instrument_key,
        ts_event.isoformat() if ts_event else "",
    ]

    for field in unique_fields:
        if isinstance(field, Decimal):
            parts.append(str(float(field)))
        elif field is not None:
            parts.append(str(field))

    data = "|".join(parts)
    return hashlib.blake2b(data.encode("utf-8"), digest_size=16, usedforsecurity=False).hexdigest()


# Feed-specific unique field extractors for event ID computation
# Format: feed -> list of (primary_key, fallback_key, default_value)
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


def extract_unique_fields(feed: str, payload: dict) -> list[Any]:
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


def infer_instrument_type(feed: str, symbol: str, payload: dict) -> str:
    """Infer instrument type from feed and payload."""
    if feed in {"flow", "flow_alerts"} or payload.get("strike") or payload.get("expiry"):
        return "option"

    if "/" in symbol or any(crypto in symbol for crypto in ["BTC", "ETH", "USD", "USDT"]):
        if len(symbol) >= 6:
            return "crypto"

    return "equity"
