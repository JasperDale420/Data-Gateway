"""EventEnvelope — universal wrapper for all gateway events.

Provides:
- EventEnvelope: Pydantic model wrapping all events for downstream routing/storage
- make_instrument_key: Canonical key generator for consistent instrument identification
- compute_event_id: BLAKE2b idempotency hash for dedupe across reconnects/retries
- FEED_UNIQUE_FIELDS: Feed-specific unique field extractors for event ID computation

This module is a faithful port of Data-Gateway's canonical implementation
(gateway/core/envelope.py). compute_event_id, FEED_UNIQUE_FIELDS,
make_instrument_key, and infer_instrument_type must stay byte-identical in
behavior to the gateway copy — event_id is the dedup key for Heber's
three-layer dedup, so any divergence silently changes every hash.

NOTE: wrap_event() and fast_wrap_streaming_event() are NOT included here because they
depend on gateway-specific metrics. Those remain in Data-Gateway's gateway/core/envelope.py.
"""

import hashlib
import re
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

# Schema version for envelope format
SCHEMA_VERSION = "v1"
_OCC_OPTION_KEY_RE = re.compile(r"^option:OCC:[A-Z]{1,6}\d{6}[CP]\d{8}$")


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

    Decimals are stringified via ``str(field)`` (preserving precision:
    Decimal("1.50") -> "1.50"), never ``str(float(field))`` ("1.5") — the
    float form would change every quote event_id relative to the gateway.

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
        if isinstance(field, Decimal) or field is not None:
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
    # One row per expiry of the SAME underlying (per-underlying analytics, not
    # per-OCC-contract). The payload carries no date/timestamp, so ts_event falls
    # back to now() — with no unique field, two expiries in one EOD run collide
    # whenever their now() coincides, collapsing all but one at Bronze. expiry is
    # the natural per-row key (mirrors historic_option_volume).
    "iv_term_structure": [("symbol", None, ""), ("expiry", None, "")],
    # option_symbol distinguishes per-contract rows that share call_oi_change
    # (often 0) — without it most contracts collapsed to one event_id.
    "oi_change": [("option_symbol", None, ""), ("symbol", None, ""), ("date", None, ""), ("call_oi_change", None, 0)],
    "historic_option_volume": [
        ("symbol", None, ""),
        ("date", None, ""),
        ("expiry", None, ""),
    ],
    "short_interest": [("symbol", None, ""), ("date", None, ""), ("short_interest", None, 0)],
    "short_volume": [("symbol", None, ""), ("date", None, ""), ("short_interest", None, 0)],
    "ftds": [("symbol", None, ""), ("date", None, ""), ("quantity", None, 0)],
    # txn_type + amounts distinguish multiple same-day same-ticker trades by one
    # politician (e.g. a buy and a sell, or different size buckets). Without them
    # such distinct disclosures collapsed to a single event_id.
    "congress_trades": [
        ("ticker", None, ""),
        ("name", None, ""),
        ("transaction_date", None, ""),
        ("txn_type", None, ""),
        ("amounts", None, ""),
    ],
    # id is UW's stable per-record identifier. Without it, multiple Form-4 lines
    # from the same insider/ticker on the same day hashed identically — the root
    # cause of the insider_trades 200→~2 Bronze collapse.
    "insider_trades": [
        ("ticker", None, ""),
        ("owner_name", None, ""),
        ("transaction_date", None, ""),
        ("id", None, ""),
    ],
    "treasury_yields": [("date", None, ""), ("maturity", None, ""), ("yield_pct", None, 0)],
    # Company financial statements: one row per (ticker, fiscal period, report_type).
    # A single call returns ~102 quarterly rows spanning 2005→now, so the period
    # identity MUST be in the event_id or every quarter collapses to one hash (the
    # same failure mode fixed for oi_change/insider_trades). UW keys the period by
    # ``fiscal_date_ending`` (verified against the live endpoint 2026-07-03).
    "income_statement": [
        ("ticker", "symbol", ""),
        ("fiscal_date_ending", None, ""),
        ("report_type", None, ""),
    ],
    "balance_sheet": [
        ("ticker", "symbol", ""),
        ("fiscal_date_ending", None, ""),
        ("report_type", None, ""),
    ],
    "cash_flow": [
        ("ticker", "symbol", ""),
        ("fiscal_date_ending", None, ""),
        ("report_type", None, ""),
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

    # Options indicators
    if feed in {"flow", "flow_alerts"} or payload.get("strike") or payload.get("expiry"):
        return "option"

    # Crypto indicators
    if "/" in symbol or any(symbol.startswith(prefix) or symbol.endswith(prefix) for prefix in ["BTC", "ETH", "USDT"]):
        if len(symbol) >= 6:
            return "crypto"

    # Default to equity
    return "equity"


def _validate_instrument_key(instrument_type: str, instrument_key: str) -> None:
    """Reject key shapes known to be dropped by Heber's writer validator."""
    if not instrument_key or instrument_key.startswith("unknown:"):
        raise ValueError(f"invalid instrument_key: {instrument_key!r}")
    if instrument_type == "option" and not _OCC_OPTION_KEY_RE.match(instrument_key):
        raise ValueError(f"invalid instrument_key: {instrument_key!r}")
