"""EventEnvelope middleware - wraps all REST API responses in universal envelope."""

import asyncio
import inspect
import json
import re
from collections.abc import Awaitable, Callable
from typing import Any, cast

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from gateway.core.envelope import wrap_event
from gateway.core.logger import logger

# Route patterns to extract provider/feed from API paths
ROUTE_PATTERNS = [
    # /api/v1/{provider}/{feed}/{symbol} or /api/v1/{provider}/{endpoint}
    re.compile(r"^/api/v1/(?P<provider>alpaca|finnhub|alphavantage|yf|uw|sec|news)/(?P<feed>\w+)"),
    # /api/v1/{provider} for health/status endpoints
    re.compile(r"^/api/v1/(?P<provider>\w+)$"),
]

# Map endpoint patterns to feed types
FEED_MAPPING = {
    # Core market data
    "quote": "quotes",
    "quotes": "quotes",
    "bars": "bars",
    "intraday": "bars",
    "daily": "bars",
    "weekly": "bars",
    "monthly": "bars",
    "trades": "trades",
    "trade": "trades",
    # Options & flow
    "flow": "flow_alerts",
    "darkpool": "darkpool",
    "options": "option_contract",
    "chain": "option_contract",
    "greeks": "greek_exposure",
    "gex": "greek_exposure",
    # Fundamentals & news
    "news": "news",
    "articles": "news",
    "profile": "stock_fundamentals",
    "overview": "stock_fundamentals",
    "earnings": "earnings",
    "financials": "stock_fundamentals",
    "filings": "filings",
    "snapshots": "snapshots",
    "snapshot": "snapshots",
    "facts": "filings",
    # UW market sentiment
    "tide": "market_tide",
    "market_tide": "market_tide",
    "sector_tide": "sector_tide",
    # UW alternative data
    "etf": "etf_metadata",
    "holdings": "etf_holding",
    "flows": "etf_flow",
    "shorts": "short_data",
    "short_interest": "short_data",
    "ftd": "ftd",
    "screener": "screener_result",
    # UW political/institutional
    "insiders": "insider_trades",
    "institutions": "institution_holdings",
    "politicians": "politician_trades",
    # Analytics
    "volatility": "volatility_stats",
    "iv_rank": "iv_rank",
    "seasonality": "seasonality",
    "max_pain": "max_pain",
    # Forex
    "forex": "forex",
    "rates": "forex",
    # Company fundamentals
    "company_overview": "stock_fundamentals",
    "income_statement": "stock_fundamentals",
    "balance_sheet": "stock_fundamentals",
    "cash_flow": "stock_fundamentals",
}

# Cerberus-critical routes that need canonical sink feed names.
CANONICAL_ROUTE_FEEDS = [
    (
        re.compile(r"^/api/v1/alpaca/stocks/bars$"),
        "alpaca",
        "bars",
    ),
    (
        re.compile(r"^/api/v1/alpaca/stocks/trades$"),
        "alpaca",
        "trades",
    ),
    (
        re.compile(r"^/api/v1/alpaca/stocks/[^/]+/bars$"),
        "alpaca",
        "bars",
    ),
    (
        re.compile(r"^/api/v1/alpaca/stocks/[^/]+/trades$"),
        "alpaca",
        "trades",
    ),
    (
        re.compile(r"^/api/v1/uw/flow/[^/]+$"),
        "unusual_whales",
        "flow_alerts",
    ),
    (
        re.compile(r"^/api/v1/uw/gex/[^/]+$"),
        "unusual_whales",
        "greek_exposure",
    ),
    (
        re.compile(r"^/api/v1/uw/darkpool/[^/]+$"),
        "unusual_whales",
        "darkpool",
    ),
]

# Paths to skip envelope wrapping (health, metrics, admin, etc)
SKIP_PATHS = {
    "/health",
    "/ready",
    "/metrics",
    "/api/v1/health",
    "/api/v1/admin",
    "/docs",
    "/redoc",
    "/openapi.json",
}


class EventEnvelopeMiddleware:
    """Wraps all REST API responses in EventEnvelope for universal routing/storage.

    This middleware intercepts successful API responses and wraps the data payload
    in an EventEnvelope, providing:
    - Idempotent event_id for deduplication
    - Consistent instrument_key for routing
    - Schema versioning for evolution
    - Lineage tracking for debugging

    Skipped paths: health, metrics, admin endpoints
    """

    def __init__(self, app: ASGIApp, max_body_bytes: int = 524288) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes
        self._background_tasks: set[asyncio.Task] = set()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope["path"]

        # Skip non-API and system paths
        if not path.startswith("/api/v1/") or any(path.startswith(skip) for skip in SKIP_PATHS):
            await self.app(scope, receive, send)
            return

        # Skip non-GET requests
        if scope["method"] != "GET":
            await self.app(scope, receive, send)
            return

        # Buffer response to wrap in envelope
        initial_message: Message | None = None
        body_chunks: list[bytes] = []
        should_wrap = False

        async def buffering_send(message: Message) -> None:
            nonlocal initial_message, should_wrap

            if message["type"] == "http.response.start":
                initial_message = message
                status_code = message["status"]

                if status_code == 200:
                    # Check content-type and content-length for wrappability
                    raw_headers = dict(message.get("headers", []))
                    ct = raw_headers.get(b"content-type", b"").decode().lower()
                    # Streaming response types must NOT be buffered for envelope
                    # wrapping — buffering them defeats the streaming contract and
                    # causes per-request memory growth proportional to total payload.
                    # Mirrors CacheMiddleware exclusion for the same reason.
                    is_streaming = "text/event-stream" in ct or "application/x-ndjson" in ct
                    if "application/json" in ct and not is_streaming:
                        cl = raw_headers.get(b"content-length")
                        if cl:
                            try:
                                should_wrap = int(cl) <= self.max_body_bytes
                            except ValueError:
                                should_wrap = False

                if should_wrap:
                    return  # Buffer

                await send(message)
                return

            if message["type"] == "http.response.body":
                if should_wrap and initial_message is not None:
                    body_chunks.append(message.get("body", b""))
                    if not message.get("more_body", False):
                        # Body complete — wrap in envelope
                        body = b"".join(body_chunks)
                        await self._wrap_and_send(path, body, initial_message, send)
                    return

                await send(message)

        await self.app(scope, receive, buffering_send)

    async def _wrap_and_send(
        self,
        path: str,
        body: bytes,
        initial_message: Message,
        send: Send,
    ) -> None:
        """Process the buffered body, wrap in envelope, and send."""
        if len(body) > self.max_body_bytes:
            await send(initial_message)
            await send({"type": "http.response.body", "body": body})
            return

        try:
            data = json.loads(body)

            # Skip if already wrapped or is an error
            if "envelope" in data or not data.get("success", True):
                await send(initial_message)
                await send({"type": "http.response.body", "body": body})
                return

            provider, feed = self._extract_route_info(path)
            payload = data.get("data", data)

            # Handle different payload shapes
            if isinstance(payload, list) and len(payload) > 0:
                envelope = wrap_event(
                    event={"items": payload, "count": len(payload)},
                    provider=provider,
                    feed=feed,
                    source="rest",
                    instrument_type_override="aggregate",
                    instrument_key_override=f"aggregate:{provider}:{feed}",
                    symbol_override="BATCH",
                )
            elif isinstance(payload, dict) and self._is_symbol_keyed_dict(payload):
                items = [{"symbol": sym, **snap} for sym, snap in payload.items() if isinstance(snap, dict)]
                envelope = wrap_event(
                    event={"items": items, "count": len(items)},
                    provider=provider,
                    feed=feed,
                    source="rest",
                    instrument_type_override="aggregate",
                    instrument_key_override=f"aggregate:{provider}:{feed}",
                    symbol_override="BATCH",
                )
            elif isinstance(payload, dict):
                envelope = wrap_event(
                    event=payload,
                    provider=provider,
                    feed=feed,
                    source="rest",
                )
            else:
                await send(initial_message)
                await send({"type": "http.response.body", "body": body})
                return

            wrapped = {
                "success": True,
                "envelope": envelope,
                "data": payload,
                "meta": data.get("meta", {}),
            }
            wrapped_body = json.dumps(wrapped, default=str, separators=(",", ":")).encode()

            # Publish to data sink (non-blocking)
            from gateway.api.deps import get_sink_registry

            sink_registry = get_sink_registry()
            sink_skip_reason = self._sink_publish_skip_reason(
                path=path,
                payload=payload,
                feed=feed,
            )
            should_publish_to_sink = sink_skip_reason is None
            if sink_registry and should_publish_to_sink:
                should_publish_to_sink = self._can_accept_low_priority_sink(
                    sink_registry=sink_registry,
                    path=path,
                    feed=feed,
                )
                if not should_publish_to_sink:
                    sink_skip_reason = "queue_pressure"

            if sink_registry and should_publish_to_sink:
                topic = "heber:events"
                sink_envelopes = self._build_sink_envelopes(
                    path=path,
                    provider=provider,
                    feed=feed,
                    payload=payload,
                )
                if sink_envelopes:
                    durable_for_topic = getattr(sink_registry, "has_durable_admission_for", None)
                    durable_admission = (
                        bool(durable_for_topic(topic))
                        if callable(durable_for_topic)
                        else bool(getattr(sink_registry, "has_durable_admission", False))
                    )
                    publish_flow_with_watch = getattr(sink_registry, "publish_flow_with_watch_batch_indexed", None)
                    if durable_admission and feed == "flow_alerts":
                        if not callable(publish_flow_with_watch):
                            raise RuntimeError("durable REST flow sink lacks atomic writer/watch admission")
                        atomic_publish = cast(
                            Callable[[list[tuple[str, dict[str, Any]]]], Awaitable[set[int]]],
                            publish_flow_with_watch,
                        )
                        accepted = await atomic_publish([(topic, se) for se in sink_envelopes])
                        if accepted != set(range(len(sink_envelopes))):
                            raise RuntimeError(
                                "durable REST flow atomic writer/watch admission failed "
                                f"for {len(sink_envelopes) - len(accepted)} envelope(s)"
                            )
                    else:
                        tasks = [
                            self._publish_sink_envelope(
                                sink_registry=sink_registry,
                                topic=topic,
                                envelope=se,
                                feed=feed,
                            )
                            for se in sink_envelopes
                        ]
                        if durable_admission:
                            await self._publish_sink_batch(tasks, path=path)
                        else:
                            task = asyncio.create_task(self._publish_sink_batch(tasks, path=path))
                            self._background_tasks.add(task)
                            task.add_done_callback(self._background_tasks.discard)
                else:
                    logger.debug("rest_envelope_sink_publish_empty", path=path, feed=feed)
            elif sink_registry and sink_skip_reason:
                logger.debug(
                    "rest_envelope_sink_publish_skipped",
                    path=path,
                    feed=feed,
                    reason=sink_skip_reason,
                )

            logger.debug(
                "rest_envelope_wrapped",
                path=path,
                provider=provider,
                feed=feed,
                event_id=envelope.get("event_id"),
            )

            # Build new headers: strip old content-length, add envelope headers
            new_headers: list[tuple[bytes, bytes]] = [
                (k, v) for k, v in initial_message.get("headers", []) if k.lower() != b"content-length"
            ]
            new_headers.extend(
                [
                    (b"content-length", str(len(wrapped_body)).encode()),
                    (b"x-gateway-envelope", b"true"),
                    (b"x-gateway-event-id", envelope.get("event_id", "").encode()),
                ]
            )

            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": new_headers,
                }
            )
            await send({"type": "http.response.body", "body": wrapped_body})

        except Exception as e:
            logger.warning("envelope_middleware_error", path=path, error=str(e))
            # Strict mode: re-raise so FastAPI's exception handler returns 500
            # instead of silently shipping the original unwrapped body. Consumers
            # depend on the x-gateway-envelope header contract; serving a body
            # without it is harder to diagnose than a clean 500.
            strict_envelopes = False
            try:
                from gateway.config import get_settings

                strict_envelopes = get_settings().strict_envelopes
            except Exception as settings_exc:
                # Settings unavailable — fall through to lenient behavior.
                logger.warning("envelope_middleware_settings_unavailable", path=path, error=str(settings_exc))
            if strict_envelopes:
                raise
            # Lenient (default): return original response with explicit
            # x-gateway-envelope: false so consumers can detect the unwrapped path.
            fallback_headers: list[tuple[bytes, bytes]] = list(initial_message.get("headers", []))
            fallback_headers.append((b"x-gateway-envelope", b"false"))
            await send(
                {"type": "http.response.start", "status": initial_message["status"], "headers": fallback_headers}
            )
            await send({"type": "http.response.body", "body": body})

    async def _publish_sink_batch(self, tasks: list, path: str) -> None:
        """Publish sink writes in batch without affecting request response path."""
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failures = [result for result in results if isinstance(result, Exception)]
        if failures:
            logger.warning(
                "rest_envelope_sink_publish_batch_failed",
                path=path,
                failed=len(failures),
            )

    async def _publish_sink_envelope(
        self,
        *,
        sink_registry: Any,
        topic: str,
        envelope: dict,
        feed: str,
    ) -> None:
        """Publish a REST sink envelope while supporting legacy test doubles."""
        publish_all = sink_registry.publish_all
        if self._publish_all_accepts_sink_labels(publish_all):
            await publish_all(topic, envelope, source="rest", feed=feed)
            return
        await publish_all(topic, envelope)

    @staticmethod
    def _publish_all_accepts_sink_labels(publish_all: Any) -> bool:
        """Return whether ``publish_all`` accepts Task 3 source/feed labels."""
        try:
            parameters = inspect.signature(publish_all).parameters.values()
        except (TypeError, ValueError):
            return True
        names = set()
        for parameter in parameters:
            if parameter.kind is inspect.Parameter.VAR_KEYWORD:
                return True
            names.add(parameter.name)
        return {"source", "feed"}.issubset(names)

    def _extract_route_info(self, path: str) -> tuple[str, str]:
        """Extract provider and feed from request path."""
        for pattern, provider, feed in CANONICAL_ROUTE_FEEDS:
            if pattern.match(path):
                return provider, feed

        for pattern in ROUTE_PATTERNS:
            match = pattern.match(path)
            if match:
                groups = match.groupdict()
                provider = groups.get("provider", "unknown")

                # Normalize provider names
                provider_map = {
                    "yf": "yfinance",
                    "uw": "unusual_whales",
                }
                provider = provider_map.get(provider, provider)

                # Extract feed from matched group or path segment
                feed_raw = groups.get("feed", "")
                feed = FEED_MAPPING.get(feed_raw, feed_raw or "data")

                return provider, feed

        return "unknown", "data"

    @staticmethod
    def _is_symbol_keyed_dict(payload: dict) -> bool:
        """Detect if payload is a symbol-keyed dict (e.g., snapshots keyed by ticker).

        Returns True when every value is a dict and none of the keys are
        standard event metadata fields.  This distinguishes payloads like
        ``{"AAPL": {...}, "MSFT": {...}}`` from regular event dicts like
        ``{"symbol": "AAPL", "bars": [...]}``.
        """
        if not payload:
            return False
        # Standard event/envelope metadata keys that indicate a normal dict payload
        _metadata_keys = {
            "symbol",
            "timestamp",
            "bars",
            "trades",
            "quotes",
            "quote",
            "timeframe",
            "success",
            "data",
            "meta",
            "error",
            "items",
            "count",
            "latest_bar",
            "trade",
            "bid",
            "ask",
            "open",
            "close",
            "high",
            "low",
            "volume",
        }
        if _metadata_keys & payload.keys():
            return False
        return all(isinstance(v, dict) for v in payload.values())

    def _is_sink_publish_eligible(self, path: str, payload: object, feed: str) -> bool:
        """Allow sink publishing only for routes with known ingest-safe payload shapes."""
        return self._sink_publish_skip_reason(path=path, payload=payload, feed=feed) is None

    def _sink_publish_skip_reason(self, path: str, payload: object, feed: str) -> str | None:
        """Return the reason a REST payload should not be published to the live sink."""
        try:
            from gateway.config import get_settings

            if feed in get_settings().rest_sink_excluded_feed_set:
                return "excluded_feed"
        except Exception as exc:
            logger.warning("rest_envelope_sink_settings_unavailable", path=path, feed=feed, error=str(exc))

        # Skip known aggregate-only bundles in phase 1 to avoid malformed ingest events.
        if path.startswith("/api/v1/alpaca/screener/"):
            return "ineligible_payload"

        if path.startswith("/api/v1/alpaca/stocks/") and path.endswith("/bars"):
            bars_items = payload.get("bars") if isinstance(payload, dict) else None
            return None if isinstance(bars_items, list) and len(bars_items) > 0 else "ineligible_payload"

        if path.startswith("/api/v1/alpaca/stocks/") and path.endswith("/trades"):
            trades_items = payload.get("trades") if isinstance(payload, dict) else None
            return None if isinstance(trades_items, list) and len(trades_items) > 0 else "ineligible_payload"

        if path.startswith("/api/v1/uw/flow/") and feed == "flow_alerts":
            return None if isinstance(payload, list) and len(payload) > 0 else "ineligible_payload"

        if path.startswith("/api/v1/uw/gex/") and feed == "greek_exposure":
            return None if isinstance(payload, list) and len(payload) > 0 else "ineligible_payload"

        if path.startswith("/api/v1/uw/darkpool/") and feed == "darkpool":
            return None if isinstance(payload, list) and len(payload) > 0 else "ineligible_payload"

        return "ineligible_payload"

    def _can_accept_low_priority_sink(self, *, sink_registry: object, path: str, feed: str) -> bool:
        """Check optional sink-registry pressure hook without affecting response wrapping."""
        can_accept = getattr(sink_registry, "can_accept_low_priority", None)
        if not callable(can_accept):
            return True

        try:
            from gateway.config import get_settings

            settings = get_settings()
            accepted = bool(
                can_accept(
                    "redis_streams",
                    max_utilization=settings.rest_sink_low_priority_max_queue_utilization,
                )
            )
            if not accepted:
                record_shed = getattr(sink_registry, "record_low_priority_shed", None)
                if callable(record_shed):
                    try:
                        record_shed(feed=feed, source="rest")
                    except Exception as shed_exc:
                        logger.warning(
                            "rest_envelope_sink_shed_record_failed",
                            path=path,
                            feed=feed,
                            error=str(shed_exc),
                        )
            return accepted
        except Exception as exc:
            logger.warning(
                "rest_envelope_sink_pressure_check_failed",
                path=path,
                feed=feed,
                error=str(exc),
            )
            return True

    def _build_sink_envelopes(
        self,
        *,
        path: str,
        provider: str,
        feed: str,
        payload: object,
    ) -> list[dict]:
        """Build one or more sink envelopes from route payload shape."""
        if isinstance(payload, list):
            return self._wrap_list_payload(provider=provider, feed=feed, items=payload, path=path)

        if isinstance(payload, dict):
            if path.startswith("/api/v1/alpaca/stocks/") and path.endswith("/bars"):
                return self._explode_nested_items(
                    provider=provider,
                    feed=feed,
                    payload=payload,
                    list_key="bars",
                    context_keys=("symbol", "timeframe"),
                    path=path,
                )
            if path.startswith("/api/v1/alpaca/stocks/") and path.endswith("/trades"):
                return self._explode_nested_items(
                    provider=provider,
                    feed=feed,
                    payload=payload,
                    list_key="trades",
                    context_keys=("symbol",),
                    path=path,
                )
            return [wrap_event(event=payload, provider=provider, feed=feed, source="rest")]

        return []

    @staticmethod
    def _uw_symbol_from_path(path: str) -> str | None:
        """Extract the ticker from a /api/v1/uw/{feed}/{SYMBOL}[/...] path."""
        parts = path.strip("/").split("/")
        # ["api", "v1", "uw", "<feed>", "<SYMBOL>", ...]
        if len(parts) >= 5 and parts[2] == "uw":
            return parts[4].upper()
        return None

    def _wrap_list_payload(
        self,
        *,
        provider: str,
        feed: str,
        items: list,
        path: str,
    ) -> list[dict]:
        # Greek-exposure (incl. /strike and /expiry sub-routes) is per-UNDERLYING
        # analytics whose rows carry strike/expiry. Without an override those rows
        # are misinferred as option contracts → malformed option:{SYMBOL} keys
        # that Heber rejects, silently dropping every by-strike/by-expiry row.
        # Force the equity key, mirroring the poller's iv_term_structure handling.
        overrides: dict = {}
        if feed == "greek_exposure":
            sym = self._uw_symbol_from_path(path)
            if sym:
                overrides = {
                    "instrument_type_override": "equity",
                    "instrument_key_override": f"equity:{sym}",
                    "symbol_override": sym,
                }
        envelopes: list[dict] = []
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                logger.warning(
                    "rest_envelope_explode_failed",
                    path=path,
                    error="non_dict_item",
                    item_index=idx,
                )
                continue
            try:
                envelopes.append(
                    wrap_event(
                        event=item,
                        provider=provider,
                        feed=feed,
                        source="rest",
                        **overrides,
                    )
                )
            except Exception as exc:
                logger.warning(
                    "rest_envelope_explode_failed",
                    path=path,
                    error=str(exc),
                    item_index=idx,
                )
        return envelopes

    def _explode_nested_items(
        self,
        *,
        provider: str,
        feed: str,
        payload: dict,
        list_key: str,
        context_keys: tuple[str, ...],
        path: str,
    ) -> list[dict]:
        raw_items = payload.get(list_key)
        if not isinstance(raw_items, list):
            return []

        envelopes: list[dict] = []
        for idx, item in enumerate(raw_items):
            if not isinstance(item, dict):
                logger.warning(
                    "rest_envelope_explode_failed",
                    path=path,
                    error="non_dict_item",
                    list_key=list_key,
                    item_index=idx,
                )
                continue
            event = dict(item)
            for key in context_keys:
                context_value = payload.get(key)
                if context_value is not None and key not in event:
                    event[key] = context_value
            try:
                envelopes.append(
                    wrap_event(
                        event=event,
                        provider=provider,
                        feed=feed,
                        source="rest",
                    )
                )
            except Exception as exc:
                logger.warning(
                    "rest_envelope_explode_failed",
                    path=path,
                    error=str(exc),
                    list_key=list_key,
                    item_index=idx,
                )

        return envelopes
