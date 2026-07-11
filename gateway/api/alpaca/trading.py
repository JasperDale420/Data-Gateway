"""Alpaca trading endpoints - orders, positions, account, assets, clock, calendar."""

import asyncio
import concurrent.futures
import re
import uuid
from collections.abc import Callable
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE, HTTP_504_GATEWAY_TIMEOUT

from gateway.api.alpaca.common import (
    ALPACA_ROUTER_PREFIX,
    DESC_COMMA_SYMBOLS,
    Client,
    execute_alpaca_cached_call,
    execute_alpaca_provider_call,
    get_cache,
    get_registry,
    require_api_key,
)
from gateway.config import get_settings
from gateway.core.cache import HybridCache, InMemoryCache
from gateway.core.logger import logger
from gateway.core.registry import ProviderRegistry
from gateway.schemas import SuccessResponse

router = APIRouter()
TRADING_ASSETS_CACHE_TTL_SECONDS = 600
TRADING_CALENDAR_CACHE_TTL_SECONDS = 3600

# ─────────────────────────────────────────────────────────────────────────────
# Per-client ownership-prefix scheme for ``client_order_id`` values sent to
# Alpaca. Multiple Empire trading systems (Cerberus, 3Roses, Kairos, Orion,
# Atlas, Orbit) authenticate to this gateway with distinct API keys but
# SHARE A SINGLE upstream Alpaca account. Without per-client namespacing,
# any auth'd trading client could:
#   - Collide with another client's idempotency key (their POST /orders
#     short-circuits to the *other* client's existing order at Alpaca).
#   - Probe / enumerate another client's order state via
#     GET /orders:by_client_order_id (the lookup confirms existence).
#   - Replay another client's key from its own session.
#
# Fix: the gateway prefixes EVERY effective ``client_order_id`` going to
# Alpaca with ``c-{client.id}-``. The prefix IS the ownership marker — we
# don't need a separate persisted (order_id → client_id) map for the common
# ownership-check cases (list / by_client_order_id / get / replace /
# cancel). Order-id-based lookups still need to fetch from Alpaca and
# inspect the order's ``client_order_id`` for prefix match (see
# ``_assert_order_owned_by_caller`` below); that's the unavoidable cost of
# not maintaining a process-local (order_id → owner) map. We chose
# stateless verification over an in-memory map for correctness after
# restart and simplicity — live-capital callers already tolerate Alpaca's
# baseline API latency, and the extra round-trip happens only on the read
# paths that hit a specific order_id (replace, cancel, get_order).
#
# Auto-generated keys: ``c-{client.id}-dg-{uuid4hex}`` (was ``dg-{uuid4hex}``).
# Caller-supplied keys: ``c-{client.id}-{caller_value}`` (caller value is
# transparently prefixed; the caller sees the FULL prefixed string in
# ``meta.client_order_id`` so they know what to retry with).
#
# Alpaca's 128-character ceiling applies to the FINAL prefixed string.
# Caller-supplied keys are validated AFTER prefixing — see
# ``_validate_client_order_id``.
# ─────────────────────────────────────────────────────────────────────────────
_GATEWAY_CLIENT_ORDER_ID_PREFIX = "dg-"
_OWNERSHIP_PREFIX_FORMAT = "c-{client_id}-"

# Alpaca enforces a 128-character ceiling on ``client_order_id`` (REST API
# docs: https://docs.alpaca.markets/reference/postorder — "length <= 128").
# Rejecting at the gateway with a structured 400 surfaces caller bugs early
# and gives a better error than the 422 Alpaca returns when the SDK forwards
# an oversize key. The installed alpaca-py SDK does not enforce this on its
# own (its ``OrderRequest`` schema has no max_length validator), so the
# gateway is the only place this check happens before the wire.
_CLIENT_ORDER_ID_MAX_LENGTH = 128

# Client IDs are embedded in the wire-level ``client_order_id`` prefix.
# Restrict to characters that Alpaca and the SDK accept (and that don't
# introduce ambiguity in our own prefix parsing): alphanumeric, hyphen,
# underscore. ``config/clients.yaml`` ids in production today (cerberus,
# 3roses, orion, atlas, orbit, kairos, heber-watch, test) all satisfy this.
# Validated at startup (gateway.core.auth) AND on every prefix build so a
# misconfigured client (e.g. a colon in the id) fails loudly rather than
# emitting a malformed key that Alpaca rejects mid-burst.
_CLIENT_ID_SAFE_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _ownership_prefix(client_id: str) -> str:
    """Return the ownership prefix ``c-{client_id}-`` for a given client id.

    Raises 500 GW-E5008 if the client id contains characters that would
    break our parsing or Alpaca's accepted character set. This is a
    "should-never-happen" guard — client ids are loaded from
    ``config/clients.yaml`` at startup and validated there — but defense
    in depth matters for live-capital trading.
    """
    if not client_id or not _CLIENT_ID_SAFE_RE.match(client_id):
        # 500 (not 400) intentionally: this is a server-side config error,
        # not a client input fault. Surfacing 400 would mislead the caller
        # into thinking they could fix it by changing the request body.
        logger.error("client_id_unsafe_for_order_prefix", client_id=client_id)
        raise HTTPException(
            status_code=500,
            detail={
                "code": "GW-E5008",
                "message": (
                    "Gateway client id contains characters not allowed in "
                    "the order-ownership prefix (must match [A-Za-z0-9_-]+). "
                    "Fix config/clients.yaml and restart the gateway."
                ),
            },
        )
    return _OWNERSHIP_PREFIX_FORMAT.format(client_id=client_id)


def _validate_client_order_id(raw: str | None, client_id: str) -> str | None:
    """Validate a caller-supplied client_order_id AND apply the ownership prefix.

    Returns the caller value PREFIXED with ``c-{client_id}-`` ready to send
    to Alpaca, or ``None`` when the caller omitted the field (in which case
    the route auto-generates a fully-prefixed key via
    ``_generate_client_order_id``).

    Idempotent prefix handling (CRITICAL — DO NOT REGRESS):
      The gateway returns the FULL prefixed string in ``meta.client_order_id``
      on success AND in the 504 retry hint. Callers SHOULD retry POST /
      PATCH with that exact prefixed value (per the retry hint). To make
      that retry idempotent at Alpaca, a caller-supplied value that is
      ALREADY prefixed with the caller's own ownership tag must NOT be
      double-prefixed (``c-cerberus-c-cerberus-abc`` would defeat Alpaca-
      side dedup and could double-place on retry). We detect the already-
      owned case and forward the value verbatim.

      Conversely, a key that starts with ``c-<foreign_id>-`` is a
      misconfigured caller (or cross-client replay attempt) and must be
      rejected with 400 GW-E4007 — sending it to Alpaca verbatim would
      either dedupe against another client's order (collision) or
      pollute the foreign client's idempotency namespace. We DO NOT
      auto-strip the foreign prefix because that would silently mask an
      operator error / attack signal.

    Validation:
      - Empty / whitespace-only strings → 400 GW-E4006 (silent fallback
        to UUID would defeat Alpaca-side dedup on retry).
      - Foreign-owner-prefixed keys (``c-<other>-...``) → 400 GW-E4007.
      - The FINAL prefixed string must fit in Alpaca's 128-char ceiling.
        Already-owned keys are validated as-is; un-prefixed keys are
        validated AFTER prepending the ownership prefix. Oversize keys
        raise 400 GW-E4006 with a message that surfaces the post-prefix
        budget so callers can adapt rather than guess.
    """
    if raw is None:
        return None
    if not raw.strip():
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GW-E4006",
                "message": (
                    "client_order_id, when supplied, must be a non-empty, "
                    "non-whitespace string. Omit the parameter entirely to "
                    "let the gateway auto-generate an idempotency key."
                ),
            },
        )

    prefix = _ownership_prefix(client_id)

    # Idempotent retry path: caller passed back the prefixed key they
    # received in meta.client_order_id (or in a 504 retry hint).
    # Accept as-is; do NOT re-prefix.
    if raw.startswith(prefix):
        if len(raw) > _CLIENT_ORDER_ID_MAX_LENGTH:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "GW-E4006",
                    "message": (
                        f"client_order_id length {len(raw)} exceeds Alpaca's "
                        f"{_CLIENT_ORDER_ID_MAX_LENGTH}-char ceiling. Even "
                        "though the supplied key already carries this "
                        "caller's ownership prefix, the final value still "
                        "needs to fit. Shorten the key."
                    ),
                },
            )
        return raw

    # Foreign-owner-prefixed key — caller is either misconfigured (passed
    # back a key from a different gateway client's meta) or actively
    # probing another client's namespace. Reject loudly. We DO NOT
    # rewrite the prefix silently because that would mask the operator
    # error / attack signal.
    if raw.startswith("c-"):
        # Defense-in-depth audit signal — emit GW-A4001 so the same
        # alert wired for foreign GET / PATCH / DELETE also covers
        # foreign-prefix POST attempts.
        _log_foreign_order_access_attempt(
            caller_id=client_id,
            order_owner=_parse_owner_from_client_order_id(raw),
            client_order_id=raw,
            operation="create_or_replace_order.foreign_prefix_in_caller_key",
        )
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GW-E4007",
                "message": (
                    "client_order_id carries a foreign gateway-client "
                    "ownership prefix and cannot be used by this client. "
                    "Either omit the parameter to let the gateway auto-"
                    "generate one, or supply a key that does NOT start "
                    "with 'c-' (the gateway will prefix it for you)."
                ),
            },
        )

    prefixed = f"{prefix}{raw}"
    if len(prefixed) > _CLIENT_ORDER_ID_MAX_LENGTH:
        # Compute the per-caller budget so the error tells the caller
        # exactly how many characters they have to work with after the
        # gateway adds the ownership prefix.
        caller_budget = _CLIENT_ORDER_ID_MAX_LENGTH - len(prefix)
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GW-E4006",
                "message": (
                    f"client_order_id length {len(raw)} exceeds the "
                    f"per-client budget of {caller_budget} chars "
                    f"(Alpaca's {_CLIENT_ORDER_ID_MAX_LENGTH}-char ceiling "
                    f"minus the {len(prefix)}-char ownership prefix "
                    f"'{prefix}'). Shorten your key or omit the parameter "
                    "to let the gateway auto-generate one."
                ),
            },
        )
    return prefixed


def _generate_client_order_id(client_id: str) -> str:
    """Generate a gateway-side, ownership-prefixed client_order_id.

    Returns ``c-{client_id}-dg-{uuid4hex}``. Alpaca natively dedupes
    ``submit_order`` by client_order_id — if a call times out mid-flight
    (e.g. our 15s asyncio wait_for fires while the underlying executor
    thread is still talking to Alpaca), the caller can safely retry with
    the same key: Alpaca returns the existing order rather than placing a
    second one. The gateway returns the auto-generated key in the 504
    response detail and in successful response meta so callers always know
    which key to retry with.
    """
    return f"{_ownership_prefix(client_id)}{_GATEWAY_CLIENT_ORDER_ID_PREFIX}{uuid.uuid4().hex}"


def _client_order_id_matches_owner(client_order_id: Any, client_id: str) -> bool:
    """Return True iff ``client_order_id`` carries the caller's ownership prefix.

    Defensive against non-string values (Alpaca's SDK may return None or
    enum-wrapped strings depending on the order shape) — anything that
    isn't a real string returns False so we fail closed rather than open.
    """
    if not isinstance(client_order_id, str):
        return False
    return client_order_id.startswith(_ownership_prefix(client_id))


def _known_client_ids() -> list[str]:
    """Return the set of client ids known to the authenticator.

    Used for longest-prefix-match parsing in
    ``_parse_owner_from_client_order_id``. Client IDs containing hyphens
    (e.g. ``heber-watch``) would otherwise be mis-parsed by a naive
    first-hyphen split — ``c-heber-watch-abc`` would attribute the order
    to ``heber`` instead of ``heber-watch``.

    Imports ``get_authenticator`` lazily to avoid a dep cycle at module
    import time (deps.py imports gateway.core.auth which doesn't import
    this module, but tightening that loop is brittle). The error
    catches are NARROW (ImportError + AttributeError + FileNotFoundError)
    so we don't silently swallow real config errors — those should
    surface elsewhere via the auth-load path. The fallback (empty list
    → naive first-hyphen parse) is correct for any non-hyphenated id,
    which covers all production clients except ``heber-watch``; the
    misattribution is best-effort audit enrichment, not a security
    control (the foreign-prefix REJECTION uses the caller's own
    ``client.id`` directly and is not affected).
    """
    try:
        from gateway.api.deps import get_authenticator

        return list(get_authenticator().list_client_ids())
    except (ImportError, AttributeError, FileNotFoundError):
        return []


def _parse_owner_from_client_order_id(client_order_id: Any) -> str | None:
    """Best-effort extract the owner client_id from a ``c-{id}-...`` prefix.

    Used for audit logging when a foreign-prefix lookup is rejected — lets
    operators see WHICH legitimate client the attempted access actually
    belonged to, which is the load-bearing signal for "is this a
    misconfigured caller or an active enumeration attack?". Returns None
    for orders without the gateway prefix (e.g. orders placed directly
    against Alpaca outside the gateway, before this change shipped, or
    that lack any client_order_id at all).

    Hyphenated client ids (e.g. ``heber-watch``) need longest-prefix-match
    against the authenticator's known client list — a naive first-hyphen
    split would mis-attribute ``c-heber-watch-abc`` to ``heber`` instead
    of ``heber-watch``. We fall back to the naive split only when the
    authenticator isn't reachable (e.g. some unit-test paths), which is
    still correct for any non-hyphenated id.
    """
    if not isinstance(client_order_id, str) or not client_order_id.startswith("c-"):
        return None

    # Strip the "c-" sentinel. Remaining shape: "{client_id}-{rest}".
    after_c = client_order_id[2:]

    # Preferred path: longest-prefix match against the authenticator's
    # known client ids. This correctly handles hyphenated ids.
    known = _known_client_ids()
    # Sort by length DESC so e.g. "heber-watch" beats "heber" if both
    # are configured.
    for candidate in sorted(known, key=len, reverse=True):
        if not _CLIENT_ID_SAFE_RE.match(candidate):
            # Defensive: skip ids that wouldn't have been accepted by
            # the auth-side validator. Shouldn't happen, but if it does
            # we don't want the lookup to crash on bad config.
            continue
        if after_c.startswith(f"{candidate}-"):
            return candidate

    # Fallback for environments where the authenticator isn't wired
    # (some unit tests). Naive first-hyphen split — correct for
    # non-hyphenated ids, may mis-attribute hyphenated ids. Audit log
    # still carries the full client_order_id so operators can resolve
    # the truth manually if needed.
    sep_idx = after_c.find("-")
    if sep_idx <= 0:
        return None
    candidate = after_c[:sep_idx]
    return candidate if _CLIENT_ID_SAFE_RE.match(candidate) else None


def _log_foreign_order_access_attempt(
    *,
    caller_id: str,
    order_owner: str | None,
    order_id: str | None = None,
    client_order_id: str | None = None,
    operation: str,
) -> None:
    """Emit a structured WARNING for any cross-client order access attempt.

    Code GW-A4001 (audit warning). Operators alert on this — a non-zero
    rate means either (a) a misconfigured caller, or (b) an active
    enumeration / replay attempt against a foreign client's order state.
    Both deserve immediate attention against the shared Alpaca account.
    """
    logger.warning(
        "alpaca_trading_foreign_order_access_attempt",
        code="GW-A4001",
        operation=operation,
        client_id_attempted=caller_id,
        order_owner_actual=order_owner,
        order_id=order_id,
        client_order_id=client_order_id,
    )


def _foreign_order_not_found(*, order_id: str | None = None) -> HTTPException:
    """Build the 404 returned for cross-client order access attempts.

    We return 404 (not 403) intentionally: a 403 would confirm the order
    EXISTS but belongs to someone else, which is exactly the enumeration
    signal we're trying to deny. A 404 is indistinguishable from a
    genuinely-unknown order_id.
    """
    detail: dict[str, Any] = {
        "code": "GW-E4404",
        "message": "Order not found.",
    }
    if order_id is not None:
        detail["order_id"] = order_id
    return HTTPException(status_code=404, detail=detail)


def _extract_client_order_id_from_order(order: Any) -> Any:
    """Pull ``client_order_id`` from an order returned by the Alpaca provider.

    The provider normalises orders into dicts via ``_model_to_dict``, but
    we keep this defensive for object-style responses (e.g. attribute
    access on SDK models if a future provider path skips serialisation).
    """
    if isinstance(order, dict):
        return order.get("client_order_id")
    return getattr(order, "client_order_id", None)


# ─────────────────────────────────────────────────────────────────────────────
# Write-class trading operations get a longer wall-clock timeout than reads.
#
# Reads (get_account, get_orders, get_position, get_clock, get_calendar,
# get_portfolio_history, get_assets, etc.) are idempotent at the broker —
# a 504 is safely retryable, so a tighter ceiling keeps per-call latency
# predictable.
#
# Writes (create_order, replace_order, cancel_order, cancel_all_orders,
# close_position, close_all_positions) need the idempotency-retry contract
# kicking in on 504 — but surfacing a 504 to the caller forces them through
# that retry contract (GET by_client_order_id / GET position / etc.), which
# is more expensive than letting a merely-slow successful call complete. The
# 2026-05-15 opening-bell window showed 13 write-class timeouts at the prior
# shared 15s ceiling (3 × create_order, 1 × close_position, 9 × cancel/all)
# — Alpaca's broker latency on the burst can exceed 15s without failing.
# Bumping ONLY writes to 25s (configurable via
# ``alpaca_trading_write_call_timeout_seconds``) lets those calls complete
# while keeping reads tight. The HTTP-level safety net
# (``alpaca_trading_http_timeout_seconds``, default 30s) still releases the
# executor thread on either path.
#
# Cancels are grouped with writes because cancelling at the broker is a
# state mutation — even though it's idempotent at the symbol level, the
# caller cares whether the cancel applied (the position/order state).
# ─────────────────────────────────────────────────────────────────────────────
_WRITE_TRADING_OPERATIONS: frozenset[str] = frozenset(
    {
        "create_order",
        "replace_order",
        "cancel_order",
        "cancel_all_orders",
        "close_position",
        "close_all_positions",
    }
)


# Module-level dedicated executor for trading calls (created lazily)
_trading_executor: concurrent.futures.ThreadPoolExecutor | None = None

# Bounded semaphore that caps concurrent in-flight trading calls. When all
# permits are held the next call fast-fails with 503 rather than queueing in
# the executor's unbounded internal queue. Queue pile-up during Alpaca
# slowdowns holds outstanding asyncio tasks/timers that contend with the
# WebSocket keepalive task for event-loop CPU, causing clients to disconnect
# with "keepalive ping timeout".
_trading_inflight_sem: asyncio.BoundedSemaphore | None = None


def _get_trading_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _trading_executor
    if _trading_executor is None:
        _trading_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=get_settings().alpaca_trading_thread_pool_size,
            thread_name_prefix="alpaca-trading",
        )
    return _trading_executor


def _get_trading_inflight_sem() -> asyncio.BoundedSemaphore:
    global _trading_inflight_sem
    if _trading_inflight_sem is None:
        _trading_inflight_sem = asyncio.BoundedSemaphore(get_settings().alpaca_trading_max_inflight)
    return _trading_inflight_sem


def _reset_trading_inflight_sem_for_tests() -> None:
    """Reset the in-flight semaphore so test-level settings overrides apply.

    Module-level lazy singletons don't observe monkeypatched settings after
    first initialisation; tests that tweak ``alpaca_trading_max_inflight``
    should call this between cases.
    """
    global _trading_inflight_sem
    _trading_inflight_sem = None


async def _execute_trading_call(
    *,
    registry: ProviderRegistry,
    provider_fn: Callable[[Any], Any],
    operation: str,
    log_context: dict[str, Any] | None = None,
) -> Any:
    async def call(provider: Any) -> Any:
        return await _run_trading_provider_call(
            provider=provider,
            provider_fn=provider_fn,
            operation=operation,
            log_context=log_context,
        )

    call.__qualname__ = f"trading.{operation}"
    return await execute_alpaca_provider_call(
        registry=registry,
        provider_call=call,
    )


async def _execute_trading_cached_call(
    *,
    registry: ProviderRegistry,
    cache: InMemoryCache | HybridCache,
    cache_key: str,
    ttl: int,
    route_label: str,
    provider_fn: Callable[[Any], Any],
    operation: str,
    log_context: dict[str, Any] | None = None,
) -> Any:
    async def call(provider: Any) -> Any:
        return await _run_trading_provider_call(
            provider=provider,
            provider_fn=provider_fn,
            operation=operation,
            log_context=log_context,
        )

    call.__qualname__ = f"trading.{operation}"
    return await execute_alpaca_cached_call(
        registry=registry,
        cache=cache,
        cache_key=cache_key,
        ttl=ttl,
        route_label=route_label,
        provider_call=call,
    )


def _merge_idempotency_context_into_5xx(
    exc: HTTPException,
    idempotency_context: dict[str, Any],
) -> HTTPException:
    """Attach idempotency context to a 5xx HTTPException's detail.

    ``execute_alpaca_provider_call`` (in ``common.py``) rewrites Alpaca SDK
    errors (``APIError``, ``httpx.HTTPStatusError``, bare ``Exception``) into
    ``HTTPException`` with a plain-string ``detail`` — losing the
    ``client_order_id`` / ``symbol`` retry key the caller needs to safely
    resolve order state. This helper merges the context back in on ANY 5xx
    so non-timeout failures (e.g. 503 from Alpaca during a deployment) still
    surface the retry contract.

    For sub-500 statuses the exception is returned unchanged — those are
    deterministic client errors where retry semantics don't apply.
    """
    if exc.status_code < 500:
        return exc

    existing_detail = exc.detail
    if isinstance(existing_detail, dict):
        # _run_trading_provider_call's own 503/504 paths already merged the
        # context; don't clobber.
        merged: dict[str, Any] = {**idempotency_context, **existing_detail}
    else:
        # Common case: execute_alpaca_provider_call set detail to a string
        # like "Alpaca API Error: ...". Promote to a dict so the caller has
        # both the human message AND the retry key.
        merged = {
            "code": "GW-E5007",
            "message": str(existing_detail) if existing_detail is not None else "Upstream provider error",
            **idempotency_context,
        }
    return HTTPException(status_code=exc.status_code, detail=merged)


async def _run_trading_provider_call(
    *,
    provider: Any,
    provider_fn: Callable[[Any], Any],
    operation: str,
    idempotency_context: dict[str, Any] | None = None,
    log_context: dict[str, Any] | None = None,
) -> Any:
    """Run a single trading-SDK call with timeout + backpressure protection.

    Args:
        provider: Alpaca provider instance.
        provider_fn: Synchronous callable invoked in the trading executor.
        operation: Stable name used in metrics/logs.
        idempotency_context: Optional dict of fields to surface in 503/504
            error responses so callers can safely retry. For ``create_order``
            this carries ``{"client_order_id": "...", "retry_with":
            "client_order_id"}`` — the caller reads this from the 504 body
            and either (a) GETs the order by client_order_id to see whether
            the order actually placed, or (b) retries POST with the same
            client_order_id (Alpaca natively dedupes by that key). For
            ``close_position`` it carries
            ``{"symbol": "...", "retry_with": "get_position"}`` — Alpaca's
            ClosePositionRequest does not accept client_order_id, so the
            caller checks GET /positions/<symbol> to decide whether to
            retry the close.
    """
    settings = get_settings()
    # Writes get a longer wall-clock budget than reads — see
    # _WRITE_TRADING_OPERATIONS for the rationale. Reads stay at the
    # existing alpaca_trading_call_timeout_seconds default (15s); writes
    # use alpaca_trading_write_call_timeout_seconds (default 25s).
    timeout_seconds = (
        settings.alpaca_trading_write_call_timeout_seconds
        if operation in _WRITE_TRADING_OPERATIONS
        else settings.alpaca_trading_call_timeout_seconds
    )
    sem = _get_trading_inflight_sem()

    # Fast-fail when the in-flight cap is fully reserved. ``locked()`` is safe
    # to inspect without a lock: asyncio is single-threaded so no other
    # coroutine can change the permit count between this check and the
    # subsequent acquire (acquire does not suspend when a permit is free).
    if sem.locked():
        logger.warning(
            "alpaca_trading_backpressure_reject",
            operation=operation,
            max_inflight=settings.alpaca_trading_max_inflight,
            **(log_context or {}),
            **(idempotency_context or {}),
        )
        detail_503: dict[str, Any] = {
            "code": "GW-E5005",
            "message": (
                f"Alpaca trading API backpressure during {operation}: "
                f"{settings.alpaca_trading_max_inflight} calls already in-flight. "
                "Retry shortly."
            ),
        }
        if idempotency_context:
            # Backpressure rejects BEFORE the call hits Alpaca, so the
            # order definitely did NOT place. But surfacing the
            # idempotency key here lets the caller retry with the same
            # key if they happen to retry the *same logical order*, which
            # protects against double-place if the original 503 attempt
            # is racing in the caller's reconciliation logic.
            detail_503.update(idempotency_context)
        raise HTTPException(
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
            detail=detail_503,
        )

    async with sem:
        try:
            loop = asyncio.get_running_loop()
            return await asyncio.wait_for(
                loop.run_in_executor(_get_trading_executor(), provider_fn, provider),
                timeout=timeout_seconds,
            )
        except TimeoutError as exc:
            logger.error(
                "alpaca_trading_call_timeout",
                operation=operation,
                timeout_seconds=timeout_seconds,
                **(log_context or {}),
                **(idempotency_context or {}),
            )
            detail_504: dict[str, Any] = {
                "code": "GW-E5004",
                "message": f"Timed out waiting for Alpaca trading API during {operation}",
            }
            if idempotency_context:
                # CRITICAL: the asyncio task is cancelled, but the
                # executor thread keeps running until the Alpaca SDK call
                # completes — so the order MAY have placed at Alpaca by
                # the time the caller sees the 504. The retry contract
                # surfaced here lets callers either verify the order's
                # actual status or retry idempotently. DO NOT REMOVE.
                detail_504.update(idempotency_context)
            raise HTTPException(
                status_code=HTTP_504_GATEWAY_TIMEOUT,
                detail=detail_504,
            ) from exc


async def _assert_order_owned_by_caller(
    *,
    registry: ProviderRegistry,
    order_id: str,
    client_id: str,
    operation: str,
) -> None:
    """Stateless ownership verification for an order_id-keyed mutation/read.

    Fetches the order from Alpaca via ``provider.get_order(order_id)``,
    then checks the returned ``client_order_id`` for this caller's
    ``c-{client.id}-`` prefix. If absent (foreign client, or a pre-prefix
    legacy order), logs the GW-A4001 audit warning and raises 404 — NOT
    403, to avoid confirming existence.

    Used by ``replace_order``, ``cancel_order``, and ``get_order`` so that
    NO Alpaca write side-effect runs against a foreign client's order. The
    fetch happens BEFORE the mutating SDK call so a cross-client cancel /
    replace attempt never reaches Alpaca's executor thread.

    Note: this adds one extra Alpaca round-trip per mutating call. We
    accept that latency hit in exchange for the simplicity / restart-safety
    of a stateless ownership check — see ``get_order`` docstring for the
    Option A vs Option B trade-off discussion.

    Raises:
        HTTPException 404 GW-E4404 for cross-client / unowned orders.
        Other HTTPExceptions (5xx from upstream, 404 from Alpaca for an
        order that genuinely doesn't exist) bubble through unchanged so
        callers see the broker's view directly.
    """
    fetched = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_order(order_id),
        operation=f"{operation}.ownership_check",
    )
    fetched_client_order_id = _extract_client_order_id_from_order(fetched)
    if not _client_order_id_matches_owner(fetched_client_order_id, client_id):
        _log_foreign_order_access_attempt(
            caller_id=client_id,
            order_owner=_parse_owner_from_client_order_id(fetched_client_order_id),
            order_id=order_id,
            client_order_id=fetched_client_order_id if isinstance(fetched_client_order_id, str) else None,
            operation=operation,
        )
        raise _foreign_order_not_found(order_id=order_id)


@router.get("/account", response_model=SuccessResponse)
async def get_account(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account information."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_account(),
        operation="get_account",
        log_context={"client_id": client.id, "method": "GET", "path": "/api/v1/alpaca/account"},
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.post("/orders", response_model=SuccessResponse)
async def create_order(
    symbol: str,
    side: str,
    qty: float | None = None,
    notional: float | None = None,
    order_type: str = "market",
    time_in_force: str = "day",
    limit_price: float | None = None,
    stop_price: float | None = None,
    client_order_id: str | None = None,
    extended_hours: bool = False,
    order_class: str | None = None,
    take_profit_limit_price: float | None = None,
    stop_loss_stop_price: float | None = None,
    stop_loss_limit_price: float | None = None,
    position_intent: str | None = None,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Create a new order.

    Idempotency contract (DO NOT REGRESS):
      - The gateway PREFIXES every effective ``client_order_id`` with
        ``c-{client.id}-`` so distinct trading clients sharing the upstream
        Alpaca account cannot collide on, enumerate, or replay each other's
        idempotency keys.
      - If the caller supplies ``client_order_id``, the gateway transparently
        prefixes it. Caller sees the FULL prefixed value in
        ``meta.client_order_id`` so they know what to retry with. Empty /
        whitespace-only / oversize (after prefixing) keys are rejected with
        400 GW-E4006 — there is no silent fallback, because a silent
        fallback breaks Alpaca-side dedup on retry.
      - If the caller does NOT supply one, the gateway auto-generates a
        ``c-{client.id}-dg-<uuid4hex>`` key and uses that.
      - The effective client_order_id is returned in ``meta.client_order_id``
        on success AND in the 504 timeout error detail. Alpaca natively
        dedupes ``submit_order`` by client_order_id — a caller that sees
        a 504 can safely retry POST with the same key (returning the
        existing order rather than placing a second) or GET
        ``{ALPACA_ROUTER_PREFIX}/orders:by_client_order_id`` to check status.
    """
    # Validate the caller-supplied key BEFORE auto-generating, and apply
    # the per-client ownership prefix. ``""`` and whitespace previously
    # slipped through ``client_order_id or _generate_...`` and ended up
    # with a fresh UUID labelled "caller" — which destroyed idempotency on
    # retry. The prefix application is also where we enforce the 128-char
    # ceiling AFTER prefixing.
    validated_caller_key = _validate_client_order_id(client_order_id, client.id)
    effective_client_order_id = validated_caller_key or _generate_client_order_id(client.id)
    gateway_generated = validated_caller_key is None
    idempotency_context: dict[str, Any] = {
        "client_order_id": effective_client_order_id,
        "client_order_id_source": "gateway" if gateway_generated else "caller",
        "retry_with": "client_order_id",
        "retry_hint": (
            "Order may have placed at Alpaca despite the 5xx — Alpaca natively "
            "dedupes by client_order_id. Either "
            f"GET {ALPACA_ROUTER_PREFIX}/orders:by_client_order_id"
            f"?client_order_id={effective_client_order_id} to check status, "
            f"or retry POST {ALPACA_ROUTER_PREFIX}/orders with the same "
            "client_order_id to idempotently re-attempt."
        ),
    }
    order_log_context: dict[str, Any] = {
        "client_id": client.id,
        "symbol": symbol.upper(),
        "side": side.lower(),
        "order_type": order_type.lower(),
        "time_in_force": time_in_force.lower(),
        "order_class": order_class.lower() if order_class else None,
        "qty_provided": qty is not None,
        "notional_provided": notional is not None,
        "client_order_id_source": "gateway" if gateway_generated else "caller",
    }

    async def _call(provider: Any) -> Any:
        try:
            return await _run_trading_provider_call(
                provider=provider,
                provider_fn=lambda provider: provider.create_order(
                    symbol=symbol,
                    qty=qty,
                    notional=notional,
                    side=side,
                    order_type=order_type,
                    time_in_force=time_in_force,
                    limit_price=limit_price,
                    stop_price=stop_price,
                    client_order_id=effective_client_order_id,
                    extended_hours=extended_hours,
                    order_class=order_class,
                    take_profit_limit_price=take_profit_limit_price,
                    stop_loss_stop_price=stop_loss_stop_price,
                    stop_loss_limit_price=stop_loss_limit_price,
                    position_intent=position_intent,
                ),
                operation="create_order",
                idempotency_context=idempotency_context,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    try:
        data = await execute_alpaca_provider_call(
            registry=registry,
            provider_call=_call,
            log_context=order_log_context,
        )
    except HTTPException as exc:
        # execute_alpaca_provider_call rewrites APIError/HTTPStatusError into
        # HTTPException with a plain-string detail — losing the
        # idempotency_context that _run_trading_provider_call attaches to
        # its own 503/504. Re-merge so EVERY 5xx surfaces the retry key.
        raise _merge_idempotency_context_into_5xx(exc, idempotency_context) from exc
    return {
        "success": True,
        "data": data,
        "meta": {
            "provider": "alpaca",
            "client_order_id": effective_client_order_id,
            "client_order_id_source": "gateway" if gateway_generated else "caller",
        },
    }


_VALID_ORDER_QUERY_STATUSES = frozenset({"open", "closed", "all"})
_VALID_ORDER_SORT_DIRECTIONS = frozenset({"asc", "desc"})
_VALID_ORDER_SIDES = frozenset({"buy", "sell"})


@router.get("/orders", response_model=SuccessResponse)
async def get_orders(
    status: str = Query(default="open", description="Order status: open, closed, all"),
    limit: int = Query(default=50, ge=1, le=500, description="Max orders to return (Alpaca hard cap 500)"),
    direction: str = Query(default="desc", description="Sort direction: asc, desc"),
    symbols: str | None = Query(default=None, description=DESC_COMMA_SYMBOLS),
    nested: bool = Query(default=True, description="Include nested multi-leg orders"),
    side: str | None = Query(default=None, description="Filter by side: buy, sell"),
    after: datetime | None = Query(
        default=None,
        description=(
            "Only orders with submitted_at AFTER this timestamp (ISO 8601, tz-aware). "
            "Filters by submitted_at, NOT filled_at."
        ),
    ),
    until: datetime | None = Query(
        default=None,
        description=(
            "Only orders with submitted_at UNTIL/BEFORE this timestamp (ISO 8601, tz-aware). "
            "Filters by submitted_at, NOT filled_at."
        ),
    ),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get all orders with optional filters.

    ──────────────────────────────────────────────────────────────────────────
    ORION SAME-DAY-FILL COVERAGE CONTRACT (consumed by Orion reconciliation)
    ──────────────────────────────────────────────────────────────────────────
    Params:
      - ``status``     : "open" | "closed" | "all". Use "all" for fill coverage.
      - ``after``      : ISO 8601 tz-aware datetime. Returns orders with
                         submitted_at STRICTLY AFTER this value.
      - ``until``      : ISO 8601 tz-aware datetime. Returns orders with
                         submitted_at STRICTLY BEFORE this value.
      - ``limit``      : ≤ 500 (Alpaca hard cap per page).
      - ``direction``  : "asc" | "desc" — pagination walk order over submitted_at.
      - ``nested``     : keep True so bracket legs (the GTC exit fills) are nested
                         under the parent order.

    Semantics & caveat (IMPORTANT):
      ``after``/``until`` filter by **submitted_at**, NOT by filled_at — this is
      Alpaca's GetOrdersRequest behaviour, and Alpaca's orders API exposes no
      page_token (the activities/FILL endpoint that filters by transaction_time
      is NOT surfaced by the installed alpaca-py 0.43.2 SDK). Consequence: a GTC
      bracket exit submitted on a PRIOR day but FILLED today will NOT appear in a
      ``after=<today 00:00 ET>`` window.

    How Orion PROVES it has every same-day FILL despite the submitted_at filter:
      1. Page with ``status=all`` and ``direction=asc``, starting ``after`` far
         enough back to include every order that could still be open today
         (i.e. back past the oldest still-open submitted_at — in practice the
         GTC exits live on the entry day, so reach back to the oldest entry).
      2. Advance the cursor: set the next page's ``after`` to the max
         submitted_at seen in the page (exclusive), repeat until a page returns
         < limit rows. This walks the full submitted_at history with no 500 cap
         truncation.
      3. Client-side, keep only orders whose ``client_order_id`` starts with
         ``orion_`` (Orion attribution) AND whose ``filled_at`` falls in today's
         session. Each order carries ``order_id`` (``id``), ``client_order_id``,
         ``symbol``, ``filled_qty``, ``filled_avg_price``, and ``filled_at`` —
         giving the order_id→client_order_id map and the fill data directly. With
         ``nested=True``, bracket child fills appear under ``legs``.
      Because every order that can fill today must have been submitted at-or-
      before today, paging the full submitted_at range with ``status=all``
      guarantees the order is in the result set; the client-side ``filled_at``
      filter then proves same-day fill coverage with no 500-cap blind spot.
    ──────────────────────────────────────────────────────────────────────────

    Ownership filtering (BLOCKER 1, DO NOT REGRESS): the upstream Alpaca
    account is SHARED across all gateway clients (Cerberus, 3Roses,
    Kairos, Orion, Atlas, Orbit). Returning the unfiltered list would
    leak every other client's open orders. We filter client-side here
    to ONLY return orders whose ``client_order_id`` starts with this
    caller's ownership prefix ``c-{client.id}-``. Orders that lack a
    ``client_order_id`` or were placed by another client are stripped
    silently — there is no signal in the response that any orders were
    filtered out. ``meta.count`` reflects the FILTERED count.

    Limit-after-filter (DO NOT REGRESS): we MUST fetch the full open-
    order list (up to Alpaca's 500-per-call ceiling) and filter LOCALLY
    before honouring the caller's ``limit``. Pushing the limit upstream
    risks the broker returning N foreign-client orders that get fully
    filtered out, leaving the caller with an empty list even when they
    have older owned orders — a reconciliation hazard that has caused
    duplicate-order placement in similar fan-out gateways. We always
    request the upstream ceiling (500) and then ``[:limit]`` post-filter.

    Page-boundary limitation (tracked separately): when the shared
    Alpaca account has MORE THAN 500 open orders, only the newest 500
    are inspected — owned orders beyond that page are invisible. In
    practice no Empire trading system runs anywhere near that scale,
    but a follow-up should add explicit ``after``-cursor pagination so
    we walk the full open-order set. The current 500-per-call ceiling
    is enforced upstream by Alpaca's REST API (``GET /v2/orders`` accepts
    ``limit <= 500``); the bounded result is preferable to the prior
    silent-truncation behaviour.
    """
    if status not in _VALID_ORDER_QUERY_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status '{status}'. Must be one of: {sorted(_VALID_ORDER_QUERY_STATUSES)}",
        )
    if direction not in _VALID_ORDER_SORT_DIRECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid direction '{direction}'. Must be one of: {sorted(_VALID_ORDER_SORT_DIRECTIONS)}",
        )
    if side is not None and side not in _VALID_ORDER_SIDES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid side '{side}'. Must be one of: {sorted(_VALID_ORDER_SIDES)}",
        )

    # Reject NAIVE after/until. A tz-naive datetime serializes to the wire as
    # if it were UTC, silently shifting the submitted_at window for any caller
    # whose intent was a different zone — Orion pages with UTC tz-aware values,
    # so a naive value that slipped through would skew the fill-coverage window
    # and miss fills. Forcing tz-aware makes the contract unambiguous.
    for _name, _value in (("after", after), ("until", until)):
        if isinstance(_value, datetime) and _value.tzinfo is None:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "GW-E4007",
                    "message": (
                        f"'{_name}' must be timezone-aware (e.g. ISO 8601 with a UTC offset like "
                        f"'2026-06-10T00:00:00Z'). A naive datetime would be silently treated as UTC "
                        "and shift the submitted_at window."
                    ),
                },
            )
    if isinstance(after, datetime) and isinstance(until, datetime) and after > until:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GW-E4007",
                "message": f"'after' ({after.isoformat()}) must not be later than 'until' ({until.isoformat()}).",
            },
        )

    symbols_list = symbols.split(",") if symbols else None
    # Always request the upstream ceiling (500) so the caller's ``limit``
    # is applied AFTER ownership filtering — see docstring rationale.
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_orders(
            status=status,
            limit=500,
            direction=direction,
            symbols=symbols_list,
            nested=nested,
            side=side,
            after=after,
            until=until,
        ),
        operation="get_orders",
        log_context={"client_id": client.id, "method": "GET", "path": "/api/v1/alpaca/orders"},
    )
    # Filter to only orders owned by this caller, THEN truncate to the
    # caller-requested limit. Defensive against a provider that returns
    # a non-list (current implementation always returns list, but
    # typing-wise nothing pins it).
    if isinstance(data, list):
        owned = [
            order
            for order in data
            if _client_order_id_matches_owner(_extract_client_order_id_from_order(order), client.id)
        ]
        owned = owned[:limit]
    else:
        owned = data
    return {
        "success": True,
        "data": owned,
        "meta": {"count": len(owned) if isinstance(owned, list) else 0, "provider": "alpaca"},
    }


@router.get("/orders/{order_id}", response_model=SuccessResponse)
async def get_order(
    order_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get a specific order by ID.

    Ownership-verification design (Option A — stateless, BLOCKER 1):
      The gateway does NOT maintain an in-process ``(order_id → client_id)``
      map. Instead, it fetches the order from Alpaca and inspects the
      returned ``client_order_id`` for this caller's ownership prefix
      ``c-{client.id}-``. If absent, the gateway returns 404 (NOT 403 —
      a 403 would confirm the order exists and enable enumeration).

      Trade-off: one extra Alpaca round-trip per read. Acceptable because
      live-capital callers already tolerate Alpaca's baseline API
      latency, and statelessness avoids the "map lost on restart" failure
      mode that would otherwise require a startup rebuild from Alpaca's
      order list.

      A cross-client attempt is logged as ``GW-A4001`` so operators can
      alert on enumeration / misconfiguration.
    """
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_order(order_id),
        operation="get_order",
    )
    order_client_order_id = _extract_client_order_id_from_order(data)
    if not _client_order_id_matches_owner(order_client_order_id, client.id):
        _log_foreign_order_access_attempt(
            caller_id=client.id,
            order_owner=_parse_owner_from_client_order_id(order_client_order_id),
            order_id=order_id,
            client_order_id=order_client_order_id if isinstance(order_client_order_id, str) else None,
            operation="get_order",
        )
        raise _foreign_order_not_found(order_id=order_id)
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.get("/orders:by_client_order_id", response_model=SuccessResponse)
async def get_order_by_client_id(
    client_order_id: str = Query(..., description="Client order ID"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get order by client order ID.

    Ownership-verification (BLOCKER 2, DO NOT REGRESS): the gateway
    prefixes every effective ``client_order_id`` going to Alpaca with
    ``c-{client.id}-``. A lookup whose supplied key does NOT carry the
    caller's prefix is either (a) a misconfigured caller passing a raw
    legacy key (callers MUST pass the prefixed string they received in
    ``meta.client_order_id`` — that's the contract), or (b) an active
    enumeration attempt against another client's keys. Both yield 404
    (NOT 403) so we don't confirm whether the key exists at Alpaca.
    """
    if not _client_order_id_matches_owner(client_order_id, client.id):
        _log_foreign_order_access_attempt(
            caller_id=client.id,
            order_owner=_parse_owner_from_client_order_id(client_order_id),
            client_order_id=client_order_id,
            operation="get_order_by_client_id",
        )
        # Reuse the same 404 shape as foreign order-id lookups; do NOT
        # echo the supplied key back in the body to avoid log/metric
        # signals that could be used for enumeration timing analysis.
        raise HTTPException(
            status_code=404,
            detail={
                "code": "GW-E4404",
                "message": "Order not found.",
            },
        )
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_order_by_client_id(client_order_id),
        operation="get_order_by_client_id",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.patch("/orders/{order_id}", response_model=SuccessResponse)
async def replace_order(
    order_id: str,
    qty: float | None = None,
    limit_price: float | None = None,
    stop_price: float | None = None,
    time_in_force: str | None = None,
    client_order_id: str | None = None,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Replace/modify an existing order.

    Idempotency contract (DO NOT REGRESS — mirrors create_order):
      Alpaca's ``replace_order_by_id`` accepts a ``client_order_id`` on the
      *replacement* order — same dedup semantics as ``submit_order``. If the
      gateway-side ``asyncio.wait_for`` fires (writes use the 25s
      ``alpaca_trading_write_call_timeout_seconds`` knob, NOT the 15s read
      knob) while the underlying executor thread is still talking to Alpaca,
      the replacement MAY have already applied at the broker. Without an
      idempotency key the caller has no safe retry path:
        - Retrying PATCH naively against a replaced order will either no-op
          (the old order_id is now in a non-replaceable state) OR replace
          *again* with a new key, double-modifying the position.
        - The original ``order_id`` is transitioned to ``replaced`` status
          when the replacement applies; the NEW replacement order is
          assigned its own id by Alpaca.

      Same plumbing as ``create_order``:
        - Caller-supplied ``client_order_id`` validated by
          ``_validate_client_order_id`` (empty/whitespace/oversize → 400
          GW-E4006).
        - Missing ``client_order_id`` auto-generated as ``dg-<uuid4hex>``.
        - Effective key returned in ``meta.client_order_id`` +
          ``meta.client_order_id_source`` on success.
        - 503 backpressure + 504 timeout + non-timeout 5xx all carry the
          key in ``detail.client_order_id`` plus a ``retry_hint`` that
          points at BOTH:
            1. ``GET {ALPACA_ROUTER_PREFIX}/orders/{order_id}`` — check
               the original order's current state (e.g. ``replaced`` means
               the replacement applied).
            2. ``GET {ALPACA_ROUTER_PREFIX}/orders:by_client_order_id`` —
               check whether the replacement order exists under our key.

        Both lookups together resolve "did my replacement land?" without
        re-issuing the PATCH and risking a second modification.
    """
    # Ownership pre-check (BLOCKER 1): replace is a state mutation against
    # the SHARED Alpaca account, so we MUST verify this caller owns the
    # original order before letting any Alpaca write side-effect fire. The
    # check runs BEFORE both the caller-key validation (to avoid leaking
    # validation-error timing as an enumeration signal) and the SDK call.
    await _assert_order_owned_by_caller(
        registry=registry,
        order_id=order_id,
        client_id=client.id,
        operation="replace_order",
    )

    # Same validation contract as create_order — empty / whitespace /
    # oversize keys are rejected BEFORE auto-generation so callers can't
    # silently slip a bad key past Alpaca-side dedup on retry. Also applies
    # the per-client ownership prefix ``c-{client.id}-`` so the replacement
    # order can be uniquely attributed back to this caller on later lookups.
    validated_caller_key = _validate_client_order_id(client_order_id, client.id)
    effective_client_order_id = validated_caller_key or _generate_client_order_id(client.id)
    gateway_generated = validated_caller_key is None
    idempotency_context: dict[str, Any] = {
        "client_order_id": effective_client_order_id,
        "client_order_id_source": "gateway" if gateway_generated else "caller",
        "retry_with": "client_order_id",
        "retry_hint": (
            "Replacement may have applied at Alpaca despite the 5xx — "
            "Alpaca natively dedupes by client_order_id. Verify state via "
            "EITHER "
            f"GET {ALPACA_ROUTER_PREFIX}/orders/{order_id} (the original "
            "order transitions to 'replaced' status when a replacement "
            f"applies) OR GET {ALPACA_ROUTER_PREFIX}/orders:by_client_order_id"
            f"?client_order_id={effective_client_order_id} (the replacement "
            "order is keyed by the supplied client_order_id). DO NOT retry "
            "PATCH naively — re-issuing without these checks risks a "
            "double-modify against a replaced order."
        ),
    }

    async def _call(provider: Any) -> Any:
        try:
            return await _run_trading_provider_call(
                provider=provider,
                provider_fn=lambda provider: provider.replace_order(
                    order_id,
                    qty=qty,
                    limit_price=limit_price,
                    stop_price=stop_price,
                    time_in_force=time_in_force,
                    client_order_id=effective_client_order_id,
                ),
                operation="replace_order",
                idempotency_context=idempotency_context,
            )
        except ValueError as e:
            # Mirrors create_order — provider-side input validation (e.g.
            # TimeInForce(time_in_force) on an unknown enum value) raises
            # ValueError. Without this branch the exception propagates into
            # execute_alpaca_provider_call which rewrites unknowns into a
            # synthetic 502 — surfacing caller-fault input as a retryable
            # 5xx that the new idempotency-context-merge logic then labels
            # with retry hints. Caller would chase a phantom Alpaca outage.
            raise HTTPException(status_code=400, detail=str(e)) from e

    try:
        data = await execute_alpaca_provider_call(
            registry=registry,
            provider_call=_call,
        )
    except HTTPException as exc:
        # Same reason as create_order: execute_alpaca_provider_call rewrites
        # APIError/HTTPStatusError into HTTPException with a plain-string
        # detail — losing the idempotency_context. Re-merge so every 5xx
        # surfaces the retry contract.
        raise _merge_idempotency_context_into_5xx(exc, idempotency_context) from exc
    return {
        "success": True,
        "data": data,
        "meta": {
            "provider": "alpaca",
            "client_order_id": effective_client_order_id,
            "client_order_id_source": "gateway" if gateway_generated else "caller",
        },
    }


@router.delete("/orders/{order_id}", response_model=SuccessResponse)
async def cancel_order(
    order_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Cancel an order by ID.

    Ownership pre-check (BLOCKER 1): cancel is a state mutation against
    the SHARED Alpaca account. We fetch the order from Alpaca and verify
    its ``client_order_id`` carries this caller's ``c-{client.id}-``
    prefix BEFORE the SDK ``cancel_order_by_id`` call. A cross-client
    attempt returns 404 GW-E4404 (NOT 403, to avoid confirming existence)
    and the Alpaca cancel never fires. Audit log: GW-A4001.
    """
    await _assert_order_owned_by_caller(
        registry=registry,
        order_id=order_id,
        client_id=client.id,
        operation="cancel_order",
    )
    success = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.cancel_order(order_id),
        operation="cancel_order",
    )
    return {
        "success": success,
        "data": {"order_id": order_id, "cancelled": success},
        "meta": {"provider": "alpaca"},
    }


@router.delete("/orders", response_model=SuccessResponse)
async def cancel_all_orders(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Cancel all open orders OWNED BY THIS CALLER.

    Ownership filtering (BLOCKER 1, DO NOT REGRESS): the upstream Alpaca
    ``cancel_orders`` SDK call cancels EVERY open order in the shared
    account — that would mass-liquidate every other Empire trading
    client's pending orders on a single misfired DELETE /orders. To
    isolate the blast radius per gateway client, we (1) list open orders,
    (2) filter to those whose ``client_order_id`` carries this caller's
    ``c-{client.id}-`` ownership prefix, and (3) cancel each owned order
    individually. The provider's bulk ``cancel_all_orders()`` is NEVER
    called from this endpoint after this change.

    Trade-off: N + 1 round-trips (list + per-order cancel) instead of a
    single bulk cancel. Acceptable because (a) live-capital callers
    already tolerate Alpaca's per-call latency, (b) each Empire client's
    open-order count is typically small (well below the broker rate
    limit), and (c) the safety guarantee — never cancel another client's
    orders — is non-negotiable.

    Failures on individual cancels are captured per-order in the response
    so the caller can reconcile which orders actually cancelled. We do
    NOT short-circuit on the first failure: cancelling SOME of a
    client's orders is strictly better than cancelling NONE if a
    transient Alpaca error hits mid-batch.

    Page-boundary limitation (tracked separately, same as get_orders):
    only the newest 500 open orders are inspected — owned orders beyond
    that page won't be cancelled. Follow-up should add cursor-based
    pagination across the full open-order set.
    """
    # List open orders FIRST so we know what's owned. Reuse get_orders'
    # filtering by inlining the same prefix check — we can't call the
    # route function directly here because it expects FastAPI dependency
    # injection, and we want to bypass the validation of status/direction
    # query params (we hardcode them).
    raw_orders = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_orders(
            status="open",
            limit=500,
            direction="desc",
            symbols=None,
            nested=True,
            side=None,
        ),
        operation="cancel_all_orders.list",
    )
    owned: list[dict[str, Any]] = []
    if isinstance(raw_orders, list):
        owned = [
            order
            for order in raw_orders
            if _client_order_id_matches_owner(_extract_client_order_id_from_order(order), client.id)
        ]

    # Cancel concurrently rather than sequentially. With the default
    # 25s write timeout × N owned orders, a sequential loop could run
    # for minutes during an opening-bell mass-cancel; parallel cancels
    # bounded by the existing in-flight semaphore
    # (alpaca_trading_max_inflight, default 24) compress that into the
    # latency of the slowest single cancel.
    async def _one_cancel(order: dict[str, Any]) -> dict[str, Any]:
        order_id = order.get("id") if isinstance(order, dict) else getattr(order, "id", None)
        if not order_id:
            return {"order_id": None, "cancelled": False, "error": "order missing id field"}
        try:
            await _execute_trading_call(
                registry=registry,
                # Bind order_id via default-arg trick to dodge the
                # classic loop-closure capture footgun.
                provider_fn=(lambda _provider, _oid=order_id: _provider.cancel_order(_oid)),
                operation="cancel_all_orders.cancel",
            )
            return {"order_id": order_id, "cancelled": True}
        except HTTPException as exc:
            return {
                "order_id": order_id,
                "cancelled": False,
                "status_code": exc.status_code,
                "detail": exc.detail,
            }

    if owned:
        # gather with return_exceptions=False; _one_cancel itself never
        # raises (caught HTTPException above), so any propagating
        # exception is a real bug we WANT to surface.
        results = await asyncio.gather(*[_one_cancel(order) for order in owned])
    else:
        results = []

    cancelled = [r for r in results if r.get("cancelled")]
    errors = [r for r in results if not r.get("cancelled")]
    return {
        "success": True,
        "data": cancelled,
        "meta": {
            "count": len(cancelled),
            "owned_count": len(owned),
            "errors": errors,
            "provider": "alpaca",
        },
    }


@router.get("/positions", response_model=SuccessResponse)
async def get_positions(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get all open positions.

    KNOWN LIMITATION (tracked separately — DO NOT extend scope of the
    per-client-order-isolation PR): Alpaca positions are aggregated at
    the ACCOUNT level, not per-order. Two gateway clients placing buy
    orders for AAPL net to a single AAPL position on the shared
    account. There's no per-client position concept at the broker, so
    we can't simply filter positions the way we filter orders. A real
    fix requires a gateway-side ledger that tracks per-client fills
    derived from owned ``client_order_id`` prefixes — out of scope for
    the BLOCKER 1/2 emergency patch. Position endpoints therefore
    remain account-wide for now; trading clients sharing symbols MUST
    coordinate out-of-band until the ledger lands.
    """
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_positions(),
        operation="get_positions",
        log_context={"client_id": client.id, "method": "GET", "path": "/api/v1/alpaca/positions"},
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/positions/{symbol}", response_model=SuccessResponse)
async def get_position(
    symbol: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get position for a specific symbol."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_position(symbol),
        operation="get_position",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.delete("/positions/{symbol}", response_model=SuccessResponse)
async def close_position(
    symbol: str,
    qty: float | None = Query(default=None, description="Quantity to close"),
    percentage: float | None = Query(default=None, description="Percentage to close (0-100)"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Close a position (fully or partially).

    Idempotency contract (DO NOT REGRESS):
      Alpaca's ClosePositionRequest does NOT accept a client_order_id —
      the SDK generates its own order id server-side. So unlike
      create_order, the gateway can't use Alpaca-side dedup. Instead, the
      504 timeout body includes ``retry_with: "get_position"`` and the
      symbol, so the caller can resolve "did the close actually
      happen?" by calling GET ``{ALPACA_ROUTER_PREFIX}/positions/<symbol>``:
        - 404 POSITION_NOT_FOUND → close succeeded (or position never
          existed); do NOT retry the close.
        - 200 with position data → close did NOT take effect; safe to
          retry the close.
      This avoids the broker-side double-place problem because a "close"
      that lands twice is bounded by the current position size — Alpaca
      rejects close requests for non-existent positions with 40410000,
      which the provider already translates into a clean 404.
    """
    canonical_symbol = symbol.upper()
    if qty is not None and qty < 0:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GW-E4006",
                "message": f"close_position qty must be non-negative; got {qty}",
                "symbol": canonical_symbol,
                "qty": qty,
            },
        )

    logger.info(
        "alpaca_close_position",
        client_id=client.id,
        symbol=canonical_symbol,
        qty=qty,
        percentage=percentage,
    )

    idempotency_context: dict[str, Any] = {
        "symbol": canonical_symbol,
        "retry_with": "get_position",
        "retry_hint": (
            "Close may have succeeded at Alpaca despite the 5xx — "
            "ClosePositionRequest does not accept client_order_id. Check "
            f"GET {ALPACA_ROUTER_PREFIX}/positions/{canonical_symbol}: "
            "404 means the close succeeded (or position is gone), 200 "
            "means safe to retry the close."
        ),
    }

    async def _call(provider: Any) -> Any:
        return await _run_trading_provider_call(
            provider=provider,
            provider_fn=lambda provider: provider.close_position(symbol, qty, percentage),
            operation="close_position",
            idempotency_context=idempotency_context,
        )

    try:
        data = await execute_alpaca_provider_call(
            registry=registry,
            provider_call=_call,
        )
    except HTTPException as exc:
        # Same reason as create_order: execute_alpaca_provider_call's 5xx
        # rewrites lose the retry contract. Re-merge so a non-timeout 5xx
        # (e.g. APIError -> 503) still tells the caller to GET the position.
        raise _merge_idempotency_context_into_5xx(exc, idempotency_context) from exc
    return {
        "success": True,
        "data": data,
        "meta": {
            "provider": "alpaca",
            "symbol": canonical_symbol,
        },
    }


@router.delete("/positions", response_model=SuccessResponse)
async def close_all_positions(
    cancel_orders: bool = Query(
        default=False, description="Also cancel ALL open orders first (account-wide, off by default)"
    ),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Close all open positions on the SHARED account (super_admin only)."""
    logger.warning(
        "alpaca_close_all_positions",
        client_id=client.id,
        cancel_orders=cancel_orders,
        msg="account-wide flatten of the shared Alpaca account",
    )
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.close_all_positions(cancel_orders),
        operation="close_all_positions",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/portfolio/history", response_model=SuccessResponse)
async def get_portfolio_history(
    period: str | None = Query(default="1M", description="Period: 1D, 1W, 1M, 3M, 6M, 1A, all"),
    timeframe: str | None = Query(default="1D", description="Timeframe: 1Min, 5Min, 15Min, 1H, 1D"),
    extended_hours: bool = Query(default=False, description="Include extended hours"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get account portfolio history."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_portfolio_history(
            period=period,
            timeframe=timeframe,
            extended_hours=extended_hours,
        ),
        operation="get_portfolio_history",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.get("/assets", response_model=SuccessResponse)
async def get_assets(
    status: str | None = Query(default="active", description="Asset status: active, inactive"),
    asset_class: str | None = Query(default="us_equity", description="Asset class: us_equity, crypto"),
    exchange: str | None = Query(default=None, description="Exchange filter"),
    client: Client = Depends(require_api_key),
    cache: InMemoryCache | HybridCache = Depends(get_cache),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get available assets."""
    normalized_status = (status or "none").strip().lower()
    normalized_asset_class = (asset_class or "none").strip().lower()
    normalized_exchange = (exchange or "all").strip().lower()
    data = await _execute_trading_cached_call(
        registry=registry,
        cache=cache,
        cache_key=(f"alpaca:trading:assets:{normalized_status}:{normalized_asset_class}:{normalized_exchange}"),
        ttl=TRADING_ASSETS_CACHE_TTL_SECONDS,
        route_label="alpaca_trading_assets",
        provider_fn=lambda provider: provider.get_assets(
            status=status,
            asset_class=asset_class,
            exchange=exchange,
        ),
        operation="get_assets",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/assets/{symbol}", response_model=SuccessResponse)
async def get_asset(
    symbol: str,
    client: Client = Depends(require_api_key),
    cache: InMemoryCache | HybridCache = Depends(get_cache),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get a specific asset by symbol."""
    normalized_symbol = symbol.strip().upper()
    data = await _execute_trading_cached_call(
        registry=registry,
        cache=cache,
        cache_key=f"alpaca:trading:asset:{normalized_symbol}",
        ttl=TRADING_ASSETS_CACHE_TTL_SECONDS,
        route_label="alpaca_trading_asset",
        provider_fn=lambda provider: provider.get_asset(normalized_symbol),
        operation="get_asset",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.get("/clock", response_model=SuccessResponse)
async def get_clock(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get market clock."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_clock(),
        operation="get_clock",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.get("/calendar", response_model=SuccessResponse)
async def get_calendar(
    start: date | None = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end: date | None = Query(default=None, description="End date (YYYY-MM-DD)"),
    client: Client = Depends(require_api_key),
    cache: InMemoryCache | HybridCache = Depends(get_cache),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get trading calendar."""
    start_key = start.isoformat() if start else "none"
    end_key = end.isoformat() if end else "none"
    data = await _execute_trading_cached_call(
        registry=registry,
        cache=cache,
        cache_key=f"alpaca:trading:calendar:{start_key}:{end_key}",
        ttl=TRADING_CALENDAR_CACHE_TTL_SECONDS,
        route_label="alpaca_trading_calendar",
        provider_fn=lambda provider: provider.get_calendar(start, end),
        operation="get_calendar",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }
