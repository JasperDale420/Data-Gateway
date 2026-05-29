"""Alpaca trading endpoints - orders, positions, account, assets, clock, calendar."""

import asyncio
import concurrent.futures
import uuid
from collections.abc import Callable
from datetime import date
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
# Idempotency-key prefix for gateway-generated client_order_id values. Used to
# distinguish auto-generated keys from caller-supplied ones in logs/dashboards.
# Alpaca's client_order_id max length is 128 chars (per their REST API docs);
# "dg-" + 32-char UUID hex leaves ample headroom under the ceiling.
# ─────────────────────────────────────────────────────────────────────────────
_GATEWAY_CLIENT_ORDER_ID_PREFIX = "dg-"

# Alpaca enforces a 128-character ceiling on ``client_order_id`` (REST API
# docs: https://docs.alpaca.markets/reference/postorder — "length <= 128").
# Rejecting at the gateway with a structured 400 surfaces caller bugs early
# and gives a better error than the 422 Alpaca returns when the SDK forwards
# an oversize key. The installed alpaca-py SDK does not enforce this on its
# own (its ``OrderRequest`` schema has no max_length validator), so the
# gateway is the only place this check happens before the wire.
_CLIENT_ORDER_ID_MAX_LENGTH = 128


def _validate_client_order_id(raw: str | None) -> str | None:
    """Validate a caller-supplied client_order_id.

    Returns the value unchanged when ``None`` (caller omitted the field —
    gateway will auto-generate downstream). Raises 400 GW-E4006 for empty
    strings, whitespace-only strings, and strings exceeding Alpaca's 128-char
    ceiling.

    Rejecting empty/whitespace is critical for idempotency: previously, a
    caller passing ``client_order_id=""`` would receive a fresh
    gateway-minted UUID labelled ``client_order_id_source="caller"``, and
    each retry would mint a new UUID — defeating Alpaca-side dedup and
    risking double-place on 504 retries. Forcing callers to either supply a
    real key or omit the field entirely makes the idempotency contract
    unambiguous.
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
    if len(raw) > _CLIENT_ORDER_ID_MAX_LENGTH:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GW-E4006",
                "message": (
                    f"client_order_id length {len(raw)} exceeds Alpaca's {_CLIENT_ORDER_ID_MAX_LENGTH}-char limit."
                ),
            },
        )
    return raw


def _generate_client_order_id() -> str:
    """Generate a gateway-side client_order_id for order idempotency.

    Alpaca natively dedupes `submit_order` by client_order_id — if a call
    times out mid-flight (e.g. our 15s asyncio wait_for fires while the
    underlying executor thread is still talking to Alpaca), the caller can
    safely retry with the same client_order_id: Alpaca returns the existing
    order rather than placing a second one. The gateway returns the
    auto-generated key in the 504 response detail and in successful
    response meta so callers always know which key to retry with.
    """
    return f"{_GATEWAY_CLIENT_ORDER_ID_PREFIX}{uuid.uuid4().hex}"


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
) -> Any:
    async def call(provider: Any) -> Any:
        return await _run_trading_provider_call(
            provider=provider,
            provider_fn=provider_fn,
            operation=operation,
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
) -> Any:
    async def call(provider: Any) -> Any:
        return await _run_trading_provider_call(
            provider=provider,
            provider_fn=provider_fn,
            operation=operation,
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
      - If the caller supplies ``client_order_id``, we use it verbatim.
        Empty / whitespace-only / oversize keys are rejected with 400
        GW-E4006 — there is no silent fallback, because a silent fallback
        breaks Alpaca-side dedup on retry.
      - If the caller does NOT supply one, the gateway auto-generates a
        ``dg-<uuid4hex>`` key and uses that.
      - The effective client_order_id is returned in ``meta.client_order_id``
        on success AND in the 504 timeout error detail. Alpaca natively
        dedupes ``submit_order`` by client_order_id — a caller that sees
        a 504 can safely retry POST with the same key (returning the
        existing order rather than placing a second) or GET
        ``{ALPACA_ROUTER_PREFIX}/orders:by_client_order_id`` to check status.
    """
    # Validate the caller-supplied key BEFORE auto-generating. ``""`` and
    # whitespace previously slipped through ``client_order_id or
    # _generate_...`` and ended up with a fresh UUID labelled "caller" —
    # which destroyed idempotency on retry. See _validate_client_order_id
    # for the full reasoning.
    validated_caller_key = _validate_client_order_id(client_order_id)
    effective_client_order_id = validated_caller_key or _generate_client_order_id()
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
    limit: int = Query(default=50, le=500, description="Max orders to return"),
    direction: str = Query(default="desc", description="Sort direction: asc, desc"),
    symbols: str | None = Query(default=None, description=DESC_COMMA_SYMBOLS),
    nested: bool = Query(default=True, description="Include nested multi-leg orders"),
    side: str | None = Query(default=None, description="Filter by side: buy, sell"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get all orders with optional filters."""
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

    symbols_list = symbols.split(",") if symbols else None
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_orders(
            status=status,
            limit=limit,
            direction=direction,
            symbols=symbols_list,
            nested=nested,
            side=side,
        ),
        operation="get_orders",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/orders/{order_id}", response_model=SuccessResponse)
async def get_order(
    order_id: str,
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get a specific order by ID."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_order(order_id),
        operation="get_order",
    )
    return {"success": True, "data": data, "meta": {"provider": "alpaca"}}


@router.get("/orders:by_client_order_id", response_model=SuccessResponse)
async def get_order_by_client_id(
    client_order_id: str = Query(..., description="Client order ID"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get order by client order ID."""
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
    # Same validation contract as create_order — empty / whitespace /
    # oversize keys are rejected BEFORE auto-generation so callers can't
    # silently slip a bad key past Alpaca-side dedup on retry.
    validated_caller_key = _validate_client_order_id(client_order_id)
    effective_client_order_id = validated_caller_key or _generate_client_order_id()
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
    """Cancel an order by ID."""
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
    """Cancel all open orders."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.cancel_all_orders(),
        operation="cancel_all_orders",
    )
    return {
        "success": True,
        "data": data,
        "meta": {"count": len(data), "provider": "alpaca"},
    }


@router.get("/positions", response_model=SuccessResponse)
async def get_positions(
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Get all open positions."""
    data = await _execute_trading_call(
        registry=registry,
        provider_fn=lambda provider: provider.get_positions(),
        operation="get_positions",
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
    cancel_orders: bool = Query(default=True, description="Cancel open orders first"),
    client: Client = Depends(require_api_key),
    registry: ProviderRegistry = Depends(get_registry),
):
    """Close all open positions."""
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
