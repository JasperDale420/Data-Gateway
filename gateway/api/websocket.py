"""WebSocket endpoint with authentication and heartbeat."""

import asyncio
import json
import re
import time
import uuid
from typing import Any

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from gateway.api.deps import get_authenticator, get_connection_manager
from gateway.config import Settings, get_settings
from gateway.core.auth import ClientAuthenticator
from gateway.core.connections import ConnectionManager, is_benign_ws_close_error
from gateway.core.flow_fanout import FLOW_FEED
from gateway.core.logger import logger
from gateway.core.security import get_input_validator

router = APIRouter(tags=["websocket"])

# Heartbeat settings per PRD
HEARTBEAT_TIMEOUT = 15  # seconds
MAX_MISSED_HEARTBEATS = 4
_REDIS_STREAM_ID = re.compile(r"^(?:\$|\d+-\d+)$")


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    auth: ClientAuthenticator = Depends(get_authenticator),
    connections: ConnectionManager = Depends(get_connection_manager),
    settings: Settings = Depends(get_settings),
):
    """Main WebSocket endpoint with authentication."""
    connection_id = str(uuid.uuid4())
    connection = await connections.connect(connection_id, websocket)
    if connection is None:
        return
    heartbeat_task = None

    try:
        # Wait for authentication with timeout
        authenticated = await _wait_for_auth(
            websocket=websocket,
            connection_id=connection_id,
            auth=auth,
            connections=connections,
            timeout=settings.auth_timeout_seconds,
        )

        if not authenticated:
            await websocket.close(code=4001, reason="Authentication failed or timed out")
            return

        # Send auth success
        await websocket.send_json(
            {
                "type": "auth_result",
                "status": "ok",
                "client_id": connection.client_id,
                "message": "Authenticated successfully",
            }
        )

        # Shared timestamp updated by the message loop on every received message.
        # Used by the heartbeat loop to detect dead clients that keep TCP open but
        # stop sending data.  A single-element list is used so both coroutines
        # reference the same mutable container (safe under asyncio — no true
        # concurrency).
        last_received: list[float] = [time.time()]

        # Start heartbeat task
        heartbeat_task = asyncio.create_task(_heartbeat_loop(websocket, connection_id, last_received, settings))

        # Main message loop
        await _message_loop(
            websocket,
            connection_id,
            connections,
            max_message_size=settings.ws_max_message_size,
            last_received=last_received,
        )

    except WebSocketDisconnect:
        logger.info("client_disconnected", connection_id=connection_id)
    except Exception as e:
        # Downgrade to debug for errors on already-dead connections (close code 1006,
        # missing transfer_data_task, etc.) — these are normal during reconnect cycles
        # and generate massive log noise at ERROR level.
        if is_benign_ws_close_error(e):
            logger.debug("websocket_error_on_closed", connection_id=connection_id, error=str(e))
        else:
            logger.error("websocket_error", connection_id=connection_id, error=str(e))
    finally:
        if heartbeat_task:
            heartbeat_task.cancel()
        # Ensure upstream subscriptions are cleaned up
        try:
            from gateway.api.deps import get_multiplexer

            multiplexer = get_multiplexer()
            await multiplexer.client_disconnect(connection_id)
        except RuntimeError:
            # Multiplexer not initialized
            pass
        except Exception as e:
            logger.warning("multiplexer_disconnect_error", connection_id=connection_id, error=str(e))

        # Drop any UW flow fan-out subscriptions held by this connection.
        try:
            from gateway.api.deps import get_flow_fanout

            flow_fanout = get_flow_fanout()
            if flow_fanout is not None:
                flow_fanout.client_disconnect(connection_id)
        except Exception as e:
            logger.warning("flow_fanout_disconnect_error", connection_id=connection_id, error=str(e))

        await connections.disconnect(connection_id)


async def _heartbeat_loop(
    websocket: WebSocket,
    connection_id: str,
    last_received: list[float],
    settings: Settings,
) -> None:
    """Send heartbeats and disconnect clients that stop responding.

    Per PRD: Send heartbeat every ``ws_heartbeat_interval`` seconds,
    disconnect after ``MAX_MISSED_HEARTBEATS`` missed, or after
    ``ws_idle_timeout`` seconds of silence.

    Detection strategy:
    - The JSON heartbeat is still sent for backward compatibility (clients may
      use it for keep-alive or latency measurement).
    - The *real* liveness check is the ``last_received`` timestamp, which is
      updated by ``_message_loop`` every time the client sends *any* message
      (subscribe, heartbeat ack, ping, etc.).
    - If ``last_received`` is older than ``heartbeat_interval * MAX_MISSED_HEARTBEATS``
      seconds the client is considered dead and the connection is closed.
    - If ``last_received`` exceeds ``ws_idle_timeout`` the connection is closed
      with code 4003 (idle timeout).

    This correctly handles the case where TCP stays open but the remote end is
    unresponsive (e.g. laptop lid closed, network partition without RST).
    """
    heartbeat_interval = settings.ws_heartbeat_interval
    send_failures = 0

    while True:
        await asyncio.sleep(heartbeat_interval)

        # --- 1. Send the JSON heartbeat (best-effort, backward compat) ---
        try:
            await websocket.send_json(
                {
                    "type": "heartbeat",
                    "ts": int(time.time()),
                }
            )
            send_failures = 0
            logger.debug("heartbeat_sent", connection_id=connection_id)
        except Exception as e:
            # Downgrade to debug when sending to an already-closed connection —
            # the reconnect cycle handles recovery, no need to alarm.
            if is_benign_ws_close_error(e):
                logger.debug("heartbeat_send_failed_closed", connection_id=connection_id, error=str(e))
            else:
                logger.warning("heartbeat_send_failed", connection_id=connection_id, error=str(e))
            send_failures += 1

            if send_failures >= MAX_MISSED_HEARTBEATS:
                logger.warning(
                    "heartbeat_send_disconnect",
                    connection_id=connection_id,
                    send_failures=send_failures,
                )
                # nosemgrep: empire-no-bare-exception -- close on possibly-dead socket; failure is expected and logged at debug
                try:
                    await websocket.close(code=4002, reason="Heartbeat timeout")
                except Exception:
                    logger.debug("heartbeat_close_failed", connection_id=connection_id)
                return

        # --- 2. Check client liveness via last_received timestamp ---
        silence = time.time() - last_received[0]
        max_silence = heartbeat_interval * MAX_MISSED_HEARTBEATS

        if silence > max_silence:
            logger.warning(
                "heartbeat_timeout_disconnect",
                connection_id=connection_id,
                silence_seconds=round(silence, 1),
                max_silence_seconds=max_silence,
            )
            # nosemgrep: empire-no-bare-exception -- close on possibly-dead socket; failure is expected and logged at debug
            try:
                await websocket.close(code=4002, reason="Heartbeat timeout")
            except Exception:
                logger.debug("heartbeat_close_failed", connection_id=connection_id)
            return

        # --- 3. Check idle timeout (ws_idle_timeout) ---
        if silence > settings.ws_idle_timeout:
            logger.info("ws_idle_disconnect", connection_id=connection_id, idle_seconds=round(silence, 1))
            # nosemgrep: empire-no-bare-exception -- close on possibly-dead socket; failure is expected and logged at debug
            try:
                await websocket.close(code=4003, reason="Idle timeout")
            except Exception:
                logger.debug("idle_close_failed", connection_id=connection_id)
            return


async def _wait_for_auth(
    websocket: WebSocket,
    connection_id: str,
    auth: ClientAuthenticator,
    connections: ConnectionManager,
    timeout: int,
) -> bool:
    """Wait for authentication message within timeout."""
    try:
        message = await asyncio.wait_for(
            websocket.receive_json(),
            timeout=timeout,
        )
    except TimeoutError:
        logger.warning("auth_timeout", connection_id=connection_id)
        await websocket.send_json(
            {
                "type": "auth_result",
                "status": "error",
                "error_code": "GW-E2004",
                "message": "Authentication timeout",
            }
        )
        return False
    except WebSocketDisconnect as e:
        # Client dropped the connection before sending auth (TCP reset, tab closed,
        # 1006 unclean close). Not an error — just a short-lived connection.
        logger.info(
            "auth_client_disconnected",
            connection_id=connection_id,
            code=getattr(e, "code", None),
        )
        return False
    except Exception as e:
        # Downgrade benign close races (1006, transport gone, "not connected")
        # to info — the client went away before completing auth, which is noise
        # rather than a server-side problem to alert on.
        if is_benign_ws_close_error(e):
            logger.info("auth_client_disconnected", connection_id=connection_id, error=str(e))
        else:
            logger.error("auth_receive_error", connection_id=connection_id, error=str(e))
        return False

    # Validate message format
    if not isinstance(message, dict):
        await websocket.send_json(
            {
                "type": "auth_result",
                "status": "error",
                "error_code": "GW-E2002",
                "message": "Invalid message format",
            }
        )
        return False

    action = message.get("action")
    request_id = message.get("request_id")

    if action != "auth":
        response = {
            "type": "auth_result",
            "status": "error",
            "error_code": "GW-E2003",
            "message": f"Expected 'auth' action, got '{action}'",
        }
        if request_id:
            response["request_id"] = request_id
        await websocket.send_json(response)
        return False

    # Authenticate
    api_key = message.get("key", "")
    client = auth.authenticate(api_key)

    if not client:
        response = {
            "type": "auth_result",
            "status": "error",
            "error_code": "GW-E2001",
            "message": "Invalid API key",
        }
        if request_id:
            response["request_id"] = request_id
        await websocket.send_json(response)
        return False

    # Mark connection as authenticated
    await connections.authenticate(connection_id, client)
    return True


async def _message_loop(
    websocket: WebSocket,
    connection_id: str,
    connections: ConnectionManager,
    max_message_size: int | None = None,
    last_received: list[float] | None = None,
) -> None:
    """Handle messages after authentication.

    NOTE: The application-level size check below runs AFTER starlette has
    already received the full frame into Python memory. To prevent OOM from
    very large attack frames, the ASGI server (uvicorn) MUST be configured
    with --ws-max-size at or below GATEWAY_WS_MAX_MESSAGE_SIZE * a small
    multiplier. The check here exists to (a) emit clean error codes when
    a message is within the ASGI cap but over policy, and (b) terminate
    the connection on oversize so the client can't repeat the attack on
    the same socket.
    """
    resolved_max_size = max_message_size if max_message_size is not None else get_settings().ws_max_message_size
    max_bytes = max(1, int(resolved_max_size))
    while True:
        try:
            raw = await websocket.receive()
            if raw.get("text") is not None:
                raw_text = raw["text"]
                if len(raw_text.encode("utf-8")) > max_bytes:
                    logger.warning(
                        "ws_message_oversize",
                        connection_id=connection_id,
                        size=len(raw_text.encode("utf-8")),
                        max_bytes=max_bytes,
                    )
                    # nosemgrep: empire-no-bare-exception -- best-effort error notice to a client we are about to disconnect; logged at debug
                    try:
                        await websocket.send_json(
                            {
                                "type": "error",
                                "error_code": "GW-E8005",
                                "message": "WebSocket message exceeds size limit",
                            }
                        )
                    except Exception:
                        logger.debug("ws_oversize_notify_failed", connection_id=connection_id)
                    # Close 1009 (Message Too Big) and terminate the loop so the
                    # client cannot retry oversize frames on the same connection.
                    # nosemgrep: empire-no-bare-exception -- close on possibly-dead socket; failure is expected and logged at debug
                    try:
                        await websocket.close(code=1009, reason="Message too large")
                    except Exception:
                        logger.debug("ws_oversize_close_failed", connection_id=connection_id)
                    return
                message = json.loads(raw_text)
            elif raw.get("bytes") is not None:
                raw_bytes = raw["bytes"]
                if len(raw_bytes) > max_bytes:
                    logger.warning(
                        "ws_message_oversize",
                        connection_id=connection_id,
                        size=len(raw_bytes),
                        max_bytes=max_bytes,
                    )
                    # nosemgrep: empire-no-bare-exception -- best-effort error notice to a client we are about to disconnect; logged at debug
                    try:
                        await websocket.send_json(
                            {
                                "type": "error",
                                "error_code": "GW-E8005",
                                "message": "WebSocket message exceeds size limit",
                            }
                        )
                    except Exception:
                        logger.debug("ws_oversize_notify_failed", connection_id=connection_id)
                    # nosemgrep: empire-no-bare-exception -- close on possibly-dead socket; failure is expected and logged at debug
                    try:
                        await websocket.close(code=1009, reason="Message too large")
                    except Exception:
                        logger.debug("ws_oversize_close_failed", connection_id=connection_id)
                    return
                message = json.loads(raw_bytes.decode("utf-8"))
            else:
                continue
        except WebSocketDisconnect:
            raise
        except RuntimeError as e:
            # Starlette raises RuntimeError after a disconnect frame is received.
            # Treat it as a terminal disconnect so we don't spin indefinitely.
            if "disconnect message has been received" in str(e):
                logger.info("receive_after_disconnect", connection_id=connection_id)
                break
            logger.warning("receive_runtime_error", connection_id=connection_id, error=str(e))
            continue
        except Exception as e:
            logger.warning("receive_error", connection_id=connection_id, error=str(e))
            continue

        # Record that we received a message from the client (liveness signal)
        if last_received is not None:
            last_received[0] = time.time()

        # Handle message based on action
        request_id = message.get("request_id")

        response = await _handle_message(message, connection_id, connections)

        if request_id:
            response["request_id"] = request_id

        await websocket.send_json(response)
        if response.get("type") == "subscription_ack" and response.get("resume_supported") is True:
            from gateway.api.deps import get_flow_fanout

            flow_fanout = get_flow_fanout()
            if flow_fanout is not None:
                flow_fanout.launch_pending_replay(connection_id)


async def _handle_message(
    message: dict[str, Any],
    connection_id: str,
    connections: ConnectionManager,
) -> dict[str, Any]:
    """Route and handle a message."""
    validator = get_input_validator()
    action = message.get("action", "")

    if action == "heartbeat":
        # Client pong response
        return {"type": "heartbeat_ack"}

    if action == "ping":
        return {"type": "pong"}

    if action == "subscribe":
        provider = message.get("provider", "alpaca")
        feeds = message.get("feeds")
        if feeds is None:
            feed = message.get("feed", "stock_bars")
            feeds = [feed]
        if not isinstance(feeds, list) or not feeds:
            return {
                "type": "error",
                "error_code": "GW-E8001",
                "message": "feeds must be a non-empty list",
            }
        feeds = list(dict.fromkeys(feeds))

        symbols = message.get("symbols", [])
        if not isinstance(symbols, list):
            return {
                "type": "error",
                "error_code": "GW-E8001",
                "message": "symbols must be a list",
            }

        connection = connections.get(connection_id)
        if not connection or not connection.client:
            return {
                "type": "error",
                "error_code": "GW-E2001",
                "message": "Not authenticated",
            }

        # UW flow accepts an empty symbol list (firehose / ALL); only validate
        # when symbols are present. Alpaca feeds still require ≥1 symbol.
        flow_request = _is_uw_flow_request(provider, feeds)
        if symbols or not flow_request:
            validation_error = validator.validate_symbols_array(
                symbols, max_symbols=connection.client.permissions.max_symbols
            )
            if validation_error:
                return {
                    "type": "error",
                    "error_code": validation_error.code,
                    "message": validation_error.message,
                }

        # Enforce provider permissions
        if not _has_provider_permission(connection.client, provider):
            return {
                "type": "error",
                "error_code": "GW-E2006",
                "message": f"Provider access denied: {provider}",
            }

        # Enforce feed permissions
        for feed in feeds:
            required_feed = _normalize_feed_permission(feed)
            if not _has_feed_permission(connection.client, required_feed):
                return {
                    "type": "error",
                    "error_code": "GW-E2007",
                    "message": f"Feed access denied: {required_feed}",
                }

        # Enforce total subscription limit. Flow feeds are STORED under the
        # canonical FLOW_FEED key (flow_alerts:<symbol>), not the caller's
        # alias (flow:<symbol>). Computing the quota off the raw alias would
        # make an idempotent re-subscribe of the same flow symbol look like a
        # brand-new slot (the stored flow_alerts:<symbol> never matches the
        # alias flow:<symbol>), wrongly rejecting at the cap. Normalize flow
        # feeds to FLOW_FEED here so re-subscribes are free.
        new_entries = {f"{FLOW_FEED if (flow_request and f in _FLOW_FEEDS) else f}:{s}" for f in feeds for s in symbols}
        # A symbol-less UW flow request is the firehose: it produces no per-symbol
        # entries above, so without counting its sentinel here it could be added
        # even at the cap (quota off-by-one). Account it as a single entry that
        # subsumes any prior per-symbol flow entries for this connection.
        if flow_request and not symbols:
            new_entries.add(f"{FLOW_FEED}:*")
            prior_flow_symbols = {
                s for s in connection.subscriptions if s.startswith(f"{FLOW_FEED}:") and s != f"{FLOW_FEED}:*"
            }
        else:
            prior_flow_symbols = set()
        current = len(connection.subscriptions)
        # Per-symbol flow entries the firehose will subsume don't add to the
        # post-subscribe total, so exclude them from the count.
        total_after = current - len(prior_flow_symbols) + len(new_entries - connection.subscriptions)
        if total_after > connection.client.permissions.ws_subscriptions_max:
            return {
                "type": "error",
                "error_code": "GW-E8002",
                "message": (f"Maximum {connection.client.permissions.ws_subscriptions_max} subscriptions allowed"),
            }

        # UW flow channel: route to the fan-out (additive; does not touch the
        # Alpaca multiplexer). An empty symbols list ⇒ ALL flow (firehose).
        if _is_uw_flow_request(provider, feeds):
            from gateway.api.deps import get_flow_fanout

            flow_fanout = get_flow_fanout()
            if flow_fanout is None:
                return {
                    "type": "subscription_ack",
                    "status": "error",
                    "error_code": "GW-E5002",
                    "message": "Flow fan-out not initialized — UW flow streaming unavailable",
                    "provider": provider,
                    "feeds": feeds,
                }
            after_stream_id = message.get("after_stream_id")
            if after_stream_id is not None and (
                not isinstance(after_stream_id, str) or _REDIS_STREAM_ID.fullmatch(after_stream_id) is None
            ):
                return {
                    "type": "subscription_ack",
                    "status": "error",
                    "error_code": "GW-E8001",
                    "message": "after_stream_id must be a Redis stream ID or '$'",
                    "provider": provider,
                    "feeds": [FLOW_FEED],
                }
            if after_stream_id is None:
                flow_fanout.subscribe(connection_id, symbols)
                replay_state: dict[str, Any] = {}
            else:
                replay_state = await flow_fanout.prepare_replay(
                    connection_id,
                    symbols,
                    after_stream_id=after_stream_id,
                )
            # Record the subscription for status/quota accounting. An empty
            # symbols list is the firehose (ALL flow); without an explicit
            # entry it would count as zero subscriptions and be invisible in
            # status/quota, so register it under a sentinel.
            if symbols:
                connection.subscriptions.update({f"{FLOW_FEED}:{s}" for s in symbols})
            else:
                # Firehose subsumes any prior per-symbol flow entries — the
                # fan-out drops those buckets internally (flow_fanout.subscribe),
                # so leaving them in connection.subscriptions would be stale
                # accounting that double-counts against the quota and misreports
                # status. Clear them and keep only the firehose sentinel.
                stale_flow = {
                    s for s in connection.subscriptions if s.startswith(f"{FLOW_FEED}:") and s != f"{FLOW_FEED}:*"
                }
                connection.subscriptions.difference_update(stale_flow)
                connection.subscriptions.add(f"{FLOW_FEED}:*")
            ack: dict[str, Any] = {
                "type": "subscription_ack",
                "status": "ok",
                "provider": provider,
                "feeds": [FLOW_FEED],
                "subscribed": sorted(symbols),
                "failed": [],
                **replay_state,
            }
            # The fan-out exists but no producer is attached (data sink / Redis
            # UW poller never started): the subscribe is registered but no flow
            # envelope can ever arrive. Surface that instead of a bare "ok" so
            # the client isn't silently starved.
            if not flow_fanout.producer_wired:
                ack["status"] = "warning"
                ack["warning_code"] = "GW-W5003"
                ack["message"] = (
                    "Flow producer not wired — subscription registered but no flow data will arrive "
                    "until the UW poller starts"
                )
            elif after_stream_id is not None and not ack.get("resume_supported"):
                ack["status"] = "warning"
                ack["warning_code"] = "GW-W5004"
                ack["message"] = "Durable flow replay is unavailable; reconnect completeness cannot be guaranteed"
            return ack

        # Try to use multiplexer if available
        try:
            from gateway.api.deps import get_multiplexer
            from gateway.core.stream import AlpacaStreamType

            multiplexer = get_multiplexer()
            responses = []
            subscribed_feeds: list[str] = []
            for feed in feeds:
                stream_type = AlpacaStreamType.from_feed(feed)

                # Map feed to subscription type
                bars = symbols if "bars" in feed else None
                quotes = symbols if "quotes" in feed else None
                trades = symbols if "trades" in feed else None
                news = symbols if "news" in feed else None

                response = await multiplexer.client_subscribe(
                    client_id=connection_id,
                    stream_type=stream_type,
                    bars=bars,
                    quotes=quotes,
                    trades=trades,
                    news=news,
                )
                if response.get("status") == "error":
                    # Roll back any prior subscriptions for this request
                    rollback_errors: list[str] = []
                    for rollback_feed in subscribed_feeds:
                        try:
                            rollback_stream = AlpacaStreamType.from_feed(rollback_feed)
                            rollback_bars = symbols if "bars" in rollback_feed else None
                            rollback_quotes = symbols if "quotes" in rollback_feed else None
                            rollback_trades = symbols if "trades" in rollback_feed else None
                            rollback_news = symbols if "news" in rollback_feed else None
                            await multiplexer.client_unsubscribe(
                                client_id=connection_id,
                                stream_type=rollback_stream,
                                bars=rollback_bars,
                                quotes=rollback_quotes,
                                trades=rollback_trades,
                                news=rollback_news,
                            )
                        except Exception as rollback_err:
                            rollback_errors.append(f"{rollback_feed}: {rollback_err}")
                            logger.error(
                                "subscription_rollback_failed",
                                connection_id=connection_id,
                                feed=rollback_feed,
                                error=str(rollback_err),
                            )
                    error_response: dict[str, Any] = {
                        "type": "subscription_ack",
                        "status": "error",
                        "provider": provider,
                        "feeds": feeds,
                        "error_code": response.get("error_code"),
                        "message": response.get("message"),
                    }
                    if rollback_errors:
                        error_response["rollback_errors"] = rollback_errors
                    return error_response

                subscribed_feeds.append(feed)
                responses.append(response)

            subscribed: set[str] = set()
            failed: list[str] = []
            all_warnings: list[str] = []
            for response in responses:
                subscribed.update(response.get("subscribed", []))
                failed.extend(response.get("failed", []))
                all_warnings.extend(response.get("warnings", []))

            # Track subscriptions locally
            connection.subscriptions.update({f"{feed}:{s}" for feed in feeds for s in symbols})
            result: dict[str, Any] = {
                "type": "subscription_ack",
                "status": "ok",
                "provider": provider,
                "feeds": feeds,
                "subscribed": sorted(subscribed),
                "failed": failed,
            }
            if all_warnings:
                result["warnings"] = all_warnings
            return result
        except RuntimeError:
            # Multiplexer not initialized — return honest error so clients can retry
            logger.error(
                "subscribe_multiplexer_unavailable",
                provider=provider,
                feeds=feeds,
                symbols=symbols,
            )
            return {
                "type": "subscription_ack",
                "status": "error",
                "error_code": "GW-E5001",
                "message": "Stream multiplexer not initialized — upstream data unavailable",
                "provider": provider,
                "feeds": feeds,
                "subscribed": [],
                "failed": sorted(set(symbols)),
            }

    if action == "unsubscribe":
        provider = message.get("provider", "alpaca")
        feeds = message.get("feeds")
        if feeds is None:
            feed = message.get("feed", "stock_bars")
            feeds = [feed]
        if not isinstance(feeds, list) or not feeds:
            return {
                "type": "error",
                "error_code": "GW-E8001",
                "message": "feeds must be a non-empty list",
            }
        feeds = list(dict.fromkeys(feeds))
        symbols = message.get("symbols", [])
        if not isinstance(symbols, list):
            return {
                "type": "error",
                "error_code": "GW-E8001",
                "message": "symbols must be a list",
            }
        connection = connections.get(connection_id)
        if not connection or not connection.client:
            return {
                "type": "error",
                "error_code": "GW-E2001",
                "message": "Not authenticated",
            }

        if symbols:
            validation_error = validator.validate_symbols_array(
                symbols, max_symbols=connection.client.permissions.max_symbols
            )
            if validation_error:
                return {
                    "type": "error",
                    "error_code": validation_error.code,
                    "message": validation_error.message,
                }

        # Enforce provider permissions
        if not _has_provider_permission(connection.client, provider):
            return {
                "type": "error",
                "error_code": "GW-E2006",
                "message": f"Provider access denied: {provider}",
            }

        # UW flow channel: route to the fan-out. Empty symbols ⇒ drop all flow.
        if _is_uw_flow_request(provider, feeds):
            from gateway.api.deps import get_flow_fanout

            flow_fanout = get_flow_fanout()
            if flow_fanout is not None:
                flow_fanout.unsubscribe(connection_id, symbols)
            if symbols:
                for symbol in symbols:
                    connection.subscriptions.discard(f"{FLOW_FEED}:{symbol}")
            else:
                # Empty symbols ⇒ the fan-out's client_disconnect drops EVERY
                # flow bucket for this connection (firehose + all per-symbol).
                # Mirror that fully in connection accounting: discarding only
                # the firehose sentinel would leave stale flow_alerts:<symbol>
                # entries from earlier per-symbol subscribes, which would
                # double-count against the quota and misreport status.
                stale_flow = {s for s in connection.subscriptions if s.startswith(f"{FLOW_FEED}:")}
                connection.subscriptions.difference_update(stale_flow)
            return {
                "type": "unsubscription_ack",
                "status": "ok",
                "provider": provider,
                "feeds": [FLOW_FEED],
                "unsubscribed": sorted(set(symbols)),
            }

        # Try to use multiplexer if available
        try:
            from gateway.api.deps import get_multiplexer
            from gateway.core.stream import AlpacaStreamType

            multiplexer = get_multiplexer()
            for feed in feeds:
                stream_type = AlpacaStreamType.from_feed(feed)

                bars = symbols if "bars" in feed else None
                quotes = symbols if "quotes" in feed else None
                trades = symbols if "trades" in feed else None
                news = symbols if "news" in feed else None

                await multiplexer.client_unsubscribe(
                    client_id=connection_id,
                    stream_type=stream_type,
                    bars=bars,
                    quotes=quotes,
                    trades=trades,
                    news=news,
                )

            # Track subscriptions locally
            for feed in feeds:
                for symbol in symbols:
                    connection.subscriptions.discard(f"{feed}:{symbol}")
            return {
                "type": "unsubscription_ack",
                "status": "ok",
                "provider": provider,
                "feeds": feeds,
                "unsubscribed": sorted(set(symbols)),
            }
        except RuntimeError:
            # Multiplexer not initialized
            for feed in feeds:
                for symbol in symbols:
                    connection.subscriptions.discard(f"{feed}:{symbol}")
            return {
                "type": "unsubscription_ack",
                "status": "ok",
                "provider": provider,
                "feeds": feeds,
                "unsubscribed": sorted(set(symbols)),
            }

    if action == "status":
        connection = connections.get(connection_id)
        return {
            "type": "status",
            "client_id": connection.client_id if connection else None,
            "subscriptions": list(connection.subscriptions) if connection else [],
            "authenticated": connection.authenticated if connection else False,
        }

    # Unknown action
    return {
        "type": "error",
        "error_code": "GW-E3001",
        "message": f"Unknown action: {action}",
    }


def _has_provider_permission(client: Any, provider: str) -> bool:
    allowed = set(client.permissions.providers or [])
    if not allowed:
        return True
    provider = provider.lower()
    if provider in allowed:
        return True
    if provider == "uw" and "unusual_whales" in allowed:
        return True
    return bool(provider == "yf" and "yfinance" in allowed)


def _normalize_feed_permission(feed: str) -> str:
    f = (feed or "").lower()
    if "option" in f:
        return "options"
    if "news" in f:
        return "news"
    if "bars" in f:
        return "bars"
    if "quotes" in f:
        return "quotes"
    if "trades" in f:
        return "trades"
    if "flow" in f:
        return "flow"
    return f


# UW provider aliases and the feed names that route to the flow fan-out instead
# of the Alpaca multiplexer.
_UW_PROVIDERS = {"uw", "unusual_whales"}
_FLOW_FEEDS = {"flow", "flow_alerts"}


def _is_uw_flow_request(provider: str, feeds: list[str]) -> bool:
    """True when this subscribe/unsubscribe targets the UW flow channel."""
    if provider.lower() not in _UW_PROVIDERS:
        return False
    return any(str(f).lower() in _FLOW_FEEDS for f in feeds)


def _has_feed_permission(client: Any, required_feed: str) -> bool:
    allowed = set(client.permissions.feeds or [])
    if not allowed:
        return True
    return required_feed in allowed
