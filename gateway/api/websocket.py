"""WebSocket endpoint with authentication and heartbeat."""

import asyncio
import json
import time
import uuid
from typing import Any

import structlog
from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from gateway.api.deps import get_authenticator, get_connection_manager
from gateway.config import Settings, get_settings
from gateway.core.auth import ClientAuthenticator
from gateway.core.connections import ConnectionManager
from gateway.core.security import get_input_validator

logger = structlog.get_logger()

router = APIRouter(tags=["websocket"])

# Heartbeat settings per PRD
HEARTBEAT_INTERVAL = 30  # seconds
HEARTBEAT_TIMEOUT = 10  # seconds
MAX_MISSED_HEARTBEATS = 3


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

        # Start heartbeat task
        heartbeat_task = asyncio.create_task(_heartbeat_loop(websocket, connection_id))

        # Main message loop
        await _message_loop(
            websocket,
            connection_id,
            connections,
            max_message_size=settings.ws_max_message_size,
        )

    except WebSocketDisconnect:
        logger.info("client_disconnected", connection_id=connection_id)
    except Exception as e:
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

        await connections.disconnect(connection_id)


async def _heartbeat_loop(websocket: WebSocket, connection_id: str) -> None:
    """Send heartbeats and monitor for pong responses.

    Per PRD: Send heartbeat every 30s, disconnect after 3 missed.
    """
    missed_heartbeats = 0

    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)

        try:
            await websocket.send_json(
                {
                    "type": "heartbeat",
                    "ts": int(time.time()),
                }
            )
            missed_heartbeats = 0
            logger.debug("heartbeat_sent", connection_id=connection_id)
        except Exception as e:
            logger.warning("heartbeat_send_failed", connection_id=connection_id, error=str(e))
            missed_heartbeats += 1

            if missed_heartbeats >= MAX_MISSED_HEARTBEATS:
                logger.warning(
                    "heartbeat_timeout_disconnect",
                    connection_id=connection_id,
                    missed=missed_heartbeats,
                )
                await websocket.close(code=4002, reason="Heartbeat timeout")
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
    except Exception as e:
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
) -> None:
    """Handle messages after authentication."""
    resolved_max_size = max_message_size if max_message_size is not None else get_settings().ws_max_message_size
    max_bytes = max(1, int(resolved_max_size))
    while True:
        try:
            raw = await websocket.receive()
            if raw.get("text") is not None:
                raw_text = raw["text"]
                if len(raw_text.encode("utf-8")) > max_bytes:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "error_code": "GW-E8005",
                            "message": "WebSocket message exceeds size limit",
                        }
                    )
                    continue
                message = json.loads(raw_text)
            elif raw.get("bytes") is not None:
                raw_bytes = raw["bytes"]
                if len(raw_bytes) > max_bytes:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "error_code": "GW-E8005",
                            "message": "WebSocket message exceeds size limit",
                        }
                    )
                    continue
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

        # Handle message based on action
        request_id = message.get("request_id")

        response = await _handle_message(message, connection_id, connections)

        if request_id:
            response["request_id"] = request_id

        await websocket.send_json(response)


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

        # Enforce total subscription limit
        new_entries = {f"{feed}:{s}" for feed in feeds for s in symbols}
        current = len(connection.subscriptions)
        total_after = current + len(new_entries - connection.subscriptions)
        if total_after > connection.client.permissions.ws_subscriptions_max:
            return {
                "type": "error",
                "error_code": "GW-E8002",
                "message": (f"Maximum {connection.client.permissions.ws_subscriptions_max} subscriptions allowed"),
            }

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
                    for rollback_feed in subscribed_feeds:
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
                    return {
                        "type": "subscription_ack",
                        "status": "error",
                        "provider": provider,
                        "feeds": feeds,
                        "error_code": response.get("error_code"),
                        "message": response.get("message"),
                    }

                subscribed_feeds.append(feed)
                responses.append(response)

            subscribed: set[str] = set()
            failed: list[str] = []
            for response in responses:
                subscribed.update(response.get("subscribed", []))
                failed.extend(response.get("failed", []))

            # Track subscriptions locally
            connection.subscriptions.update({f"{feed}:{s}" for feed in feeds for s in symbols})
            return {
                "type": "subscription_ack",
                "status": "ok",
                "provider": provider,
                "feeds": feeds,
                "subscribed": sorted(subscribed),
                "failed": failed,
            }
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
    return f


def _has_feed_permission(client: Any, required_feed: str) -> bool:
    allowed = set(client.permissions.feeds or [])
    if not allowed:
        return True
    return required_feed in allowed
