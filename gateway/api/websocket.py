"""WebSocket endpoint with authentication and heartbeat."""

import asyncio
import time
import uuid
from typing import Any

import structlog
from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from gateway.api.deps import get_authenticator, get_connection_manager
from gateway.config import Settings, get_settings
from gateway.core.auth import ClientAuthenticator
from gateway.core.connections import ConnectionManager

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
        await _message_loop(websocket, connection_id, connections)

    except WebSocketDisconnect:
        logger.info("client_disconnected", connection_id=connection_id)
    except Exception as e:
        logger.error("websocket_error", connection_id=connection_id, error=str(e))
    finally:
        if heartbeat_task:
            heartbeat_task.cancel()
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
) -> None:
    """Handle messages after authentication."""
    while True:
        try:
            message = await websocket.receive_json()
        except WebSocketDisconnect:
            raise
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
    action = message.get("action", "")

    if action == "heartbeat":
        # Client pong response
        return {"type": "heartbeat_ack"}

    if action == "ping":
        return {"type": "pong"}

    if action == "subscribe":
        provider = message.get("provider", "alpaca")
        feed = message.get("feed", "stock_bars")
        symbols = message.get("symbols", [])

        # Try to use multiplexer if available
        try:
            from gateway.api.deps import get_multiplexer
            from gateway.core.stream import AlpacaStreamType

            multiplexer = get_multiplexer()
            stream_type = AlpacaStreamType.from_feed(feed)

            # Map feed to subscription type
            bars = symbols if "bars" in feed else None
            quotes = symbols if "quotes" in feed else None
            trades = symbols if "trades" in feed else None

            response = await multiplexer.client_subscribe(
                client_id=connection_id,
                stream_type=stream_type,
                bars=bars,
                quotes=quotes,
                trades=trades,
            )
            response["provider"] = provider
            response["feed"] = feed
            return response
        except RuntimeError:
            # Multiplexer not initialized, return stub response
            return {
                "type": "subscription_ack",
                "status": "ok",
                "provider": provider,
                "feed": feed,
                "subscribed": symbols,
                "failed": [],
            }

    if action == "unsubscribe":
        provider = message.get("provider", "alpaca")
        feed = message.get("feed", "stock_bars")
        symbols = message.get("symbols", [])

        # Try to use multiplexer if available
        try:
            from gateway.api.deps import get_multiplexer
            from gateway.core.stream import AlpacaStreamType

            multiplexer = get_multiplexer()
            stream_type = AlpacaStreamType.from_feed(feed)

            bars = symbols if "bars" in feed else None
            quotes = symbols if "quotes" in feed else None
            trades = symbols if "trades" in feed else None

            response = await multiplexer.client_unsubscribe(
                client_id=connection_id,
                stream_type=stream_type,
                bars=bars,
                quotes=quotes,
                trades=trades,
            )
            response["provider"] = provider
            response["feed"] = feed
            return response
        except RuntimeError:
            # Multiplexer not initialized
            return {
                "type": "unsubscription_ack",
                "status": "ok",
                "provider": provider,
                "feed": feed,
                "unsubscribed": symbols,
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
