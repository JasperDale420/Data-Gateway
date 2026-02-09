"""Historical Replay API endpoints.

Implements replay session management and WebSocket streaming as specified in PRD.
"""

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import Any, Protocol

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from gateway.api.deps import get_authenticator, get_endpoint_rate_limiter, require_api_key
from gateway.config import get_settings
from gateway.core.auth import ClientAuthenticator
from gateway.core.rate_limiter import EndpointRateLimitExceeded
from gateway.core.replay import (
    ReplayConfig,
    ReplayState,
    get_replay_manager,
)
from gateway.schemas import SuccessResponse

router = APIRouter(prefix="/api/v1/replay", tags=["Historical Replay"])


class _ReplayControlSocket(Protocol):
    async def receive_json(self) -> dict[str, Any]: ...


# ─────────────────────────────────────────────────────────────────────────────
# Request/Response Models
# ─────────────────────────────────────────────────────────────────────────────


class CreateReplaySessionRequest(BaseModel):
    """Request body for creating a replay session."""

    name: str = Field(..., description="Session name for identification")
    symbols: list[str] = Field(
        ...,
        description="List of symbols to replay",
        min_length=1,
        max_length=100,
    )
    feeds: list[str] = Field(
        ...,
        description="Data feeds to include (bars, quotes, trades)",
        min_length=1,
    )
    start: datetime = Field(..., description="Start timestamp for replay")
    end: datetime = Field(..., description="End timestamp for replay")
    speed: float = Field(
        default=1.0,
        description="Playback speed multiplier (1.0 = real-time)",
        gt=0,
        le=100,
    )
    include_premarket: bool = Field(
        default=False,
        description="Include pre-market data",
    )


class ReplaySessionResponse(BaseModel):
    """Response for replay session creation."""

    session_id: str
    ws_endpoint: str
    estimated_messages: int
    estimated_duration_seconds: float


class ReplayControlRequest(BaseModel):
    """Request for replay control actions."""

    action: str = Field(
        ...,
        description="Control action: pause, resume, seek, stop",
    )
    speed: float | None = Field(
        default=None,
        description="New speed for resume action",
    )
    timestamp: datetime | None = Field(
        default=None,
        description="Target timestamp for seek action",
    )


class ReplaySessionStatusResponse(BaseModel):
    """Response for session status."""

    session_id: str
    name: str
    state: str
    symbols: list[str]
    feeds: list[str]
    start: str
    end: str
    speed: float
    progress: float
    messages_sent: int
    sequence: int
    created_at: str
    started_at: str | None
    ended_at: str | None


# ─────────────────────────────────────────────────────────────────────────────
# REST Endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/sessions",
    response_model=ReplaySessionResponse,
    summary="Create replay session",
    description="Create a new historical replay session for backtesting.",
)
async def create_replay_session(
    request: CreateReplaySessionRequest,
    client: Any = Depends(require_api_key),
) -> ReplaySessionResponse:
    """Create a replay session."""
    # Enforce concurrent session limit (PRD 7.5.3)
    ep_limiter = get_endpoint_rate_limiter()

    manager = get_replay_manager()
    settings = get_settings()
    if not manager.has_data_loader() and not settings.allow_stub_data:
        raise HTTPException(status_code=501, detail="Replay data loader not configured")

    config = ReplayConfig(
        name=request.name,
        symbols=[s.upper() for s in request.symbols],
        feeds=request.feeds,
        start=request.start.replace(tzinfo=UTC) if request.start.tzinfo is None else request.start,
        end=request.end.replace(tzinfo=UTC) if request.end.tzinfo is None else request.end,
        speed=request.speed,
        include_premarket=request.include_premarket,
    )

    errors = config.validate()
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})

    try:
        session = await manager.create_session(config, client.id)
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))

    # Reserve concurrent slot after successful creation
    try:
        ep_limiter.acquire_concurrent("replay_session", session_id=session.session_id)
    except EndpointRateLimitExceeded as exc:
        # Clean up the session we just created
        await manager.delete_session(session.session_id)
        raise HTTPException(
            status_code=429,
            detail={
                "code": "GW-E4029",
                "message": exc.message,
            },
        )

    # Estimate messages (rough: 1 msg/min per symbol per feed)
    market_minutes = (config.end - config.start).total_seconds() / 60
    estimated_messages = int(market_minutes * len(config.symbols) * len(config.feeds))

    return ReplaySessionResponse(
        session_id=session.session_id,
        ws_endpoint=f"ws://localhost:8080/ws/replay/{session.session_id}",
        estimated_messages=estimated_messages,
        estimated_duration_seconds=session.estimated_duration_seconds,
    )


@router.get(
    "/sessions/{session_id}",
    response_model=ReplaySessionStatusResponse,
    summary="Get session status",
    description="Get status of a replay session.",
)
async def get_session_status(
    session_id: str,
    client: Any = Depends(require_api_key),
) -> ReplaySessionStatusResponse:
    """Get status of a replay session."""
    manager = get_replay_manager()
    session = manager.get_session(session_id)

    if not session or session.client_id != client.id:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")

    data = session.to_dict()
    return ReplaySessionStatusResponse(**data)


@router.post(
    "/sessions/{session_id}/control",
    response_model=SuccessResponse,
    summary="Control replay session",
    description="Control a replay session (pause, resume, seek, stop).",
)
async def control_session(
    session_id: str,
    request: ReplayControlRequest,
    client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Control a replay session."""
    manager = get_replay_manager()
    session = manager.get_session(session_id)

    if not session or session.client_id != client.id:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")

    action = request.action.lower()

    if action == "pause":
        session.pause()
        return {"session_id": session_id, "action": "pause", "state": session.state.value}

    elif action == "resume":
        session.resume(request.speed)
        return {
            "session_id": session_id,
            "action": "resume",
            "state": session.state.value,
            "speed": session.speed,
        }

    elif action == "seek":
        if not request.timestamp:
            raise HTTPException(status_code=400, detail="timestamp required for seek action")
        session.seek(request.timestamp)
        return {
            "session_id": session_id,
            "action": "seek",
            "timestamp": request.timestamp.isoformat(),
        }

    elif action == "stop":
        session.stop()
        return {"session_id": session_id, "action": "stop", "state": session.state.value}

    else:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid action: {action}. Use pause, resume, seek, or stop.",
        )


@router.delete(
    "/sessions/{session_id}",
    response_model=SuccessResponse,
    summary="Delete replay session",
    description="Delete a replay session and stop if running.",
)
async def delete_session(
    session_id: str,
    client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Delete a replay session."""
    manager = get_replay_manager()

    session = manager.get_session(session_id)
    if not session or session.client_id != client.id:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")

    deleted = await manager.delete_session(session_id)

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")

    # Release concurrent slot
    get_endpoint_rate_limiter().release_concurrent("replay_session", session_id=session_id)

    return {"session_id": session_id, "deleted": True}


@router.get(
    "/sessions",
    response_model=SuccessResponse,
    summary="List replay sessions",
    description="List all replay sessions.",
)
async def list_sessions(
    state: str | None = Query(default=None, description="Filter by replay session state"),
    limit: int | None = Query(
        default=None,
        ge=1,
        le=1000,
        description="Optional max number of sessions to return",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="Number of sessions to skip before returning results",
    ),
    client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """List all replay sessions."""
    manager = get_replay_manager()
    sessions, total, has_more, next_offset = await manager.list_sessions_page(
        client_id=client.id,
        state=state,
        limit=limit,
        offset=offset,
    )

    return {
        "sessions": [s.to_dict() for s in sessions],
        "count": len(sessions),
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": has_more,
        "next_offset": next_offset,
    }


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket Endpoint
# ─────────────────────────────────────────────────────────────────────────────


def _apply_replay_ws_action(session: Any, data: dict[str, Any]) -> bool:
    """Apply WebSocket replay control action. Returns True when replay should stop."""
    action = str(data.get("action", "")).lower()

    if action == "pause":
        session.pause()
        return False

    if action == "resume":
        session.resume(data.get("speed"))
        return False

    if action == "seek":
        ts = data.get("timestamp")
        if not ts:
            return False
        if isinstance(ts, datetime):
            timestamp = ts
        elif isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
        else:
            raise ValueError("Invalid replay seek timestamp")
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        session.seek(timestamp)
        return False

    if action == "stop":
        session.stop()
        return True

    return False


async def _receive_replay_control_messages(websocket: _ReplayControlSocket, session: Any) -> None:
    """Receive replay control messages until replay completes, stops, or disconnects."""
    while session.state in (ReplayState.RUNNING, ReplayState.PAUSED):
        try:
            data = await websocket.receive_json()
        except WebSocketDisconnect:
            session.stop()
            return

        if _apply_replay_ws_action(session, data):
            return


@router.websocket("/ws/{session_id}")
async def replay_websocket(
    websocket: WebSocket,
    session_id: str,
    auth: ClientAuthenticator = Depends(get_authenticator),
) -> None:
    """WebSocket endpoint for replay streaming."""
    manager = get_replay_manager()
    settings = get_settings()

    api_key = websocket.headers.get("x-gateway-key")
    if not api_key:
        await websocket.close(code=4001, reason="Missing X-Gateway-Key")
        return

    client = auth.authenticate(api_key)
    if not client:
        await websocket.close(code=4001, reason="Invalid API key")
        return

    session = manager.get_session(session_id)
    if not session:
        await websocket.close(code=4004, reason="Session not found")
        return
    if not manager.has_data_loader() and not settings.allow_stub_data:
        await websocket.close(code=4005, reason="Replay data not available")
        return
    if session.client_id != client.id:
        await websocket.close(code=4003, reason="Session access denied")
        return

    await websocket.accept()

    # Send initial status
    await websocket.send_json(
        {
            "type": "status",
            "session_id": session_id,
            "state": session.state.value,
            "speed": session.speed,
        }
    )

    async def send_message(msg: dict[str, Any]) -> None:
        """Callback to send messages to WebSocket."""
        try:
            await websocket.send_json(msg)
        except Exception:
            session.stop()

    control_task: asyncio.Task[None] | None = None
    try:
        # Start replay
        await manager.start_session(session_id, send_message)
        replay_task = session._task
        if replay_task is None:
            raise RuntimeError("Replay session task not initialized")

        control_task = asyncio.create_task(_receive_replay_control_messages(websocket, session))
        done, _ = await asyncio.wait(
            {replay_task, control_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if control_task in done:
            control_error = control_task.exception()
            if control_error is not None:
                raise control_error

        if replay_task in done:
            replay_error = replay_task.exception()
            if replay_error is not None:
                raise replay_error

        # Send completion message
        await websocket.send_json(
            {
                "type": "status",
                "session_id": session_id,
                "state": session.state.value,
                "messages_sent": session.messages_sent,
            }
        )

    except WebSocketDisconnect:
        session.stop()
    except Exception as e:
        await websocket.send_json(
            {
                "type": "error",
                "message": str(e),
            }
        )
    finally:
        if control_task is not None and not control_task.done():
            control_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await control_task
        # Release concurrent slot when session ends
        get_endpoint_rate_limiter().release_concurrent("replay_session", session_id=session_id)
        await websocket.close()
