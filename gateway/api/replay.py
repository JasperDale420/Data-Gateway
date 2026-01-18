"""Historical Replay API endpoints.

Implements replay session management and WebSocket streaming as specified in PRD.
"""

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from gateway.api.deps import require_api_key
from gateway.core.replay import (
    ReplayConfig,
    ReplayState,
    get_replay_manager,
)

router = APIRouter(prefix="/api/v1/replay", tags=["Historical Replay"])


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
    manager = get_replay_manager()

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
        session = await manager.create_session(config)
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))

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

    if not session:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")

    data = session.to_dict()
    return ReplaySessionStatusResponse(**data)


@router.post(
    "/sessions/{session_id}/control",
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

    if not session:
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
    summary="Delete replay session",
    description="Delete a replay session and stop if running.",
)
async def delete_session(
    session_id: str,
    client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """Delete a replay session."""
    manager = get_replay_manager()

    deleted = await manager.delete_session(session_id)

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")

    return {"session_id": session_id, "deleted": True}


@router.get(
    "/sessions",
    summary="List replay sessions",
    description="List all replay sessions.",
)
async def list_sessions(
    client: Any = Depends(require_api_key),
) -> dict[str, Any]:
    """List all replay sessions."""
    manager = get_replay_manager()
    sessions = await manager.list_sessions()

    return {
        "sessions": [s.to_dict() for s in sessions],
        "count": len(sessions),
    }


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket Endpoint
# ─────────────────────────────────────────────────────────────────────────────


@router.websocket("/ws/{session_id}")
async def replay_websocket(
    websocket: WebSocket,
    session_id: str,
) -> None:
    """WebSocket endpoint for replay streaming."""
    manager = get_replay_manager()
    session = manager.get_session(session_id)

    if not session:
        await websocket.close(code=4004, reason="Session not found")
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

    try:
        # Start replay
        await manager.start_session(session_id, send_message)

        # Wait for completion or control messages
        while session.state in (ReplayState.RUNNING, ReplayState.PAUSED):
            try:
                data = await asyncio.wait_for(
                    websocket.receive_json(),
                    timeout=1.0,
                )

                # Handle control messages
                action = data.get("action", "").lower()
                if action == "pause":
                    session.pause()
                elif action == "resume":
                    session.resume(data.get("speed"))
                elif action == "seek":
                    ts = data.get("timestamp")
                    if ts:
                        session.seek(datetime.fromisoformat(ts))
                elif action == "stop":
                    session.stop()
                    break

            except TimeoutError:
                continue
            except WebSocketDisconnect:
                session.stop()
                break

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
        await websocket.close()


# Import asyncio at module level
import asyncio
