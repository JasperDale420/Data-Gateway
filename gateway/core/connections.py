"""WebSocket connection management."""

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime

import orjson
from fastapi import WebSocket

from gateway.core.auth import Client
from gateway.core.logger import logger


@dataclass
class Connection:
    """Active WebSocket connection."""

    websocket: WebSocket
    client: Client | None = None
    connected_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    authenticated: bool = False
    subscriptions: set[str] = field(default_factory=set)

    @property
    def client_id(self) -> str:
        """Get client ID or 'anonymous'."""
        return self.client.id if self.client else "anonymous"


class ConnectionManager:
    """Manages active WebSocket connections."""

    def __init__(self):
        self._connections: dict[str, Connection] = {}
        self._client_map: dict[str, set[str]] = {}  # client_id -> set of connection_ids
        self._lock = asyncio.Lock()
        self._broadcast_semaphore = asyncio.Semaphore(100)

    async def connect(self, connection_id: str, websocket: WebSocket) -> Connection:
        """Register a new connection."""
        await websocket.accept()

        connection = Connection(websocket=websocket)

        async with self._lock:
            self._connections[connection_id] = connection
            # Anonymous connections are not in client_map yet

        logger.info("connection_opened", connection_id=connection_id)
        return connection

    async def disconnect(self, connection_id: str) -> None:
        """Remove a connection."""
        async with self._lock:
            connection = self._connections.pop(connection_id, None)
            if connection and connection.client:
                # Remove from client_map
                c_id = connection.client.id
                if c_id in self._client_map:
                    self._client_map[c_id].discard(connection_id)
                    if not self._client_map[c_id]:
                        del self._client_map[c_id]

        if connection:
            logger.info(
                "connection_closed",
                connection_id=connection_id,
                client_id=connection.client_id,
                subscriptions=len(connection.subscriptions),
            )

    async def authenticate(self, connection_id: str, client: Client) -> bool:
        """Mark connection as authenticated."""
        async with self._lock:
            connection = self._connections.get(connection_id)
            if not connection:
                return False

            old_client_id = connection.client.id if connection.client else None
            connection.client = client
            connection.authenticated = True

            # Update client_map
            if old_client_id:
                # Should not happen in normal flow, but handle purely for correctness
                if old_client_id in self._client_map:
                    self._client_map[old_client_id].discard(connection_id)
                    if not self._client_map[old_client_id]:
                        del self._client_map[old_client_id]

            if client.id not in self._client_map:
                self._client_map[client.id] = set()
            self._client_map[client.id].add(connection_id)

        logger.info("connection_authenticated", connection_id=connection_id, client_id=client.id)
        return True

    def get(self, connection_id: str) -> Connection | None:
        """Get connection by ID."""
        return self._connections.get(connection_id)

    def is_authenticated(self, connection_id: str) -> bool:
        """Check if connection is authenticated."""
        connection = self._connections.get(connection_id)
        return connection.authenticated if connection else False

    @property
    def active_count(self) -> int:
        """Count of active connections."""
        return len(self._connections)

    @property
    def authenticated_count(self) -> int:
        """Count of authenticated connections."""
        return sum(1 for c in self._connections.values() if c.authenticated)

    def get_stats(self) -> dict:
        """Get connection statistics."""
        total_subscriptions = sum(len(connection.subscriptions) for connection in self._connections.values())
        unique_subscriptions = len(
            {subscription for connection in self._connections.values() for subscription in connection.subscriptions}
        )
        return {
            "active": self.active_count,
            "authenticated": self.authenticated_count,
            "anonymous": self.active_count - self.authenticated_count,
            "subscriptions_total": total_subscriptions,
            "subscriptions_unique": unique_subscriptions,
        }

    async def broadcast(
        self,
        message: dict | str | bytes,
        client_ids: list[str] | None = None,
    ) -> int:
        """Broadcast message to connections.

        Args:
            message: Message to send (dict, str, or bytes)
            client_ids: Optional list of client IDs to target. If None, broadcast to all.

        Returns:
            Number of connections that received the message.
        """
        # Pre-serialize once if needed
        # orjson.dumps returns bytes, which is optimal for network I/O
        if isinstance(message, str | bytes):
            payload = message
        else:
            payload = orjson.dumps(message)

        # Identify target connections
        # If client_ids is provided, use the index to find connections O(TargetClients)
        # If not provided, iterate all connections O(TotalConnections)
        targets: list[Connection] = []

        if client_ids:
            # Targeted broadcast using index
            # Read lock optimization: copying required connection IDs is fast
            # We don't hold lock during send
            async with self._lock:
                for cid in client_ids:
                    conn_ids = self._client_map.get(cid)
                    if conn_ids:
                        for conn_id in conn_ids:
                            conn = self._connections.get(conn_id)
                            if conn and conn.authenticated:
                                targets.append(conn)
        else:
            # Broadcast to all authenticated
            targets = [c for c in self._connections.values() if c.authenticated]

        if not targets:
            return 0

        async def _send(connection: Connection) -> bool:
            try:
                # Use semaphore to bound concurrency
                async with self._broadcast_semaphore:
                    # WebSocket.send_text handles str, send_bytes handles bytes
                    if isinstance(payload, str):
                        await connection.websocket.send_text(payload)
                    else:
                        await connection.websocket.send_bytes(payload)
                return True
            except Exception as e:
                logger.warning(
                    "broadcast_send_failed",
                    client_id=connection.client_id,
                    error=str(e),
                )
                return False

        # Gather all sends
        results = await asyncio.gather(*(_send(conn) for conn in targets))
        return sum(1 for sent in results if sent)

    async def broadcast_to_connection_ids(
        self,
        message: dict | str | bytes,
        connection_ids: list[str] | None = None,
    ) -> int:
        """Broadcast message to explicitly named WebSocket connections."""
        if isinstance(message, str | bytes):
            payload = message
        else:
            payload = orjson.dumps(message)

        targets: list[Connection] = []
        if connection_ids:
            async with self._lock:
                for connection_id in connection_ids:
                    conn = self._connections.get(connection_id)
                    if conn and conn.authenticated:
                        targets.append(conn)
        else:
            targets = [c for c in self._connections.values() if c.authenticated]

        if not targets:
            return 0

        async def _send(connection: Connection) -> bool:
            try:
                async with self._broadcast_semaphore:
                    if isinstance(payload, str):
                        await connection.websocket.send_text(payload)
                    else:
                        await connection.websocket.send_bytes(payload)
                return True
            except Exception as e:
                logger.warning(
                    "broadcast_send_failed",
                    client_id=connection.client_id,
                    error=str(e),
                )
                return False

        results = await asyncio.gather(*(_send(conn) for conn in targets))
        return sum(1 for sent in results if sent)

    async def broadcast_shutdown(self, timeout_seconds: int = 30) -> int:
        """Send shutdown warning to all authenticated clients (PRD §Graceful Shutdown step 2).

        Returns:
            Number of clients notified.
        """
        message = {
            "type": "system",
            "event": "shutdown",
            "timeout_seconds": timeout_seconds,
        }
        count = 0
        async with self._lock:
            targets = [conn for conn in self._connections.values() if conn.authenticated]
        for conn in targets:
            try:
                await conn.websocket.send_json(message)
                count += 1
            except Exception:
                pass
        return count

    async def close_all(self, code: int = 1001, reason: str = "Going Away") -> None:
        """Close all WebSocket connections (PRD §Graceful Shutdown step 5).

        Args:
            code: WebSocket close code (1001 = Going Away).
            reason: Human-readable close reason.
        """
        async with self._lock:
            connection_ids = list(self._connections.keys())

        for cid in connection_ids:
            conn = self._connections.get(cid)
            if not conn:
                continue
            try:
                await conn.websocket.close(code=code, reason=reason)
            except Exception as e:
                logger.warning("close_connection_failed", connection_id=cid, error=str(e))

        logger.info("all_connections_closed", count=len(connection_ids), code=code)
