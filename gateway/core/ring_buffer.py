"""Ring buffer for recent WebSocket messages per symbol."""

from collections import deque
from datetime import UTC, datetime
from threading import Lock

import structlog

logger = structlog.get_logger()


class MessageRingBuffer:
    """Thread-safe ring buffer for recent WebSocket messages per symbol.

    Used to:
    - Store recent messages for replay on reconnection
    - Provide message history for catch-up
    - Support `/api/v1/ws/history/{symbol}` endpoint
    """

    def __init__(self, max_per_symbol: int = 100):
        self._buffers: dict[str, deque] = {}
        self._max = max_per_symbol
        self._lock = Lock()
        self._total_messages = 0

    def append(self, symbol: str, message: dict) -> None:
        """Append a message to the symbol's buffer."""
        with self._lock:
            if symbol not in self._buffers:
                self._buffers[symbol] = deque(maxlen=self._max)

            # Add timestamp if not present
            if "buffered_at" not in message:
                message["buffered_at"] = datetime.now(UTC).isoformat()

            self._buffers[symbol].append(message)
            self._total_messages += 1

    def get_recent(self, symbol: str, count: int = 10) -> list[dict]:
        """Get most recent N messages for a symbol."""
        with self._lock:
            buf = self._buffers.get(symbol, deque())
            return list(buf)[-count:]

    def get_all(self, symbol: str) -> list[dict]:
        """Get all buffered messages for a symbol."""
        with self._lock:
            buf = self._buffers.get(symbol, deque())
            return list(buf)

    def get_since(self, symbol: str, since: datetime) -> list[dict]:
        """Get messages since a specific timestamp."""
        with self._lock:
            buf = self._buffers.get(symbol, deque())
            result = []
            for msg in buf:
                msg_time = msg.get("buffered_at") or msg.get("timestamp")
                if msg_time:
                    if isinstance(msg_time, str):
                        msg_dt = datetime.fromisoformat(msg_time.replace("Z", "+00:00"))
                    else:
                        msg_dt = msg_time
                    if msg_dt >= since:
                        result.append(msg)
            return result

    def clear(self, symbol: str = None) -> None:
        """Clear buffer for a symbol, or all buffers if symbol is None."""
        with self._lock:
            if symbol:
                if symbol in self._buffers:
                    del self._buffers[symbol]
            else:
                self._buffers.clear()
                self._total_messages = 0

    def get_stats(self) -> dict:
        """Get buffer statistics."""
        with self._lock:
            return {
                "symbols_tracked": len(self._buffers),
                "max_per_symbol": self._max,
                "total_messages": self._total_messages,
                "buffer_sizes": {symbol: len(buf) for symbol, buf in self._buffers.items()},
            }


# Singleton instance
_buffer: MessageRingBuffer = None


def get_ring_buffer() -> MessageRingBuffer:
    """Get or create the singleton ring buffer."""
    global _buffer
    if _buffer is None:
        _buffer = MessageRingBuffer()
    return _buffer
