"""Correlation / trace-id middleware.

Reads an inbound ``x-trace-id`` (or mints one), binds it into the structlog
context so every log line emitted while handling the request carries it, echoes
it back on the response, and clears the context on the way out so ids never
leak across requests served by the same worker. This is the single seam that
makes a request — and its downstream sink publishes — followable across the
JSON logs by id.
"""

import uuid

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from gateway.core.logger import bind_context, clear_context

_TRACE_HEADER = b"x-trace-id"


def _inbound_trace_id(scope: Scope) -> str | None:
    for name, value in scope.get("headers", []):
        if name == _TRACE_HEADER:
            decoded = value.decode("latin-1").strip()
            if decoded:
                return decoded
    return None


class CorrelationIdMiddleware:
    """Bind a per-request ``x-trace-id`` into the log context and echo it."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        trace_id = _inbound_trace_id(scope) or uuid.uuid4().hex
        bind_context(trace_id=trace_id, correlation_id=trace_id)
        trace_header = (_TRACE_HEADER, trace_id.encode())

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append(trace_header)
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            clear_context()
