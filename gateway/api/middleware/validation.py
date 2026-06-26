"""Input validation middleware."""

from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class InputValidationMiddleware:
    """Apply basic request input validation limits."""

    _SKIP_EXACT = frozenset({"/metrics", "/openapi.json", "/docs", "/redoc"})

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope["path"]
        if path.startswith("/health") or path in self._SKIP_EXACT:
            await self.app(scope, receive, send)
            return

        # Enforce the URL-length limit (previously a dead constant): reject an
        # over-long request target — path + query string — with 414 before any
        # route or query parsing, bounding query-string amplification at the door.
        from gateway.core.security import PARAM_LIMITS

        query_string = scope.get("query_string", b"") or b""
        if len(path.encode("utf-8")) + len(query_string) > PARAM_LIMITS["url_max_bytes"]:
            response = JSONResponse(
                status_code=414,
                content={"detail": {"code": "GW-E8003", "message": "Request URI too long"}},
            )
            await response(scope, receive, send)
            return

        # Check content-length from raw headers (fast-path / honest clients)
        headers = dict(scope.get("headers", []))
        content_length_raw = headers.get(b"content-length")
        from gateway.core.security import get_input_validator

        validator = get_input_validator()
        if content_length_raw:
            try:
                error = validator.validate_request_size(int(content_length_raw), endpoint_type="rest")
                if error:
                    response = JSONResponse(
                        status_code=413,
                        content={"detail": error.to_dict()},
                    )
                    await response(scope, receive, send)
                    return
            except ValueError:
                pass

        # Body-byte enforcement for chunked transfer encoding / missing-CL
        # requests. The Content-Length check above honors well-behaved
        # clients, but a request without Content-Length (chunked transfer
        # or HTTP/2 streaming) would otherwise stream unbounded bytes into
        # the FastAPI request body — DoS / memory exhaustion vector flagged
        # in the 2026-05-21 audit. Wrap ASGI ``receive`` to count bytes
        # across ``http.request`` chunks and signal 413 via the
        # ``send`` path if accumulated bytes exceed the configured cap.
        bytes_seen = 0
        oversize_error: object | None = None

        async def _byte_counting_receive() -> Message:
            nonlocal bytes_seen, oversize_error
            message = await receive()
            if message.get("type") == "http.request":
                body = message.get("body") or b""
                bytes_seen += len(body)
                if oversize_error is None:
                    err = validator.validate_request_size(bytes_seen, endpoint_type="rest")
                    if err is not None:
                        oversize_error = err
                        # Signal end-of-body so downstream framework sees a
                        # truncated request; the wrapped ``send`` will then
                        # emit our 413.
                        return {"type": "http.request", "body": b"", "more_body": False}
            return message

        sent_413 = False

        async def _wrapped_send(message: Message) -> None:
            nonlocal sent_413
            if oversize_error is not None and not sent_413:
                sent_413 = True
                response = JSONResponse(
                    status_code=413,
                    content={"detail": oversize_error.to_dict()},
                )
                await response(scope, receive, send)
                return
            if sent_413:
                # Drop any subsequent downstream send — 413 already emitted.
                return
            await send(message)

        await self.app(scope, _byte_counting_receive, _wrapped_send)
