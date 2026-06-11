"""Input validation middleware."""

from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


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

        # Check content-length from raw headers
        headers = dict(scope.get("headers", []))
        content_length_raw = headers.get(b"content-length")
        if content_length_raw:
            try:
                from gateway.core.security import get_input_validator

                validator = get_input_validator()
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

        await self.app(scope, receive, send)
