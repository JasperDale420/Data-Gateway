"""Security headers middleware (PRD 7.2.2)."""

from starlette.types import ASGIApp, Message, Receive, Scope, Send


class SecurityHeadersMiddleware:
    """Adds security headers to all responses.

    Headers added:
    - Strict-Transport-Security (HSTS): max-age=31536000; includeSubDomains
    - X-Content-Type-Options: nosniff
    - X-Frame-Options: DENY
    - X-XSS-Protection: 1; mode=block
    - Referrer-Policy: strict-origin-when-cross-origin
    """

    def __init__(self, app: ASGIApp, hsts_max_age: int = 31536000, include_subdomains: bool = True) -> None:
        self.app = app
        hsts_value = f"max-age={hsts_max_age}"
        if include_subdomains:
            hsts_value += "; includeSubDomains"
        # Pre-encode headers once at init, not per-request
        self._extra_headers: list[tuple[bytes, bytes]] = [
            (b"strict-transport-security", hsts_value.encode()),
            (b"x-content-type-options", b"nosniff"),
            (b"x-frame-options", b"DENY"),
            (b"x-xss-protection", b"1; mode=block"),
            (b"referrer-policy", b"strict-origin-when-cross-origin"),
        ]

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        extra = self._extra_headers

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend(extra)
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_wrapper)
