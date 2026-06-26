"""Global and per-IP rate limit middleware (PRD 7.5.1-2)."""

import json
import time
from dataclasses import dataclass, field

from fastapi import Response
from starlette.types import ASGIApp, Receive, Scope, Send

from gateway.core.logger import logger
from gateway.core.metrics import record_rate_limit_exceeded


@dataclass
class IPConnectionTracker:
    """Track connections per IP for DDoS protection."""

    requests: int = 0
    first_request: float = field(default_factory=time.time)
    blocked_until: float = 0.0
    last_seen: float = field(default_factory=time.time)


_global_ratelimiter: "GlobalRateLimitMiddleware | None" = None


def get_global_ratelimiter() -> "GlobalRateLimitMiddleware | None":
    """Return the installed global rate-limit middleware instance, if any.

    ``app.add_middleware`` does not expose the instance, so the middleware
    registers itself here on construction. Admin blocklist endpoints use this
    to manage the ENFORCED blocklist rather than a separate dead dict.
    """
    return _global_ratelimiter


class GlobalRateLimitMiddleware:
    """Global and per-IP rate limiting.

    Implements:
    - PRD 7.5.1: Global limit 10,000 req/min
    - PRD 7.5.2: Per-IP limit 1,000 req/min

    Uses sliding window for rate limiting.
    """

    _HEALTH_PATHS = frozenset({"/health", "/health/ready"})

    def __init__(
        self,
        app: ASGIApp,
        global_limit: int = 10000,
        per_ip_limit: int = 1000,
        window_seconds: int = 60,
        trust_proxy_headers: bool = False,
        trusted_proxy_cidrs: str = "",
    ) -> None:
        self.app = app
        self.global_limit = global_limit
        self.per_ip_limit = per_ip_limit
        self.window_seconds = window_seconds
        self._trust_proxy_headers = trust_proxy_headers

        # Pre-parse trusted_proxy_cidrs once at startup. Validation already happened
        # in Settings._validate_trusted_proxy_cidrs so any error here is a programming bug.
        import ipaddress

        self._trusted_proxies: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
        if trusted_proxy_cidrs:
            for cidr in trusted_proxy_cidrs.split(","):
                cidr = cidr.strip()
                if cidr:
                    self._trusted_proxies.append(ipaddress.ip_network(cidr, strict=False))

        if trust_proxy_headers and not self._trusted_proxies:
            logger.warning(
                "trusted_proxy_misconfig",
                msg=(
                    "behind_trusted_proxy=True without trusted_proxy_cidrs — "
                    "leftmost X-Forwarded-For will be trusted, which is spoofable. "
                    "Set GATEWAY_TRUSTED_PROXY_CIDRS to your proxy/load balancer CIDRs."
                ),
            )

        # Tracking state
        self._global_requests = 0
        self._global_window_start = time.time()
        self._ip_trackers: dict[str, IPConnectionTracker] = {}
        self._blocked_ips: set[str] = set()
        self._last_prune = time.time()
        self._prune_interval = 60.0

        # Register as the process-wide instance so admin blocklist endpoints can
        # manage the enforced blocklist (last one installed wins).
        global _global_ratelimiter
        _global_ratelimiter = self

    def _get_client_ip(self, scope: Scope) -> str:
        """Extract client IP from ASGI scope.

        Resolution order:
        1. trust_proxy_headers=False (default) → socket peer IP. Safest.
        2. trust_proxy_headers=True with trusted_proxy_cidrs → walk XFF
           rightmost-to-leftmost, return first IP NOT in the trusted set.
           This is the real client behind a known proxy chain.
        3. trust_proxy_headers=True without trusted_proxy_cidrs → fall back to
           legacy leftmost-XFF (insecure; warning emitted at startup).
        """
        if not self._trust_proxy_headers:
            client = scope.get("client")
            return client[0] if client else "unknown"

        headers = dict(scope.get("headers", []))
        forwarded = headers.get(b"x-forwarded-for")
        if not forwarded:
            client = scope.get("client")
            return client[0] if client else "unknown"

        ips = [ip.strip() for ip in forwarded.decode().split(",") if ip.strip()]
        if not ips:
            client = scope.get("client")
            return client[0] if client else "unknown"

        # Without a trusted-proxy list we can't tell which hops are ours, so
        # fall back to the legacy leftmost behavior (with the startup warning).
        if not self._trusted_proxies:
            return ips[0]

        # Walk rightmost (closest hop) backward, skipping IPs in trusted CIDRs.
        # The first non-trusted IP is the real client. If every IP is trusted
        # (e.g. an internal-only request), fall back to the leftmost entry.
        import ipaddress

        for ip in reversed(ips):
            try:
                ip_obj = ipaddress.ip_address(ip)
            except ValueError:
                continue
            if not any(ip_obj in net for net in self._trusted_proxies):
                return ip
        return ips[0]

    def _reset_window_if_needed(self) -> None:
        """Reset global window if expired."""
        now = time.time()
        if now - self._global_window_start >= self.window_seconds:
            self._global_requests = 0
            self._global_window_start = now

    def _check_ip_limit(self, ip: str) -> bool:
        """Check if IP is within rate limit. Returns True if allowed."""
        now = time.time()

        # Check if blocked
        if ip in self._blocked_ips:
            tracker = self._ip_trackers.get(ip)
            if tracker and tracker.blocked_until > now:
                return False
            # Unblock if time expired
            self._blocked_ips.discard(ip)

        # Get or create tracker
        tracker = self._ip_trackers.get(ip)
        if not tracker or now - tracker.first_request >= self.window_seconds:
            self._ip_trackers[ip] = IPConnectionTracker(requests=1, first_request=now, last_seen=now)
            return True

        # Check limit
        tracker.requests += 1
        tracker.last_seen = now
        if tracker.requests > self.per_ip_limit:
            # Block for remainder of window
            tracker.blocked_until = tracker.first_request + self.window_seconds
            self._blocked_ips.add(ip)
            logger.warning("ip_rate_limited", ip=ip, requests=tracker.requests)
            return False

        return True

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Check global and per-IP limits."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        self._prune_ip_trackers()

        # Skip for health endpoints
        if scope["path"] in self._HEALTH_PATHS:
            await self.app(scope, receive, send)
            return

        # Reset window if needed
        self._reset_window_if_needed()

        # Check global limit (PRD 7.5.1)
        if self._global_requests >= self.global_limit:
            logger.warning("global_rate_limit_exceeded", current=self._global_requests)
            record_rate_limit_exceeded("global")
            retry_after = max(1, int(self._global_window_start + self.window_seconds - time.time()))
            response = Response(
                content=json.dumps(
                    {
                        "success": False,
                        "error": {
                            "code": "GW-E4029",
                            "message": "Global rate limit exceeded",
                        },
                    }
                ),
                status_code=429,
                media_type="application/json",
                headers={"Retry-After": str(retry_after)},
            )
            await response(scope, receive, send)
            return

        # Check per-IP limit (PRD 7.5.2)
        client_ip = self._get_client_ip(scope)
        if not self._check_ip_limit(client_ip):
            record_rate_limit_exceeded(f"ip:{client_ip}")
            tracker = self._ip_trackers.get(client_ip)
            retry_after = max(1, int(tracker.first_request + self.window_seconds - time.time())) if tracker else 60
            response = Response(
                content=json.dumps(
                    {
                        "success": False,
                        "error": {
                            "code": "GW-E4029",
                            "message": "IP rate limit exceeded",
                        },
                    }
                ),
                status_code=429,
                media_type="application/json",
                headers={"Retry-After": str(retry_after)},
            )
            await response(scope, receive, send)
            return

        # Increment global counter
        self._global_requests += 1

        await self.app(scope, receive, send)

    def _prune_ip_trackers(self) -> None:
        """Prune idle IP trackers and expired blocks."""
        now = time.time()
        if now - self._last_prune < self._prune_interval:
            return

        cutoff = now - (self.window_seconds * 2)
        for ip, tracker in list(self._ip_trackers.items()):
            if tracker.blocked_until > now:
                continue
            if tracker.last_seen < cutoff:
                self._ip_trackers.pop(ip, None)
                self._blocked_ips.discard(ip)

        # Clean up expired blocks with no tracker
        for ip in list(self._blocked_ips):
            maybe_tracker = self._ip_trackers.get(ip)
            if maybe_tracker is None or maybe_tracker.blocked_until <= now:
                self._blocked_ips.discard(ip)

        self._last_prune = now

    def block_ip(self, ip: str, duration_seconds: int = 3600) -> None:
        """Manually block an IP (PRD 7.6.3)."""
        tracker = self._ip_trackers.get(ip)
        if not tracker:
            tracker = IPConnectionTracker()
            self._ip_trackers[ip] = tracker
        tracker.blocked_until = time.time() + duration_seconds
        self._blocked_ips.add(ip)
        logger.info("ip_blocked", ip=ip, duration=duration_seconds)

    def unblock_ip(self, ip: str) -> None:
        """Remove IP from blocklist."""
        self._blocked_ips.discard(ip)
        if ip in self._ip_trackers:
            self._ip_trackers[ip].blocked_until = 0.0
        logger.info("ip_unblocked", ip=ip)

    def get_blocked_ips(self) -> list[str]:
        """Get list of currently blocked IPs."""
        return list(self._blocked_ips)
