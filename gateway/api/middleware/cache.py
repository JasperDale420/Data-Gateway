"""Response caching middleware."""

import asyncio
import time
from dataclasses import dataclass

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from gateway.core.logger import logger
from gateway.core.metrics import (
    record_cache_error,
    record_cache_hit,
    record_cache_miss,
)


@dataclass
class CacheEntry:
    """Cached response with metadata."""

    content: bytes
    media_type: str
    headers: dict[str, str]
    created_at: float
    ttl: int

    @property
    def age_ms(self) -> int:
        """Age in milliseconds."""
        return int((time.time() - self.created_at) * 1000)

    @property
    def ttl_remaining(self) -> int:
        """Remaining TTL in milliseconds."""
        elapsed = time.time() - self.created_at
        return max(0, int((self.ttl - elapsed) * 1000))

    def is_expired(self) -> bool:
        """Check if entry has expired."""
        return time.time() - self.created_at > self.ttl

    def to_dict(self) -> dict:
        """Serialize for Redis storage."""
        import base64

        return {
            "content": base64.b64encode(self.content).decode("ascii"),
            "media_type": self.media_type,
            "headers": self.headers,
            "created_at": self.created_at,
            "ttl": self.ttl,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CacheEntry":
        """Deserialize from Redis storage."""
        import base64

        return cls(
            content=base64.b64decode(data["content"]),
            media_type=data["media_type"],
            headers=data.get("headers", {}),
            created_at=data["created_at"],
            ttl=data["ttl"],
        )


class CacheMiddleware:
    """Adds cache headers and caching for GET requests.

    Uses HybridCache (L1 memory + L2 Redis) when available for durable caching.

    Headers added:
    - X-Gateway-Cache: HIT or MISS
    - X-Gateway-Cache-Age: Age in milliseconds
    - X-Gateway-Cache-TTL: Remaining TTL in milliseconds
    """

    _HOP_BY_HOP = frozenset({b"content-length", b"set-cookie", b"connection", b"keep-alive"})

    # Write-namespace → list of read-side path prefixes to invalidate on a
    # successful 2xx write. Conservative — only mutating endpoints that have
    # cacheable read counterparts. Matching is "request path starts with the
    # write namespace" so /api/v1/alpaca/orders matches both POST /orders
    # and PATCH /orders/{order_id} and DELETE /orders/{order_id}.
    #
    # Live-capital rationale: a POST /orders changes the result of subsequent
    # GET /orders and GET /positions and GET /account (buying power). Without
    # invalidation a cached read could show stale broker state for up to
    # 300s — a real safety problem for retry/reconciliation logic.
    _WRITE_INVALIDATION_NAMESPACES: dict[str, tuple[str, ...]] = {
        "/api/v1/alpaca/orders": (
            "/api/v1/alpaca/orders",
            "/api/v1/alpaca/positions",
            "/api/v1/alpaca/account",
            "/api/v1/alpaca/portfolio",
        ),
        "/api/v1/alpaca/positions": (
            "/api/v1/alpaca/positions",
            "/api/v1/alpaca/orders",
            "/api/v1/alpaca/account",
            "/api/v1/alpaca/portfolio",
        ),
        "/api/v1/alpaca/watchlists": ("/api/v1/alpaca/watchlists",),
        "/api/v1/alpaca/account": ("/api/v1/alpaca/account",),
    }

    def __init__(
        self,
        app: ASGIApp,
        default_ttl: int = 60,
        max_size: int = 10000,
        max_body_bytes: int = 524288,
    ) -> None:
        self.app = app
        self.default_ttl = default_ttl
        self.max_body_bytes = max_body_bytes
        self._cache = None  # Lazy initialization
        self._cache_initialized = False

    def _invalidation_prefixes_for_write(self, path: str) -> tuple[str, ...]:
        """Return the read-side path prefixes that this write should invalidate,
        or empty tuple if this write doesn't trigger any invalidation."""
        for ns, prefixes in self._WRITE_INVALIDATION_NAMESPACES.items():
            if path.startswith(ns):
                return prefixes
        return ()

    async def _invalidate_for_write(self, scope: Scope, invalidate_prefixes: tuple[str, ...]) -> None:
        """Invalidate cached GET entries for THIS client whose path starts with
        any of ``invalidate_prefixes``.

        Cache key format is ``METHOD:path:query:scope`` (see _cache_key).
        We match on ``GET:{prefix}`` to scope to the read side, then on the
        per-client suffix ``:{scope}`` so we don't blow away other clients'
        caches. The middleware was added because cached /orders /positions
        /account reads stayed stale for the full TTL after writes.
        """
        cache = self._get_cache()
        if not hasattr(cache, "invalidate_matching"):
            return  # not all cache backends support invalidation yet

        # Build the client-scoped suffix the same way _cache_key does.
        request = Request(scope)
        is_public = self._is_public_path(scope["path"])
        request.state.cache_public = is_public
        if not is_public:
            api_key = request.headers.get("X-Gateway-Key")
            # Best-effort attach client from API key for scope derivation.
            # If auth wasn't run yet for this write, fall back to api-key hash.
            await self._ensure_authenticated(request, api_key)
        scope_str = self._client_cache_scope(request)
        suffix = f":{scope_str}"

        prefixes_with_get = tuple(f"GET:{p}" for p in invalidate_prefixes)

        def _predicate(key: str) -> bool:
            return key.endswith(suffix) and key.startswith(prefixes_with_get)

        if asyncio.iscoroutinefunction(cache.invalidate_matching):
            removed = await cache.invalidate_matching(_predicate)
        else:
            removed = cache.invalidate_matching(_predicate)
        if removed:
            logger.info(
                "cache_invalidated_on_write",
                client_scope=scope_str,
                write_path=scope["path"],
                read_prefixes=list(invalidate_prefixes),
                entries_removed=removed,
            )

    def _get_cache(self):
        """Get cache instance (lazy initialization).

        Checks FastAPI dependency overrides first so tests can inject a
        test cache instance without going through the DI machinery.
        Falls back to the ``get_cache()`` LRU singleton for production.
        """
        from gateway.api.deps import get_cache

        # Respect DI overrides (e.g. test fixtures) when available.
        # We import ``app`` lazily to avoid circular imports and check
        # overrides on every call so test teardown is picked up.
        from gateway.main import app as _app

        override = _app.dependency_overrides.get(get_cache)
        if override is not None:
            return override()

        # Production path: cache the instance after first creation
        if not self._cache_initialized:
            self._cache = get_cache()
            self._cache_initialized = True
        return self._cache

    def _cache_type(self, cache) -> str:
        """Derive cache type for metrics labels (cached after first call)."""
        if hasattr(self, "_cache_type_label"):
            return self._cache_type_label

        try:
            from gateway.core.cache import HybridCache, InMemoryCache, RedisCache

            if isinstance(cache, HybridCache):
                label = "hybrid"
            elif isinstance(cache, RedisCache):
                label = "redis"
            elif isinstance(cache, InMemoryCache):
                label = "memory"
            else:
                label = cache.__class__.__name__.lower()
        except Exception:
            label = cache.__class__.__name__.lower()

        self._cache_type_label = label
        return label

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope["path"]

        # Non-GET: pass through, AND invalidate cached GETs for this client's
        # write namespace AFTER the upstream response. Without this, cached
        # /orders, /positions, /account, /watchlists reads stayed stale for
        # the full TTL after corresponding mutations — flagged as a BLOCKER
        # in the 2026-05-21 audit (live-capital trading callers could see
        # stale order state for up to 300s after placing/cancelling/replacing).
        if scope["method"] != "GET":
            invalidate_prefixes = self._invalidation_prefixes_for_write(path)
            if not invalidate_prefixes:
                await self.app(scope, receive, send)
                return

            # Capture the response status so we only invalidate on success.
            status_seen: dict[str, int] = {}

            async def status_capturing_send(message: Message) -> None:
                if message.get("type") == "http.response.start":
                    status_seen["code"] = int(message.get("status", 0))
                await send(message)

            await self.app(scope, receive, status_capturing_send)

            # 2xx writes invalidate; failed writes (4xx/5xx) leave the cache
            # alone since the broker state didn't change.
            code = status_seen.get("code", 0)
            if 200 <= code < 300:
                try:
                    await self._invalidate_for_write(scope, invalidate_prefixes)
                except Exception as e:
                    # Invalidation failure must not break the response.
                    # Log and move on; worst case is stale reads until TTL.
                    logger.warning("cache_invalidate_failed", path=path, error=str(e))
            return

        # Skip caching for dynamic endpoints
        if path.startswith("/api/v1/backfill"):
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        is_public = self._is_public_path(path)
        request.state.cache_public = is_public

        if not is_public:
            api_key = request.headers.get("X-Gateway-Key")
            auth_error = await self._ensure_authenticated(request, api_key)
            if auth_error is not None:
                await auth_error(scope, receive, send)
                return

        # Check for cache bypass
        if request.headers.get("X-Gateway-Cache") == "bypass":

            async def bypass_send(message: Message) -> None:
                if message["type"] == "http.response.start":
                    headers = MutableHeaders(scope=message)
                    headers.append("X-Gateway-Cache", "BYPASS")
                await send(message)

            await self.app(scope, receive, bypass_send)
            return

        # Generate cache key
        cache_key = self._cache_key(request)

        # Get cache instance
        cache = self._get_cache()
        cache_type = self._cache_type(cache)

        # Check cache (async for HybridCache)
        try:
            cached_data = await cache.get(cache_key)
            if cached_data:
                entry = CacheEntry.from_dict(cached_data)
                if not entry.is_expired():
                    record_cache_hit(cache_type)
                    headers = dict(entry.headers or {})
                    headers.update(
                        {
                            "X-Gateway-Cache": "HIT",
                            "X-Gateway-Cache-Age": str(entry.age_ms),
                            "X-Gateway-Cache-TTL": str(entry.ttl_remaining),
                        }
                    )
                    response = Response(
                        content=entry.content,
                        status_code=200,
                        media_type=entry.media_type,
                        headers=headers,
                    )
                    await response(scope, receive, send)
                    return
            record_cache_miss(cache_type)
        except Exception as e:
            # A failing cache read silently degrades the request to an upstream
            # call; surface it at WARNING so it shows in error logs. No
            # The miss keeps the hit-rate denominator honest; the cache-error
            # counter drives alerting for backend degradation.
            record_cache_miss(cache_type)
            record_cache_error(cache_type, "read")
            logger.warning("cache_read_error", key=cache_key, error=str(e))

        # Buffer downstream response to decide whether to cache
        response_started = False
        initial_message: Message | None = None
        body_chunks: list[bytes] = []
        status_code = 0
        should_cache = False

        async def buffering_send(message: Message) -> None:
            nonlocal response_started, initial_message, status_code, should_cache

            if message["type"] == "http.response.start":
                response_started = True
                initial_message = message
                status_code = message["status"]

                if status_code == 200:
                    # Check content-type and content-length to decide cacheability
                    raw_headers = dict(message.get("headers", []))
                    ct = raw_headers.get(b"content-type", b"").decode().lower()
                    if "text/event-stream" in ct or "application/x-ndjson" in ct:
                        should_cache = False
                    else:
                        cl = raw_headers.get(b"content-length")
                        if cl:
                            try:
                                should_cache = int(cl) <= self.max_body_bytes
                            except ValueError:
                                should_cache = False

                    if should_cache:
                        return  # Buffer; don't send yet

                # Add cache header: BYPASS for non-cacheable 200, MISS for non-200
                cache_header = "BYPASS" if status_code == 200 else "MISS"
                headers = MutableHeaders(scope=message)
                headers.append("X-Gateway-Cache", cache_header)
                await send(message)
                return

            if message["type"] == "http.response.body":
                if should_cache:
                    body_chunks.append(message.get("body", b""))
                    if not message.get("more_body", False):
                        # Body complete — cache it and send
                        body = b"".join(body_chunks)
                        request.state._gateway_cached_response_body = body

                        # Build cached headers (excluding hop-by-hop)
                        hop = self._HOP_BY_HOP
                        cached_headers = {
                            k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
                            for k, v in (initial_message.get("headers", []))
                            if (k if isinstance(k, bytes) else k.encode()).lower() not in hop
                        }

                        # Determine media type from headers
                        raw_headers = dict(initial_message.get("headers", []))
                        media_type = raw_headers.get(b"content-type", b"application/json").decode()

                        entry = CacheEntry(
                            content=body,
                            media_type=media_type,
                            headers=cached_headers,
                            created_at=time.time(),
                            ttl=self.default_ttl,
                        )

                        try:
                            await cache.set(cache_key, entry.to_dict(), ttl=self.default_ttl)
                        except Exception as e:
                            # A failing write means the next identical request
                            # also misses; surface at WARNING so a degraded
                            # cache backend is visible in error logs and metrics
                            # rather than hidden at DEBUG.
                            record_cache_error(cache_type, "write")
                            logger.warning("cache_write_error", key=cache_key, error=str(e))

                        # Send the response with MISS headers
                        out_headers = MutableHeaders(scope=initial_message)
                        out_headers.append("X-Gateway-Cache", "MISS")
                        out_headers.append("X-Gateway-Cache-Age", "0")
                        out_headers.append("X-Gateway-Cache-TTL", str(self.default_ttl * 1000))
                        await send(initial_message)
                        await send(message)
                    return

                # Non-cacheable path: pass through body as-is
                await send(message)

        await self.app(scope, receive, buffering_send)

    def _cache_key(self, request: Request) -> str:
        """Generate cache key from request."""
        scope = self._client_cache_scope(request)
        return f"{request.method}:{request.url.path}:{request.url.query}:{scope}"

    def _client_cache_scope(self, request: Request) -> str:
        """Derive a cache scope to avoid cross-client data leakage.

        Prefer authenticated client ID when available; otherwise hash the API key.
        Falls back to 'public' for unauthenticated requests.
        """
        if getattr(request.state, "cache_public", False):
            return "public"

        # Prefer authenticated client if set elsewhere in the stack
        if hasattr(request.state, "client") and request.state.client:
            permission_hash = self._permissions_hash(request.state.client)
            return f"client:{request.state.client.id}:{permission_hash}"

        # Hash API key if present to avoid storing raw keys in cache
        api_key = request.headers.get("X-Gateway-Key")
        if api_key:
            import hashlib

            digest = hashlib.sha256(api_key.encode()).hexdigest()[:16]
            return f"key:{digest}"

        return "public"

    def _permissions_hash(self, client) -> str:
        """Stable hash of permissions to avoid stale cache on permission changes."""
        import hashlib
        import json

        providers = sorted(client.permissions.providers or [])
        feeds = sorted(client.permissions.feeds or [])
        ws_subscriptions_max = getattr(client.permissions, "ws_subscriptions_max", 0)
        role = getattr(client, "role", "client")

        cache_key = (
            tuple(providers),
            tuple(feeds),
            client.permissions.max_symbols,
            client.permissions.rate_limit,
            ws_subscriptions_max,
            role,
        )

        # Cache the hash on the client to avoid repeated JSON serialization per request.
        cached_key = getattr(client, "_permissions_hash_key", None)
        cached_value = getattr(client, "_permissions_hash_value", None)
        if cached_key == cache_key and cached_value:
            return cached_value

        payload = {
            "providers": providers,
            "feeds": feeds,
            "max_symbols": client.permissions.max_symbols,
            "rate_limit": client.permissions.rate_limit,
            "ws_subscriptions_max": ws_subscriptions_max,
            "role": role,
        }
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        value = digest[:8]
        client._permissions_hash_key = cache_key
        client._permissions_hash_value = value
        return value

    _PUBLIC_EXACT = frozenset({"/", "/openapi.json", "/docs", "/redoc"})
    _PUBLIC_PREFIXES = ("/health",)

    def _is_public_path(self, path: str) -> bool:
        return path in self._PUBLIC_EXACT or path.startswith(self._PUBLIC_PREFIXES)

    async def _ensure_authenticated(self, request: Request, api_key: str | None) -> Response | None:
        """Authenticate request to avoid serving cached data without auth checks."""
        from gateway.api.deps import get_authenticator

        if not api_key:
            return JSONResponse(
                status_code=401,
                content={"detail": {"code": "GW-E2001", "message": "Missing X-Gateway-Key header"}},
            )

        authenticator = get_authenticator()
        client = authenticator.authenticate(api_key)
        if client is None:
            return JSONResponse(
                status_code=401,
                content={"detail": {"code": "GW-E2002", "message": "Invalid API key"}},
            )

        # Attach client for downstream middleware and cache key scoping.
        request.state.client = client
        return None
