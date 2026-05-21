---
commit_hash: 869790a85d14
---

## Clusters

| Cluster | Files | Key Entities | External Deps | Risk Areas |
|---------|-------|-------------|---------------|------------|
| Application Bootstrap | main.py | lifespan, create_app, _check_port_available | uvicorn, signal | CORS misconfig in debug; SIGHUP scope; port-bind race |
| Event Wrapping | core/envelope.py | EventEnvelope, wrap_event, fast_wrap_streaming_event, compute_event_id | hashlib, pydantic | **Random ID in fast path** breaks dedup; instrument_type inference footgun; silent error envelope |
| Stream-to-Sink Dispatch | main.py:114-268 | _on_stream_data, _schedule_stream_sink_publish, _drain_stream_sink_publish_tasks | asyncio | Backpressure drops events silently; in-memory buffer not persisted |
| WebSocket Endpoint | api/websocket.py | websocket_endpoint, _wait_for_auth, _message_loop, _handle_message | starlette WebSocket | Permission-deny default is permissive when list empty; partial-success rollback gaps; post-receive size check |
| Authentication | core/auth.py | ClientAuthenticator, Client, ClientPermissions | yaml, hashlib | YAML key uniqueness not enforced; partial key in failure logs |
| Redis Sink | core/redis_sink.py + core/data_sink.py | RedisStreamsSink, DataSinkRegistry, _drain_buffer | redis.asyncio | Pool size silently capped; in-memory failure buffer (10K events ~ 10MB); race on concurrent reset |
| Provider Registry | core/registry.py + config/providers.yaml | ProviderRegistry, dynamic import | importlib | Silent provider drop on import/init failure; routes config not validated |
| Middleware Stack | api/middleware.py | RateLimitMiddleware, GlobalRateLimitMiddleware, CacheMiddleware, EventEnvelopeMiddleware, SecurityHeadersMiddleware | starlette ASGI | Cache circular import via lazy app ref; XFF first-IP trust; envelope wrap buffers entire body |
| Background Pollers | core/uw_poller.py + others | UWPoller, TreasuryPoller, QuotesPoller, etc. | TradingCalendar | EOD pollers must remember `instrument_type_override` per feed (easy to miss) |
| Request Dedup | core/dedup.py | RequestDeduplicator | asyncio | Per-process `hash()` randomization; future cleanup race on cancel |
