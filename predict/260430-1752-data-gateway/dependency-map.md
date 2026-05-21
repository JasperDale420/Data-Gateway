---
commit_hash: 869790a85d14
---

## Import Graph (subset)

| File | Imports From | Symbols |
|------|-------------|---------|
| gateway/main.py | gateway.api | All routers |
| gateway/main.py | gateway.core.{registry,stream,shutdown,globals,metrics} | ProviderRegistry, StreamMultiplexer, ShutdownCoordinator |
| gateway/api/middleware.py | gateway.core.envelope | wrap_event |
| gateway/api/middleware.py | gateway.main (lazy, in request path) | app — circular |
| gateway/api/websocket.py | gateway.api.deps | get_authenticator, get_connection_manager, get_multiplexer |
| gateway/api/websocket.py | gateway.core.{auth,connections,security} | ClientAuthenticator, ConnectionManager, get_input_validator |
| gateway/core/redis_sink.py | gateway.core.data_sink | DataSink (ABC) |
| gateway/core/redis_sink.py | redis.asyncio (lazy in _create_client) | BlockingConnectionPool, Redis |
| gateway/core/uw_poller.py | gateway.core.{envelope,base_poller,calendar,dedup} | wrap_event, BasePoller, TradingCalendar |
| gateway/core/auth.py | gateway.core.audit | get_audit_logger |
| gateway/core/envelope.py | gateway.core.{logger,metrics,timeutils} | record_envelope_created, parse_timestamp |

## Call Graph (hot path)

| Caller | Callee | File:Line | Type |
|--------|--------|-----------|------|
| Alpaca WS | StreamMultiplexer fanout | stream.py:739+ | upstream → fanout |
| StreamMultiplexer fanout | _on_stream_data (per client) | main.py:114 | callback |
| _on_stream_data | _schedule_stream_sink_publish | main.py:226 | dispatch |
| _schedule_stream_sink_publish | sink_registry.publish_all | data_sink.py | sink fanout |
| sink_registry.publish_all | RedisStreamsSink.publish | redis_sink.py:377 | xadd |
| RedisStreamsSink.publish | _buffer_failed_event (on exhausted retry) | redis_sink.py:475 | in-memory buffer |
| UWPoller._poll_loop | wrap_event | envelope.py:282 | per event |
| UWPoller._publish_envelopes | sink_registry.publish_all_batch | uw_poller.py:258 | batch publish |
| WebSocket /ws | _wait_for_auth | websocket.py:200 | gate |
| WebSocket /ws | _message_loop → _handle_message | websocket.py:291,359 | dispatch |
| _handle_message (subscribe) | multiplexer.client_subscribe | websocket.py:461 | upstream subscribe |
| EventEnvelopeMiddleware | wrap_event (per response) | middleware.py:864 | per request |
| EventEnvelopeMiddleware | sink_registry.publish_all (background task) | middleware.py:916 | per request |

## Data Flows

| Source | Transform | Sink | Risk Areas |
|--------|-----------|------|------------|
| Alpaca WS msgpack frame | msgpack decode → fast_wrap_streaming_event (random event_id) | client WS + Redis Streams | **Random event_id defeats dedup**; backpressure drops |
| UW REST poll | provider call → wrap_event (BLAKE2b id) → publish_all_batch | Redis Streams | Per-underlying analytics need `instrument_type_override` (footgun) |
| Client REST | router → cache lookup → upstream provider → cache store → envelope wrap | Response + Redis Streams (eligible routes) | Body buffered for wrapping (memory pressure) |
| Client WS auth | receive_json → authenticator.authenticate(key) | Connection state | Logs first 10 chars of failed key (partial credential leak) |
| Inbound IP | GlobalRateLimit._get_client_ip(scope) | rate-limit decision | First X-Forwarded-For is spoofable through untrusted proxy chain |
