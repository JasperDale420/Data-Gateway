# Hypothesis Queue — Data-Gateway Predict

Ranked, testable hypotheses for downstream chain consumption (debug, security, fix, ship).

| Rank | ID | Hypothesis | Confidence | Location | Source Persona |
|------|----|-----------|-----------|----------|----------------|
| 1 | H-01 | Under sustained 50K events/sec aggregate stream load (opening bell), `_stream_sink_publish_tasks` reaches `max_pending_tasks=512` and silently drops events; observable as `record_stream_sink_dispatch_event("dropped_backpressure")` counter rising while no alarm fires | HIGH | gateway/main.py:226-241 | RE (4/5 confirmed) |
| 2 | H-02 | The same Alpaca WebSocket event delivered twice (e.g. on reconnect with replayed messages) produces two distinct envelopes with two distinct `event_id` values, and the gateway's `uw_poller` dedup state never sees streaming events at all because they bypass `_mark_seen` | HIGH | gateway/core/envelope.py:496-500 | AR (4/5 confirmed) |
| 3 | H-03 | If Alpaca's `provider.initialize()` raises during boot, the gateway starts with `/health` returning 200 and `/health/ready` also returning 200, while WebSocket subscribe and Alpaca routes fail at runtime with "Provider access denied" | HIGH | gateway/core/registry.py:38-46 | RE+AR (3/5 confirmed) |
| 4 | H-04 | Two clients with `permissions.providers=[]` and `permissions.feeds=[]` share the same `permissions_hash` in `CacheMiddleware._client_cache_scope`, allowing client B to read client A's cached API responses on the same URL | HIGH | gateway/api/middleware.py:566-603 | SA (3/5 confirmed) |
| 5 | H-05 | `wrap_event` exception path is unreachable in production (the dict assembly cannot raise after typing checks); the broad except + fallback envelope is dead defensive code that hides genuine errors when added later | MEDIUM | gateway/core/envelope.py:403-427 | RE (3/5 confirmed) |
| 6 | H-06 | A streaming JSON response from a backfill or bulk endpoint, larger than `max_body_bytes=524288`, is fully buffered by `EventEnvelopeMiddleware` despite content-type indicating streaming intent | HIGH | gateway/api/middleware.py:826 | PE (3/5 confirmed) |
| 7 | H-07 | An API key beginning with `gw_xxxxxxx...` will appear in `logs/data-gateway_errors_*.log` as `key_prefix=gw_xxxxxxx...` after a single failed authentication, leaking the first 7 random characters | HIGH | gateway/core/auth.py:118 | SA (2/5 confirmed) |
| 8 | H-08 | A non-browser client sending `Origin: https://attacker.example` to the dev container (with `GATEWAY_DEBUG=true`) will receive `Access-Control-Allow-Origin: *` and `Access-Control-Allow-Credentials: true` in the response | HIGH | gateway/main.py:660-666 + docker-compose.yml:11 | SA (2/5 confirmed) |
| 9 | H-09 | A 100MB JSON frame sent via the /ws WebSocket endpoint will be fully buffered into Python memory before being rejected with "WebSocket message exceeds size limit"; the connection stays open allowing repeated attempts | HIGH | gateway/api/websocket.py:303 | SA (3/5 confirmed) |
| 10 | H-10 | When the gateway shuts down with 30 in-flight stream-sink publishes that each take ~3s, the 2s drain timeout cancels them mid-publish; cancelled publishes do NOT re-enter `_failed_buffer` and the events are lost | HIGH | gateway/main.py:251 | RE (3/5 confirmed) |
| 11 | H-11 | The git history of `config/perf_baseline.json` over the past 3 months shows monotonically increasing budgets, indicating CI runner drift rather than real performance regressions; production performance trends are unknown because no separate prod baseline exists | MEDIUM | git log + config/perf_baseline.json | DA (4/5 confirmed) |
| 12 | H-12 | A request with `X-Forwarded-For: 1.1.1.1, <real-ip>` (when the gateway is configured with `behind_trusted_proxy=true`) will be rate-limited under the bucket for 1.1.1.1, allowing trivial bypass of the per-IP 1000-req/min limit | HIGH | gateway/api/middleware.py:1265-1273 | SA (2/5 confirmed) |
| 13 | H-13 | The `clients.yaml` file in the repo contains plaintext keys for production clients (cerberus, 3roses, orion); on any clone with read access, the keys are immediately usable | HIGH | gateway/core/auth.py:79 + config/clients.yaml | SA (2/5 confirmed) |
| 14 | H-14 | Killing the gateway process (SIGKILL, OOM-killer) while `_failed_buffer` contains N events results in those N events never reaching Heber, and there is no on-disk record they ever existed | HIGH | gateway/core/redis_sink.py:96 | RE (2/5 confirmed) |
| 15 | H-15 | An operator setting `GATEWAY_DATA_SINK_REDIS_POOL_SIZE=128` in the environment will see `redis_sink_connected pool_size=64` in startup logs without any warning that the value was clamped | HIGH | gateway/core/redis_sink.py:86 | PE (2/5 confirmed) |

## Recommended downstream chain

For each top-5 hypothesis, the natural chain target:

| Hypothesis | Suggested chain | Reason |
|-----------|-----------------|--------|
| H-01 | `--chain debug` then `--chain ship` | Reproduce drop conditions with synthetic load; gate ship on alert presence |
| H-02 | `--chain debug` | Test by replaying same Alpaca event; verify gateway dedup behavior |
| H-03 | `--chain debug` | Inject init failure in test, observe /health/ready response |
| H-04 | `--chain security` | Cross-tenant data leak — STRIDE Information Disclosure |
| H-05 | `--chain fix` | Remove dead code |
| H-06 | `--chain debug` | Send streaming response, observe memory profile |
| H-07 | `--chain security` | OWASP A04 Insecure Design / A09 Logging |
| H-08 | `--chain security` | OWASP A05 Security Misconfiguration |
| H-09 | `--chain security` then `--chain debug` | DoS surface; verify with attack script |
| H-10 | `--chain debug` | Reproduce shutdown with in-flight publishes |
| H-11 | `--chain scenario` | Edge cases of CI runner variance vs prod targets |
| H-12 | `--chain security` | OWASP A01 Broken Access Control |
| H-13 | `--chain security` | OWASP A07 Identification & Auth Failures |
| H-14 | `--chain debug` | Reproduce kill-during-buffer-nonempty scenario |
| H-15 | `--chain fix` | Add startup warning |
