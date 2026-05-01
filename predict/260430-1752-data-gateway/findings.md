# Findings — Data-Gateway Predict (ranked by priority score)

**Score formula:** `severity_weight*0.4 + confidence_boost*0.2 + consensus_ratio*0.4`

---

## Finding 1: Stream-to-sink backpressure silently drops events

**Severity:** CRITICAL
**Confidence:** HIGH
**Location:** `gateway/main.py:226-241`
**Consensus:** 4/5 (AR confirm, SA abstain, PE confirm, RE confirm, DA confirm-with-conditions)
**Priority Score:** 2.12

**Evidence:**
`_schedule_stream_sink_publish` at line 227 checks `if len(_stream_sink_publish_tasks) >= _stream_sink_max_pending_tasks: record_stream_sink_dispatch_event("dropped_backpressure"); return`. Default `_stream_sink_max_pending_tasks=512` and `_stream_sink_max_inflight_publish=32`. At opening-bell volume across SIP equities + OPRA options + crypto + news (~50K events/sec aggregate), the 32 in-flight × ~5ms publish latency = ~6.4K/sec sustained throughput. The pipeline drops events with only a metric increment — no alarm wiring observed in `config/prometheus_alerts.yml`.

**Recommendation:**
1. **Short-term (operator):** Tune `data_sink_redis_pool_size` (currently capped at 64), bump `max_inflight_publish` to 128, replace `BlockingConnectionPool` with `ConnectionPool`. (DA-2's capacity-planning angle.)
2. **Short-term (observability):** Add Prometheus alert: `rate(stream_sink_dispatch_event_total{event="dropped_backpressure"}[5m]) > 0` → page.
3. **Medium-term:** Persist drop overflow to local SQLite or disk-backed queue for replay. Or kill the multiplexer on sustained drops (fail loud).
4. **Long-term:** Pre-filter at the multiplexer (DA-9) — drop sub-second quote updates that have no downstream consumers.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | Module-globals state makes this hard to test |
| Security Analyst | abstain | Outside core security domain |
| Performance Engineer | confirm | Capacity tuning is the immediate fix |
| Reliability Engineer | confirm | Silent data loss in financial pipeline |
| Devil's Advocate | confirm-with-conditions | Alarm prereq agreed; tuning is the actual fix |

**Debate Log:** [Round 1 PE→RE-1, Round 1 RE→DA-2, Round 2 DA→RE-1](./persona-debates.md)

---

## Finding 2: Random event_id in `fast_wrap_streaming_event` breaks gateway-level dedup

**Severity:** HIGH
**Confidence:** HIGH
**Location:** `gateway/core/envelope.py:496-500`
**Consensus:** 4/5
**Priority Score:** 1.72

**Evidence:**
Line 500: `event_id = os.urandom(16).hex()`. The standard path `wrap_event` computes a deterministic BLAKE2b hash from (provider, feed, instrument_key, ts_event, unique_fields). The streaming path generates pure random IDs. The same upstream Alpaca event wrapped twice (e.g. on reconnect with replayed messages) produces two distinct envelopes with two distinct event_ids. Gateway-level dedup at `gateway/core/uw_poller.py:240-256` uses `_mark_seen(event_id)` and Redis `_redis_dedupe.set_many(redis_items, ttl=...)` — this code path was designed assuming content-derived IDs. Random IDs make streaming-path dedup non-functional regardless of what Heber does.

**Recommendation:**
Pick ONE contract:
- **A (preferred):** Replace `os.urandom` with a cheap content hash. Modern xxhash or fnv1a on `f"{symbol}|{ts_event_str}|{event.get('S','')}|{sequence}"` is sub-microsecond (PE-5 evidence: BLAKE2b on 50 bytes ≈ 1µs). At 100K events/sec that's ~10% of one core — measurable but acceptable for correctness.
- **B:** Rename `fast_wrap_streaming_event` → `wrap_streaming_with_random_id` and add `quality_flags=["streaming","random_id"]`. Document explicitly that streaming envelopes have opaque IDs and consumers must derive their own dedup keys. Ensure Heber actually does so (PE/RE/DA agree this requires verification).

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | Two-contracts-one-name is wrong |
| Security Analyst | confirm | Dedup gaps = data integrity issue |
| Performance Engineer | confirm | Perf reasoning sound but stale benchmark assumption |
| Reliability Engineer | confirm | Same silent-failure pattern as F-05 |
| Devil's Advocate | dispute-then-conditional | Heber may handle it; needs verification before code change |

---

## Finding 3: ProviderRegistry tolerates init failure → degraded "healthy" gateway

**Severity:** HIGH
**Confidence:** HIGH
**Location:** `gateway/core/registry.py:38-46, :75-82`
**Consensus:** 3/5
**Priority Score:** 1.64

**Evidence:**
`load_from_config` wraps `_load_provider` in `try/except Exception` and continues. `_load_provider` itself catches `ImportError, AttributeError` and only logs a warning. If Alpaca's `initialize()` raises (bad creds, transient network), the gateway boots without Alpaca, `/health` returns 200, WebSocket subscribe returns "Provider access denied" at runtime — operator chases the wrong issue. `routes:` configured to use `[alpaca, yfinance]` silently degrades to `[yfinance]`. Same risk for any provider that an enabled route references.

**Recommendation:**
Add `required: true|false` field per provider in `config/providers.yaml`. Default `required: true` for any provider referenced by a route. In `load_from_config`:
```python
if config.get("required", True) and not settings.debug:
    raise GatewayBootError(f"required provider {name} failed to init")
```
Local dev (`GATEWAY_DEBUG=true`) keeps current tolerance for partial-key setups. Production fails fast.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | AR-6 — same recommendation independently |
| Security Analyst | abstain | Operational concern |
| Performance Engineer | abstain | Out of domain |
| Reliability Engineer | confirm | Healthy-but-broken violates contract |
| Devil's Advocate | confirm-with-conditions | DA-6 — prod-only enforcement is the right knob |

---

## Finding 4: Silent error swallowing pattern in envelope/middleware data path

**Severity:** HIGH
**Confidence:** HIGH
**Location:** `gateway/core/envelope.py:403-427`, `gateway/api/middleware.py:954-958`
**Consensus:** 3/5
**Priority Score:** 1.64

**Evidence:**
**Pattern A** — `wrap_event` (envelope.py:403): try/except around envelope construction returns "minimal fallback envelope" with `instrument_type="unknown"`, `instrument_key=f"unknown:{symbol}"`, `quality_flags=["error"]`. The fallback can't realistically be reached (the dict assembly itself doesn't raise), but if any field-extraction does fail, Heber receives a malformed envelope rather than the call raising.

**Pattern B** — `EventEnvelopeMiddleware._wrap_and_send` (middleware.py:954): bare `except Exception as e: logger.warning(...)` returns the ORIGINAL non-wrapped body with no `x-gateway-envelope: true` header. Sink publish is also silently skipped. Downstream consumers cannot distinguish "this endpoint never wraps" from "wrap failed."

**Why HIGH:** Both patterns convert errors into corrupt-but-routable data. For a financial pipeline this is worse than failures — failures get retried; corruption is permanent.

**Recommendation:**
- envelope.py:403: Remove the broad `except`. Identify what specific operation could fail (likely `parse_timestamp` on user-controlled input) and guard it precisely. Re-raise everything else.
- middleware.py:954: Either propagate to FastAPI's error handler (returning 500) or add `x-gateway-envelope: false` explicitly so consumers can detect the unwrapped path. Consider a feature flag `GATEWAY_STRICT_ENVELOPES=true` for environments that prefer fail-fast.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | Anti-pattern repeated in 2+ places |
| Security Analyst | confirm | Data corruption attack surface |
| Performance Engineer | abstain | Not a perf issue |
| Reliability Engineer | confirm | Promoted to meta-finding in Round 2 |
| Devil's Advocate | abstain | Defensive coding has its place; not challenged |

---

## Finding 5: Permissive empty-list permissions + cache scoping = cross-tenant data leak

**Severity:** HIGH (UPGRADED in debate)
**Confidence:** HIGH
**Location:** `gateway/api/websocket.py:661-670, 688-692` + `gateway/api/middleware.py:566-603`
**Consensus:** 3/5
**Priority Score:** 1.64

**Evidence:**
`_has_provider_permission` and `_has_feed_permission` return `True` if the client's permission list is empty (`if not allowed: return True`). Independently this is "permissive default" (DA-1: defensible, breaks-on-deploy is worse). BUT `CacheMiddleware._client_cache_scope` (middleware.py:546-554) keys cached responses by `f"client:{client.id}:{permissions_hash}"`. `_permissions_hash` computes a stable hash of the permissions tuple. Two clients with empty permissions produce the SAME `permissions_hash`. With permissive defaults: client A makes a UW flow request (cached under hash X), client B (also empty permissions) requests the same URL → cache HIT → reads A's response without a real permissions check.

**Concrete attack:** misconfigured client B can read cached upstream responses fetched on behalf of client A as long as they share the empty-permissions hash and request the same URL.

**Recommendation:**
Two-part:
1. **Cache scoping:** Salt `permissions_hash` with `client.id`. Two clients should never share a cache scope, regardless of permissions content.
2. **Permission default:** Add a startup-time validator (`gateway/cli.py audit-clients`) that warns/blocks clients with empty `providers` or `feeds`. Operators who genuinely want all-access use explicit `providers: ["*"]` token.

Either alone partly mitigates; both are recommended.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | Promoted to HIGH after cache-scope linkage in Round 2 |
| Security Analyst | confirm | SA-3 + Round 2 evidence — cross-tenant leak |
| Performance Engineer | abstain | |
| Reliability Engineer | confirm | Correctness, not just security |
| Devil's Advocate | dispute-then-partial | DA-1 still wants permissive default; conceded cache scope |

---

## Finding 6: EventEnvelopeMiddleware fully buffers response body, breaking streaming

**Severity:** HIGH
**Confidence:** HIGH
**Location:** `gateway/api/middleware.py:826-832, 1038-1097`
**Consensus:** 3/5
**Priority Score:** 1.64

**Evidence:**
`buffering_send` accumulates `body_chunks: list[bytes]` until `more_body=False`. The cacheability check at line 818 only excludes `text/event-stream` and `application/x-ndjson` from the **cache** path; the **envelope** path has no such exclusion. Any JSON streaming response (NDJSON, server-sent events, chunked transfer) is fully buffered up to `max_body_bytes=524288` before being wrapped. Per-request memory floor: 512KB × concurrent requests. For bulk endpoints returning >512KB, both EventEnvelopeMiddleware AND CacheMiddleware buffer independently.

**Recommendation:**
- Add NDJSON/SSE content-type exclusion to EventEnvelopeMiddleware (mirror CacheMiddleware:469).
- For non-streaming responses, consider streaming-wrap: emit `{"success":true,"envelope":{...},"data":` first, stream payload bytes, close `}`. Eliminates the 512KB floor.
- Document: `EventEnvelopeMiddleware` is incompatible with streaming response types; routes must opt-out via known content-types.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | Memory pressure under load |
| Security Analyst | abstain | |
| Performance Engineer | confirm | PE-1 + PE-9 (NDJSON evidence in Round 2) |
| Reliability Engineer | confirm | Breaks streaming response contract |
| Devil's Advocate | abstain | |

---

## Finding 7: API key prefix (10 chars) logged on auth failure → partial credential leak

**Severity:** HIGH
**Confidence:** HIGH
**Location:** `gateway/core/auth.py:118-124`
**Consensus:** 2/5
**Priority Score:** 1.56

**Evidence:**
On invalid key, lines 118-119:
```python
key_preview = api_key[:10] + "..." if len(api_key) > 10 else api_key
logger.warning("auth_failed_invalid_key", key_prefix=key_preview)
audit.auth_failure(..., metadata={"reason": "invalid_key", "key_prefix": key_preview})
```
Keys begin with `gw_` (per CLI generator), leaving 7 chars of secret material in the prefix. Logs at `logs/data-gateway_*.log` and the audit trail (used for compliance). A log breach gives an attacker 7-char-prefix narrowing for brute force AND a regulatory disclosure problem (partial credentials in compliance logs).

**Recommendation:**
Log only a hash:
```python
import hashlib
key_fingerprint = hashlib.sha256(api_key.encode()).hexdigest()[:8]
logger.warning("auth_failed_invalid_key", key_fingerprint=key_fingerprint, key_length=len(api_key))
```
Same diagnostic value (you can correlate failures), zero plaintext leakage.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | abstain | |
| Security Analyst | confirm | SA-1 + Round 1: also affects compliance audit trail |
| Performance Engineer | abstain | |
| Reliability Engineer | confirm | Pattern of data leakage |
| Devil's Advocate | abstain | Not contested |

---

## Finding 8: Debug-mode CORS allows `*` + credentials — actively used in docker-compose

**Severity:** HIGH
**Confidence:** HIGH
**Location:** `gateway/main.py:660-666` + `Data-Gateway/docker-compose.yml:11`
**Consensus:** 2/5
**Priority Score:** 1.56

**Evidence:**
`gateway/main.py:660`:
```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.debug else [],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```
`docker-compose.yml:11`: `GATEWAY_DEBUG=true`. The default development setup runs with the violating combination. CORS spec forbids `*` + credentials; browsers refuse, but non-browser clients (curl, custom HTTP clients, server-side attacks) are not protected. If the gateway dev container is ever exposed (port forward, port-mapping mistake), CSRF-style attacks become trivial.

**Recommendation:**
- Replace `allow_origins=["*"]` in debug with explicit local origins: `["http://localhost:5173", "http://127.0.0.1:5173", ...]` (EmpireUI dev port).
- Add a startup assertion:
  ```python
  if "*" in cors_origins and allow_credentials:
      raise RuntimeError("CORS misconfig: '*' + credentials is forbidden")
  ```
- Never combine wildcard origin with credentials. Either remove credentials in debug or specify origins.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | abstain | |
| Security Analyst | confirm | SA-2 — explicit spec violation, used in production-like config |
| Performance Engineer | abstain | |
| Reliability Engineer | confirm | Configuration safety |
| Devil's Advocate | abstain | Not contested |

---

## Finding 9: WebSocket message size validation AFTER full receive → memory amplification

**Severity:** MEDIUM
**Confidence:** HIGH
**Location:** `gateway/api/websocket.py:303-326`
**Consensus:** 3/5
**Priority Score:** 1.24

**Evidence:**
`_message_loop` does `raw = await websocket.receive()` first, then `if len(raw_text.encode("utf-8")) > max_bytes`. Starlette's WebSocket.receive does not enforce a frame-size cap. A 100MB JSON frame is fully allocated before being rejected. Repeated from many connections → OOM. The handler `continue`s rather than disconnecting on oversize, allowing the attack to repeat on the same connection.

**Recommendation:**
- Set frame cap at the ASGI layer: `uvicorn --ws-max-size N` (configurable via `GATEWAY_WS_MAX_FRAME_BYTES`).
- On oversize, close 1009 (Message Too Big) instead of `continue`.
- Keep the application-level check for clean error codes when within frame size but over policy size.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | abstain | |
| Security Analyst | confirm | SA-5 |
| Performance Engineer | confirm | Memory pressure |
| Reliability Engineer | confirm | DoS surface |
| Devil's Advocate | abstain | |

---

## Finding 10: Stream-sink shutdown drain timeout (2s) shorter than publish timeout (5s)

**Severity:** MEDIUM
**Confidence:** HIGH
**Location:** `gateway/main.py:251` (default), called from `:581`
**Consensus:** 3/5
**Priority Score:** 1.24

**Evidence:**
`_drain_stream_sink_publish_tasks(timeout_seconds=2.0)` cancels in-flight publishes after 2 seconds. `redis_sink._operation_timeout_seconds` defaults to 5.0s. With 32 inflight at shutdown, worst-case 5s publishes get cancelled at 2s. Cancelled publishes don't re-enter `_failed_buffer` — they're lost. Combined with F-14, the data-loss window at shutdown is the (publishes_in_flight + buffered_events).

**Recommendation:**
Set drain timeout to `max(operation_timeout_seconds, 5.0) + 1.0` so normal publishes complete. Two-pass drain (graceful wait then forced cancel) is cleaner.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | Mismatched timeouts indicate design oversight |
| Security Analyst | abstain | |
| Performance Engineer | confirm | |
| Reliability Engineer | confirm | RE-8 |
| Devil's Advocate | abstain | |

---

## Finding 11: CI perf-gate erosion — recent commit pattern masks regressions

**Severity:** MEDIUM (process, not code)
**Confidence:** MEDIUM
**Location:** `git log` recent — `869790a, c901c4a, 695486e, 27394e7, 6062e40`
**Consensus:** 4/5
**Priority Score:** 1.24

**Evidence:**
Recent 5 commits are all CI perf-gate adjustments: "raise perf baselines to CI runner reality", "extend static merge to cover baselines", "relax path-normalization perf budget", "propagate static budget increases", "stable contract sort key". Pattern: budgets keep going up to keep CI green. The signal "did the gateway regress against production targets?" is being lost in the CI tuning noise.

**Recommendation:**
- Audit `config/perf_baseline.json` git history. If trend over past 3 months is monotonically increasing, that's CI runner drift, not real regression.
- Split perf gates: `ci-perf-budgets.json` (loose, for noisy GitHub runners) vs `prod-perf-budgets.json` (strict, run nightly on bare metal). Use the strict gate for product decisions.
- Add a weekly review of `prod-perf-budgets.json` trend lines.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | Process drift |
| Security Analyst | abstain | |
| Performance Engineer | confirm | Direct domain concern |
| Reliability Engineer | confirm | Visibility into perf is reliability concern |
| Devil's Advocate | confirm | DA-8 originated this; DA-11 reinforced |

---

## Finding 12: X-Forwarded-For first-IP trust → spoofable through untrusted proxy chain

**Severity:** MEDIUM
**Confidence:** HIGH
**Location:** `gateway/api/middleware.py:1265-1273`
**Consensus:** 2/5
**Priority Score:** 1.16

**Evidence:**
`_get_client_ip` returns `forwarded.decode().split(",")[0].strip()` — the first (leftmost) IP, which is fully attacker-controlled. Per-IP rate limit (1000 req/min) and IP block list bypassable via `X-Forwarded-For: 1.1.1.1, real-ip`.

**Recommendation:**
Walk XFF from rightmost, returning the first IP not in a `trusted_proxy_cidrs` allowlist. Require explicit configuration of trusted proxies when `trust_proxy_headers=true`. Default to socket peer IP otherwise.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | abstain | |
| Security Analyst | confirm | SA-4 |
| Performance Engineer | abstain | |
| Reliability Engineer | confirm | Operational footgun |
| Devil's Advocate | abstain | |

---

## Finding 13: Plaintext API keys in clients.yaml — preferred over hashed at lookup

**Severity:** MEDIUM
**Confidence:** HIGH
**Location:** `gateway/core/auth.py:79-86, 109-115`
**Consensus:** 2/5
**Priority Score:** 1.16

**Evidence:**
The authenticator checks plaintext keys FIRST then hashed. `clients.yaml` is committed (referenced via symlink). Even with `# pragma: allowlist secret` annotations, plaintext production keys in YAML are one mistake from leaking via any clone, branch, or CI artifact.

**Recommendation:**
- Log a warning at load time when `key:` (plaintext) is used in non-debug mode.
- Roadmap to deprecate plaintext fully. Production should use `key_hash:` or env-var injection (`!ENV ${CERBERUS_KEY}`).

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | Tech debt |
| Security Analyst | confirm | SA-7 |
| Performance Engineer | abstain | |
| Reliability Engineer | abstain | |
| Devil's Advocate | abstain | |

---

## Finding 14: Failed-event buffer in memory; lost on process death

**Severity:** MEDIUM (downgraded from HIGH in debate)
**Confidence:** HIGH
**Location:** `gateway/core/redis_sink.py:54, 96-97, close():632`
**Consensus:** 2/5
**Priority Score:** 1.16

**Evidence:**
`FAILED_EVENT_BUFFER_CAPACITY = 10_000` in-memory deque. `close()` does NOT drain the buffer — only awaits in-flight tasks. Process death (OOM, SIGKILL, container restart) loses up to 10K events.

**Recommendation:**
- Add explicit drain attempt in `close()` before returning.
- For higher reliability tier: persist `_failed_buffer` to disk on shutdown (JSONL to a known path). Restore on next boot.
- Document the data-loss window in `runbook.md`.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | |
| Security Analyst | abstain | |
| Performance Engineer | abstain | |
| Reliability Engineer | confirm | RE-2 — downgraded after DA-4's trading-rhythm point |
| Devil's Advocate | dispute | DA-4 — current behavior matches operator profile |

---

## Finding 15: Redis pool size silently capped at 64

**Severity:** MEDIUM
**Confidence:** HIGH
**Location:** `gateway/core/redis_sink.py:86`
**Consensus:** 2/5
**Priority Score:** 1.16

**Evidence:**
`self._pool_size = max(1, min(64, int(pool_size)))`. Operator-passed `data_sink_redis_pool_size=128` silently becomes 64 with no log. For a system that already drops events under backpressure (F-01), an under-provisioned pool is a contributing factor.

**Recommendation:**
Log at WARNING when caller exceeds the cap. Or move the cap into Settings validation so it's visible at startup. Or raise the cap with a documented justification.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | abstain | |
| Security Analyst | abstain | |
| Performance Engineer | confirm | PE-2 |
| Reliability Engineer | confirm | Footgun for capacity tuning (relates to F-01) |
| Devil's Advocate | abstain | |

---

## Finding 16: Six pollers duplicate lifecycle plumbing (~120 lines)

**Severity:** LOW
**Confidence:** HIGH
**Location:** `gateway/main.py:411-538`
**Consensus:** 2/5
**Priority Score:** 0.76

**Evidence:**
UW, Treasury, Quotes, Trades, Crypto, News pollers each have: (a) feature gate, (b) `start_*_poller` import, (c) initialization log, (d) corresponding `stop_*_poller` in shutdown. New pollers add multiplicative entries.

**Recommendation:**
Extract `PollerRegistry.register(name, factory, gate_fn)`. Lifecycle becomes: `await registry.start_all(); ... await registry.stop_all()`. Adding a poller is one line.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | AR-5 |
| Security Analyst | abstain | |
| Performance Engineer | confirm | PE-6 (related) |
| Reliability Engineer | abstain | |
| Devil's Advocate | abstain | |

---

## Finding 17: SIGHUP partial reload — providers.yaml ignored

**Severity:** LOW
**Confidence:** HIGH
**Location:** `gateway/main.py:384-393`
**Consensus:** 1/5 (DA disputes; recommendation reduced to docs)
**Priority Score:** 0.68

**Evidence:**
SIGHUP reloads `Settings` and `ClientAuthenticator`, NOT `ProviderRegistry`. Adding/removing providers, changing routes, or toggling `enabled` requires a full restart.

**Recommendation:**
DA-7's framing wins: provider hot-reload is genuinely complex (stateful httpx clients, WS connections). The fix is documentation: add to `runbook.md` an explicit table of what SIGHUP does and doesn't reload. No code change.

**Persona Votes:**
| Persona | Vote | Note |
|---------|------|------|
| Architecture Reviewer | confirm | AR-4 |
| Security Analyst | abstain | |
| Performance Engineer | abstain | |
| Reliability Engineer | abstain | |
| Devil's Advocate | dispute | DA-7 — intentional; doc-only fix |

---

## Minority / Preserved Findings (anti-herd)

These received only 1 confirm but are recorded per anti-herd protocol — minorities are often right on non-obvious issues.

| ID | Title | Severity | Source |
|----|-------|----------|--------|
| M-01 | BLAKE2b 16-byte digest = 2^64 collision space (theoretical risk at multi-billion-event scale) | LOW | SA-8 (DA disputed via math, but preserved) |
| M-02 | WS subscribe partial-success leaves successful subs despite reported failures | MEDIUM | RE-4 |
| M-03 | RateLimitBucket sliding-window deque growth under burst | MEDIUM | PE-3 |
| M-04 | Stream-sink dispatch via module globals — hard to unit-test | LOW | AR-7 |
| M-05 | TOCTOU race in _check_port_available vs uvicorn bind | LOW | RE-5 |
| M-06 | Lazy circular import of gateway.main from CacheMiddleware | MEDIUM | AR-2 |
| M-07 | API key 16-char prefix in rate-limit bucket (math says non-issue with random keys) | LOW | SA-6 (DA-5 disputed convincingly; preserved as caveat) |
| M-08 | orjson hot-path duplicate codepath in `_on_stream_data` | LOW | PE-7 |
| M-09 | WebSocket subscription set f-string allocation churn | LOW | PE-8 |
