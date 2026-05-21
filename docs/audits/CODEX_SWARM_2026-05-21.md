# Codex Swarm Audit — Data-Gateway — 2026-05-21

## Provenance

- 6 specialized review agents, each dispatched to a slice of the codebase with a distinct review lens (security, concurrency, providers, schemas, background services, API+middleware)
- Each agent drove `codex exec` (gpt-5.4 / gpt-5.5, high reasoning, read-only sandbox) for independent verification
- Reviewed against commit `1d418f4` (master, post PR #32) over ~95K LOC
- 82 findings total: 23 BLOCKERs, 47 SIGNIFICANTs, 11 MINOR/NIT

## Headline outcome

The most consequential finding was a **silent production data-integrity emergency**: `StreamMultiplexer` had been bypassing the Heber sink for ALL upstream Alpaca WebSocket events. Verified by runtime evidence — 192 151 entries in `heber:events` over 6 hours of uptime, **zero** with `source:stream`. Fixed in PR #33 (commit `c1fdce2`).

The whole bounded-queue refactor in PR #32 (`6a54115`, `b36b4de`, et al.) was protecting a code path streaming events never traversed. Friday 2026-05-15's 32 757 `data_sink_backpressure_drop` events were poller bursts, not streaming bursts — the streaming-source RCA was miscalibrated.

## BLOCKERs by theme

### A. Live-capital cross-client exposure (2)

Multiple Empire trading systems share one Alpaca account behind distinct gateway keys. The shared account + missing per-client scoping creates real attack/accident surface between trading bots.

- **No per-client order ownership isolation** — `gateway/api/alpaca/trading.py:497,575,693`. `replace_order`, `cancel_order`, `get_order`, `get_orders` never bind `order_id` to `client.id` → any auth'd trading client can list / mutate / cancel another client's open orders.
- **`client_order_id` not namespaced per client** — `gateway/api/alpaca/trading.py:408-657`. PR #32's idempotency plumbing forwards caller-supplied `client_order_id` verbatim to Alpaca. Client B can collide with A's idempotency key (gets routed to A's order), probe A's order state, or replay A's key. The PR #32 plumbing actively makes this worse than the prior unscoped state.

**Fix shape**: prefix every effective `client_order_id` with `c-{client.id}-` server-side; persist `(client.id, order_id)` ownership map at create-time; gate all read/mutate on it.

### B. Silent data loss to Heber (11)

- Cancelled dedup leader hangs all followers — `gateway/core/dedup.py:71-84` (CancelledError not caught by `except Exception`)
- Half-open circuit admits unlimited concurrent probes — `gateway/core/circuit_breaker.py:154-180` (guard only consulted in OPEN branch)
- Lazy-connect race spawns duplicate upstream WS connections — `gateway/core/stream.py:1006-1055`
- Slow WS client stalls fanout + upstream reads — `gateway/core/connections.py:217-235` (no per-send timeout)
- Sink shutdown happens before producers stop — `gateway/main.py:513` (8-step shutdown step 6 closes sink before step 7 stops pollers)
- Batch-publish dedupe marks wrong events as seen — `gateway/core/{quotes,trades,crypto,news}_poller.py` (`to_publish[:published]` wrong on partial batch failure)
- UW EOD work blocks main loop — `gateway/core/uw_poller.py:424-429` (inline await starves darkpool polling)
- `historic_option_volume` emits malformed option keys — `gateway/core/uw_poller.py:917` (per-underlying analytics with expiry mis-classified as `option:{ticker}` without OCC suffix)
- Date-based feeds hash wall-clock time — `gateway/core/envelope.py:338` (defeats Heber's three-layer dedup)
- Alpaca option backfill classified as equity — `gateway/core/envelope.py:275`
- `CircuitOpenError` in worker drops the queued event — `gateway/core/data_sink.py:447-468` (regression from PR #32 — breaks "single buffer site" invariant)

### C. Schema integrity (6)

- Naive datetimes accepted across schema surface (`gateway/schemas/*.py` + `empire_schemas`)
- Prices/strikes/sizes/volume/OI have no positivity constraints (`gateway/schemas/options.py:34` + numeric fields across all)
- `ResponseMeta` silently mislabels missing provider as `alpaca` (`gateway/schemas/base.py:8` → `empire_schemas/responses.py:92-99`)
- Corporate-action normalization missing required `ex_date` in 7 branches (`gateway/providers/alpaca/corporate.py:131-331`)
- AlphaVantage / yfinance falsely advertise `supports_bars=True` (`gateway/providers/{alphavantage,yfinance}.py`)
- UW `get_realized_volatility()` calls wrong endpoint (`gateway/providers/uw/market.py:194-220`)

### D. API contract / caching (3)

- Cached authenticated GETs never invalidated by writes — `gateway/api/middleware.py:383-386`
- `response_model=SuccessResponse` strips actual payload to `{success:true,data:null,...}` — 12+ handlers across `replay.py`, `bulk.py`, `quality.py`, `admin.py`
- `/health/ready` returns 200 even when readiness fails — `gateway/api/health.py:35,140` (orchestrators read status code, not body)

### E. Pre-existing security (1)

- Body-size limit only honors `Content-Length` header — `gateway/api/middleware.py:72`. Chunked transfer encoding bypasses the 413 guard. DoS / memory exhaustion vector.

## Phased action plan

| Phase | Scope | Status |
|---|---|---|
| 1 | Streaming sink bypass (PR #33) | ✅ merged 2026-05-21 |
| 2 | Cross-client `client_order_id` namespacing + ownership map | 🚧 swarm in flight |
| 3a | Async correctness BLOCKERs (5) | 🚧 swarm in flight |
| 3b | Schema integrity BLOCKERs (6) | 🚧 swarm in flight (cross-repo coordination flagged) |
| 3c | Sink + envelope BLOCKERs (5) | 🚧 swarm in flight |
| 3d | API contract + caching BLOCKERs (3) | 🚧 in this PR |
| 3e | Chunked body DoS BLOCKER | 🚧 in this PR |
| 4 | Auth + caching SIGNIFICANTs | pending |
| 5 | SIGNIFICANT batches (47 items) | pending |
| 6 | MINOR/NIT cleanup (11 items) | pending |

## Cross-repo follow-ups

Schema-level BLOCKERs in `empire-schemas` (naive datetimes, positivity constraints, `ResponseMeta` default) ripple to every Empire service. Coordinated rollout needed; track separately. Data-Gateway-side validators are landing first as the defensive layer.

## Lessons

1. **The streaming-sink bypass had been silent for an unknown period** — the production gateway was logging detailed metrics about a sink path streaming events never traversed. Dashboards were partially fictional.
2. **Two reviewers found different BLOCKERs in adjacent code**: the security agent caught the cross-client `client_order_id` exposure that the concurrency agent missed. Diverse review lenses on the same codebase reveal categorically different findings.
3. **Codex caught real follow-up bugs in the emergency fix itself**: 2 rounds of codex review on PR #33 caught (a) the wrong-prefix retry URL and (b) the zero-subscriber edge case. The fix-and-re-review loop was load-bearing.
