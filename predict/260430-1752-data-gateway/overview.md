# Predict Analysis — Data-Gateway

**Date:** 2026-04-30 17:52
**Scope:** `gateway/main.py`, `gateway/core/{envelope,redis_sink,dedup,auth,registry,data_sink,uw_poller,security}.py`, `gateway/api/{websocket,middleware}.py` (10 files, ~5,800 LOC)
**Personas:** 5 (Architecture Reviewer, Security Analyst, Performance Engineer, Reliability Engineer, Devil's Advocate)
**Debate Rounds:** 2 completed
**Commit Hash:** 869790a85d14
**Branch:** fix/eager-stocks-ws-readiness
**Anti-Herd Status:** PASSED (flip_rate ≈ 0.16, entropy high across 4 severities, minorities preserved)

## Summary

- **Total Findings:** 17 ranked + 9 minority preserved = 26
  - Confirmed (≥3 of 5): 11
  - Probable (2 of 5): 6
  - Minority (1 of 5): 9
  - Discarded: 0
- **Severity Breakdown (ranked findings):** Critical: 1 | High: 7 | Medium: 7 | Low: 2
- **Composite Score:** `predict_score = 11*15 + 6*8 + 9*3 + (5/5)*20 + (2/2)*10 + 5 = 165+48+27+20+10+5 = 275`

## Top Findings

1. [Stream-to-sink backpressure silently drops events](./findings.md#finding-1-stream-to-sink-backpressure-silently-drops-events) — **CRITICAL** | 4/5 consensus | priority 2.12
2. [Random event_id in fast_wrap_streaming_event breaks gateway-level dedup](./findings.md#finding-2-random-event_id-in-fast_wrap_streaming_event-breaks-gateway-level-dedup) — **HIGH** | 4/5 | 1.72
3. [ProviderRegistry tolerates init failure → degraded "healthy" gateway](./findings.md#finding-3-providerregistry-tolerates-init-failure--degraded-healthy-gateway) — **HIGH** | 3/5 | 1.64
4. [Silent error swallowing pattern in envelope/middleware data path](./findings.md#finding-4-silent-error-swallowing-pattern-in-envelopemiddleware-data-path) — **HIGH** | 3/5 | 1.64
5. [Permissive empty-list permissions + cache scoping = cross-tenant data leak](./findings.md#finding-5-permissive-empty-list-permissions--cache-scoping--cross-tenant-data-leak) — **HIGH** | 3/5 | 1.64
6. [EventEnvelopeMiddleware fully buffers response body, breaking streaming](./findings.md#finding-6-eventenvelopemiddleware-fully-buffers-response-body-breaking-streaming) — **HIGH** | 3/5 | 1.64
7. [API key prefix (10 chars) logged on auth failure → partial credential leak](./findings.md#finding-7-api-key-prefix-10-chars-logged-on-auth-failure--partial-credential-leak) — **HIGH** | 2/5 | 1.56
8. [Debug-mode CORS allows `*` + credentials — actively used in docker-compose](./findings.md#finding-8-debug-mode-cors-allows---credentials--actively-used-in-docker-compose) — **HIGH** | 2/5 | 1.56

## Cross-Cutting Themes

Three patterns recur across findings:

**Theme A — Silent failure as design choice.** F-01 (backpressure drop), F-04 (envelope wrap fallback + middleware envelope fallback), F-14 (failed-event buffer loss on crash). Each individually has its rationale, but in aggregate the gateway's data path leans heavily on "log a warning and keep going." For a financial pipeline, undetected silent failures are worse than loud failures — failures get retried; corruption is permanent. Pair this report with an audit of every `except Exception as e: logger.warning(...)` in the data path.

**Theme B — Permissive defaults compound badly.** F-03 (provider load tolerance) + F-05 (empty permission lists + cache scoping). Each defends as "operator-friendly default," but composition produces emergent unsafe behavior. Recommendation: a single `gateway lint` or `audit-clients` command that surfaces all permissive-default usage.

**Theme C — Operational visibility gaps.** F-01's missing alarm rule, F-11's CI-perf-gate erosion, F-15's silent cap on pool size. These aren't code bugs; they're observability decisions. Most lethal because they make all the other findings harder to detect in production.

## What the Devil's Advocate Saved

DA actively challenged 5 of 11 confirmed findings. Notable saves:
- **DA-1** (against SA-3): refused to make permission default deny — would break every existing client deployment when new providers are added. Forced SA to identify the cache-scoping linkage which strengthened the finding.
- **DA-5** (against SA-6): math eliminated the 16-char prefix collision concern — preserved as M-07 caveat only.
- **DA-2** (against RE-1): "alarm prerequisite, tuning is the fix" — the recommendation in F-01 reflects this two-part action.
- **DA-6** (against RE-6): "prod-only enforcement" prevented breaking local-dev workflows in F-03's recommendation.
- **DA-3** (against AR-1): "Heber may handle dedup itself" — dispute partially conceded, but the gateway-level dedup evidence in `uw_poller` made AR-1 stand.

## Files in This Report

- [Findings](./findings.md) — 17 ranked + 9 minority, full evidence and per-persona votes
- [Hypothesis Queue](./hypothesis-queue.md) — 15 testable hypotheses with suggested chain targets
- [Persona Debates](./persona-debates.md) — full Phase 4 + Round 1 + Round 2 transcripts
- [Iteration Log](./predict-results.tsv) — per-persona per-round metrics
- [Handoff](./handoff.json) — machine-readable schema for downstream chain tools
- [Knowledge: Codebase Analysis](./codebase-analysis.md)
- [Knowledge: Dependency Map](./dependency-map.md)
- [Knowledge: Component Clusters](./component-clusters.md)

## Recommended Next Steps

If you have time for ONE follow-up:
- **`/autoresearch:debug` on H-01** — empirically reproduce the backpressure drops under synthetic load. This is the single most impactful issue.

If you have time for TWO:
- Add the above + **`/autoresearch:security` on H-04, H-07, H-08, H-12, H-13** — the security-class findings together make a coherent audit pass.

If you have time for THREE:
- Add **`/autoresearch:fix` on H-05, H-15** — fast wins (dead code removal, startup warnings) that reduce the finding surface before deeper investigation.

The full chain (`debug → security → fix → ship`) was NOT auto-triggered. Run individually after reviewing this report.
