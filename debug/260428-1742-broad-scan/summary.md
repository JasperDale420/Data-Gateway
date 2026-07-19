# Debug Summary — 2026-04-28 broad-scan

## Setup

- **Mode:** Autonomous bug hunt (no specific symptom)
- **Scope:** Entire `gateway/` codebase
- **Depth:** 41 hypothesis iterations
- **Baseline:** 900 tests pass, ruff clean, mypy clean, types clean

## Bugs Found

| Severity | Count |
|----------|-------|
| HIGH | 1 |
| MEDIUM | 1 |
| LOW | 4 |
| **Total** | **6** |

## Distribution by File

| File | Bugs |
|------|------|
| `gateway/core/redis_sink.py` | 1 (HIGH) |
| `gateway/core/uw_poller.py` | 2 (MEDIUM, LOW) |
| `gateway/core/calendar.py` | 1 (LOW) |
| `gateway/providers/alpaca/_base.py` | 1 (LOW) |
| `gateway/api/middleware.py` (+ rate_limiter, cache, circuit_breaker) | 1 (LOW, pervasive) |

## Top Recommendation

**Fix Bug 1 (`redis_sink.py:151`) before any release** — it negates the value of every other Redis-resilience improvement in the recent CHANGELOG. The fix is one line plus a `set` attribute initialization. Risk of fix: near-zero.

## Investigation Quality

- **Files investigated:** 11 (`main.py`, `redis_sink.py`, `data_sink.py`, `option_capture.py`, `connections.py`, `uw_poller.py`, `backfill.py`, `alpaca/_base.py`, `middleware.py`, `auth.py`, `cache.py`, `websocket.py`, `calendar.py`, `symbology.py`, `stream.py`)
- **Hypothesis-to-confirmation rate:** 6 / 41 ≈ 15% — typical for autonomous hunt on a healthy codebase. Most disproven hypotheses caught patterns that look like bugs but had defensive guards or are by-design.
- **Techniques used:** direct inspection (primary), pattern search (`grep` for `create_task`, `time.time()`, `or {}/[]`), differential reasoning against known-good patterns (e.g., `data_sink.py:198` correct task storage vs `redis_sink.py:151` incorrect).

## Handoff to /autoresearch:fix

User selected "Find and fix (chain to /autoresearch:fix)". The fix loop should target the 6 bugs above in severity order. Each bug has a concrete `Suggested fix` in `findings.md`.

Recommended fix-loop input:
```
/autoresearch:fix --from-debug
Iterations: 8   # 6 bugs + 2 verification iterations
Scope: gateway/core/redis_sink.py gateway/core/uw_poller.py gateway/core/calendar.py gateway/providers/alpaca/_base.py
```

(Skipping the pervasive `time.time()` cleanup from a single fix loop — it touches too many files for an atomic change. Recommend a separate dedicated PR.)
