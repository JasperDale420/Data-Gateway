# Debug Summary — Data-Gateway

**Session:** 2026-03-20
**Method:** Autonomous bug hunting (scientific method + parallel agent reconnaissance)
**Scope:** 155 source files in `gateway/`

## Results

| Metric | Value |
|--------|-------|
| Hypotheses tested | 15 |
| Bugs confirmed & fixed | 6 |
| Hypotheses disproven | 7 |
| Risks noted (not fixed) | 3 |
| Files modified | 7 |
| Tests passing | 811/811 |
| Lint status | Clean |

## Bugs Fixed

1. **Division by zero** in `bulk.py:225` — ETA calculation crashes on fast jobs
2. **Finnhub rate limit misconfiguration** in `rate_limiter.py:59` — 30 req/sec burst on 60 req/min API
3. **IP spoofing bypasses rate limits** in `middleware.py:1254` — X-Forwarded-For trusted unconditionally
4. **Backfill error swallowing** in `backfill.py:574` — `return_exceptions=True` without inspection
5. **Naive datetime** in `uw/flow.py:418,466,512` — server-local time instead of ET for market dates
6. **Lazy background task init** in `middleware.py:905` — `getattr` instead of `__init__`

## Modified Files

- `gateway/core/bulk.py` — zero-guard on elapsed time
- `gateway/core/rate_limiter.py` — Finnhub per-second from 30 to 1
- `gateway/api/middleware.py` — trust_proxy_headers flag, proper __init__
- `gateway/main.py` — pass behind_trusted_proxy to GlobalRateLimitMiddleware
- `gateway/config.py` — added behind_trusted_proxy setting
- `gateway/providers/uw/flow.py` — timezone-aware datetime for market dates
- `gateway/core/backfill.py` — inspect gather results for exceptions

## Techniques Used

- Static analysis (grep patterns for anti-patterns)
- Parallel agent reconnaissance (3 agents: race conditions, logic bugs, security)
- Deep agent scan (unbounded collections, resource leaks, error swallowing)
- Code reading and manual verification of each finding
- Test suite validation after each fix
