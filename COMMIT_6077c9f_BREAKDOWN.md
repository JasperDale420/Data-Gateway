# Commit 6077c9f Breakdown

This document clarifies what landed in commit `6077c9f` ("test(pytest): pin asyncio fixture loop scope").
That commit included a larger pre-staged set than its message implied.

## Why this exists

- Preserve current history without rewriting commits.
- Give reviewers and future maintainers a map of what changed.
- Reduce ambiguity for release notes and PR review.

## Summary

- Commit hash: `6077c9f`
- Scope size: 63 files changed
- Net effect: mixed functional changes, documentation/config updates, and dead-code removals

## Change Buckets

### 1) API surface and endpoint behavior

Primary files:
- `gateway/api/bulk.py`
- `gateway/api/calendar.py`
- `gateway/api/corporate.py`
- `gateway/api/deps.py`
- `gateway/api/health.py`
- `gateway/api/metrics.py`
- `gateway/api/news.py`
- `gateway/api/quality.py`
- `gateway/api/replay.py`
- `gateway/api/symbology.py`

What this bucket covers:
- Endpoint additions and response-shape/security alignment.
- Dependency/auth enforcement updates.
- Behavior updates for bulk/replay/calendar/corporate paths.

### 2) Core runtime, auth, streaming, and caching internals

Primary files:
- `gateway/core/bulk.py`
- `gateway/core/corporate_actions.py`
- `gateway/core/data_sink.py`
- `gateway/core/metrics.py`
- `gateway/core/multiplexer.py`
- `gateway/core/redis_cache.py`
- `gateway/core/redis_sink.py`
- `gateway/core/replay.py`
- `gateway/core/stream.py`
- `gateway/core/auth.py`
- `gateway/core/connections.py`

What this bucket covers:
- Pipeline and stream lifecycle behavior.
- Cache/metrics/sink integration updates.
- Auth and connection management adjustments.

### 3) Provider-layer alignment and capability updates

Primary files:
- `gateway/providers/alpaca.py`
- `gateway/providers/alphavantage.py`
- `gateway/providers/finnhub.py`
- `gateway/providers/news.py`
- `gateway/providers/sec.py`
- `gateway/providers/uw.py`
- `gateway/providers/__init__.py`

What this bucket covers:
- Provider contract/capability consistency.
- Provider-specific behavior and mapping updates.

### 4) Schema and validation updates

Primary files:
- `gateway/schemas/__init__.py`
- `tests/test_endpoint_validation.py`

What this bucket covers:
- Schema alignment with evolving endpoint payloads.
- Regression coverage updates.

### 5) Configuration and operational docs

Primary files:
- `.env.example`
- `config/clients.yaml`
- `config/providers.yaml`
- `docker-compose.yml`
- `production.md`
- `SECURITY.md`
- `CONTRIBUTING.md`
- `Audit_Checklist.md`
- `CLAUDE.MD`
- `pyproject.toml`

What this bucket covers:
- Runtime/config defaults and operational guidance updates.
- Tooling/test config changes.

### 6) Dead code and obsolete tests removed

Removed runtime files:
- `gateway/core/asof.py`
- `gateway/core/labels.py`
- `gateway/core/lineage.py`
- `gateway/core/ring_buffer.py`

Removed tests:
- `tests/test_asof.py`
- `tests/test_labels.py`
- `tests/test_lineage.py`

What this bucket covers:
- Cleanup of unused code paths and associated tests.

## Reviewer guidance

For targeted review, start with these paths:
1. `gateway/api/`
2. `gateway/core/`
3. `gateway/providers/`
4. `config/` and operational docs
5. Deletion set under `gateway/core/*` and `tests/test_*` removed in the same commit

## Follow-up policy

- Future commits should remain narrowly scoped to one subsystem when possible.
- If a larger staged set is intentional, call out major buckets directly in the commit message.
