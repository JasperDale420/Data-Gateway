# News Provider Performance Audit Deep Dive

Date: 2026-02-06
Auditor: Codex (GPT-5)
Primary scope: `gateway/providers/news.py`
Secondary scope: usage context in `gateway/api/news.py`

## Objective

Complete a full deep performance pass of the News provider implementation with low-risk recommendations that avoid significant logic changes, and clearly track remaining audit scope.

## Audit Completion Status

| Area | Files | Status | Notes |
|---|---:|---|---|
| News provider | 1 (`gateway/providers/news.py`) | COMPLETE | Full provider pass across article fetch + sentiment paths |
| News routes (usage context) | 1 (`gateway/api/news.py`) | COMPLETE | Already audited previously; referenced here for integration context |
| Runtime profiling/benchmarks | N/A | PENDING | Static/code-path audit in this run |

## Inventory and Measured Hotspots

- Provider size: `333` LOC.
- Method mix:
  - `6` async methods
  - `5` sync methods (constructor/properties/helpers)
- Request/response path metrics:
  - `self._client.get(...)`: `3`
  - `response.raise_for_status()`: `2`
  - `response.json()`: `2`
  - `if not self._client` guards: `4`
  - `if not self._api_key` guards: `4`
  - `except Exception as e` blocks: `3`
  - `datetime.now(UTC)` calls: `8`
  - `strftime(...)` calls: `4`

Key shape observations:
- `get_articles(...)` currently normalizes results with append-in-loop allocation (`gateway/providers/news.py:169-171`).
- `get_sentiment(...)` performs per-request keyword list allocation and per-article substring scans (`gateway/providers/news.py:236-269`).
- `get_article(...)` is intentionally unsupported and raises `NotImplementedError` (`gateway/providers/news.py:202`).

## Priority Findings (Low-Risk Changes Only)

### P0-1: Sentiment path reallocates keyword lists and performs repeated O(n*k) scans

Evidence:
- Keyword lists created inside method on each call:
  - `gateway/providers/news.py:236-257`
- Per-article substring checks run twice for each article:
  - `gateway/providers/news.py:263-269`

Impact:
- Avoidable allocations and CPU on every sentiment request.
- Cost scales with article count and keyword list growth.

Low-risk fix path:
1. Hoist positive/negative keyword collections to module-level constants.
2. Keep existing keyword-substring behavior, but reuse shared constants and local references.
3. Optionally precompile matcher once if/when keyword set expands.

### P0-2: Repeated readiness checks and duplicated request boilerplate across methods

Evidence:
- Guard duplication:
  - `gateway/providers/news.py:126-130`
  - `gateway/providers/news.py:206-210`
- Repeated fetch flow (`get -> raise_for_status -> json -> status check`) in both paths:
  - `gateway/providers/news.py:162-167`
  - `gateway/providers/news.py:226-231`

Impact:
- Small but persistent overhead in hot paths.
- Higher maintenance drift risk for retries, timeout behavior, and provider error mapping.

Low-risk fix path:
1. Add `_ensure_ready()` helper for `_client` + `_api_key` checks.
2. Add shared `_fetch_everything(params)` helper for common request/error flow.
3. Keep response schemas and exceptions unchanged.

### P1-3: Pagination math uses requested `limit` instead of effective `pageSize`

Evidence:
- `pageSize` is capped to 100:
  - `gateway/providers/news.py:152`
- `has_more` uses requested `limit`:
  - `gateway/providers/news.py:176`

Impact:
- If callers pass `limit > 100`, pagination continuation logic can miscompute.
- Can reduce throughput by causing under-fetch patterns in consumers.

Low-risk fix path:
1. Compute and reuse `page_size = min(limit, 100)` once.
2. Use `page_size` in both request params and pagination math.
3. Preserve external payload shape.

### P1-4: Article normalization loop uses append pattern in hot path

Evidence:
- `gateway/providers/news.py:169-171`

Impact:
- Minor but frequent overhead on article-heavy responses.

Low-risk fix path:
1. Replace append loop with list comprehension.
2. Keep `_normalize_article(...)` output contract unchanged.

### P2-5: Unsupported article lookup still traverses route dedupe/rate-limit path

Evidence:
- Provider always raises `NotImplementedError`:
  - `gateway/providers/news.py:202`
- Route calls dedupe + provider fetch before returning 501:
  - `gateway/api/news.py:111-132`

Impact:
- Repeated `/articles/{article_id}` traffic pays avoidable overhead before deterministic 501 response.

Low-risk fix path:
1. Short-circuit unsupported route at the API layer (static 501 contract) when provider capability is known.
2. Preserve existing error code/message contract.

## Implementation Plan to Start Addressing Issues

### Wave NEWS-PROV-1 (Immediate, lowest risk)

1. Hoist sentiment keyword lists to module-level constants.
2. Add `_ensure_ready()` and `_fetch_everything(...)` helpers.
3. Introduce `page_size` local and reuse it for pagination and request params.
4. Convert article normalization loop to list comprehension.

### Wave NEWS-PROV-2

1. Add shared 7-day window/date formatting helper for `get_articles` + `get_sentiment`.
2. Evaluate lightweight tokenization-based sentiment matcher behind parity tests.
3. Route-level short-circuit for unsupported article lookup.

### Wave NEWS-PROV-3

1. Add provider-level benchmarks for:
   - article normalization throughput
   - sentiment classification CPU cost at 50/100/200 article loads
   - pagination behavior with `limit > 100`.

## File-Level Audit Tracker (This Run)

Legend: COMPLETE = audited in this run; FUTURE = implementation/profiling follow-up still needed.

| File | Methods | Audit Status | Future Run Focus |
|---|---:|---|---|
| `gateway/providers/news.py` | 11 methods (6 async, 5 sync) | COMPLETE | Ready for Wave NEWS-PROV-1 implementation |

## Remaining Audit Scope (Future Runs)

1. Deeper computational hotspot audits for sampled core modules:
   - `gateway/core/security.py`
   - `gateway/core/quality.py`
   - `gateway/core/calendar.py`
   - `gateway/core/symbology.py`
   - `gateway/core/validator.py`
2. Runtime benchmark/profiling suite execution for middleware, stream/replay fanout, bulk memory behavior, and provider microbenchmarks.
3. Full performance audit of `tests/` and `scripts/` runtime paths.
