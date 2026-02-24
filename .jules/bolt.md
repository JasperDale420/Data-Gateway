## 2025-02-14 - [Pydantic Serialization Optimization & Cache Headers]
**Learning:** Pydantic's `model_dump(mode='json')` recursively traverses and validates/converts all fields. For large nested structures (like a list of 100k items) that are already known to be JSON-compatible (e.g. from `json.loads`), this is an expensive O(N) operation.
**Action:** Use `exclude={'payload'}` in `model_dump` and manually assign the payload to the result dictionary to achieve O(1) wrapping.

**Learning:** `CacheMiddleware` must explicitly store and restore response headers. A naïve implementation that only stores the body content will drop critical headers like `Strict-Transport-Security`, CORS headers, and custom tracing headers (`X-Gateway-Event-Id`), rendering the cache dangerous for production use despite the performance gain.
**Action:** Always verify header preservation when implementing or reviewing caching middleware.

## 2026-02-24 - [Auth-Gated Cache Header Blind Spot]
**Learning:** Cache middleware can silently skip cache accounting on unauthenticated non-public GETs, which hides whether requests are uncached by policy vs cache miss. This obscures real cache effectiveness during perf checks.
**Action:** For auth-gated cache paths that bypass storage, always return explicit `X-Gateway-Cache: BYPASS` so perf and behavior diagnostics stay trustworthy.
