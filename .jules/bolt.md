## 2025-02-14 - [Pydantic Serialization Optimization & Cache Headers]
**Learning:** Pydantic's `model_dump(mode='json')` recursively traverses and validates/converts all fields. For large nested structures (like a list of 100k items) that are already known to be JSON-compatible (e.g. from `json.loads`), this is an expensive O(N) operation.
**Action:** Use `exclude={'payload'}` in `model_dump` and manually assign the payload to the result dictionary to achieve O(1) wrapping.

**Learning:** `CacheMiddleware` must explicitly store and restore response headers. A naïve implementation that only stores the body content will drop critical headers like `Strict-Transport-Security`, CORS headers, and custom tracing headers (`X-Gateway-Event-Id`), rendering the cache dangerous for production use despite the performance gain.
**Action:** Always verify header preservation when implementing or reviewing caching middleware.
