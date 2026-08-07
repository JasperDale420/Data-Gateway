"""Baseline performance tests for core hot paths.

Run explicitly with:
    pytest -m perf tests/perf -q
"""

import time

import pytest

from gateway.core.envelope import wrap_event
from gateway.core.metrics import _normalize_path

pytestmark = pytest.mark.perf


def test_wrap_event_serialization_baseline() -> None:
    """Keep a coarse performance floor for large envelope serialization."""
    payload = [{"id": i, "val": f"test_{i}"} for i in range(10_000)]

    start = time.perf_counter()
    for _ in range(100):
        result = wrap_event(
            event={"items": payload, "count": len(payload)},
            provider="test_provider",
            feed="test_feed",
            source="rest",
        )
    duration = time.perf_counter() - start

    assert result["provider"] == "test_provider"
    assert result["feed"] == "test_feed"
    assert duration < 0.5


def test_metrics_path_normalization_baseline() -> None:
    """Track normalization throughput with a high-cardinality path sample."""
    paths = [f"/api/v1/alpaca/bars/AAPL/{i}/2025-01-01" for i in range(200_000)]

    start = time.perf_counter()
    normalized = [_normalize_path(path) for path in paths]
    duration = time.perf_counter() - start

    assert normalized[0] == "/api/v1/alpaca/bars/{symbol}/0/{id}"
    # Gross-regression backstop only. 200k normalizations run ~0.35s locally and
    # ~0.94s on shared CI runners: the step up from the earlier ~0.66s CI figure
    # is the regex-based _looks_like_symbol that replaced the plain
    # len<=5/isupper/isalpha check to stop crypto/forex/dotted symbols leaking
    # metric label cardinality. That ~1.4x cost is accepted — this path is
    # memoized, so production pays it only on cache misses. The perf-guardrail
    # budget/ratchet (scripts/perf_gate.py + config/perf_baseline.json) is the
    # real fine-grained tracker; this bound just catches an order-of-magnitude
    # blow-up (e.g. accidental O(n^2)).
    assert duration < 2.0
