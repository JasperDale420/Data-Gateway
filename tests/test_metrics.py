"""Tests for metrics helpers."""

import gateway.core.metrics as metrics


def test_normalize_path_uses_expected_placeholders() -> None:
    path = "/api/v1/alpaca/bars/AAPL/123456789/2025-01-01"
    normalized = metrics._normalize_path(path)
    assert normalized == "/api/v1/alpaca/bars/{symbol}/{id}/{id}"


def test_normalize_path_cache_is_bounded() -> None:
    metrics._PATH_NORMALIZATION_CACHE.clear()

    max_size = metrics._PATH_NORMALIZATION_CACHE_MAX
    for i in range(max_size + 20):
        metrics._normalize_path(f"/api/v1/finnhub/quote/SYM{i:05d}")

    assert len(metrics._PATH_NORMALIZATION_CACHE) <= max_size
