"""Propagate explicit budget/baseline increases from static configs into the active cache files.

The ratchet system can only tighten budgets and baselines over time.  When
either is deliberately raised in ``config/perf_budgets.json`` or
``config/perf_baseline.json`` (e.g. to account for a slower CI runner), that
change is ignored by CI because the cached ``.perf/*.active.json`` files take
precedence.  This script merges both pairs by taking the *more lenient*
(larger) value so explicit raises propagate immediately on the next run.

Usage:
    python scripts/merge_static_budgets.py
    python scripts/merge_static_budgets.py \\
      --static-budgets  config/perf_budgets.json \\
      --active-budgets  .perf/perf_budgets.active.json \\
      --static-baseline config/perf_baseline.json \\
      --active-baseline .perf/perf_baseline.active.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--static-budgets", default="config/perf_budgets.json")
    parser.add_argument("--active-budgets", default=".perf/perf_budgets.active.json")
    parser.add_argument("--static-baseline", default="config/perf_baseline.json")
    parser.add_argument("--active-baseline", default=".perf/perf_baseline.active.json")
    return parser.parse_args()


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid json in '{path}': {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"invalid json in '{path}': expected top-level object")
    return data


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _merge_tests(static: dict[str, Any], active: dict[str, Any], changes: list[str], label: str) -> None:
    """Propagate per-test changes from static to active.

    Two-way sync:
      * Static > active: raise active to static (lenient direction; existing
        behaviour).
      * Test removed from static: remove from active as well so renames take
        effect across CI cache boundaries. Without this, a test renamed in
        static config (e.g. ``backpressure_task_growth_profile`` ->
        ``queue_drops_after_block_timeout``) leaves a stale orphan entry in
        the active cache that the perf gate then complains about as
        ``missing in junit results``.
    """
    static_tests = static.get("tests", {})
    # First pass: raise active values where static is higher.
    for test_key, static_val in static_tests.items():
        if not isinstance(static_val, int | float):
            continue
        active_val = active.get("tests", {}).get(test_key, 0)
        if not isinstance(active_val, int | float):
            continue
        if float(static_val) > float(active_val):
            active.setdefault("tests", {})[test_key] = float(static_val)
            changes.append(f"[{label}] {test_key}: {active_val:.4f} -> {static_val:.4f}")
    # Second pass: prune active entries not in static (handles test renames).
    active_tests = active.get("tests", {})
    stale_keys = [k for k in active_tests if k not in static_tests]
    for k in stale_keys:
        removed_val = active_tests.pop(k)
        changes.append(f"[{label}] {k}: REMOVED (was {removed_val})")


def _merge_scalar(
    static: dict[str, Any],
    active: dict[str, Any],
    key: str,
    changes: list[str],
    label: str,
) -> None:
    """Propagate a top-level scalar where static > active."""
    sv = static.get(key)
    av = active.get(key)
    if isinstance(sv, int | float) and isinstance(av, int | float):
        if float(sv) > float(av):
            active[key] = float(sv)
            changes.append(f"[{label}] {key}: {av:.4f} -> {sv:.4f}")


def _merge_file(static_path: Path, active_path: Path, suite_key: str, label: str) -> list[str]:
    """Merge one static→active pair; return list of change descriptions."""
    if not active_path.exists():
        return []

    try:
        static = _load_json_object(static_path)
        active = _load_json_object(active_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"merge_static_budgets_failed [{label}]: {exc}", file=sys.stderr)
        return []

    changes: list[str] = []
    _merge_tests(static, active, changes, label)
    _merge_scalar(static, active, suite_key, changes, label)
    _write_json(active_path, active)
    return changes


def main() -> int:
    args = _parse_args()

    all_changes: list[str] = []

    all_changes += _merge_file(
        static_path=Path(args.static_budgets),
        active_path=Path(args.active_budgets),
        suite_key="suite_max_seconds",
        label="budgets",
    )
    all_changes += _merge_file(
        static_path=Path(args.static_baseline),
        active_path=Path(args.active_baseline),
        suite_key="suite_baseline_seconds",
        label="baseline",
    )

    if all_changes:
        print(f"merge_static_budgets_applied: {len(all_changes)} value(s) raised")
        for change in all_changes:
            print(f"  {change}")
    else:
        print("merge_static_budgets_ok: active files already >= static configs")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
