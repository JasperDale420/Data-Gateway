"""Propagate explicit budget increases from the static config into the active cache file.

The ratchet system can only tighten budgets over time.  When a budget is
deliberately raised in ``config/perf_budgets.json`` (e.g. to account for a
newly-measured performance characteristic), that increase is ignored by CI
because the cached ``.perf/perf_budgets.active.json`` takes precedence.  This
script merges the two files by taking the *more lenient* (larger) value for
each budget, so explicit raises propagate immediately.

Usage:
    python scripts/merge_static_budgets.py
    python scripts/merge_static_budgets.py \\
      --static-file config/perf_budgets.json \\
      --active-file .perf/perf_budgets.active.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--static-file", default="config/perf_budgets.json")
    parser.add_argument("--active-file", default=".perf/perf_budgets.active.json")
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


def main() -> int:
    args = _parse_args()
    static_path = Path(args.static_file)
    active_path = Path(args.active_file)

    try:
        static = _load_json_object(static_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"merge_static_budgets_failed: {exc}", file=sys.stderr)
        return 1

    if not active_path.exists():
        # No active file — nothing to merge into; the gate will fall back to
        # the static config directly, so this is a no-op.
        print("merge_static_budgets_skipped: no active file found")
        return 0

    try:
        active = _load_json_object(active_path)
    except ValueError as exc:
        print(f"merge_static_budgets_failed: {exc}", file=sys.stderr)
        return 1

    changes: list[str] = []

    # Per-test budgets: take the more lenient (larger) value.
    for test_key, static_val in static.get("tests", {}).items():
        if not isinstance(static_val, int | float):
            continue
        active_val = active.get("tests", {}).get(test_key, 0)
        if not isinstance(active_val, int | float):
            continue
        if float(static_val) > float(active_val):
            active.setdefault("tests", {})[test_key] = float(static_val)
            changes.append(f"{test_key}: {active_val:.4f} -> {static_val:.4f}")

    # Suite budget: take the more lenient (larger) value.
    static_suite = static.get("suite_max_seconds")
    active_suite = active.get("suite_max_seconds")
    if isinstance(static_suite, int | float) and isinstance(active_suite, int | float):
        if float(static_suite) > float(active_suite):
            active["suite_max_seconds"] = float(static_suite)
            changes.append(f"suite_max_seconds: {active_suite:.4f} -> {static_suite:.4f}")

    _write_json(active_path, active)

    if changes:
        print(f"merge_static_budgets_applied: {len(changes)} budget(s) raised")
        for change in changes:
            print(f"  {change}")
    else:
        print("merge_static_budgets_ok: active budgets already >= static config")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
