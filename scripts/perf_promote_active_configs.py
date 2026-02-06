"""Promote active perf configs into versioned repository config files.

Usage:
    python scripts/perf_promote_active_configs.py

    python scripts/perf_promote_active_configs.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--active-budgets-file", default=".perf/perf_budgets.active.json")
    parser.add_argument("--active-baseline-file", default=".perf/perf_baseline.active.json")
    parser.add_argument("--target-budgets-file", default="config/perf_budgets.json")
    parser.add_argument("--target-baseline-file", default="config/perf_baseline.json")
    parser.add_argument("--dry-run", action="store_true")
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


def _require_numeric_map(value: Any, field_name: str, path: Path) -> None:
    if not isinstance(value, dict):
        raise ValueError(f"invalid '{field_name}' in '{path}': expected object")
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, int | float):
            raise ValueError(f"invalid '{field_name}' entry in '{path}': {key!r}")


def _validate_budgets(payload: dict[str, Any], path: Path) -> None:
    suite = payload.get("suite_max_seconds")
    if not isinstance(suite, int | float):
        raise ValueError(f"missing/invalid suite_max_seconds in '{path}'")
    _require_numeric_map(payload.get("tests"), "tests", path)


def _validate_baseline(payload: dict[str, Any], path: Path) -> None:
    suite = payload.get("suite_baseline_seconds")
    if not isinstance(suite, int | float):
        raise ValueError(f"missing/invalid suite_baseline_seconds in '{path}'")
    _require_numeric_map(payload.get("tests"), "tests", path)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _count_tests(payload: dict[str, Any]) -> int:
    tests = payload.get("tests", {})
    if not isinstance(tests, dict):
        return 0
    return len(tests)


def main() -> int:
    args = _parse_args()
    active_budgets_path = Path(args.active_budgets_file)
    active_baseline_path = Path(args.active_baseline_file)
    target_budgets_path = Path(args.target_budgets_file)
    target_baseline_path = Path(args.target_baseline_file)

    try:
        active_budgets = _load_json_object(active_budgets_path)
        active_baseline = _load_json_object(active_baseline_path)
        _validate_budgets(active_budgets, active_budgets_path)
        _validate_baseline(active_baseline, active_baseline_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"perf_promote_failed: {exc}", file=sys.stderr)
        return 1

    if not args.dry_run:
        _write_json(target_budgets_path, active_budgets)
        _write_json(target_baseline_path, active_baseline)

    action = "dry_run" if args.dry_run else "promoted"
    print(
        "perf_promote_active_configs_"
        f"{action}: budgets_tests={_count_tests(active_budgets)} "
        f"baseline_tests={_count_tests(active_baseline)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
