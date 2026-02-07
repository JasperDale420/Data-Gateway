"""Manage perf history, baseline refresh, and budget ratcheting.

Usage:
    python scripts/perf_baseline_manager.py \
      --summary-file perf-summary.json \
      --history-file .perf/perf-history.json \
      --budgets-file config/perf_budgets.json \
      --baseline-file config/perf_baseline.json \
      --out-budgets-file .perf/perf_budgets.active.json \
      --out-baseline-file .perf/perf_baseline.active.json \
      --report-file perf-history-report.json
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
from pathlib import Path
from statistics import median
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary-file", required=True)
    parser.add_argument("--history-file", default=".perf/perf-history.json")
    parser.add_argument("--budgets-file", default="config/perf_budgets.json")
    parser.add_argument("--baseline-file", default="config/perf_baseline.json")
    parser.add_argument("--out-history-file", default=None)
    parser.add_argument("--out-budgets-file", default=None)
    parser.add_argument("--out-baseline-file", default=None)
    parser.add_argument("--report-file", default="perf-history-report.json")
    return parser.parse_args()


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid json file '{path}': {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"invalid json file '{path}': expected top-level object")
    return data


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _round_seconds(value: float) -> float:
    return round(value, 4)


def _to_run_entry(summary: dict[str, Any]) -> dict[str, Any]:
    measured = summary.get("measured_seconds")
    if not isinstance(measured, int | float):
        raise ValueError("summary file missing numeric measured_seconds")
    tests = summary.get("test_times_seconds", {})
    if not isinstance(tests, dict):
        raise ValueError("summary file has invalid test_times_seconds")

    normalized_tests: dict[str, float] = {}
    for test_name, test_value in tests.items():
        if isinstance(test_name, str) and isinstance(test_value, int | float):
            normalized_tests[test_name] = float(test_value)

    return {
        "timestamp": dt.datetime.now(tz=dt.UTC).isoformat(),
        "status": str(summary.get("status", "unknown")),
        "suite_seconds": float(measured),
        "tests": normalized_tests,
    }


def _median_suite(runs: list[dict[str, Any]]) -> float | None:
    samples = [
        float(run["suite_seconds"])
        for run in runs
        if isinstance(run.get("suite_seconds"), int | float)
    ]
    if not samples:
        return None
    return float(median(samples))


def _median_tests(runs: list[dict[str, Any]]) -> dict[str, float]:
    by_test: dict[str, list[float]] = {}
    for run in runs:
        tests = run.get("tests", {})
        if not isinstance(tests, dict):
            continue
        for test_name, value in tests.items():
            if isinstance(test_name, str) and isinstance(value, int | float):
                by_test.setdefault(test_name, []).append(float(value))

    medians: dict[str, float] = {}
    for test_name, values in by_test.items():
        if values:
            medians[test_name] = float(median(values))
    return medians


def _ratchet_budget(
    current: float, baseline: float, mult: float, slack: float, floor: float
) -> float:
    candidate = max(floor, (baseline * mult) + slack)
    if candidate >= current:
        return current
    return _round_seconds(candidate)


def main() -> int:
    args = _parse_args()
    summary_path = Path(args.summary_file)
    history_path = Path(args.history_file)
    budgets_path = Path(args.budgets_file)
    baseline_path = Path(args.baseline_file)

    out_history = Path(args.out_history_file) if args.out_history_file else history_path
    out_budgets = Path(args.out_budgets_file) if args.out_budgets_file else budgets_path
    out_baseline = Path(args.out_baseline_file) if args.out_baseline_file else baseline_path
    report_path = Path(args.report_file)

    summary = _load_json_object(summary_path)
    history = _load_json_object(history_path)
    budgets = _load_json_object(budgets_path)
    baseline = _load_json_object(baseline_path)

    runs = history.get("runs", [])
    if not isinstance(runs, list):
        runs = []

    run_entry = _to_run_entry(summary)
    runs.append(run_entry)

    ratchet_cfg = budgets.get("ratchet", {})
    if not isinstance(ratchet_cfg, dict):
        raise ValueError("invalid budgets file: 'ratchet' must be an object")

    history_window = int(ratchet_cfg.get("history_window", 20))
    baseline_window = int(ratchet_cfg.get("baseline_window", 5))
    min_samples = int(ratchet_cfg.get("min_samples", 3))
    ratchet_enabled = bool(ratchet_cfg.get("enabled", True))
    suite_multiplier = float(ratchet_cfg.get("suite_multiplier", 3.0))
    test_multiplier = float(ratchet_cfg.get("test_multiplier", 3.0))
    absolute_slack = float(ratchet_cfg.get("absolute_slack_seconds", 0.02))
    min_suite = float(ratchet_cfg.get("min_suite_seconds", 0.5))
    min_test = float(ratchet_cfg.get("min_test_seconds", 0.03))

    if history_window > 0:
        runs = runs[-history_window:]

    pass_runs = [run for run in runs if run.get("status") == "pass"]
    if baseline_window > 0:
        pass_runs = pass_runs[-baseline_window:]

    baseline_suite = _median_suite(pass_runs)
    baseline_tests = _median_tests(pass_runs)

    next_baseline = copy.deepcopy(baseline)
    if isinstance(baseline_suite, float):
        next_baseline["suite_baseline_seconds"] = _round_seconds(baseline_suite)
    if baseline_tests:
        next_baseline["tests"] = {
            key: _round_seconds(value) for key, value in sorted(baseline_tests.items())
        }

    next_budgets = copy.deepcopy(budgets)
    if "ratchet" not in next_budgets:
        next_budgets["ratchet"] = {
            "enabled": True,
            "history_window": history_window,
            "baseline_window": baseline_window,
            "min_samples": min_samples,
            "suite_multiplier": suite_multiplier,
            "test_multiplier": test_multiplier,
            "absolute_slack_seconds": absolute_slack,
            "min_suite_seconds": min_suite,
            "min_test_seconds": min_test,
        }

    suite_budget_before = next_budgets.get("suite_max_seconds")
    suite_budget_after = suite_budget_before
    test_budget_changes: list[dict[str, Any]] = []

    if ratchet_enabled and len(pass_runs) >= min_samples:
        if isinstance(suite_budget_before, int | float) and isinstance(baseline_suite, float):
            suite_budget_after = _ratchet_budget(
                current=float(suite_budget_before),
                baseline=float(baseline_suite),
                mult=suite_multiplier,
                slack=absolute_slack,
                floor=min_suite,
            )
            next_budgets["suite_max_seconds"] = suite_budget_after

        tests_budget = next_budgets.get("tests", {})
        if not isinstance(tests_budget, dict):
            raise ValueError("invalid budgets file: 'tests' must be an object")

        for test_name, current_budget in list(tests_budget.items()):
            if not isinstance(current_budget, int | float):
                continue
            baseline_value = baseline_tests.get(test_name)
            if baseline_value is None:
                continue
            updated_budget = _ratchet_budget(
                current=float(current_budget),
                baseline=float(baseline_value),
                mult=test_multiplier,
                slack=absolute_slack,
                floor=min_test,
            )
            tests_budget[test_name] = updated_budget
            if updated_budget != float(current_budget):
                test_budget_changes.append(
                    {
                        "test": test_name,
                        "before_seconds": float(current_budget),
                        "after_seconds": updated_budget,
                    }
                )

    next_history = {
        "version": 1,
        "updated_at": dt.datetime.now(tz=dt.UTC).isoformat(),
        "runs": runs,
    }

    report = {
        "summary_file": str(summary_path),
        "history_file": str(out_history),
        "budgets_file": str(out_budgets),
        "baseline_file": str(out_baseline),
        "history_count": len(runs),
        "pass_samples_used": len(pass_runs),
        "ratchet_enabled": ratchet_enabled,
        "ratchet_applied": bool(ratchet_enabled and len(pass_runs) >= min_samples),
        "ratchet_min_samples": min_samples,
        "suite_budget_before": suite_budget_before,
        "suite_budget_after": suite_budget_after,
        "test_budget_changes": sorted(test_budget_changes, key=lambda item: item["test"]),
    }

    _write_json(out_history, next_history)
    _write_json(out_baseline, next_baseline)
    _write_json(out_budgets, next_budgets)
    _write_json(report_path, report)

    print(
        "perf_baseline_manager_updated: "
        f"history={len(runs)} pass_samples={len(pass_runs)} "
        f"ratchet_applied={report['ratchet_applied']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
