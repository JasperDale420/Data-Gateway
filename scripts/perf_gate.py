"""Run perf tests with suite/test budgets and trend-delta guardrails.

Usage:
    python scripts/perf_gate.py \
      --budgets-file config/perf_budgets.json \
      --baseline-file config/perf_baseline.json
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-seconds", type=float, default=None)
    parser.add_argument("--budgets-file", default="config/perf_budgets.json")
    parser.add_argument("--baseline-file", default="config/perf_baseline.json")
    parser.add_argument("--junit-xml", default="perf-junit.xml")
    parser.add_argument("--log-file", default="perf-output.txt")
    parser.add_argument("--summary-file", default="perf-summary.json")
    return parser.parse_args()


def _extract_pytest_elapsed_seconds(output: str) -> float | None:
    match = re.search(r"in\s+([0-9]+(?:\.[0-9]+)?)s\s*=*", output)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


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


def _test_id(classname: str, name: str) -> str:
    return f"{classname.replace('.', '/')}.py::{name}" if classname else name


def _parse_junit_test_times(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    tree = ET.parse(path)
    out: dict[str, float] = {}
    for testcase in tree.findall(".//testcase"):
        classname = testcase.attrib.get("classname", "")
        name = testcase.attrib.get("name", "")
        test_name = _test_id(classname, name)
        try:
            out[test_name] = float(testcase.attrib.get("time", "0"))
        except ValueError:
            out[test_name] = 0.0
    return out


def _allowed_with_delta(baseline: float, max_pct: float, min_abs: float) -> float:
    return (baseline * (1.0 + (max_pct / 100.0))) + min_abs


def main() -> int:
    args = _parse_args()
    junit_path = Path(args.junit_xml)
    log_path = Path(args.log_file)
    summary_path = Path(args.summary_file)
    budgets_path = Path(args.budgets_file)
    baseline_path = Path(args.baseline_file)

    budgets = _load_json_object(budgets_path)
    baseline = _load_json_object(baseline_path)

    suite_budget = args.max_seconds
    if suite_budget is None:
        suite_budget = float(budgets.get("suite_max_seconds", 20.0))
    per_test_budget = budgets.get("tests", {})
    if not isinstance(per_test_budget, dict):
        raise ValueError("invalid budgets file: 'tests' must be an object")

    trend_guard = budgets.get("trend_guard", {})
    if not isinstance(trend_guard, dict):
        raise ValueError("invalid budgets file: 'trend_guard' must be an object")
    trend_enabled = bool(trend_guard.get("enabled", False))
    max_suite_reg_pct = float(trend_guard.get("max_suite_regression_pct", 50.0))
    max_test_reg_pct = float(trend_guard.get("max_test_regression_pct", 60.0))
    min_abs_seconds = float(trend_guard.get("min_absolute_seconds", 0.02))

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "tests/perf",
        "-m",
        "perf",
        "--durations=15",
        f"--junitxml={junit_path}",
    ]

    start = time.perf_counter()
    completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
    elapsed = time.perf_counter() - start

    combined_output = completed.stdout + ("\n" if completed.stdout else "") + completed.stderr
    log_path.write_text(combined_output)

    pytest_elapsed = _extract_pytest_elapsed_seconds(combined_output)
    effective_elapsed = pytest_elapsed if pytest_elapsed is not None else elapsed
    test_times = _parse_junit_test_times(junit_path)

    per_test_budget_failures: list[dict[str, Any]] = []
    for test_name, max_seconds in per_test_budget.items():
        if not isinstance(max_seconds, int | float):
            raise ValueError(f"invalid budget for {test_name}: expected number")
        actual = test_times.get(test_name)
        if actual is None:
            per_test_budget_failures.append(
                {
                    "test": test_name,
                    "reason": "missing",
                    "budget_seconds": float(max_seconds),
                }
            )
            continue
        if actual > float(max_seconds):
            per_test_budget_failures.append(
                {
                    "test": test_name,
                    "actual_seconds": round(actual, 4),
                    "budget_seconds": float(max_seconds),
                    "reason": "exceeded",
                }
            )

    trend_regressions: list[dict[str, Any]] = []
    trend_warnings: list[str] = []
    if trend_enabled:
        baseline_suite = baseline.get("suite_baseline_seconds")
        baseline_tests = baseline.get("tests", {})
        if not isinstance(baseline_tests, dict):
            raise ValueError("invalid baseline file: 'tests' must be an object")

        if isinstance(baseline_suite, int | float):
            suite_allowed = _allowed_with_delta(
                baseline=float(baseline_suite),
                max_pct=max_suite_reg_pct,
                min_abs=min_abs_seconds,
            )
            if effective_elapsed > suite_allowed:
                trend_regressions.append(
                    {
                        "scope": "suite",
                        "actual_seconds": round(effective_elapsed, 4),
                        "baseline_seconds": float(baseline_suite),
                        "allowed_seconds": round(suite_allowed, 4),
                        "max_regression_pct": max_suite_reg_pct,
                    }
                )
        else:
            trend_warnings.append("missing suite_baseline_seconds in baseline file")

        for test_name, baseline_value in baseline_tests.items():
            if not isinstance(baseline_value, int | float):
                trend_warnings.append(f"invalid baseline value for {test_name}")
                continue
            actual = test_times.get(test_name)
            if actual is None:
                trend_warnings.append(f"missing test in junit results: {test_name}")
                continue
            allowed = _allowed_with_delta(
                baseline=float(baseline_value),
                max_pct=max_test_reg_pct,
                min_abs=min_abs_seconds,
            )
            if actual > allowed:
                trend_regressions.append(
                    {
                        "scope": "test",
                        "test": test_name,
                        "actual_seconds": round(actual, 4),
                        "baseline_seconds": float(baseline_value),
                        "allowed_seconds": round(allowed, 4),
                        "max_regression_pct": max_test_reg_pct,
                    }
                )

    gate_passed = (
        completed.returncode == 0
        and effective_elapsed <= suite_budget
        and not per_test_budget_failures
        and not trend_regressions
    )

    summary = {
        "command": " ".join(cmd),
        "exit_code": completed.returncode,
        "max_seconds": suite_budget,
        "measured_seconds": round(effective_elapsed, 4),
        "timer_seconds": round(elapsed, 4),
        "budgets_file": str(budgets_path),
        "baseline_file": str(baseline_path),
        "trend_guard": {
            "enabled": trend_enabled,
            "max_suite_regression_pct": max_suite_reg_pct,
            "max_test_regression_pct": max_test_reg_pct,
            "min_absolute_seconds": min_abs_seconds,
        },
        "test_times_seconds": {k: round(v, 4) for k, v in sorted(test_times.items())},
        "per_test_budget_failures": per_test_budget_failures,
        "trend_regressions": trend_regressions,
        "trend_warnings": trend_warnings,
        "status": "pass" if gate_passed else "fail",
    }
    _write_summary(summary_path, summary)

    sys.stdout.write(combined_output)

    if completed.returncode != 0:
        return completed.returncode

    if effective_elapsed > suite_budget:
        print(f"perf_gate_failed: runtime {effective_elapsed:.2f}s exceeded threshold {suite_budget:.2f}s")
        return 1

    if per_test_budget_failures:
        print("perf_gate_failed: one or more per-test budgets were exceeded")
        for failure in per_test_budget_failures:
            if failure["reason"] == "missing":
                print(f"  - {failure['test']}: missing in junit results (budget {failure['budget_seconds']:.2f}s)")
            else:
                print(f"  - {failure['test']}: {failure['actual_seconds']:.3f}s > {failure['budget_seconds']:.3f}s")
        return 1

    if trend_regressions:
        print("perf_gate_failed: trend regression guardrail exceeded")
        for regression in trend_regressions:
            if regression["scope"] == "suite":
                print(f"  - suite: {regression['actual_seconds']:.3f}s > {regression['allowed_seconds']:.3f}s")
            else:
                print(
                    f"  - {regression['test']}: {regression['actual_seconds']:.3f}s > "
                    f"{regression['allowed_seconds']:.3f}s"
                )
        return 1

    print(f"perf_gate_passed: runtime {effective_elapsed:.2f}s within threshold {suite_budget:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
