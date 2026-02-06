"""Run perf tests with suite and per-test budgets plus artifact output.

Usage:
    python scripts/perf_gate.py --budgets-file config/perf_budgets.json
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
    parser.add_argument("--junit-xml", default="perf-junit.xml")
    parser.add_argument("--log-file", default="perf-output.txt")
    parser.add_argument("--summary-file", default="perf-summary.json")
    return parser.parse_args()


def _extract_pytest_elapsed_seconds(output: str) -> float | None:
    match = re.search(r"in\\s+([0-9]+(?:\\.[0-9]+)?)s\\s*=*", output)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _load_budgets(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid budgets file '{path}': {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"invalid budgets file '{path}': expected top-level object")
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


def main() -> int:
    args = _parse_args()
    junit_path = Path(args.junit_xml)
    log_path = Path(args.log_file)
    summary_path = Path(args.summary_file)
    budgets_path = Path(args.budgets_file)
    budgets = _load_budgets(budgets_path)

    suite_budget = args.max_seconds
    if suite_budget is None:
        suite_budget = float(budgets.get("suite_max_seconds", 20.0))
    per_test_budget = budgets.get("tests", {})
    if not isinstance(per_test_budget, dict):
        raise ValueError("invalid budgets file: 'tests' must be an object")

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

    per_test_failures: list[dict[str, Any]] = []
    for test_name, max_seconds in per_test_budget.items():
        if not isinstance(max_seconds, int | float):
            raise ValueError(f"invalid budget for {test_name}: expected number")
        actual = test_times.get(test_name)
        if actual is None:
            per_test_failures.append(
                {
                    "test": test_name,
                    "reason": "missing",
                    "budget_seconds": float(max_seconds),
                }
            )
            continue
        if actual > float(max_seconds):
            per_test_failures.append(
                {
                    "test": test_name,
                    "actual_seconds": round(actual, 4),
                    "budget_seconds": float(max_seconds),
                    "reason": "exceeded",
                }
            )

    gate_passed = (
        completed.returncode == 0 and effective_elapsed <= suite_budget and not per_test_failures
    )

    summary = {
        "command": " ".join(cmd),
        "exit_code": completed.returncode,
        "max_seconds": suite_budget,
        "measured_seconds": round(effective_elapsed, 4),
        "timer_seconds": round(elapsed, 4),
        "budgets_file": str(budgets_path),
        "test_times_seconds": {k: round(v, 4) for k, v in sorted(test_times.items())},
        "per_test_failures": per_test_failures,
        "status": "pass" if gate_passed else "fail",
    }
    _write_summary(summary_path, summary)

    sys.stdout.write(combined_output)

    if completed.returncode != 0:
        return completed.returncode

    if effective_elapsed > suite_budget:
        print(
            f"perf_gate_failed: runtime {effective_elapsed:.2f}s exceeded threshold "
            f"{suite_budget:.2f}s"
        )
        return 1

    if per_test_failures:
        print("perf_gate_failed: one or more per-test budgets were exceeded")
        for failure in per_test_failures:
            if failure["reason"] == "missing":
                print(
                    f"  - {failure['test']}: missing in junit results "
                    f"(budget {failure['budget_seconds']:.2f}s)"
                )
            else:
                print(
                    f"  - {failure['test']}: {failure['actual_seconds']:.3f}s > "
                    f"{failure['budget_seconds']:.3f}s"
                )
        return 1

    print(
        f"perf_gate_passed: runtime {effective_elapsed:.2f}s within threshold "
        f"{suite_budget:.2f}s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
