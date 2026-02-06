"""Run perf tests with a coarse runtime budget and artifact output.

Usage:
    python scripts/perf_gate.py --max-seconds 20
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-seconds", type=float, default=20.0)
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


def main() -> int:
    args = _parse_args()
    junit_path = Path(args.junit_xml)
    log_path = Path(args.log_file)
    summary_path = Path(args.summary_file)

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

    summary = {
        "command": " ".join(cmd),
        "exit_code": completed.returncode,
        "max_seconds": args.max_seconds,
        "measured_seconds": round(effective_elapsed, 4),
        "timer_seconds": round(elapsed, 4),
        "status": (
            "pass"
            if completed.returncode == 0 and effective_elapsed <= args.max_seconds
            else "fail"
        ),
    }
    _write_summary(summary_path, summary)

    sys.stdout.write(combined_output)

    if completed.returncode != 0:
        return completed.returncode

    if effective_elapsed > args.max_seconds:
        print(
            f"perf_gate_failed: runtime {effective_elapsed:.2f}s exceeded threshold "
            f"{args.max_seconds:.2f}s"
        )
        return 1

    print(
        f"perf_gate_passed: runtime {effective_elapsed:.2f}s within threshold "
        f"{args.max_seconds:.2f}s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
