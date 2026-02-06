from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def test_perf_promote_active_configs_promotes_files(tmp_path: Path) -> None:
    active_budgets = tmp_path / ".perf" / "perf_budgets.active.json"
    active_baseline = tmp_path / ".perf" / "perf_baseline.active.json"
    target_budgets = tmp_path / "config" / "perf_budgets.json"
    target_baseline = tmp_path / "config" / "perf_baseline.json"

    _write_json(
        active_budgets,
        {
            "suite_max_seconds": 4.2,
            "tests": {"tests/perf/test_a.py::test_x": 0.3},
        },
    )
    _write_json(
        active_baseline,
        {
            "suite_baseline_seconds": 1.1,
            "tests": {"tests/perf/test_a.py::test_x": 0.08},
        },
    )

    cmd = [
        sys.executable,
        "scripts/perf_promote_active_configs.py",
        "--active-budgets-file",
        str(active_budgets),
        "--active-baseline-file",
        str(active_baseline),
        "--target-budgets-file",
        str(target_budgets),
        "--target-baseline-file",
        str(target_baseline),
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    assert _read_json(target_budgets)["suite_max_seconds"] == 4.2
    assert _read_json(target_baseline)["suite_baseline_seconds"] == 1.1


def test_perf_promote_active_configs_dry_run_does_not_write(tmp_path: Path) -> None:
    active_budgets = tmp_path / ".perf" / "perf_budgets.active.json"
    active_baseline = tmp_path / ".perf" / "perf_baseline.active.json"
    target_budgets = tmp_path / "config" / "perf_budgets.json"
    target_baseline = tmp_path / "config" / "perf_baseline.json"

    _write_json(
        active_budgets,
        {
            "suite_max_seconds": 4.2,
            "tests": {"tests/perf/test_a.py::test_x": 0.3},
        },
    )
    _write_json(
        active_baseline,
        {
            "suite_baseline_seconds": 1.1,
            "tests": {"tests/perf/test_a.py::test_x": 0.08},
        },
    )
    _write_json(
        target_budgets,
        {
            "suite_max_seconds": 9.0,
            "tests": {"tests/perf/test_a.py::test_x": 1.0},
        },
    )
    _write_json(
        target_baseline,
        {
            "suite_baseline_seconds": 3.0,
            "tests": {"tests/perf/test_a.py::test_x": 0.5},
        },
    )

    cmd = [
        sys.executable,
        "scripts/perf_promote_active_configs.py",
        "--active-budgets-file",
        str(active_budgets),
        "--active-baseline-file",
        str(active_baseline),
        "--target-budgets-file",
        str(target_budgets),
        "--target-baseline-file",
        str(target_baseline),
        "--dry-run",
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr

    assert _read_json(target_budgets)["suite_max_seconds"] == 9.0
    assert _read_json(target_baseline)["suite_baseline_seconds"] == 3.0
