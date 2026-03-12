from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def _run_manager(
    summary_file: Path,
    history_file: Path,
    budgets_file: Path,
    baseline_file: Path,
    out_history_file: Path,
    out_budgets_file: Path,
    out_baseline_file: Path,
    report_file: Path,
) -> None:
    cmd = [
        sys.executable,
        "scripts/perf_baseline_manager.py",
        "--summary-file",
        str(summary_file),
        "--history-file",
        str(history_file),
        "--budgets-file",
        str(budgets_file),
        "--baseline-file",
        str(baseline_file),
        "--out-history-file",
        str(out_history_file),
        "--out-budgets-file",
        str(out_budgets_file),
        "--out-baseline-file",
        str(out_baseline_file),
        "--report-file",
        str(report_file),
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_perf_baseline_manager_rotates_history_and_ratchets_budgets(tmp_path: Path) -> None:
    summary_file = tmp_path / "perf-summary.json"
    history_file = tmp_path / "perf-history.json"
    budgets_file = tmp_path / "perf-budgets.json"
    baseline_file = tmp_path / "perf-baseline.json"
    out_history_file = tmp_path / "out-history.json"
    out_budgets_file = tmp_path / "out-budgets.json"
    out_baseline_file = tmp_path / "out-baseline.json"
    report_file = tmp_path / "out-report.json"

    _write_json(
        summary_file,
        {
            "status": "pass",
            "measured_seconds": 1.4,
            "test_times_seconds": {
                "tests/perf/test_perf_baseline.py::test_wrap_event_serialization_baseline": 0.35,
                "tests/perf/test_perf_baseline.py::test_metrics_path_normalization_baseline": 0.25,
            },
        },
    )
    _write_json(
        history_file,
        {
            "version": 1,
            "runs": [
                {
                    "status": "pass",
                    "suite_seconds": 2.0,
                    "tests": {
                        "tests/perf/test_perf_baseline.py::test_wrap_event_serialization_baseline": 0.5,
                        "tests/perf/test_perf_baseline.py::test_metrics_path_normalization_baseline": 0.4,
                    },
                },
                {
                    "status": "fail",
                    "suite_seconds": 9.0,
                    "tests": {
                        "tests/perf/test_perf_baseline.py::test_wrap_event_serialization_baseline": 1.0,
                    },
                },
                {
                    "status": "pass",
                    "suite_seconds": 1.6,
                    "tests": {
                        "tests/perf/test_perf_baseline.py::test_wrap_event_serialization_baseline": 0.4,
                        "tests/perf/test_perf_baseline.py::test_metrics_path_normalization_baseline": 0.3,
                    },
                },
                {
                    "status": "pass",
                    "suite_seconds": 1.8,
                    "tests": {
                        "tests/perf/test_perf_baseline.py::test_wrap_event_serialization_baseline": 0.45,
                        "tests/perf/test_perf_baseline.py::test_metrics_path_normalization_baseline": 0.35,
                    },
                },
            ],
        },
    )
    _write_json(
        budgets_file,
        {
            "suite_max_seconds": 6.0,
            "ratchet": {
                "enabled": True,
                "history_window": 4,
                "baseline_window": 3,
                "min_samples": 3,
                "suite_multiplier": 2.5,
                "test_multiplier": 2.5,
                "absolute_slack_seconds": 0.05,
                "min_suite_seconds": 0.5,
                "min_test_seconds": 0.03,
            },
            "tests": {
                "tests/perf/test_perf_baseline.py::test_wrap_event_serialization_baseline": 1.8,
                "tests/perf/test_perf_baseline.py::test_metrics_path_normalization_baseline": 1.2,
            },
        },
    )
    _write_json(
        baseline_file,
        {
            "suite_baseline_seconds": 2.5,
            "tests": {
                "tests/perf/test_perf_baseline.py::test_wrap_event_serialization_baseline": 0.6,
                "tests/perf/test_perf_baseline.py::test_metrics_path_normalization_baseline": 0.4,
            },
        },
    )

    _run_manager(
        summary_file=summary_file,
        history_file=history_file,
        budgets_file=budgets_file,
        baseline_file=baseline_file,
        out_history_file=out_history_file,
        out_budgets_file=out_budgets_file,
        out_baseline_file=out_baseline_file,
        report_file=report_file,
    )

    history = _read_json(out_history_file)
    assert len(history["runs"]) == 4

    baseline = _read_json(out_baseline_file)
    assert baseline["suite_baseline_seconds"] == 1.6
    assert baseline["tests"]["tests/perf/test_perf_baseline.py::test_wrap_event_serialization_baseline"] == 0.4
    assert baseline["tests"]["tests/perf/test_perf_baseline.py::test_metrics_path_normalization_baseline"] == 0.3

    budgets = _read_json(out_budgets_file)
    assert budgets["suite_max_seconds"] == 4.05
    assert budgets["tests"]["tests/perf/test_perf_baseline.py::test_wrap_event_serialization_baseline"] == 1.05
    assert budgets["tests"]["tests/perf/test_perf_baseline.py::test_metrics_path_normalization_baseline"] == 0.8

    report = _read_json(report_file)
    assert report["ratchet_applied"] is True
    assert report["history_count"] == 4
    assert report["pass_samples_used"] == 3


def test_perf_baseline_manager_skips_ratchet_when_samples_insufficient(tmp_path: Path) -> None:
    summary_file = tmp_path / "perf-summary.json"
    history_file = tmp_path / "perf-history.json"
    budgets_file = tmp_path / "perf-budgets.json"
    baseline_file = tmp_path / "perf-baseline.json"
    out_history_file = tmp_path / "out-history.json"
    out_budgets_file = tmp_path / "out-budgets.json"
    out_baseline_file = tmp_path / "out-baseline.json"
    report_file = tmp_path / "out-report.json"

    _write_json(
        summary_file,
        {
            "status": "pass",
            "measured_seconds": 1.0,
            "test_times_seconds": {"tests/perf/test_perf_stream_sink.py::test_x": 0.2},
        },
    )
    _write_json(history_file, {"version": 1, "runs": []})
    _write_json(
        budgets_file,
        {
            "suite_max_seconds": 2.0,
            "ratchet": {
                "enabled": True,
                "history_window": 10,
                "baseline_window": 5,
                "min_samples": 3,
                "suite_multiplier": 2.0,
                "test_multiplier": 2.0,
                "absolute_slack_seconds": 0.05,
                "min_suite_seconds": 0.5,
                "min_test_seconds": 0.03,
            },
            "tests": {"tests/perf/test_perf_stream_sink.py::test_x": 0.5},
        },
    )
    _write_json(baseline_file, {"suite_baseline_seconds": 3.0, "tests": {}})

    _run_manager(
        summary_file=summary_file,
        history_file=history_file,
        budgets_file=budgets_file,
        baseline_file=baseline_file,
        out_history_file=out_history_file,
        out_budgets_file=out_budgets_file,
        out_baseline_file=out_baseline_file,
        report_file=report_file,
    )

    budgets = _read_json(out_budgets_file)
    assert budgets["suite_max_seconds"] == 2.0
    assert budgets["tests"]["tests/perf/test_perf_stream_sink.py::test_x"] == 0.5

    baseline = _read_json(out_baseline_file)
    assert baseline["suite_baseline_seconds"] == 1.0
    assert baseline["tests"]["tests/perf/test_perf_stream_sink.py::test_x"] == 0.2

    report = _read_json(report_file)
    assert report["ratchet_applied"] is False
    assert report["history_count"] == 1
    assert report["pass_samples_used"] == 1
