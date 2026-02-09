from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def test_stream_tuning_report_text_outputs_export_lines(tmp_path: Path) -> None:
    status_file = tmp_path / "status.json"
    _write_json(
        status_file,
        {
            "data": {
                "stream_tuning_summary": {
                    "overall_level": "critical",
                    "suggested_env": {
                        "GATEWAY_DATA_SINK_STREAM_PUBLISH_MAX_PENDING": 640,
                        "GATEWAY_STREAM_FANOUT_BATCH_SIZE": 40,
                    },
                    "recommendations": ["Increase sink pending queue."],
                }
            }
        },
    )

    cmd = [sys.executable, "scripts/stream_tuning_report.py", "--status-file", str(status_file)]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "overall_level=critical" in completed.stdout
    assert "export GATEWAY_DATA_SINK_STREAM_PUBLISH_MAX_PENDING=640" in completed.stdout
    assert "export GATEWAY_STREAM_FANOUT_BATCH_SIZE=40" in completed.stdout


def test_stream_tuning_report_json_mode_is_machine_readable(tmp_path: Path) -> None:
    status_file = tmp_path / "status.json"
    _write_json(
        status_file,
        {
            "stream_tuning_summary": {
                "overall_level": "warning",
                "suggested_env": {"GATEWAY_STREAM_FANOUT_MAX_INFLIGHT": 125},
                "recommendations": [],
            }
        },
    )

    cmd = [
        sys.executable,
        "scripts/stream_tuning_report.py",
        "--status-file",
        str(status_file),
        "--format",
        "json",
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["overall_level"] == "warning"
    assert payload["suggested_env"]["GATEWAY_STREAM_FANOUT_MAX_INFLIGHT"] == 125


def test_stream_tuning_report_can_load_from_status_url(tmp_path: Path) -> None:
    status_file = tmp_path / "status.json"
    _write_json(
        status_file,
        {
            "data": {
                "stream_tuning_summary": {
                    "overall_level": "warning",
                    "suggested_env": {"GATEWAY_STREAM_FANOUT_BATCH_SIZE": 40},
                    "recommendations": [],
                }
            }
        },
    )

    cmd = [
        sys.executable,
        "scripts/stream_tuning_report.py",
        "--status-url",
        status_file.resolve().as_uri(),
        "--format",
        "json",
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["overall_level"] == "warning"
    assert payload["suggested_env"]["GATEWAY_STREAM_FANOUT_BATCH_SIZE"] == 40


def test_stream_tuning_report_can_write_env_file(tmp_path: Path) -> None:
    status_file = tmp_path / "status.json"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "GATEWAY_STREAM_FANOUT_BATCH_SIZE=32\nEXISTING_KEY=stable\n"  # pragma: allowlist secret
    )
    _write_json(
        status_file,
        {
            "data": {
                "stream_tuning_summary": {
                    "overall_level": "critical",
                    "suggested_env": {
                        "GATEWAY_STREAM_FANOUT_BATCH_SIZE": 40,
                        "GATEWAY_STREAM_FANOUT_MAX_INFLIGHT": 125,
                    },
                    "recommendations": [],
                }
            }
        },
    )

    cmd = [
        sys.executable,
        "scripts/stream_tuning_report.py",
        "--status-file",
        str(status_file),
        "--env-file",
        str(env_file),
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "env_file_updated=yes" in completed.stdout
    assert "env_keys_changed=2" in completed.stdout

    output = env_file.read_text()
    assert "GATEWAY_STREAM_FANOUT_BATCH_SIZE=40" in output
    assert "GATEWAY_STREAM_FANOUT_MAX_INFLIGHT=125" in output
    assert "EXISTING_KEY=stable" in output
