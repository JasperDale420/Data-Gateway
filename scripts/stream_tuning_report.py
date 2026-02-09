"""Render stream tuning recommendations from a status snapshot JSON file.

Usage:
    python scripts/stream_tuning_report.py --status-file /path/to/status.json
    python scripts/stream_tuning_report.py --status-url http://localhost:8000/api/v1/status --api-key gw_...
    python scripts/stream_tuning_report.py --status-file /path/to/status.json --format json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--status-file", help="Path to status response JSON")
    source_group.add_argument("--status-url", help="Status endpoint URL")
    parser.add_argument("--api-key", default=None, help="Optional gateway API key for status URL")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="Network timeout in seconds for --status-url fetches",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env file path to update with suggested env values",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format",
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid json in '{path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"invalid json in '{path}': expected top-level object")
    return payload


def _load_json_url(url: str, *, api_key: str | None, timeout_seconds: float) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if api_key:
        headers["X-Gateway-Key"] = api_key
    request = Request(url, headers=headers, method="GET")
    with urlopen(request, timeout=max(0.1, timeout_seconds)) as response:
        raw = response.read().decode("utf-8")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid json from '{url}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"invalid json from '{url}': expected top-level object")
    return payload


def _extract_stream_tuning_summary(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    if isinstance(data, dict):
        summary = data.get("stream_tuning_summary")
        if isinstance(summary, dict):
            return summary
    summary = payload.get("stream_tuning_summary")
    if isinstance(summary, dict):
        return summary
    raise ValueError("stream_tuning_summary not found in status payload")


def _write_env_file(path: Path, suggested_env: dict[str, Any]) -> int:
    """Upsert suggested env keys in dotenv format and return number of changed keys."""
    if not suggested_env:
        return 0

    existing_lines = path.read_text().splitlines() if path.exists() else []
    output_lines: list[str] = []
    seen_keys: set[str] = set()
    changed = 0

    for line in existing_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            output_lines.append(line)
            continue
        key, _, value = line.partition("=")
        env_key = key.strip()
        if env_key in suggested_env:
            next_value = str(suggested_env[env_key])
            if value != next_value:
                changed += 1
            output_lines.append(f"{env_key}={next_value}")
            seen_keys.add(env_key)
        else:
            output_lines.append(line)

    for key in sorted(suggested_env):
        if key in seen_keys:
            continue
        output_lines.append(f"{key}={suggested_env[key]}")
        changed += 1

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(output_lines) + "\n")
    return changed


def _render_text(summary: dict[str, Any]) -> str:
    level = str(summary.get("overall_level", "unknown"))
    recommendations = summary.get("recommendations", [])
    suggested_env = summary.get("suggested_env", {})

    lines: list[str] = [f"overall_level={level}", ""]
    lines.append("recommendations:")
    if isinstance(recommendations, list) and recommendations:
        for item in recommendations:
            if isinstance(item, str) and item.strip():
                lines.append(f"- {item.strip()}")
    else:
        lines.append("- (none)")

    lines.extend(["", "suggested_env_exports:"])
    if isinstance(suggested_env, dict) and suggested_env:
        for key in sorted(suggested_env):
            value = suggested_env[key]
            lines.append(f"export {key}={value}")
    else:
        lines.append("# none")

    return "\n".join(lines) + "\n"


def main() -> int:
    args = _parse_args()
    try:
        payload = (
            _load_json(Path(args.status_file))
            if args.status_file
            else _load_json_url(
                args.status_url,
                api_key=args.api_key,
                timeout_seconds=float(args.timeout_seconds),
            )
        )
        summary = _extract_stream_tuning_summary(payload)
    except (FileNotFoundError, ValueError) as exc:
        print(f"stream_tuning_report_failed: {exc}", file=sys.stderr)
        return 1

    suggested_env = summary.get("suggested_env", {})
    env_changed = 0
    if args.env_file:
        env_changed = _write_env_file(Path(args.env_file), suggested_env)

    if args.format == "json":
        rendered = dict(summary)
        rendered["env_file_updated"] = bool(args.env_file)
        rendered["env_keys_changed"] = env_changed
        print(json.dumps(rendered, sort_keys=True))
    else:
        output = _render_text(summary)
        if args.env_file:
            output += (
                f"\nenv_file={args.env_file}\n"
                f"env_file_updated=yes\n"
                f"env_keys_changed={env_changed}\n"
            )
        print(output, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
