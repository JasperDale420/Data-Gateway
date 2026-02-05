#!/usr/bin/env python3
"""Generate provider endpoint contract markdown from API route declarations."""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from pathlib import Path

PROVIDER_PREFIXES = {
    "unusual_whales": "/api/v1/uw",
    "finnhub": "/api/v1/finnhub",
    "alphavantage": "/api/v1/alphavantage",
    "sec": "/api/v1/sec",
    "yfinance": "/api/v1/yf",
}

PROVIDER_SOURCES = {
    "unusual_whales": Path("gateway/api/uw"),
    "finnhub": Path("gateway/api/finnhub"),
    "alphavantage": Path("gateway/api/alphavantage"),
    "sec": Path("gateway/api/sec.py"),
    "yfinance": Path("gateway/api/yf.py"),
}


def _iter_provider_files(provider: str) -> list[Path]:
    source = PROVIDER_SOURCES[provider]
    if source.is_file():
        return [source]
    return sorted(path for path in source.glob("*.py") if path.name != "__init__.py")


def _collect_routes() -> dict[str, list[tuple[str, str, str]]]:
    routes: dict[str, list[tuple[str, str, str]]] = defaultdict(list)

    route_pattern = re.compile(
        r"@(?:base_)?router\.(get|post|put|patch|delete)\(\s*[\"']([^\"']+)[\"']",
        re.MULTILINE,
    )
    func_pattern = re.compile(r"(?:async\s+def|def)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(")

    for provider, prefix in PROVIDER_PREFIXES.items():
        for file_path in _iter_provider_files(provider):
            text = file_path.read_text()
            for match in route_pattern.finditer(text):
                method = match.group(1).upper()
                subpath = match.group(2)
                if subpath.startswith("/api/v1/"):
                    full_path = subpath
                else:
                    full_path = f"{prefix}{subpath}"
                func_match = func_pattern.search(text, match.end())
                handler_name = func_match.group(1) if func_match else "unknown_handler"
                handler_ref = f"{file_path.as_posix()}:{handler_name}"
                routes[provider].append((method, full_path, handler_ref))

    for provider in routes:
        routes[provider] = sorted(set(routes[provider]), key=lambda item: (item[1], item[0]))

    return routes


def _render_markdown(routes: dict[str, list[tuple[str, str, str]]]) -> str:
    lines = [
        "# Provider Endpoint Contract",
        "",
        "This file is generated from provider route declarations in `gateway/api/*`.",
        "",
        "## Scope",
        "",
        "- `/api/v1/uw/*`",
        "- `/api/v1/finnhub/*`",
        "- `/api/v1/alphavantage/*`",
        "- `/api/v1/sec/*`",
        "- `/api/v1/yf/*`",
        "",
        "## Summary",
        "",
        "| Provider | Route Count |",
        "|---|---:|",
    ]
    for provider in PROVIDER_PREFIXES:
        lines.append(f"| `{provider}` | {len(routes.get(provider, []))} |")

    for provider in PROVIDER_PREFIXES:
        provider_routes = routes.get(provider, [])
        lines.append("")
        lines.append(f"## `{provider}`")
        lines.append("")
        if not provider_routes:
            lines.append("_No routes found._")
            continue
        lines.append("| Method | Path | Handler |")
        lines.append("|---|---|---|")
        for method, path, handler in provider_routes:
            lines.append(f"| `{method}` | `{path}` | `{handler}` |")

    lines.append("")
    lines.append("## Regeneration")
    lines.append("")
    lines.append("```bash")
    lines.append("python scripts/generate_provider_contract.py")
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default="PROVIDER_ENDPOINT_CONTRACT.md",
        help="Output markdown path (default: PROVIDER_ENDPOINT_CONTRACT.md)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if output is out-of-date.",
    )
    args = parser.parse_args()

    routes = _collect_routes()
    rendered = _render_markdown(routes)
    output_path = Path(args.output)

    if args.check:
        existing = output_path.read_text() if output_path.exists() else ""
        if existing != rendered:
            print(
                f"{output_path} is out-of-date. Regenerate with: python scripts/generate_provider_contract.py"
            )
            return 1
        print(f"{output_path} is up-to-date.")
        return 0

    output_path.write_text(rendered)
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
