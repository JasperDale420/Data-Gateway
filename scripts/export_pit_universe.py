"""Export the Atlas V2 PIT liquidity universe into a gateway-readable file.

Reads the survivorship-free PIT membership parquet (symbol, start_date, end_date)
and emits config/uw_pit_universe.json with:
  - active:   symbols in the PIT membership as-of the parquet's latest date
              (forward EOD capture set)
  - backfill: symbols active at any point in the trailing `window_days`, each with
              a clamped [start, end] backfill window (survivorship-free set)

Run with any pandas-equipped python (e.g. Atlasv2's venv):
    /Users/jacobmcmillan/Empire/Atlasv2/.venv/bin/python \
        Data-Gateway/scripts/export_pit_universe.py \
        [--parquet ...] [--window-days 780] [--out Data-Gateway/config/uw_pit_universe.json]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

DEFAULT_PARQUET = Path("/Users/jacobmcmillan/Empire/Atlasv2/atlas/data/universes/pit_liquidity_top1000.parquet")
DEFAULT_OUT = Path(__file__).resolve().parent.parent / "config" / "uw_pit_universe.json"


def build(parquet: Path, window_days: int) -> dict:
    df = pd.read_parquet(parquet)
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    asof = df["end_date"].max()
    window_start = asof - pd.Timedelta(days=window_days)

    active = sorted(df.loc[(df.start_date <= asof) & (df.end_date >= asof), "symbol"].unique())

    # Symbols with any membership overlapping the trailing window.
    win = df[(df.end_date >= window_start) & (df.start_date <= asof)].copy()
    # ponytail: union bounds per symbol (min start / max end of in-window intervals),
    # over-fetching any intra-symbol gap. Fine — series feeds return one call anyway
    # and the driver caps depth. Per-interval fetch only if a symbol's gaps ever matter.
    win["start_date"] = win.start_date.clip(lower=window_start)
    win["end_date"] = win.end_date.clip(upper=asof)
    grouped = win.groupby("symbol").agg(start=("start_date", "min"), end=("end_date", "max")).reset_index()
    backfill = [
        {"symbol": r.symbol, "start": r.start.strftime("%Y-%m-%d"), "end": r.end.strftime("%Y-%m-%d")}
        for r in grouped.sort_values("symbol").itertuples()
    ]

    return {
        "generated_from": str(parquet),
        "asof": asof.strftime("%Y-%m-%d"),
        "window_days": window_days,
        "active_count": len(active),
        "backfill_count": len(backfill),
        "active": active,
        "backfill": backfill,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", type=Path, default=DEFAULT_PARQUET)
    ap.add_argument("--window-days", type=int, default=780)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    payload = build(args.parquet, args.window_days)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=1))
    print(
        f"Wrote {args.out}: {payload['active_count']} active, "
        f"{payload['backfill_count']} backfill symbols, asof {payload['asof']}"
    )


if __name__ == "__main__":
    main()
