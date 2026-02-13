#!/usr/bin/env python3
"""
Analyze darkpool trade volume distribution by time of day.
Reads raw UW darkpool CSVs, extracts executed_at timestamps,
and produces a per-minute trade rate analysis to guide adaptive polling.
Output written to /Users/jacobmcmillan/Empire/Data-Gateway/dp_analysis_output.txt
"""

import csv
import glob
import os
import sys
from collections import defaultdict

ET_OFFSET = 5  # UTC to EST

# Use only the 4 most recent files to keep it fast
files = sorted(glob.glob("/Users/jacobmcmillan/Empire/uw_raw_data/dark_pool/dp-eod-report-2026-02-0*.csv"))
if not files:
    print("No files found!", file=sys.stderr)
    sys.exit(1)

out_path = "/Users/jacobmcmillan/Empire/Data-Gateway/dp_analysis_output.txt"
out = open(out_path, "w")


def p(s=""):
    print(s, file=out)
    print(s)  # also to stdout for debug


p(f"Analyzing {len(files)} files...")

# Per-minute counts per day, keyed by ET HH:MM
bucket_counts = defaultdict(list)

for f in files:
    date = os.path.basename(f)[15:25]
    day_buckets = defaultdict(int)
    total = 0
    with open(f) as fh:
        reader = csv.reader(fh)
        header = next(reader)
        ts_idx = header.index("executed_at")
        for row in reader:
            ts = row[ts_idx]
            h_utc = int(ts[11:13])
            m = int(ts[14:16])
            h_et = (h_utc - ET_OFFSET) % 24
            bucket = f"{h_et:02d}:{(m // 5) * 5:02d}"
            day_buckets[bucket] += 1
            total += 1
    p(f"  {date}: {total:,} trades")
    for b, c in day_buckets.items():
        bucket_counts[b].append(c)

p()
p("=== TRADES PER 5-MIN BUCKET (ET) ===")
p(f"{'Time':>6} | {'Avg/5min':>9} | {'Peak/5min':>10} | {'Avg/min':>8} | {'Peak/min':>9}")
p("-" * 60)

all_data = []
for bucket in sorted(bucket_counts.keys()):
    h = int(bucket[:2])
    if h < 4:
        continue
    counts = bucket_counts[bucket]
    avg5 = sum(counts) / len(counts)
    peak5 = max(counts)
    all_data.append((bucket, avg5, peak5))
    p(f"{bucket:>6} | {avg5:>9,.0f} | {peak5:>10,.0f} | {avg5 / 5:>8,.1f} | {peak5 / 5:>9,.1f}")

p()
p("=== PERIOD SUMMARY (API limit: 200 trades per call) ===")
p(f"{'Period':>30} | {'Avg/min':>8} | {'Peak/min':>9} | Recommendation")
p("-" * 85)

periods = [
    ("Pre-market (4:00-9:30)", lambda b: (4 <= int(b[:2]) < 9) or (int(b[:2]) == 9 and int(b[3:]) < 30)),
    ("Open rush (9:30-10:00)", lambda b: int(b[:2]) == 9 and int(b[3:]) >= 30),
    ("10:00-10:30", lambda b: int(b[:2]) == 10 and int(b[3:]) < 30),
    ("10:30-11:00", lambda b: int(b[:2]) == 10 and int(b[3:]) >= 30),
    ("Midday (11:00-14:00)", lambda b: 11 <= int(b[:2]) < 14),
    ("Afternoon (14:00-15:00)", lambda b: int(b[:2]) == 14),
    ("Close rush (15:00-16:00)", lambda b: int(b[:2]) == 15),
    ("After-hours (16:00-20:00)", lambda b: 16 <= int(b[:2]) < 20),
]

for name, pred in periods:
    matching = [(b, a, pk) for b, a, pk in all_data if pred(b)]
    if not matching:
        continue
    avg = sum(a for _, a, _ in matching) / len(matching) / 5
    peak = max(pk for _, _, pk in matching) / 5
    if peak * 15 <= 200:
        rec = "15s (safe)"
    elif peak * 30 <= 200:
        rec = "30s (safe)"
    elif peak * 60 <= 200:
        rec = "60s (tight)"
    else:
        rec = f"60s (WILL MISS ~{int(peak - 200 / 60)}/min surplus)"
    p(f"{name:>30} | {avg:>8.1f} | {peak:>9.1f} | {rec}")

p()
p("peak*Xs = trades arriving in X seconds at peak rate.")
p("If peak*Xs > 200 (API limit), trades will be missed at that interval.")

out.close()
print(f"\nResults written to {out_path}")
