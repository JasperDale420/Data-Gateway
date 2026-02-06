# Perf Release Readiness

This runbook promotes stable perf guardrail outputs from `.perf/` into versioned `config/` files with explicit, reviewable diffs.

## When To Run

- After several stable perf CI runs (for example 3-5) have populated `.perf/perf_*.active.json`.
- Before a release cut when you want tracked `config/perf_budgets.json` and `config/perf_baseline.json` aligned with current trend history.

## Command

Dry-run first (shows before/after diffs and writes a markdown report):

```bash
python scripts/perf_release_readiness.py \
  --report-file perf-release-readiness-report.md
```

Apply promotion (updates tracked config files from active files):

```bash
python scripts/perf_release_readiness.py --apply
```

## Verification Checklist

1. Run the dry-run command and inspect `perf-release-readiness-report.md`.
2. Confirm diffs are expected and not too aggressive.
3. Run apply mode.
4. Review tracked changes:
   - `git diff -- config/perf_budgets.json config/perf_baseline.json`
5. Run perf gate locally:
   - `python scripts/perf_gate.py --budgets-file config/perf_budgets.json --baseline-file config/perf_baseline.json --junit-xml perf-junit.xml --log-file perf-output.txt --summary-file perf-summary.json`
6. Commit promoted config updates only when perf gate remains green.
