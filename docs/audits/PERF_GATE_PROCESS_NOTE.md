# Perf Gate Process Note — CI Drift vs Real Regression

**Created:** 2026-04-30
**Trigger:** Multi-persona predict analysis flagged the trend in recent commits (`869790a`, `c901c4a`, `695486e`, `27394e7`, `6062e40`) — the most recent five commits to `master` were all CI perf-gate adjustments. The signal "did the gateway regress against production targets?" is being lost in CI tuning.

## The Pattern

Recent commit messages follow this template: "raise perf baselines to CI runner reality", "extend static merge to cover baselines", "relax path-normalization perf budget". Each is a legitimate response to flaky CI, but the cumulative effect is that `config/perf_baseline.json` only ever moves in the looser direction. The gate stops being a regression detector and becomes a high-water-mark recorder.

## Why It Happens

GitHub Actions runners are noisy:
- Variable noisy-neighbor CPU contention
- Variable I/O latency to ephemeral disk
- Network jitter on dependency installs
- Containerized runtime overhead

Real regressions are signal; runner variance is noise. When both look the same to the gate, the only safe response is to relax the threshold.

## What This Note Is For

A reminder. This is a process discipline problem, not a code problem.

## Recommended Discipline

1. **Treat CI perf gates as smoke tests, not regression detectors.** They catch order-of-magnitude regressions. They do NOT catch 10-30% drift.

2. **Run a separate prod-truth perf benchmark on stable hardware.** Cron-driven. Same test scenarios as CI but on a dedicated machine with consistent environment. Output goes to a separate baseline file (e.g. `config/perf_prod_baseline.json`) that is NOT touched by CI auto-rotation.

3. **Audit `config/perf_baseline.json` history at every release cut.**
   ```bash
   git log --oneline --follow -- config/perf_baseline.json | head -20
   git log -p --follow -- config/perf_baseline.json | grep -E '^[+-].*"p99":'
   ```
   If the trend over the past quarter is monotonically rising, that's CI drift. If individual commits show a spike followed by a return-to-baseline, that's real perf work.

4. **Distinguish "raise budget" from "fix regression" in commit messages.** Operationally:
   - `fix(ci): raise perf baseline for X` — CI calibration, low review priority
   - `perf: improve X` — actual code change to recover headroom, high review priority
   - `perf: investigate baseline rise in X` — a discovery commit before a fix

5. **Set a hard budget freeze before each release.** No baseline changes in the last week before a cut. Forces real perf work to land before the freeze instead of being papered over with budget bumps.

6. **Quarterly perf review.** Look at the prod-truth baseline file (separate from CI), compare to last quarter, identify the top 3 drifters. Even if no individual quarter was a regression, accumulated drift is.

## Related

- `scripts/perf_gate.py` — the gate itself
- `scripts/perf_release_readiness.py` — the budget rotation tool
- `docs/audits/PERF_RELEASE_READINESS.md` — operational runbook for the rotation
- `config/perf_baseline.json` — current tracked baseline
- `config/perf_budgets.json` — current tracked budgets
- `.perf/` — CI's active scratch space (ephemeral)

## Who Should Care

Whoever is doing the next release cut, and whoever is reviewing PRs that touch `config/perf_*.json`.
