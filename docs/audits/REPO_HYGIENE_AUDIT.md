# Repo Hygiene Audit

> **Date:** 2026-02-09
> **Branch:** `feature/uw-eod-polling`
> **Scope:** Comprehensive audit of project structure, git hygiene, CI/CD, dependencies, Docker, security, code quality, and documentation

---

## Summary

| Category | Issues Found | Severity |
|----------|-------------|----------|
| Git-tracked artifacts | 4 | 🟡 Medium |
| CI/CD configuration | 2 | 🔴 High |
| Security | 1 | 🔴 High |
| Dependency management | 2 | 🟡 Medium |
| Stale/orphan files | 5 | 🟡 Medium |
| Documentation sprawl | 1 | 🟢 Low |
| Pre-commit config | 1 | 🟢 Low |
| Dockerfile | 1 | 🟡 Medium |
| Naming | 1 | 🟢 Low |

---

## Audited — Fixes Needed

### 🔴 HIGH: CI `ci.yml` Targets Wrong Branch

**File:** `.github/workflows/ci.yml` (lines 8–11)

The CI workflow triggers on `push: [main]` and `pull_request: [main]`, but the repo default branch is **`master`**. This means CI never runs on pushes or PRs to `master`.

**Fix:** Change `main` → `master` in `ci.yml`, or rename the default branch to `main`.

```diff
 on:
   push:
-    branches: [main]
+    branches: [master]
   pull_request:
-    branches: [main]
+    branches: [master]
```

> [!NOTE]
> `perf-guardrail.yml` already correctly targets both `[main, master]`.

---

### 🔴 HIGH: Plaintext API Keys in Tracked `clients.yaml`

**Files:** `clients.yaml` (root) and `config/clients.yaml`

Both files contain plaintext client API keys (`gw_cerberus_dev_key_12345`, `gw_3roses_trading_key_98765`, etc.) and are tracked in git history. These are development keys but the pattern is dangerous.

**Fix:**

1. Add both files to `.gitignore` (use `clients.yaml` glob or specific paths)
2. Remove them from git tracking with `git rm --cached`
3. Provide `config/clients.yaml.example` with placeholder keys
4. Load client keys from environment variables or a non-tracked secrets file

> [!CAUTION]
> The keys are already in git history. If they are used in any shared environment, rotate them after applying this fix.

---

### 🟡 MEDIUM: Git-Tracked Build Artifacts

The following generated/ephemeral files are tracked in git but should not be:

| File | Size | Notes |
|------|------|-------|
| `coverage.json` | 788 KB | Test coverage data — should be `.gitignore`'d |
| `.scannerwork/report-task.txt` | 276 B | SonarQube working directory — should be `.gitignore`'d |
| `.scannerwork/.sonar_lock` | 0 B | SonarQube lock file |
| `.jules/bolt.md` | 924 B | Stale Bolt agent artifact — delete and `.gitignore` |

**Fix:**

```bash
# Add to .gitignore
echo "coverage.json" >> .gitignore
echo ".scannerwork/" >> .gitignore
echo ".jules/" >> .gitignore

# Remove from tracking
git rm --cached coverage.json
git rm --cached -r .scannerwork/
git rm --cached -r .jules/
```

---

### 🟡 MEDIUM: Root-Level Duplicate Config Files

**Files:** `clients.yaml` and `providers.yaml` at the project root

These are exact duplicates of `config/clients.yaml` and `config/providers.yaml`. The codebase loads from `config/`, making the root copies orphans.

**Fix:** Delete the root copies. The `config/` directory is the canonical location.

```bash
git rm clients.yaml providers.yaml
```

---

### 🟡 MEDIUM: Empty `unusualwhales_sdk/` Directory Tracked

The directory `unusualwhales_sdk/` is an empty directory tracked in git (just a `.gitkeep`-style entry). The actual SDK lives in `vendor/unusualwhales_sdk/` and is correctly gitignored.

**Fix:** Remove from tracking.

```bash
git rm -r unusualwhales_sdk/
```

---

### 🟡 MEDIUM: Dockerfile Duplicates Dependencies from `pyproject.toml`

**File:** `Dockerfile`

The Dockerfile hardcodes all 16 dependency versions in a `pip install` command instead of using `pip install .` from `pyproject.toml`. This means dependency changes require editing two files.

**Fix:** Replace the explicit `pip install` block with:

```dockerfile
COPY pyproject.toml README.md ./
COPY gateway/ gateway/
RUN pip install --no-cache-dir .
```

This respects `pyproject.toml` as the single source of truth. The current approach was likely used to separate the `pip install` layer for Docker caching, but the explicit deps defeats that purpose since they still need to be kept in sync.

---

### 🟡 MEDIUM: Stale Branches Need Cleanup

| Branch | Status |
|--------|--------|
| `remotes/origin/bolt/optimize-envelope-serialization-*` | Stale bot branch |
| `remotes/origin/codex/release-readiness-followups` | Old Codex branch |
| `remotes/origin/dependabot/github_actions/actions/checkout-6` | Merged or stale |
| `remotes/origin/dependabot/github_actions/actions/download-artifact-7` | Merged or stale |
| `remotes/origin/dependabot/github_actions/actions/upload-artifact-6` | Merged or stale |

**Fix:** Verify each is merged, then delete via GitHub or CLI.

---

### 🟢 LOW: Redundant Pre-commit Formatters

**File:** `.pre-commit-config.yaml`

Both `ruff-format` (line 15) and `black` (line 21) run as pre-commit formatters. Since `ruff-format` is a drop-in replacement for `black`, the `black` hook is redundant and adds ~2s to each commit.

**Fix:** Remove the `black` hook. Optionally remove `black` from `[project.optional-dependencies.dev]` in `pyproject.toml` as well.

---

### 🟢 LOW: Root-Level Documentation Sprawl

18 markdown files exist at the project root. Several are specialized audit/planning artifacts that could be consolidated:

| File | Lines | Category |
|------|-------|----------|
| `AUDIT_TECHNICAL_DEBT.md` | 430 | Audit artifact |
| `Audit_Checklist.md` | 528 | Audit artifact |
| `PERFORMANCE_AUDIT.md` | 280 | Audit artifact |
| `PERF_RELEASE_READINESS.md` | 34 | Audit artifact |
| `LIVE_PROVIDER_SMOKE_CHECKLIST.md` | 43 | Testing checklist |
| `alpaca-data-breadth.md` | 88 | Provider reference |
| `unusualwhales_endpoints_with_descriptions.md` | 1,664 | Provider reference |

**Suggestion:** Move these into `docs/` subdirectories (e.g., `docs/audits/`, `docs/providers/`) to keep the root clean. Keep only canonical docs at root: `README.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, `SECURITY.md`, `PRD.md`, `API_REFERENCE.md`.

---

### 🟢 LOW: Agent Rules Filename Typo

**File:** `.agent/rules/produciton.md`

The filename is misspelled — should be `production.md`. This is loaded by the agent framework and may cause confusion.

**Fix:** `git mv .agent/rules/produciton.md .agent/rules/production.md`

---

## Audited — No Issues Found

These areas were audited and found to be in good shape:

| Area | Details |
|------|---------|
| **Unused imports** | `ruff check --select F401` — all clean |
| **Dependabot** | Properly configured for pip + GitHub Actions, weekly on Mondays |
| **`.gitignore` coverage** | Good coverage for Python, virtualenvs, IDE, macOS, perf artifacts |
| **`.env` handling** | `.env` properly gitignored, `.env.example` provided |
| **Secrets detection** | `detect-secrets` pre-commit hook with baseline file active |
| **Test structure** | 76 test files, organized by provider/feature, fixtures in `tests/fixtures/` |
| **Bandit security** | Configured in both pre-commit and `pyproject.toml` with reasonable skips |
| **SonarQube** | `sonar-project.properties` properly configured for local scans |
| **`pyproject.toml` tooling** | Ruff, Black, Pyright, Mypy, Pytest all well-configured |
| **`uv.lock`** | Lock file present for reproducible builds |
| **Docker compose** | Present and functional |
| **Pre-commit hooks** | Comprehensive: linting, formatting, type-checking, security, secrets, merge conflict detection |
| **Concurrency guards** | Both CI workflows use `cancel-in-progress: true` |

---

## Not Yet Audited — For Next Session

These areas require deeper investigation and were out of scope for this structural audit:

| Area | What to Check |
|------|---------------|
| **Dead code analysis** | Run full `vulture` or `ruff` dead code detection across `gateway/` to find unused functions, classes, and variables |
| **Test coverage gaps** | Run `pytest --cov=gateway --cov-report=term-missing` and identify uncovered modules |
| **Import graph / circular deps** | Use `importlab` or `pydeps` to check for circular import chains |
| **Type checking strictness** | Run `mypy gateway/` and evaluate whether the many `disable_error_code` overrides in `pyproject.toml` can be reduced |
| **Provider contract freshness** | Run `python scripts/generate_provider_contract.py --check` to verify `PROVIDER_ENDPOINT_CONTRACT.md` is current |
| **Outdated dependencies** | Run `pip list --outdated` or `uv pip compile --upgrade` to identify stale pins |
| **Docker image size** | Build the image and check layer sizes — the current single-stage build may benefit from multi-stage |
| **`.env.example` completeness** | Cross-reference all `os.getenv()` / `settings.*` calls with `.env.example` entries |
| **Performance test hygiene** | Audit `tests/perf/` and `scripts/perf_*.py` for stale benchmarks |
| **WebSocket test coverage** | Verify WebSocket auth handshake, subscription lifecycle, and error paths are tested |
| **CHANGELOG consistency** | Verify version numbers in `CHANGELOG.md` are sequential and match tagged releases |
| **SonarQube issues** | Run `/sonarqube` workflow to fetch and address current issues |
| **`CLAUDE.MD` vs `.agent/rules/`** | Evaluate whether `CLAUDE.MD` is needed given the `.agent/rules/` framework |
