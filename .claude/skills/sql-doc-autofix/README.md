# sql-doc-autofix

Propose **verified fixes** for documentation SQL examples that fail the doc-rot
checker — safely. It classifies each failing example, and only for the genuinely
fixable ones proposes a corrected statement, verifies it runs against a live
cluster via the StarRocks MCP server, and assembles a **draft** `[Doc]` PR for
human review. It never merges, and never rewrites an example in a way that changes
what it teaches.

This README is for the **operator**. `SKILL.md` is the runbook Claude Code follows.

## How it fits with the checker

```
docs/scripts/run_sql_samples.py    →  FAIL list      (detect: which examples don't run)
docs/scripts/autofix_candidates.py →  candidates + doc context
sql-doc-autofix skill              →  classify → propose → verify → DRAFT PR   (this)
```

The checker is the **detect** half (its own PR); this skill is the
**suggest-a-fix** half.

## Prerequisites
1. **`uv`** on PATH (launches the MCP server).
2. **A cluster whose version matches the docs under test**, via
   `docs/docker/doc-verification/` — e.g.
   `SR_VERSION=4.1 docker compose -f docs/docker/doc-verification/docker-compose-shared-nothing.yml up -d --wait`.
   > Version alignment is mandatory: test 4.1 docs against a 4.1 cluster. A
   > mismatch produces false failures, not doc rot.
3. **A docs checkout at the matching release branch** (e.g. a `branch-4.1` checkout).
4. **The `starrocks` MCP server** — configured in the repo-root `.mcp.json`, which
   reads `STARROCKS_HOST/PORT/USER/PASSWORD` from the environment (defaults to
   `127.0.0.1:9030`, `root`, empty). Claude Code attaches it automatically; approve
   on first use. Sanity check:
   ```
   STARROCKS_URL=root:@127.0.0.1:9030 uv run --with mcp-server-starrocks mcp-server-starrocks --test
   ```

## Run it
```bash
# 1. detect
python3 docs/scripts/run_sql_samples.py \
    --docs-root <release-checkout>/docs/en/sql-reference \
    --host 127.0.0.1 --port 9030 --user root --format json > /tmp/run.json
# 2. prep (FAIL items + surrounding doc prose)
python3 docs/scripts/autofix_candidates.py \
    --run-json /tmp/run.json --repo <release-checkout> --limit 20 > /tmp/candidates.json
```
Then invoke the **`sql-doc-autofix`** skill in Claude Code.

## What it produces
Two things, so nothing is lost:
1. A **draft** `[Doc]` PR (target `main`) with the **verified fixes** — its body is
   built from `.github/PULL_REQUEST_TEMPLATE.md` with the required checkboxes filled
   (Doc type, behavior-change = No, backport version), so it's actually mergeable.
   You review, then un-draft.
2. A **tracking issue** (`documentation,docs-maintainer`) listing every example it
   did **not** auto-fix (version-gated / needs-setup / illustrative / review),
   grouped and checkboxed — an existing same-title issue is updated rather than
   duplicated. The PR and issue cross-link.

## Classification (the guardrail)
**"Executes" ≠ "correct documentation."** Every candidate is classified before any
rewrite: **fixable** (propose + verify) · **version-gated** (flag / "Since" note,
no rewrite) · **needs-setup** (complete only if trivial, else flag) ·
**illustrative** (leave) · **review** (human).

## Safety
- Never un-drafts or merges; every change is human-reviewed.
- `write_query` runs **only in a scratch database**; never account/role, cluster
  (`ALTER SYSTEM`), destructive `DROP`, backup/restore, or file/routine-load ops.
- Never rewrites to "make it pass" at the cost of intent; never treats a
  version/build-gated failure as doc rot.

## Notes
- One copy on `main` serves every release — point `--docs-root` at the target
  branch's docs and set `SR_VERSION`. No per-branch backport.
- The cleanly-fixable set per run is usually small; the value is recurring.
- Checks that examples **run**; result-content verification is out of scope.
