---
name: sql-doc-autofix
description: Propose verified fixes for documentation SQL examples that fail the doc-rot checker (docs/scripts/run_sql_samples.py). Classifies each FAILing example, and only for genuinely fixable ones proposes a corrected statement, verifies it against a live cluster via the StarRocks MCP server, and opens a DRAFT [Doc] PR. Use after run_sql_samples.py produces a FAIL list. Never auto-merges.
argument-hint: "[version, e.g. 4.1 — defaults to the $SR_VERSION env var]"
allowed-tools: Read, Edit, Grep, Glob, Bash, Agent, mcp__starrocks__read_query, mcp__starrocks__write_query, mcp__starrocks__table_overview, mcp__starrocks__db_overview
---

# SQL doc auto-fix

Turn the checker's **detect** output into **suggested fixes**, safely. Golden rule:
**"executes" ≠ "correct documentation."** Making a statement run by changing what
it teaches is worse than leaving it broken. Classify first; only rewrite the
genuinely fixable; flag the rest. English docs only — never edit `docs/zh/**` or
`docs/ja/**`.

## Prerequisites
- The **`starrocks` MCP server** is attached (see repo-root `.mcp.json`; tools:
  `read_query`, `write_query`, `table_overview`, `db_overview`), pointed at the
  running cluster the docs were tested against.
- A cluster of the version under test is up:
  `SR_VERSION=<v> docker compose -f docs/docker/doc-verification/docker-compose-shared-nothing.yml up -d --wait`.
- A checkout of the docs at the matching release branch (e.g. `branch-4.1`).

## Step 0 — Resolve the version and build the candidate list
Determine the version being verified: use the skill's argument if one was given,
otherwise the `$SR_VERSION` env var (exported in the runbook's Step 1). Its docs
live in the release-branch worktree created in the runbook, at
`../sr-branch-$SR_VERSION/docs/en/sql-reference`. Run from the repo root:
```bash
: "${SR_VERSION:?set SR_VERSION (or pass a version, e.g. 4.1)}"
DOCS=../sr-branch-$SR_VERSION/docs/en/sql-reference
python3 docs/scripts/run_sql_samples.py --docs-root "$DOCS" \
    --host 127.0.0.1 --port 9030 --user root --format json > /tmp/run.json
python3 docs/scripts/autofix_candidates.py --run-json /tmp/run.json \
    --repo ../sr-branch-$SR_VERSION --limit 20 > /tmp/candidates.json
```
Check the `meta` block: if docs and cluster versions are **not aligned**, stop —
misaligned failures are not doc rot.

## Step 1 — Classify each candidate (the guardrail)
Read `doc_context` to understand what the example *teaches*, use the MCP server to
check reality, then assign exactly one class:
- **fixable** — renamed/removed function, reserved word as identifier
  (`FROM order`, `CREATE INDEX index`), clear syntax slip. Confirm the intended
  feature exists (`read_query` on `information_schema`, `SHOW FUNCTIONS`,
  `table_overview`). → propose a fix (Step 2).
- **version/build-gated** — the function/config/keyword isn't in this build
  (verify it's absent). Docs may be correct for a newer release. → **do not
  rewrite**; recommend a "Since vX.Y" note.
- **needs-setup** — references objects an isolated run can't have; fix only if
  making it self-contained is trivial and preserves intent, else flag.
- **illustrative** — synopsis, cross-dialect comparison, client transcript. → leave.
- **unsure** → flag for a human.

## Step 2 — Propose + verify (fixable only)
Work in an isolated scratch database via the MCP server:
```
write_query: CREATE DATABASE IF NOT EXISTS docfix_scratch;
write_query: USE docfix_scratch;   -- do ALL test writes here
```
- Create only the minimal setup the example implies. Run the candidate fix; on
  error, read the error + `table_overview` and refine — **max 3 attempts**.
- **Preserve intent:** the fix must still demonstrate the same point. If the only
  way to make it run changes what it teaches, it is NOT fixable — reclassify.
- On success record `file`, `line`, `before`, `after`, verified statement; then
  `write_query: DROP DATABASE docfix_scratch;`.

## Step 3 — Deliver: a DRAFT PR for the fixes + a tracking ISSUE for the rest
Produce **both**, so nothing scrolls by and gets lost:

**Draft PR — the verified fixes.** Branch off `origin/main` in a git worktree
(docs fixes target `main`, then backport). Confirm each example exists on `main`
before editing; apply each verified edit to its source `.md`/`.mdx`.
Build the PR body **from `.github/PULL_REQUEST_TEMPLATE.md`** — PRs missing the
template's checkboxes cannot be merged, so render it and fill it in (do not write a
freeform body, and `gh pr create --body` overrides the template so you must supply
the filled template yourself):
- `## What type of PR is this:` → `- [x] Doc`
- behavior-change question → `- [x] No, this PR will not result in a change in
  behavior.` (a docs-only fix) and uncheck the default `Yes`
- put the **Fixes** table (`file:line`, before → after, "verified: runs on
  `<cluster version>`") under *What I'm doing:*
- reference the tracking issue with a **non-closing** keyword — `Tracking: #<issue>`
  (or `Refs #<issue>`). Do **not** use `Fixes/Closes/Resolves #`: those auto-close
  the issue on merge, and it holds the *un*-fixed work. Clear the template's
  placeholder `Fixes #issue` line unless this PR truly closes a separate bug issue.
- backport section → `- [x] I have checked the version labels ...` and the
  `- [x] <version>` box for the release you verified (e.g. `4.1`)

Open it as a **draft** `[Doc]` PR; a human reviews and un-drafts.

**Tracking issue — everything NOT auto-fixed.** So the version-gated /
needs-setup / illustrative / review items are captured (not just flashed on
screen), open a GitHub issue labeled `documentation,docs-maintainer`, titled
`SQL doc examples needing review — <version>`. Body: a checkbox list grouped by
category, each item `file:line` + its one-line reason. **If an open issue with
that title already exists, update it instead of opening a duplicate.**

Cross-link the two (PR body → issue, issue → PR) and report both URLs at the end.

## Never
- Never un-draft or merge; never commit without operator review.
- Never run `write_query` outside the scratch DB; never account/role, cluster
  (`ALTER SYSTEM`), `DROP` on real databases, backup/restore, or file/routine-load
  statements during verification.
- Never rewrite an example to "make it pass" at the cost of what it teaches.
- Never treat a version/build-gated failure as doc rot.
