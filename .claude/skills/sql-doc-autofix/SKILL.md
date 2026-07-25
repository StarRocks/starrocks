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
- A checkout of the docs **on a local branch named exactly `branch-<version>`**
  (e.g. `branch-4.1`) — see Step 0 for why the branch *name* matters.

## Step 0 — Resolve the version and build the candidate list
Determine the version being verified: use the skill's argument if one was given,
otherwise the `$SR_VERSION` env var (exported in the runbook's Step 1). Its docs
live in the release-branch worktree created in the runbook, at
`../sr-branch-$SR_VERSION/docs/en/sql-reference`.

The worktree must sit **on a local branch named exactly `branch-$SR_VERSION`**.
`docs_version()` in `run_sql_samples.py` reads
`git rev-parse --abbrev-ref HEAD`, so a worktree created with `--detach` reports
the literal string `HEAD`, and one on any other branch name (`docs/my-fix`)
reports that name — either way the run looks unversioned and every failure is
untriageable. Create or refresh it with:
```bash
: "${SR_VERSION:?set SR_VERSION (or pass a version, e.g. 4.1)}"
git fetch origin "refs/heads/branch-$SR_VERSION:refs/remotes/origin/branch-$SR_VERSION"
git worktree add "../sr-branch-$SR_VERSION" -B "branch-$SR_VERSION" "origin/branch-$SR_VERSION"
# already exists? refresh it in place instead of creating it:
#   git -C "../sr-branch-$SR_VERSION" checkout -B "branch-$SR_VERSION" "origin/branch-$SR_VERSION"
git -C "../sr-branch-$SR_VERSION" rev-parse --abbrev-ref HEAD   # must print branch-$SR_VERSION
```
If you must run against a checkout that cannot be on that branch, pass
`--docs-version $SR_VERSION` to `run_sql_samples.py` rather than renaming it.

Then run from the repo root:
```bash
DOCS=../sr-branch-$SR_VERSION/docs/en/sql-reference
python3 docs/scripts/run_sql_samples.py --docs-root "$DOCS" \
    --host 127.0.0.1 --port 9030 --user root --format json > /tmp/run.json
python3 docs/scripts/autofix_candidates.py --run-json /tmp/run.json \
    --repo ../sr-branch-$SR_VERSION > /tmp/candidates.json
```
Triage **every** FAIL in the run — do not cap the batch. Suppression means a
triaged item won't recur, so there's no benefit to leaving a remainder for a later
run. (`autofix_candidates.py` defaults to all candidates; pass `--limit N` only if
you deliberately want a smaller batch.)

Check the `meta` block before triaging anything, and distinguish the two ways
`aligned: false` happens — they need opposite responses:
- **`NOTE: docs '<x>' is unversioned …`** — a setup problem on your side, not doc
  rot. The checkout is detached or on an off-pattern branch. Fix it as above (or
  pass `--docs-version`) and re-run. Do **not** triage this output.
- **`WARNING: docs <a> vs cluster <b> …`** — a genuine version mismatch. **Stop**;
  failures here are not doc rot.

A `verdict` starting `OK:` still carries a caveat worth heeding: a release branch
can be ahead of the released image, so a feature documented on the branch may be
absent from the build you are testing. Verify before calling it rot (Step 1).

## Step 1 — Classify each candidate (the guardrail)
Read `doc_context` to understand what the example *teaches*, use the MCP server to
check reality, then assign exactly one class. Each class routes to one of three
destinations (Step 3): **fix** → PR edit, **durably-not-runnable** → PR suppression
entry, **unsure** → tracking issue.
- **fixable** — renamed/removed function, reserved word as identifier
  (`FROM order`, `CREATE INDEX index`), clear syntax slip. Confirm the intended
  feature exists (`read_query` on `information_schema`, `SHOW FUNCTIONS`,
  `table_overview`). → propose a fix (Step 2).
  Also **fixable by reformatting** — a client transcript whose statement is itself
  runnable (e.g. `MYSQL > select … from table(generate_series(…))`). The fix is a
  doc cleanup: strip the `mysql>` / `MYSQL >` prompt from the ```sql block so it
  holds only the statement, and move the pasted result into a **separate non-SQL
  block** (```plaintext / ```text). Verify the cleaned SQL runs (Step 2) — if it
  does, this is a fix, not an illustrative suppression.
- **version/build-gated** — the function/config/keyword isn't in this build
  (verify it's absent). Docs may be correct for a newer release. → **do not
  rewrite**; *suppress* (and recommend a "Since vX.Y" note if the doc lacks one).
  **But first check whether the same example text exists on `main`.** Suppression
  is global, not per-version (see Step 3), so if the block is byte-identical on
  `main` and the feature *does* work there, an entry would silence it for the
  newer release too and mask a regression. In that case it is **unsure**, not
  version-gated: route it to the tracking issue and say why. A newer-release doc
  backported to an older branch without its code change is the usual cause, and
  the decision (revert on the release branch, add a version note, or backport the
  code) belongs to a human.
- **needs-setup** — references objects an isolated run can't have; fix only if
  making it self-contained is trivial and preserves intent, else *suppress*.
- **illustrative** — synopsis, cross-dialect comparison, documented expected-error,
  or a client transcript whose SQL genuinely can't run in isolation (placeholder
  table/columns). → *suppress*. But a transcript whose SQL **does** run is *fixable*
  by reformatting (above) — reformat it, don't suppress.
- **unsure** → do not suppress; flag for a human in the tracking issue.

The three durably-not-runnable classes (version-gated / needs-setup / illustrative)
are what previously reappeared every run. They now get a **suppression entry** so
the checker stops re-reporting them — see Step 3.

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

## Step 3 — Deliver: a DRAFT PR (fixes + suppressions) + a review-only tracking ISSUE
Three destinations, so nothing scrolls by, and so durably-not-runnable examples
**stop being re-reported every run**:

**Suppressions — the durably-not-runnable classes.** For each version-gated /
needs-setup / illustrative candidate, append an entry to
`docs/scripts/sql_verify_suppressions.json` **in the same draft PR** (below). Copy
the candidate's `fingerprint` from `/tmp/candidates.json` verbatim (do not
recompute it) into an entry:
```json
{ "fingerprint": "<from candidates.json>", "file": "<file>", "line": <line>,
  "snippet": "<first ~60 chars of the statement>", "category": "version-gated",
  "reason": "<one line: why it won't run here>", "added": "<YYYY-MM-DD>",
  "added_by": "sql-doc-autofix" }
```
`category` ∈ `version-gated | needs-setup | illustrative | expected-error`. Once
this PR merges, the checker skips these by content hash — they never reappear (and
re-surface only if the example text meaningfully changes). Suppression is a
judgment call, so it lands **only** via the human-reviewed PR — never edit the file
outside the PR, and **never suppress a `fixable` or `unsure` item**.

**A suppression is global — it is not scoped to a version or a language.**
`load_suppressions()` keeps only `fingerprint` → `category` and drops every other
field; `category` is descriptive metadata, *not* a filter, and there is no version
key in the schema (the top-level `"version": 1` is the schema version). The
default path resolves relative to the *script*, so one list applies to every
`--docs-root` you check. Consequences to respect:
- An entry added while triaging an old release also silences the same block on
  `main` and every other branch whose text matches. Before suppressing, confirm
  the example is *durably* not runnable **everywhere**, not just in the build
  under test — otherwise it belongs in the tracking issue (see Step 1).
- `expected-error` and `illustrative` are usually safe globally: a synopsis or a
  deliberate error example is one on every branch.
- Duplicated example text shares one fingerprint, so a single entry can cover
  several `file:line` locations. Dedupe by fingerprint before writing, and note
  the extra locations in the entry's `reason`.
- If a doc block is byte-identical across `docs/en`, `docs/zh`, and `docs/ja`,
  one entry covers all three; translated `--` comments change the hash and need
  their own entries.

**Draft PR — the verified fixes _and_ the suppression additions.** Branch off
`origin/main` in a git worktree (docs fixes target `main`, then backport). Confirm
each example exists on `main` before editing; apply each verified edit to its
source `.md`/`.mdx`, and add the suppression entries to
`docs/scripts/sql_verify_suppressions.json`.
Build the PR body **from `.github/PULL_REQUEST_TEMPLATE.md`** — PRs missing the
template's checkboxes cannot be merged, so render it and fill it in (do not write a
freeform body, and `gh pr create --body` overrides the template so you must supply
the filled template yourself):
- `## What type of PR is this:` → `- [x] Doc`
- behavior-change question → `- [x] No, this PR will not result in a change in
  behavior.` (a docs-only fix) and uncheck the default `Yes`
- put the **Fixes** table (`file:line`, before → after, "verified: runs on
  `<cluster version>`") under *What I'm doing:*
- add a **`## Suppressions`** section listing each entry added to
  `sql_verify_suppressions.json` (`file:line` · category · one-line reason), so the
  reviewer sees exactly which examples are being permanently silenced and can object
- reference the tracking issue with a **non-closing** keyword — `Tracking: #<issue>`
  (or `Refs #<issue>`). Do **not** use `Fixes/Closes/Resolves #`: those auto-close
  the issue on merge, and it holds the *un*-fixed work. Clear the template's
  placeholder `Fixes #issue` line unless this PR truly closes a separate bug issue.
- backport section → `- [x] I have checked the version labels ...` and the
  `- [x] <version>` box for the release you verified (e.g. `4.1`)

Open it as a **draft** `[Doc]` PR; a human reviews and un-drafts.

**Tracking issue — only what needs a human.** The durably-not-runnable classes now
go to the suppression list (above), so the issue holds **only `unsure` items** —
the ones genuinely needing judgment. Open/update a GitHub issue labeled
`documentation,docs-maintainer`, titled `SQL doc examples needing review —
<version>`, with a checkbox list, each item `file:line` + its one-line reason.
**If an open issue with that title already exists, update it** (don't duplicate):
regenerate its body from this run's `unsure` set. Any previously-listed item you
have now suppressed in this PR should be checked off with a note; if the list is
**empty**, comment "all triaged" and **close** the issue. (On the first run after
this change, the existing items are all durably-not-runnable — migrate them into
suppression entries in the PR and close/empty the issue.)

Cross-link (PR body → issue when non-empty, issue → PR) and report all URLs at the
end.

## Never
- Never un-draft or merge; never commit without operator review.
- Never run `write_query` outside the scratch DB; never account/role, cluster
  (`ALTER SYSTEM`), `DROP` on real databases, backup/restore, or file/routine-load
  statements during verification.
- Never rewrite an example to "make it pass" at the cost of what it teaches.
- Never treat a version/build-gated failure as doc rot.
- Never suppress a `fixable` or `unsure` example to silence it. Suppression is only
  for durably-not-runnable examples, and every entry is reviewed in the PR before
  merge — never edit `sql_verify_suppressions.json` outside the draft PR.
