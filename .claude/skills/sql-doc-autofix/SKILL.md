---
name: sql-doc-autofix
description: Propose verified fixes for documentation SQL examples that fail the doc-rot checker (docs/scripts/run_sql_samples.py). Verifies against every supported version (auto-discovered), classifies each FAILing example, and only for genuinely fixable ones proposes a corrected statement, verifies it against a live cluster of each version via the StarRocks MCP server, and opens DRAFT [Doc] PRs whose backport boxes match exactly the versions each fix was verified on. Never auto-merges.
argument-hint: "[optional version list, e.g. '4.1 4.0 3.5' — default: auto-discover supported versions and confirm]"
allowed-tools: Read, Edit, Grep, Glob, Bash, Agent, mcp__starrocks__read_query, mcp__starrocks__write_query, mcp__starrocks__table_overview, mcp__starrocks__db_overview
---

# SQL doc auto-fix

Turn the checker's **detect** output into **suggested fixes**, safely. Golden rule:
**"executes" ≠ "correct documentation."** Making a statement run by changing what
it teaches is worse than leaving it broken. Classify first; only rewrite the
genuinely fixable; flag the rest. English docs only — never edit `docs/zh/**` or
`docs/ja/**`.

**Every run verifies against all supported versions** (e.g. `4.1 4.0 3.5`). This
is what makes backporting safe: a fix ships to a release branch **only** if it was
verified against that version's own cluster. The other governing rule follows from
it — **a backport box is checked only for a version whose fix was verified there.**
Backport is Mergify-driven: a checked box auto-cherry-picks the merged PR to
`branch-X.Y`, so an unverified checked box means an unverified edit on that branch.

## Prerequisites
- The **`starrocks` MCP server** is attached (see repo-root `.mcp.json`; tools:
  `read_query`, `write_query`, `table_overview`, `db_overview`). It binds to
  `127.0.0.1:9030` at session start. The skill runs one version's cluster at a time
  on that endpoint, so the *same* MCP server serves every version — only the cluster
  behind `9030` swaps.
- `gh` CLI, authenticated (used for version discovery and to open PRs/issues).
- `docker compose` available; the skill brings each version's cluster up and down
  itself (you do **not** pre-start one).

## Step 0 — Resolve the version SET, then loop detect+verify over each version

### 0a. Resolve the version list
- If the operator passed a list (argument) or exported `$SR_VERSIONS`, use it
  verbatim (short-circuits discovery).
- Otherwise discover it:
  ```bash
  docs/scripts/supported_versions.sh              # e.g. -> "4.1 4.0 3.5"
  # pin the newest to the release you're working on, if not the very latest:
  #   docs/scripts/supported_versions.sh --max 4.1
  ```
  It reads GitHub Releases (non-prerelease/draft), reduces to distinct major.minor,
  semver-sorts descending, clamps to `--max`, and returns the top 3 (`-k` to change).
- **Present the proposed set to the operator and get a confirm-or-edit** before doing
  any work. This step *is* the manual fallback: if `supported_versions.sh` exits
  non-zero (no `gh`, network/API error), ask the operator to supply the list.

### 0b. Per version, refresh a correctly-named worktree
Each version's docs live in `../sr-branch-<v>/docs/en/sql-reference`, and the worktree
**must sit on a local branch named exactly `branch-<v>`**. `docs_version()` in
`run_sql_samples.py` reads `git rev-parse --abbrev-ref HEAD`; a `--detach` worktree
reports `HEAD` and any other branch name reports itself — either way the run looks
unversioned and every failure is untriageable. For each `v` in the set:
```bash
git fetch origin "refs/heads/branch-$v:refs/remotes/origin/branch-$v"
git worktree add "../sr-branch-$v" -B "branch-$v" "origin/branch-$v"
# already exists? refresh in place:
#   git -C "../sr-branch-$v" checkout -B "branch-$v" "origin/branch-$v"
git -C "../sr-branch-$v" rev-parse --abbrev-ref HEAD   # must print branch-$v
```
If a checkout truly cannot be on that branch, pass `--docs-version $v` to the checker.

### 0c. Loop: bring up each cluster, detect, verify, tear down
Run the versions **sequentially on `9030`** (one cluster at a time — three at once
collide on container names/ports and ~3× RAM). For each `v`:
```bash
SR_VERSION=$v docker compose -f docs/docker/doc-verification/docker-compose-shared-nothing.yml up -d --wait
DOCS=../sr-branch-$v/docs/en/sql-reference
python3 docs/scripts/run_sql_samples.py --docs-root "$DOCS" --docs-version "$v" \
    --host 127.0.0.1 --port 9030 --user root --format json > /tmp/run-$v.json
python3 docs/scripts/autofix_candidates.py --run-json /tmp/run-$v.json \
    --repo ../sr-branch-$v > /tmp/candidates-$v.json
# ... classify (Step 1) + verify fixable (Step 2) against this live cluster ...
SR_VERSION=$v docker compose -f docs/docker/doc-verification/docker-compose-shared-nothing.yml down
```
- **Image gate:** if `up --wait` fails (no `starrocks/{fe,be}-ubuntu:$v-latest` image
  for that version), **skip that version and report it** — do not abort the whole run.
- **MCP reconnect:** right after a cluster swap the MCP server's pooled connection to
  `9030` is stale; the first `read_query`/`write_query` of that version's Step 2 may
  error once — retry it once before treating it as a real failure.
- Triage **every** FAIL in each run — do not cap the batch. (`autofix_candidates.py`
  defaults to all candidates; use `--limit N` only to deliberately shrink a batch.)

Check each run's `meta` before triaging, and distinguish the two `aligned: false`
cases — they need opposite responses:
- **`NOTE: docs '<x>' is unversioned …`** — a setup problem on your side (detached or
  off-pattern branch). Fix per 0b (or pass `--docs-version`) and re-run. Do **not**
  triage this output.
- **`WARNING: docs <a> vs cluster <b> …`** — a genuine version mismatch (wrong image
  for this branch). **Stop** that version; failures here are not doc rot.

A `verdict` starting `OK:` still carries a caveat: a release branch can be ahead of the
released image, so a feature documented on the branch may be absent from the build.
Because you now run *all* versions, you can confirm this directly — the same example
passing on a newer version and failing on an older one is the version-gated signal.

## Step 1 — Classify each candidate (the guardrail)
Per version, read `doc_context` to understand what the example *teaches*, use the MCP
server to check reality, then assign exactly one class. Record results into a
**cross-version map keyed by `fingerprint`**:
`{ file, line, before, after, verified_on: set(), durable_fail_on: {version: category},
class_per_version, exists_on_main }`. Each class routes to one of three destinations
(Step 3): **fix** → PR edit, **durably-not-runnable** → scoped suppression entry,
**unsure** → tracking issue.

- **fixable** — renamed/removed function, reserved word as identifier
  (`FROM order`, `CREATE INDEX index`), clear syntax slip. Confirm the intended
  feature exists (`read_query` on `information_schema`, `SHOW FUNCTIONS`,
  `table_overview`). → propose a fix (Step 2).
  Also **fixable by reformatting** — a client transcript whose statement is itself
  runnable (e.g. `MYSQL > select … from table(generate_series(…))`). The fix is a doc
  cleanup: strip the `mysql>` / `MYSQL >` prompt from the ```sql block so it holds only
  the statement, and move the pasted result into a **separate non-SQL block**
  (```plaintext / ```text). Verify the cleaned SQL runs (Step 2).
- **version/build-gated** — the function/config/keyword isn't in this build (verify
  it's absent). Docs may be correct for a newer release. → **do not rewrite**;
  *suppress, scoped to the versions where it's absent* (and recommend a "Since vX.Y"
  note if the doc lacks one). Because you run all versions, scope this precisely: if it
  fails on 3.5 but passes on 4.1/4.0, the suppression's `versions` is `["3.5"]` — never
  global — so a future regression on 4.1 is still caught. If it fails on every version
  but *works on `main`* (feature added after all release branches), it's **unsure**, not
  version-gated: route to the tracking issue (a human decides revert / version-note /
  code backport).
- **needs-setup** — references objects an isolated run can't have; fix only if making it
  self-contained is trivial and preserves intent, else *suppress* (scoped to the
  versions where it failed; usually all).
- **illustrative** — synopsis, cross-dialect comparison, documented expected-error, or a
  client transcript whose SQL genuinely can't run in isolation (placeholder
  table/columns). → *suppress* (**global** — these are true on every branch). But a
  transcript whose SQL **does** run is *fixable* by reformatting (above).
- **unsure** → do not suppress; flag for a human in the tracking issue.

The three durably-not-runnable classes (version-gated / needs-setup / illustrative) get
a **suppression entry** so the checker stops re-reporting them — see Step 3.

## Step 2 — Propose + verify (fixable only), on each version's live cluster
Verify a candidate fix against the cluster of **every version where that example
appears and fails**, so you learn the exact set of versions it is verified on. Work in
an isolated scratch database via the MCP server (which points at the current version's
cluster on `9030`):
```
write_query: CREATE DATABASE IF NOT EXISTS docfix_scratch;
write_query: USE docfix_scratch;   -- do ALL test writes here
```
- Create only the minimal setup the example implies. Run the candidate fix; on error,
  read the error + `table_overview` and refine — **max 3 attempts**.
- **Preserve intent:** the fix must still demonstrate the same point. If the only way to
  make it run changes what it teaches, it is NOT fixable — reclassify.
- On success, add this version to the fingerprint's `verified_on` set and record
  `before`/`after`/verified statement; then `write_query: DROP DATABASE docfix_scratch;`.
- If the *same* fingerprint fails differently on another version (e.g. runs on 4.1 but
  the fix errors on 3.5), verify each independently — a fix verified on a subset of
  versions is fine; it just constrains which backport boxes you may check (Step 3).

## Step 3 — Deliver: DRAFT PR(s) (fixes + scoped suppressions) + a review-only tracking ISSUE

### Suppressions — scoped by observation
For each version-gated / needs-setup / illustrative candidate, append an entry to
`docs/scripts/sql_verify_suppressions.json` **in the same draft PR**. Copy the
candidate's `fingerprint` from `/tmp/candidates-<v>.json` verbatim (do not recompute):
```json
{ "fingerprint": "<from candidates-*.json>", "file": "<file>", "line": <line>,
  "snippet": "<first ~60 chars of the statement>", "category": "version-gated",
  "versions": ["3.5"],
  "reason": "<one line: why it won't run there>", "added": "<YYYY-MM-DD>",
  "added_by": "sql-doc-autofix" }
```
- `category` ∈ `version-gated | needs-setup | illustrative | expected-error`.
- **`versions`** = the major.minor set where the example is *durably not runnable*
  (from your cross-version evidence). **Omit `versions`** (global) only for classes that
  are true on every branch — `illustrative`, `expected-error`. **Scope** `version-gated`
  (and version-specific `needs-setup`) to exactly the failing versions, so the example
  is still checked on versions where it works. A version-gated example that *works on
  `main`* is **unsure**, not a suppression (route to the issue).
- The matcher (`load_suppressions`/`classify`) skips a sample when its fingerprint has a
  global entry, or a scoped entry listing the run's version. Dedupe by fingerprint
  (duplicated text shares one hash → one entry can cover several `file:line`; note the
  extras in `reason`). Suppression lands **only** via the human-reviewed PR — never edit
  the file outside it, and **never suppress a `fixable` or `unsure` item**.

### Group fixes into PR(s) by verified-version-set
Mergify cherry-picks a whole PR to each checked branch, so **every fix in a PR must be
verified on every version whose box is checked.** Therefore:
- **Group fixes by their `verified_on` set; open one draft PR per group; check exactly
  that set of backport boxes.** Fix verified on {4.1,4.0,3.5} → one PR, all three boxes.
  Fix verified on {4.1,4.0} but failing on 3.5 → a *separate* PR with only 4.1+4.0
  boxes (the 3.5 failure becomes a `versions:["3.5"]` suppression or an unsure item).
- **Never check a box for a version where the fix wasn't verified.** Invariant to
  self-check before opening: `set(checked boxes) ⊆ set(verified versions)` for every fix
  in the PR.

### Existence & divergence (where the fix lands)
Before grouping, check where the identical fingerprint lives (it's a content hash, so
divergent text across branches yields *different* fingerprints — already separate
candidates):
- **Same fingerprint present on `main`** → edit on `main`; the PR's backport boxes carry
  it to each verified branch. (Normal case.)
- **Present on `main` but divergent on an older branch** (different fingerprint there) →
  that older-branch candidate is separate: open a PR based on `branch-<v>` directly (no
  backport section), or route to the tracking issue if a human should decide.
- **Absent on `main`** (example removed on main, exists only on an older branch) → PR
  based on `branch-<v>` directly, no backport section — flag it prominently.
Prefer the tracking issue when unsure which of these applies.

### Build each PR body from the template
Build the body **from `.github/PULL_REQUEST_TEMPLATE.md`** — PRs missing the template's
checkboxes cannot be merged (`gh pr create --body` overrides the template, so supply the
filled template yourself):
- `## What type of PR is this:` → `- [x] Doc`
- behavior-change question → `- [x] No, this PR will not result in a change in
  behavior.` and uncheck the default `Yes`
- **Fixes** table under *What I'm doing:* — `file:line`, before → after, and
  **"verified on: `<v1>, <v2>, …`"** (the exact cluster versions each fix ran on).
- a **`## Suppressions`** section listing each entry added (`file:line` · category ·
  `versions` scope or "global" · one-line reason), so the reviewer sees exactly what is
  being silenced and on which versions.
- reference the tracking issue with a **non-closing** keyword — `Tracking: #<issue>` (or
  `Refs #<issue>`). Do **not** use `Fixes/Closes/Resolves #` (those auto-close the issue,
  which holds the *un*-fixed work). Clear the template's placeholder `Fixes #issue` line
  unless this PR truly closes a separate bug issue.
- backport section → `- [x] I have checked the version labels …` and a `- [x] <version>`
  box for **every version whose fix this PR verified, and only those**.

Open each as a **draft** `[Doc]` PR; a human reviews and un-drafts.

### Tracking issue — only what needs a human
The durably-not-runnable classes go to the (scoped) suppression list, so the issue holds
**only `unsure` items**. Open/update a GitHub issue labeled
`documentation,docs-maintainer`, titled `SQL doc examples needing review — <versions>`
(e.g. `— 4.1/4.0/3.5`), with a checkbox list: each item `file:line`, the versions it was
seen failing on, and its one-line reason. **If an open issue with that title already
exists, update it** (don't duplicate): regenerate its body from this run's `unsure` set.
Check off any previously-listed item you have now fixed or suppressed, with a note; if
the list is **empty**, comment "all triaged" and **close** the issue.

Cross-link (PR body → issue when non-empty, issue → PRs) and report all URLs at the end.

## Never
- Never un-draft or merge; never commit without operator review.
- Never check a backport box for a version whose fix you did not verify on that version's
  cluster.
- Never run `write_query` outside the scratch DB; never account/role, cluster
  (`ALTER SYSTEM`), `DROP` on real databases, backup/restore, or file/routine-load
  statements during verification.
- Never rewrite an example to "make it pass" at the cost of what it teaches.
- Never treat a version/build-gated failure as doc rot.
- Never suppress a `fixable` or `unsure` example to silence it. Suppression is only for
  durably-not-runnable examples, scoped to the versions where they fail, and every entry
  is reviewed in the PR — never edit `sql_verify_suppressions.json` outside the draft PR.
- Never global-suppress a version-gated example that works on another version or on
  `main` — scope it, or route it to the tracking issue.
