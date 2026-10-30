# sql-doc-autofix

Propose **verified fixes** for documentation SQL examples that fail the doc-rot
checker — safely, across **every supported version**. It verifies against each
supported version's cluster (the set is auto-discovered from GitHub), classifies
each failing example, and only for the genuinely fixable ones proposes a corrected
statement, verifies it runs via the StarRocks MCP server, and assembles **draft**
`[Doc]` PR(s) for human review — checking each PR's backport boxes to match exactly
the versions its fixes were verified on. It never merges, and never rewrites an
example in a way that changes what it teaches.

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
2. **`gh`** CLI, authenticated — used to auto-discover the supported-version set and
   to open PRs/issues.
3. **`docker compose`** available. The skill brings each version's cluster up and
   down itself, one at a time on `127.0.0.1:9030`, using
   `docs/docker/doc-verification/docker-compose-shared-nothing.yml`. You do **not**
   pre-start a cluster.
   > Version alignment is automatic: the skill runs each version's docs against a
   > cluster of that same version. Only the image behind `9030` swaps between versions.
4. **`vale` 3.x** on PATH (`brew install vale`; CI pins 3.13.1) — used to lint every file
   the skill edits before the PR opens, with that branch's own `docs/.vale.ini`. The rules
   are Tengo scripts and need 3.x. Without it the skill states in the PR body that the
   gate was skipped; it does not pretend the files are clean.
5. **The `starrocks` MCP server** — configured in the repo-root `.mcp.json`, which
   reads `STARROCKS_HOST/PORT/USER/PASSWORD` from the environment (defaults to
   `127.0.0.1:9030`, `root`, empty). Bound once at session start and reused across all
   versions (only the cluster behind `9030` changes). Claude Code attaches it
   automatically; approve on first use. Sanity check (with a cluster up):
   ```
   STARROCKS_URL=root:@127.0.0.1:9030 uv run --with mcp-server-starrocks mcp-server-starrocks --test
   ```

## Run it
Just invoke the **`sql-doc-autofix`** skill in Claude Code. It:
- resolves the version set — auto-discovered via `docs/scripts/supported_versions.sh`
  (GitHub Releases → top-3 supported major.minor), or an explicit list you pass /
  export as `$SR_VERSIONS` — and asks you to confirm or edit it;
- loops over each version: brings the cluster up on `9030`, then runs the detect
  checker and candidate prep for **each of `en`, `zh`, `ja`** against that one
  cluster, classifies, and verifies fixes;
- consolidates results across versions *and* languages, applying every fix to each
  language that carries the example;
- **Vale-lints every file it edited** — the same gate `ci-vale.yml` runs on the PR — and
  reports the result on a `## Vale` line in the PR body;
- emits a **cross-language parity report** per version;
- opens the PR(s)/issue below.

3 versions × 3 languages = 9 checker passes. The triage load does not triple —
the en/zh/ja copies of an example share a skeleton hash, so they are classified and
verified once, then edited in three files.

To preview the detect half for one version/language manually:
```bash
SR_VERSION=4.1 docker compose -f docs/docker/doc-verification/docker-compose-shared-nothing.yml up -d --wait
for lang in en zh ja; do
  python3 docs/scripts/run_sql_samples.py \
      --docs-root ../sr-branch-4.1/docs/$lang/sql-reference --docs-version 4.1 \
      --host 127.0.0.1 --port 9030 --user root --format json > /tmp/run-4.1-$lang.json
done
```

Parity needs no cluster:
```bash
python3 docs/scripts/sql_sample_parity.py --repo ../sr-branch-4.1        # markdown
python3 docs/scripts/sql_sample_parity.py --repo ../sr-branch-4.1 --format json
```

## What it produces
1. **One or more draft `[Doc]` PRs**, grouped so that every fix in a PR was verified
   on every version whose backport box is checked (`set(checked boxes) ⊆
   set(verified versions)`). A fix verified on all supported versions → one PR with
   all boxes; a fix that fails on an older version → a separate PR with a narrower box
   set. Each body is built from `.github/PULL_REQUEST_TEMPLATE.md` with the required
   checkboxes filled (Doc type, behavior-change = No, the verified backport versions)
   plus a `## Suppressions` section (with each entry's version scope). You review, then
   un-draft.
2. **Version-scoped suppression additions** in the same PR(s) for durably-not-runnable
   examples.
3. A **tracking issue** (`documentation,docs-maintainer`) holding **only the items
   that need a human** (`unsure`) — an existing same-title issue is updated rather than
   duplicated, and closed when empty. The PR(s) and issue cross-link.

## Suppression list
`docs/scripts/sql_verify_suppressions.json` is the durable "already reviewed —
stop reporting this" record, so **durably-not-runnable examples aren't re-flagged
every run**. Key points:
- **Keyed by content hash**, not `file:line` — an entry survives the block moving,
  and **re-surfaces** if the example text is meaningfully edited (reindenting doesn't
  count). The checker skips a match as `SKIP: suppressed` before running it, and
  prints a "N suppressed" line so nothing is hidden silently.
- **Two hashes are accepted**, and the difference matters now that all three languages
  are checked:
  - `skel256:` — `skeleton_fingerprint`: comments stripped (but `/*+ hints */` kept),
    whitespace collapsed, lowercased. The en/zh/ja copies of one example hash the
    same, so **one entry covers all three languages**. This is the default the skill
    writes.
  - `sha256:` — `sample_fingerprint`: the sample verbatim. Matches **one language
    only**, because the zh/ja copies carry translated comments. Use it only to
    deliberately suppress a single language's copy.
- **Lives with the tooling** (on `main`), so one list serves every version *and*
  language. Do not backport it to release branches — release-branch copies are unused.
- **Optionally version-scoped:** an entry with a `versions` list (e.g. `["3.5"]`) is
  suppressed only on those major.minor versions — so a version-gated example stays
  checked on versions where it works, and a regression there is still caught. Omit
  `versions` for a **global** entry (every version), which is right for `illustrative`
  and `expected-error`. Since the skill now verifies every version, it scopes each
  entry from direct evidence.
- **Human-reviewed only:** the skill *proposes* entries in the draft PR; nothing is
  silenced until you merge. Disable with `run_sql_samples.py --no-suppressions`.

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
- Every edited file is Vale-linted before the PR opens, and the result is stated in the
  body — including when the gate was skipped or an alert was pre-existing. Vale errors are
  never silenced with `<!-- vale off -->`. The gate's real target is the *reformat* fix:
  most rules skip fenced code, so the edit it can actually catch is an unbalanced fence
  that spills transcript text into prose.

## Notes
- Runs **all supported versions sequentially every run** on `9030` — one cluster at a
  time (three at once collide on container names/ports and ~3× RAM). Expect 3× cluster
  boots + 3× detect passes + 3 worktrees.
- Backport boxes are **coupled to verification**: a box is checked only for a version
  whose fix ran on that version's cluster, so Mergify never cherry-picks an unverified
  edit to a release branch.
- The supported set is discovered per run (`supported_versions.sh`), so it stays
  correct as releases ship (e.g. `4.2` later) with no code edit; the operator confirms
  it. If `gh` is unavailable, pass the list explicitly.
- The cleanly-fixable set per run is usually small; the value is recurring.
- Checks that examples **run**; result-content verification is out of scope.
- Loops the **shared-nothing** profile; `--profile shared-data` examples across versions
  are out of scope here.
