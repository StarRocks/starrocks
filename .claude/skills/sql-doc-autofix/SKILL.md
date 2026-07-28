---
name: sql-doc-autofix
description: Propose verified fixes for documentation SQL examples that fail the doc-rot checker (docs/scripts/run_sql_samples.py). Checks all three languages (docs/en, docs/zh, docs/ja) against every supported version (auto-discovered), classifies each FAILing example, and only for genuinely fixable ones proposes a corrected statement, verifies it against a live cluster of each version via the StarRocks MCP server, applies it to every language that carries the example, lints every file it touched with Vale (the same gate CI runs), and opens DRAFT [Doc] PRs whose backport boxes match exactly the versions each fix was verified on. Also emits a cross-language parity report. Never auto-merges.
argument-hint: "[optional version list, e.g. '4.1 4.0 3.5' — default: auto-discover supported versions and confirm]"
allowed-tools: Read, Edit, Grep, Glob, Bash, Agent, mcp__starrocks__read_query, mcp__starrocks__write_query, mcp__starrocks__table_overview, mcp__starrocks__db_overview
---

# SQL doc auto-fix

Turn the checker's **detect** output into **suggested fixes**, safely. Golden rule:
**"executes" ≠ "correct documentation."** Making a statement run by changing what
it teaches is worse than leaving it broken. Classify first; only rewrite the
genuinely fixable; flag the rest.

**Every run covers all three languages × all supported versions.** Two independent
axes, and both matter:

- **Versions** (e.g. `4.1 4.0 3.5`) — makes backporting safe. A fix ships to a
  release branch **only** if it was verified against that version's own cluster, so
  **a backport box is checked only for a version whose fix was verified there.**
  Backport is Mergify-driven: a checked box auto-cherry-picks the merged PR to
  `branch-X.Y`, so an unverified checked box means an unverified edit on that branch.
- **Languages** (`docs/en`, `docs/zh`, `docs/ja`) — this repo has a standing habit of
  updating one language and not the others, so rot and drift hide in the languages
  nobody checked. A fix **must be applied to every language that carries the example**
  (see Step 3). Never fix `en` and leave `zh`/`ja` broken; that is the exact failure
  mode this skill exists to stop.

The two axes produce **different signals** and must not be conflated:

| signal | question | tool | outcome |
|---|---|---|---|
| **rot** | does this example still run? | `run_sql_samples.py` | fix / suppress / issue |
| **parity** | does this example *exist* in the other languages? | `sql_sample_parity.py` | translation-backfill list in the issue — **never** an auto-fix |

A missing example can never fail a rot run, so parity is the only way translation
drift is visible at all.

## Prerequisites
- The **`starrocks` MCP server** is attached (see repo-root `.mcp.json`; tools:
  `read_query`, `write_query`, `table_overview`, `db_overview`). It binds to
  `127.0.0.1:9030` at session start. The skill runs one version's cluster at a time
  on that endpoint, so the *same* MCP server serves every version — only the cluster
  behind `9030` swaps.
- `gh` CLI, authenticated (used for version discovery and to open PRs/issues).
- `docker compose` available; the skill brings each version's cluster up and down
  itself (you do **not** pre-start one).
- **`vale` 3.x** on PATH (CI pins 3.13.1) — the doc lint gate in Step 3. The rules are
  `extends: script` (Tengo) and need 3.x. If `vale` is absent or 2.x, the gate is
  **skipped and declared in the PR body**, never silently treated as a pass.

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
Each version's docs live in `../sr-branch-<v>/docs/{en,zh,ja}/sql-reference`, and the
worktree **must sit on a local branch named exactly `branch-<v>`**. `docs_version()` in
`run_sql_samples.py` reads `git rev-parse --abbrev-ref HEAD`; a `--detach` worktree
reports `HEAD` and any other branch name reports itself — either way the run looks
unversioned and every failure is untriageable. For each `v` in the set:
```bash
git fetch origin "+refs/heads/branch-$v""$(printf ':refs/remotes/origin/branch-%s' "$v")"
git worktree add "../sr-branch-$v" -B "branch-$v" "origin/branch-$v"
# already exists? refresh in place:
#   git -C "../sr-branch-$v" checkout -B "branch-$v" "origin/branch-$v"
git -C "../sr-branch-$v" rev-parse --abbrev-ref HEAD   # must print branch-$v
```
The refspec quoting is deliberate: in zsh a bare `"…branch-$v:refs/…"` has its `:r`
eaten as a history modifier, silently fetching the wrong ref. Verify with
`git for-each-ref` after a scripted fetch loop.

If a checkout truly cannot be on that branch, pass `--docs-version $v` to the checker.

### 0c. Loop: bring up each cluster, run all three languages, tear down
Run the versions **sequentially on `9030`** (one cluster at a time — three at once
collide on container names/ports and ~3× RAM). Within a version, run the three
languages against that same live cluster — the cluster is language-agnostic, so this
costs three checker passes, not three cluster boots. That is **3 versions × 3
languages = 9 runs**; tell the operator up front that this is the long part.

```bash
SR_VERSION=$v docker compose -f docs/docker/doc-verification/docker-compose-shared-nothing.yml up -d --wait
for lang in en zh ja; do
  DOCS=../sr-branch-$v/docs/$lang/sql-reference
  [ -d "$DOCS" ] || { echo "no $lang docs on $v — skipping"; continue; }
  python3 docs/scripts/run_sql_samples.py --docs-root "$DOCS" --docs-version "$v" \
      --host 127.0.0.1 --port 9030 --user root --format json > /tmp/run-$v-$lang.json
  python3 docs/scripts/autofix_candidates.py --run-json /tmp/run-$v-$lang.json \
      --repo ../sr-branch-$v > /tmp/candidates-$v-$lang.json
done
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
- **The triage load does not triple.** Most zh/ja failures are the *same statement* as
  an en failure and share a `skeleton` hash — one classification, one cluster
  verification, three file edits. Only examples whose skeleton appears in one language
  alone need independent triage. Deduplicate by `skeleton` *before* you start
  classifying, or you will do the same work three times.

### 0d. Parity report (once per version, no cluster needed)
Rot and parity are different questions; run both.
```bash
python3 docs/scripts/sql_sample_parity.py --repo ../sr-branch-$v --format json \
    > /tmp/parity-$v.json
python3 docs/scripts/sql_sample_parity.py --repo ../sr-branch-$v > /tmp/parity-$v.md
```
It pairs examples on the comment-stripped `skeleton` hash — so a translated `--`
comment is **not** reported as a difference — and reports two things: pages whose
runnable SQL exists in some languages but not others, and per-page example
differences.

**Do not auto-fix parity findings.** Expect it to be noisy (on 4.1: 89 differing
pages, ~142 en examples absent elsewhere, ~119 elsewhere absent from en). Some gaps
are deliberate — a page restructured in one language only. Parity output goes to the
tracking issue as a **translation-backfill list** for a human, and to the operator as
a summary. Adding a missing example is a translation task, not a doc-rot fix, and it
is out of this skill's scope.

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
Read `doc_context` to understand what the example *teaches*, use the MCP server to
check reality, then assign exactly one class. Record results into a **cross-version,
cross-language map keyed by `skeleton`** (not `fingerprint` — the skeleton is what
makes the en/zh/ja copies of one example a single row):

```
skeleton -> {
  sites: [ {lang, version, file, line, fingerprint} ],   # every copy that FAILs
  langs_present: {version: set(langs)},   # where the example exists at all
  before, after,
  verified_on: set(versions),             # cluster-verified, per Step 2
  durable_fail_on: {version: category},
  class, exists_on_main,
}
```

`sites` is what drives the edits: one classification decision, one cluster
verification, but **an edit at every site**. Build this map before classifying so you
judge each example once — not once per language.

Each class routes to one of three destinations (Step 3): **fix** → PR edit **in every
language that has the example**, **durably-not-runnable** → skeleton-keyed suppression
entry, **unsure** → tracking issue.

Two language-specific classes to watch for — these are new failure modes that an
en-only run never saw:
- **translation-damaged SQL** — the statement was broken *by* translation: full-width
  punctuation (`（`, `）`, `，`, `；`) substituted into SQL, a translated identifier or
  column alias, a smart quote. This is **fixable**, and the fix is usually to restore
  the ASCII/en token. Verify it like any other fix.
- **language-divergent example** — the same page teaches a *different* statement in
  each language (different skeleton, not a translation artifact). These are separate
  candidates with separate skeletons; fix each on its own evidence, and note the
  divergence in the tracking issue rather than silently unifying them.

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
appears and fails**, so you learn the exact set of versions it is verified on.

**Verify once per version, not once per language.** A cluster has no notion of
language: if the en/zh/ja copies share a `skeleton`, they are the same statement and
one run on that version's cluster verifies all three. Re-running the identical SQL
because it came from a different file is wasted effort. The exceptions are real:
- the copies do **not** share a skeleton (genuinely different statements) — verify each;
- the fix is **translation-damage repair** — verify the *repaired* statement, which is
  the one that will land in that language's file, since it may differ from en's text
  in string literals or comments even after repair.

Work in an isolated scratch database via the MCP server (which points at the current
version's cluster on `9030`):
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

### Suppressions — keyed by skeleton, scoped by observation
For each version-gated / needs-setup / illustrative candidate, append an entry to
`docs/scripts/sql_verify_suppressions.json` **in the same draft PR**. Copy the
candidate's **`skeleton`** from `/tmp/candidates-<v>-<lang>.json` verbatim (do not
recompute):
```json
{ "fingerprint": "<the `skeleton` value, skel256:…>", "file": "<file>", "line": <line>,
  "snippet": "<first ~60 chars of the statement>", "category": "version-gated",
  "versions": ["3.5"],
  "reason": "<one line: why it won't run there; note the other languages it covers>",
  "added": "<YYYY-MM-DD>", "added_by": "sql-doc-autofix" }
```
- **Use the `skeleton` (`skel256:…`), not the verbatim `fingerprint`.** The field is
  still named `fingerprint` and the matcher accepts either hash, but a `sha256:` key
  matches **one language only** — the zh and ja copies carry translated comments and
  hash differently, so a verbatim key would need three entries per example and would
  silently miss the other two. One `skel256:` entry covers all three languages.
  Reserve a `sha256:` key for the rare case where you deliberately want to suppress
  one language's copy and not the others; say so in `reason`.
- `category` ∈ `version-gated | needs-setup | illustrative | expected-error`.
- **`versions`** = the major.minor set where the example is *durably not runnable*
  (from your cross-version evidence). **Omit `versions`** (global) only for classes that
  are true on every branch — `illustrative`, `expected-error`. **Scope** `version-gated`
  (and version-specific `needs-setup`) to exactly the failing versions, so the example
  is still checked on versions where it works. A version-gated example that *works on
  `main`* is **unsure**, not a suppression (route to the issue).
- The matcher (`load_suppressions`/`classify`) skips a sample when either of its hashes
  has a global entry, or a scoped entry listing the run's version. Dedupe by skeleton
  (duplicated text shares one hash → one entry can cover several `file:line` across
  several languages; note the extras in `reason`). Suppression lands **only** via the
  human-reviewed PR — never edit the file outside it, and **never suppress a `fixable`
  or `unsure` item**.

### Apply every fix in every language that has the example
This is the rule the repo keeps breaking, so it is mechanical, not a judgement call.
For each fixed skeleton, walk its `sites` and edit **every** one:

- The example exists in `en`, `zh` and `ja` → **all three files change in the same PR.**
  Do not open a language-only PR and do not defer the others to a follow-up.
- The example exists in only some languages → edit the ones that have it, and add the
  absent ones to the **parity backfill list** in the tracking issue. Do not invent a
  translated page.
- **Translate only the SQL, never the prose.** These edits are inside ```sql fences.
  If a fix forces a prose change (renaming an index the surrounding sentence names),
  make the minimal prose edit in each language; if you cannot write that language's
  prose confidently, make the SQL fix in all three, leave that language's prose alone,
  and flag the sentence in the tracking issue for a native reviewer. Never machine-
  translate a paragraph into the docs.
- Preserve each language's own comments and string literals. A `-- 创建表` comment stays
  Chinese; you are changing the statement, not normalizing the page to English.
- Before opening the PR, re-extract and confirm the target skeleton is gone from **all**
  language trees — the same check used for `en`, run per language.

### Vale-gate every file you touched (before opening the PR)
CI lints **every changed `docs/{en,zh,ja}/**/*.{md,mdx}` file in the PR**, whole-file, not
just your diff (`.github/workflows/ci-vale.yml`, `MinAlertLevel = error`) — so an error
anywhere in a file you touched turns the PR red. Reproduce that gate locally, in each
worktree where edits landed:

```bash
cd ../sr-branch-$v                       # or the primary checkout, for main-based PRs
BASE=origin/branch-$v                    # the PR's base ref (origin/main for main PRs)
{ git diff --name-only --diff-filter=ACMR "$BASE"...HEAD -- docs
  git diff --name-only --diff-filter=ACMR -- docs; } \
  | grep -E '^docs/(en|zh|ja)/.*\.mdx?$' | sort -u > /tmp/vale-files-$v.txt
wc -l < /tmp/vale-files-$v.txt            # MUST be > 0 and match your edit count
tr '\n' '\0' < /tmp/vale-files-$v.txt | xargs -0 vale --config docs/.vale.ini --output line
echo "vale exit=$?"                       # 0 = clean, 1 = errors
```
Two traps that make this gate lie, both verified — do not simplify around them:
- **`vale` exits 0 on a path that does not exist**, so an empty or malformed file list
  looks exactly like a pass. Assert the list is non-empty and every entry exists before
  believing `exit=0`. In particular, `vale $FILES` with a newline-joined variable passes
  **one** argument in zsh (no word splitting), lints nothing, and exits 0 — hence
  `tr '\n' '\0' | xargs -0`. (BSD/macOS `xargs` has no `-a`.)
- Use **that worktree's own** `docs/.vale.ini` and `docs/styles/` — the rule set is
  branch-local and a release branch may carry an older one. Never lint a 3.5 file with
  `main`'s config. `--config docs/.vale.ini` from the worktree root does this correctly
  (`StylesPath = styles` resolves relative to the config).

Suppression-list edits (`sql_verify_suppressions.json`) are JSON; Vale does not lint them.

**Why this gate earns its place in *this* skill.** All the rules are `scope: raw`, but
their Tengo scripts skip fenced code blocks — so an ordinary in-fence SQL edit is
invisible to Vale, and the gate is nearly free. The payoff is the **reformat fix**
(Step 1, "fixable by reformatting"): splitting a client transcript into a ```sql block
plus a ```plaintext block rewrites fence structure, and an unbalanced fence un-shields
the transcript text, which then trips `HTMLComment` / `HtmlEntities` / `HtmlCodeTag` /
`BackslashEscape` / `JsxCloserNeedsBlankLine` in a burst. A cluster of raw-scope errors
in a file you reformatted is a **fence-balance bug in your edit**, not a prose problem:
re-read the block and fix the fences rather than the reported text.

**New vs. pre-existing.** The `sql-reference` trees lint clean at error level in all
three languages today, so treat any error as caused by your edit until proven otherwise.
When that is unclear, diff against a baseline built from the base ref — copy the config
in so the path-scoped rules still match:
```bash
B=/tmp/vale-base-$v; rm -rf $B; mkdir -p $B/docs
while IFS= read -r f; do mkdir -p "$B/$(dirname "$f")"; git show "$BASE:$f" > "$B/$f"; done \
  < /tmp/vale-files-$v.txt
cp docs/.vale.ini $B/docs/ && cp -R docs/styles $B/docs/
lint() { (cd "$1" && tr '\n' '\0' < /tmp/vale-files-$v.txt \
          | xargs -0 vale --config docs/.vale.ini --output line) | cut -d: -f1,4- | sort; }
lint "$B" > /tmp/vale-before-$v.txt; lint . > /tmp/vale-after-$v.txt
comm -13 /tmp/vale-before-$v.txt /tmp/vale-after-$v.txt   # alerts YOUR edit introduced
```
Compare on `file + rule + message` (the `cut` drops line/col — your edit shifts lines).
- **New error** → your edit's fault; fix it in the same PR.
- **Pre-existing error** → do not fold an unrelated prose cleanup into a SQL-fix PR, but
  do not stay quiet either: CI will fail this PR anyway. Name it (`file` · rule) on the
  `## Vale` line and add it to the tracking issue. A one-token mechanical fix inside a
  file you already touched (backticks around `ARRAY<INT>`) is fine to make here — call it
  out in the body.
- Never silence a Vale error with a `<!-- vale off -->` region, and never drop a fix
  merely because Vale flagged the file — fix the edit or escalate.

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
- **Fixes** table under *What I'm doing:* — `file:line`, before → after,
  **"verified on: `<v1>, <v2>, …`"** (the exact cluster versions each fix ran on), and
  **"languages: `en, zh, ja`"** (the language files this PR actually changes for that
  fix). If a fix touches fewer than all three, the table must say which and the body
  must say why — "the example does not exist in `ja`" is a fine reason; silence is not.
- a short **`## Language coverage`** line: the per-language file count in the diff
  (e.g. `en 6 · zh 6 · ja 5`) and, if they differ, one sentence explaining the gap.
  A reviewer should be able to spot an accidentally en-only PR at a glance.
- a one-line **`## Vale`** statement of the lint gate's result for this PR's files:
  `vale --config docs/.vale.ini` on N changed files → `clean`, or the surviving alerts as
  `file` · rule · new-or-pre-existing, or `skipped — vale <3.x not available` (never
  omit the line, and never report `clean` for a run whose file list was empty). CI runs
  the same gate, so this tells the reviewer up front whether Vale will be green.
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

### Tracking issue — what needs a human, plus the parity backfill list
The durably-not-runnable classes go to the (scoped) suppression list, so the issue holds
**two sections**. Open/update a GitHub issue labeled `documentation,docs-maintainer`,
titled `SQL doc examples needing review — <versions>` (e.g. `— 4.1/4.0/3.5`).
**If an open issue with that title already exists, update it** (don't duplicate):
regenerate its body from this run. Check off any previously-listed item you have now
fixed or suppressed, with a note.

1. **`unsure` items** — a checkbox list: each item `file:line`, the languages and
   versions it was seen failing on, and its one-line reason.
2. **Cross-language parity** — from `/tmp/parity-<v>.json`. Do **not** paste the raw
   report; it is hundreds of lines. Summarize: the per-language sample counts, the
   number of differing pages, and then the **top pages by gap size** as a checkbox
   backfill list. Call it out explicitly as a *translation* task list that this skill
   does not act on, and note that some gaps are deliberate so it needs its own triage
   pass before anyone works it.

If **both** sections are empty, comment "all triaged" and **close** the issue. A
non-empty parity list alone is enough to keep it open — that is the drift this run
exists to make visible.

Cross-link (PR body → issue when non-empty, issue → PRs) and report all URLs at the end.

## Never
- Never un-draft or merge; never commit without operator review.
- Never check a backport box for a version whose fix you did not verify on that version's
  cluster.
- Never fix an example in one language and leave the other languages' copies of that
  same example broken. If you fixed `en`, the `zh` and `ja` sites in that skeleton's
  `sites` list are part of the same PR.
- Never open a PR without running the Vale gate on the files it changes, and never report
  it as clean on the strength of an `exit=0` you did not sanity-check against a non-empty
  file list — `vale` exits 0 when handed a path that doesn't exist.
- Never silence a Vale error with `<!-- vale off -->`, and never leave a Vale error
  unmentioned in the PR body because it was pre-existing — CI fails the PR either way.
- Never machine-translate documentation prose. Fix the SQL in every language; escalate
  prose that must change to a native reviewer via the tracking issue.
- Never key a suppression on the verbatim `sha256:` hash when the example exists in more
  than one language — it will silence one language and leave the others reporting.
- Never treat a cross-language parity difference as doc rot, and never auto-add a
  missing example to a language to make the parity report quiet.
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
