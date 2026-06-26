---
name: release-notes
description: Draft StarRocks release notes for a patch version from the PRs merged into its release branch, then open a [Doc] PR that hands off to the /translate workflow for Chinese and Japanese. Use when a new patch release (e.g. 3.5.19) has been tagged and English release notes are not yet written.
argument-hint: "[version] e.g. 3.5.19"
allowed-tools: Read, Edit, Grep, Glob, Bash, Agent
---

# StarRocks Release Notes

Generate the **English** release-notes section for a tagged StarRocks patch release,
open a `[Doc]` PR for it, and hand off Chinese/Japanese translation to the existing
`/translate` workflow. English only — never edit `docs/zh/**` or `docs/ja/**`.

Read `references/format.md` (exact output format) and `references/categorization.md`
(how to map and rewrite PRs) before drafting. The PR is the human review gate, so favor a
complete, accurate draft with uncertain items flagged over silent guessing.

## Inputs

- `$ARGUMENTS` = the new patch version, e.g. `3.5.19`. If absent, ask for it.
- Derive once and reuse:
  - minor = `3.5`, **branch** = `branch-3.5`
  - **file** = `docs/en/release_notes/release-3.5.md`
  - **prev tag** = the previous patch, `3.5.18` (the newest `## X.Y.Z` already in the file;
    read it to confirm rather than assuming).

## Workflow

### 1. Collect the changes
Run the collector and capture its JSON:

```bash
bash .claude/skills/release-notes/scripts/collect-prs.sh <prev_tag> <new_tag>
# e.g. collect-prs.sh 3.5.18 3.5.19
```

It returns
`{ release_date_raw, pr_count, prs: [{number,title,url,base,resolved_to_main,labels,body_excerpt}],
unresolved_count, unresolved: [...] }` for every PR backported between the two tags. If
`pr_count` is 0, stop and report — the tags likely don't exist yet or the order is wrong
(`gh api repos/StarRocks/starrocks/tags`).

Each PR's `number` is already resolved to the **original `main` PR** (release notes always
cite the main PR). `release_date_raw` is only a **suggestion** (the tag/commit date) — see
step 4 for the actual release date.

`unresolved` lists any PR whose backport chain did **not** trace back to `main`
(`resolved_to_main: false`, e.g. a fix authored directly against a release branch). These
need a PR-quality warning — see step 7.

### 2. Mirror the existing format
Read the top of the target `file` — the frontmatter, the `:::warning` block, and the
current newest `## X.Y.Z` section. Match its heading casing, the
`The following issues have been fixed:` line, and the PR-link style. Do not change
frontmatter.

### 3. Categorize and rewrite
Apply `references/categorization.md`:
- Map each PR to `Behavior Changes` / `Improvements` / `Bug fixes`; exclude
  `[Doc]`/`[UT]`/`[Tool]` and behavior-neutral `[Refactor]`.
- Rewrite each title into a user-facing sentence (strip prefixes/scope, backtick
  identifiers), grouping closely related PRs with space-separated links.
- Build a **"Needs reviewer confirmation"** list for uncertain section choices, terse
  titles, or possible upgrade/downgrade-warning items. This list goes in the PR body, not
  the release notes.
- **Release date — always ask the user.** The official release date is set by QA and
  product management and is **not** necessarily the tag/commit date. Prompt the user:
  "What is the official release date for `<new_tag>`?", offering the detected
  `release_date_raw` (formatted `Month D, YYYY`) only as a suggested default. Use the
  user's answer for the `Release date:` line. Do not proceed to write the section until
  the date is confirmed.

### 4. Write the section
Insert the new `## X.Y.Z` section into `file` per `references/format.md`: directly below
the closing `:::` of the `:::warning` block and above the current newest patch section.
Omit empty subsections. English file only.

### 5. Lint (required so the translation comment appears)
The `Translation Status Check` workflow only posts the language checkboxes after
`markdownlint` passes, so the diff must be clean:

```bash
vale --config=.vale.ini docs/en/release_notes/release-3.5.md
```

Also run the repo's markdownlint if configured (check `package.json` / `.markdownlint*`).
Fix any issues. Re-confirm frontmatter/`description` is unchanged.

### 6. Open the PR
```bash
git checkout -b release-notes-<new_tag>           # e.g. release-notes-3.5.19
git add docs/en/release_notes/release-3.5.md
git commit -s -m "[Doc] Add release notes for StarRocks v<new_tag>"
git push -u origin release-notes-<new_tag>
gh pr create --title "[Doc] Add release notes for StarRocks v<new_tag>" --body-file <body>
```

Fill `.github/PULL_REQUEST_TEMPLATE.md` for the body:
- **What type of PR**: check `- [x] Doc`.
- **Change in behavior**: check `- [x] No` (the notes themselves don't change product
  behavior).
- **Checklist**: this PR *is* the user documentation; leave test-case boxes unchecked.
- **Bugfix cherry-pick branch check**: this is a docs PR, not a code backport — leave the
  version boxes (`4.1`/`4.0`/`3.5`) unchecked unless the user says otherwise.
- Append the **"Needs reviewer confirmation"** list under "What I'm doing" so reviewers
  verify the flagged entries.

### 7. Flag PR-quality issues (when `unresolved_count > 0`)
If the collector reported any `unresolved` PRs (backport chain that never reached `main`),
post a **single warning comment** on the release-note PR so reviewers can act on it. Do not
silently drop it — these often indicate a fix that **skipped `main`** and may be missing
from `main`/future releases (a regression risk), not just a citation quirk.

```bash
gh pr comment <pr-number> --repo StarRocks/starrocks --body-file <warning>
```

For each unresolved PR, state in the comment: the number cited in the notes and its `base`
branch (not `main`), that no `main` PR was found, and the two impacts — (1) the citation is
inconsistent with the other entries, and (2) verify the fix exists on `main`; forward-port
if missing. Ask the author to confirm the correct PR number to cite. (Skip this step
entirely when `unresolved_count` is 0.)

### 8. Hand off translation
Stop and tell the user:
- Only `docs/en/release_notes/release-3.5.md` changed.
- Once `CI DOC Checker → markdownlint` passes on the PR, the
  **"🌎 Translation Required?"** comment will auto-post with `zh` and `ja` checkboxes.
- A **docs-maintainer** checks the desired boxes and replies **`/translate`**; the
  `Translation Runner` workflow (`StarRocks/doc-translator` v1.0.1) then commits the
  Chinese and Japanese versions into the same PR.

## Notes and limits

- **Whole-file translation**: `/translate` re-translates the entire file, not just the new
  section. This is the existing established behavior — rely on it; do not change the
  pipeline or pre-edit `zh`/`ja`.
- **Behavior changes** are detected heuristically; the "Needs reviewer confirmation" list
  plus PR review is the safety net (`docs/CLAUDE.md`: never assert unverified technical
  facts).
- **Portability**: the only release-specific values are version, branch, file path, and
  repo. The same skill structure serves other StarRocks lines (e.g. a commercial product)
  by swapping those constants — author original content, do not copy notes across products.
- Commits must use `git commit -s` (DCO sign-off).
