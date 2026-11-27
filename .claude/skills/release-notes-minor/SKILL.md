---
name: release-notes-minor
description: Draft StarRocks release notes for the FIRST release of a new minor line (X.Y.0, e.g. 4.2.0) by reconciling what changed between the previous minor's release and the new tag, then open a [Doc] PR that hands off to /translate. Use for X.Y.0 only; for later patches (X.Y.Z, Z>0) use the release-notes skill.
argument-hint: "[version] e.g. 4.2.0"
allowed-tools: Read, Edit, Grep, Glob, Bash, Agent
---

# StarRocks Release Notes: new minor (X.Y.0)

A minor's first release carries a whole development cycle, not a handful of backports. The
patch skill (`release-notes`) cannot be used for it: there is no previous patch tag, the
target file does not exist yet, the compare range is thousands of commits, and the notes are
organised by feature area, not by PR type. This skill covers that case.

English only. Never edit `docs/zh/**` or `docs/ja/**`; `/translate` produces those.
Read `.claude/skills/release-notes/references/categorization.md` for Behavior Changes /
Security / rewrite rules; they apply here unchanged.

## Inputs

- `$ARGUMENTS` = `X.Y.0`. If absent, ask. If the patch is not `.0`, stop and point to `release-notes`.
- **minor** = `X.Y`; **branch** = `branch-X.Y`; **file** = `docs/en/release_notes/release-X.Y.md`
  (does not exist yet: create it).
- **prev minor** = the line before (4.2 -> 4.1). **Baseline** = the fork points of both
  branches and the previous minor's branch tip (see Step 1).
- Ask the user, never guess: the official release date, and whether `X.Y.0` is already tagged.
  If the tag does not exist, draft against the tip of `branch-X.Y` and say so in the PR.

## Step 1: collect the candidates (script, local clone)

`prev_tag...new_tag` is wrong for a minor: `branch-X.Y` was cut from `main`, so the compare
mixes histories, passes the 250-commit cap, and includes everything the previous minor's
patches already shipped. Use the collector, which works on a local clone (a treeless bare
clone is enough, and makes no GitHub API calls):

```bash
git clone --bare --filter=tree:0 https://github.com/StarRocks/starrocks.git sr.git   # once
git -C sr.git fetch --filter=tree:0 origin '+refs/heads/branch-4.1:refs/heads/branch-4.1' \
    '+refs/heads/branch-4.2:refs/heads/branch-4.2' '+refs/heads/branch-4.0:refs/heads/branch-4.0' \
    '+refs/heads/branch-3.5:refs/heads/branch-3.5'
REPO_DIR=sr.git OLDER_BRANCHES='branch-4.0 branch-3.5' bash .claude/skills/release-notes-minor/scripts/collect-minor-prs.sh \
    branch-4.1 branch-4.2 branch-4.1 /tmp/notes-4.2
```

It computes: main commits between the two fork points, plus commits only on the new branch
since its fork, minus PRs already on the previous minor's branch tip (not just its last tag: fixes queued for its next patch are not new either) and on the older supported branches.
Each commit is resolved to its original `main` PR (leftmost `backport #N`, else the trailing
`(#N)`). Output: `summary.txt` and tiers `feature.tsv`, `enhancement.tsv`, `bugfix.tsv`,
`security.tsv`, `noprefix.tsv` (`number<TAB>sha<TAB>subject`). `[Refactor]`, `[Tool]`, `[Doc]`,
`[UT]` are only counted. If `X.Y.0` is tagged, pass `NEW_REF=X.Y.0`. Which older branches to list in `OLDER_BRANCHES` is the user's call (the supported lines).

On 4.2 this gave about 2260 new PRs (77 feature, 632 enhancement, 379 bugfix, 6 security, 7
unprefixed). That is the candidate pool, not the number of entries: the 4.1.0 notes have about
110 bullets for about 170 PRs. Do not read PRs one by one.

## Step 2: shortlist in tiers, then verify

1. **Features first.** Read every `feature.tsv` line, group related PRs into candidate
   headline features, and show the user the list ("these look like the 4.2 headline features,
   which are right, which are missing, which are Beta/Preview?"). Maturity is not in the code:
   ask, do not default to GA.
2. **Enhancements.** Pick user-visible ones: new syntax, functions, config items, variables,
   system tables, new connector or format support, large performance wins. Skip internal
   tuning. Group by area.
3. **Bug fixes.** Enhancements and fixes are normally backported, so most of them were already
   removed in Step 1; what is left are mostly fixes to code that only exists in the new minor, which
   users never ran. Expect a short list. Keep only what a user of the previous minor could hit: crashes, wrong results, data loss, security-relevant,
   hangs. Skip the long tail.
4. **Security.** Every `security.tsv` line, plus any bug fix that mentions CVE, SSRF, injection.
5. **Behavior changes: query the label for the whole new set, independently of the shortlist.**
   A changed default or removed option is often an `[Enhancement]` or `[BugFix]` that the title
   shortlist drops. Fetch every merged PR with the label since the fork date and intersect with
   `new.tsv` (all tiers, including the ones excluded as Refactor/Tool/Doc/UT):

   ```bash
   gh pr list --repo StarRocks/starrocks --label behavior_changed --state merged \
       --search "merged:>=<fork date>" --limit 1000 --json number --jq '.[].number' | sort -u > /tmp/bc.txt
   cut -f1 /tmp/notes-4.2/new.tsv | sort -u | comm -12 - /tmp/bc.txt
   ```

   Every PR in the result goes into the notes (Behavior Changes, or Bug fixes with a reviewer
   note if it only corrects wrong results). Then read `body` of those PRs only.
6. **`noprefix.tsv`**: look at each; they are few.

Verify against the code before writing anything that says "new", "default", or "removed":

- **Never match by PR number across lines of history**; the collector's backport marker is
  the only number resolution used.
- **A default value comes from the final file at the tag** (`git show <ref>:path`), not a diff.
- **The code beats the commit subject.**
- **"Is this new?"** Compare existence at the previous minor's branch tip and the new ref: config
  items, session variables, functions, grammar, metrics, `information_schema` columns.
- **Confirm a path exists before concluding a feature is absent.**
- Anything already in `release-<prev>.md` is not new.

## Step 3: group by feature area

Use the area headings of the previous minor's `.0` section (`release-<prev>.md`), adding or
dropping areas as the content demands. Within an area:

```markdown
- **Feature name**

  One or two sentences on what the user can now do and why it matters. [#NNNNN](https://github.com/StarRocks/starrocks/pull/NNNNN)

- Smaller improvement as a single bullet. [#NNNNN](https://github.com/StarRocks/starrocks/pull/NNNNN)
```

- Major features: bold name and a paragraph; minor ones: one-line bullet.
- Mark maturity (`[Beta]`, `[Preview]`) only where the PR or the owner says so.
- Cite the original `main` PR; several PRs for one feature: all links, space-separated.
- Do not invent technical detail; terse title: keep it minimal and flag it.

Order: feature areas, `### Security` (if the previous `.0` had one), `### Bug Fixes` (notable
ones, with `The following issues have been fixed:`), `### Behavior Changes` last, as in 4.1.0.

## Step 4: upgrade and downgrade notes

A new minor almost always needs entries in the file's `:::warning` block (what cannot be
downgraded to, format/metadata changes). Do not write these from guesses. Ask the user /
reviewers, and list candidates (changed defaults, removed options, metadata or storage
format changes) in "Needs reviewer confirmation".

## Step 5: create the file

Create `file` modelled on `release-<prev>.md`:

```markdown
---
displayed_sidebar: docs
description: "StarRocks X.Y release notes: <3 headline features>"
products: [oss]
---

# StarRocks version X.Y

:::warning
**Downgrade Notes** ... (only what Step 4 confirmed)
:::

## X.Y.0

Release Date: Month D, YYYY
...
```

Also check whether the docs sidebar/index lists release-note files and add the new one the
same way the previous minor was added (`grep -r "release-<prev>" docs/`).
Run markdownlint and Vale as in the patch skill.

## Step 6: final verification (treat your own draft as unverified)

Re-check every claim that something is new, a default changed, or an option was removed
against the code at the tag. Drop what you cannot confirm or move it to the reviewer list.

## Step 7: open the PR and hand off

Same as the patch skill: `[Doc] Add release notes for StarRocks vX.Y.0`, `git commit -s`,
body from `.github/PULL_REQUEST_TEMPLATE.md`, type Doc, behavior change No, version boxes
equal to or greater than X.Y ticked (numeric compare). Put "Needs reviewer confirmation"
(including tag-not-yet-created, unknown maturity, upgrade/downgrade candidates) in the body.
Tell the user: once `markdownlint` passes the "Translation Required?" comment appears; a docs
maintainer ticks zh/ja and replies `/translate`.
