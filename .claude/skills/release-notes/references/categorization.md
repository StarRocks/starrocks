# Categorizing and Rewriting PRs into Release-Note Entries

The collector (`scripts/collect-prs.sh`) returns each PR's `number`, `title`, `labels`,
and a `body_excerpt`. This page defines how the skill turns that raw data into clean,
user-facing release-note entries. **All prose is the model's job** — the script makes no
wording decisions.

## Section mapping (by PR title prefix)

StarRocks PR titles start with a bracketed type prefix (enforced by
`.github/pr-title-checker-config.json`). Map them as follows:

| PR title prefix          | Release-note section | Notes |
|--------------------------|----------------------|-------|
| `[BugFix]`               | **Bug fixes**        | Default home for fixes. |
| `[Enhancement]`          | **Improvements**     | Performance, diagnostics, optimizations. |
| `[Feature]`              | **Improvements**     | New user-visible capability ("Supports …"). |
| `[Refactor]`             | *(usually omit)*     | Include only if user-observable; otherwise drop. |
| `[Doc]`, `[UT]`, `[Tool]`| *(exclude)*          | Internal — never appears in release notes. |

If a PR has no recognizable prefix, fall back to its labels and `body_excerpt` to decide,
and flag it for reviewer confirmation (see below).

## Behavior Changes (cross-cutting)

`Behavior Changes` is **not** a title prefix — it is a cross-cutting category that overrides
the table above. Promote an entry to `### Behavior Changes` when the change alters
**existing behavior that was correct and that users could rely on**, regardless of whether
the PR was `[BugFix]` or `[Enhancement]`: a changed default, a removed or renamed option,
valid input that is now rejected, a new limit, a deliberate change of semantics.

**A correction is not a behavior change**, even though output differs after the upgrade: a
wrong or invalid result that is now right (or now NULL, or now an error), a crash or hang
that is now an error, a late or misleading error that is now a clear one. Those stay in
`### Bug fixes`. When such a fix changes what a query returns, its entry says that the
previous results were wrong and that results change after the upgrade. In semantic
versioning, behavior (breaking) changes belong in major releases; a patch release fixes
bugs.

Signals:

- The **`behavior_changed` label** on the PR. A PR carrying `behavior_changed` MUST appear
  in the release notes and must never be silently dropped during curation. (These are the
  entries most often missed in hand-written drafts.) Put it under `### Behavior Changes`,
  unless all it does is correct wrong behavior as above: then keep it under
  `### Bug fixes` and add it to the "Needs reviewer confirmation" list ("labeled
  `behavior_changed`, kept as a bug fix: it corrects wrong results").
- Title/body mentions: changed **default value**, changed **syntax**/parsing, changed
  **type conversion** or display, a **policy** now enabled/disabled by default, a
  feature **removed**, or upgrade/downgrade **compatibility**.
- The PR template's "change in behavior" section is checked with a non-Miscellaneous type.

These mirror the categories in `.github/PULL_REQUEST_TEMPLATE.md`. Behavior-change
detection is **best-effort**: when unsure whether something is a behavior change vs. a
plain improvement/fix, place it in the most likely section **and add it to the
"Needs reviewer confirmation" list** in the PR body. Never silently guess on a
user-facing behavior claim.

## Rewrite rules (PR title → entry)

1. **Strip the type prefix and any scope.** `[BugFix] Fix NPE in foo(bar)` →
   describe the user-visible effect, not "Fix …(#)". Drop `module:`/`scope:` noise.
2. **Write a complete, user-facing sentence**, present tense, ending in a period before
   the link(s). Prefer the effect on the user over the internal mechanism.
   - `[Enhancement] add mysql_send_packet_timeout_ms` →
     "Added a configurable FE write timeout `mysql_send_packet_timeout_ms` …"
3. **Wrap identifiers in backticks**: config keys, session variables, SQL keywords,
   function names, type names, table/column names.
4. **Group closely related PRs** into a single entry when they address the same
   feature/area, with all PR links space-separated. Do not over-merge unrelated changes.
5. **Do not fabricate technical detail.** Use only what the PR title/body supports
   (`docs/CLAUDE.md`: never assert unverified StarRocks technical facts). If the title is
   too terse to write a meaningful entry, keep it minimal and add it to the
   "Needs reviewer confirmation" list rather than inventing specifics.
6. **Order within a section**: Behavior Changes and Improvements roughly by impact;
   Bug fixes may be grouped thematically (as the existing files do — e.g. "Materialized
   view issues involving …" followed by several links).

## "Needs reviewer confirmation" list

Collect into a markdown list (for the PR body, not the release notes file) any entry where:

- the section choice (esp. Behavior Changes) was uncertain,
- the PR title was too terse to confidently describe, or
- the change might belong in the `:::warning` upgrade/downgrade block.

Each line: the PR number, the chosen section, and the one-line reason. This is the human
review gate — the PR reviewer confirms or corrects before merge.
