# StarRocks Release Notes — Format Specification

This is the exact format the `release-notes` skill must follow. It is derived from the
existing `docs/en/release_notes/release-*.md` files. When in doubt, open the target file
and copy the style of the most recent `## X.Y.Z` section rather than relying on this page.

## File layout

- **One file per minor version**: `docs/en/release_notes/release-<MAJOR>.<MINOR>.md`
  (e.g. release `3.5.19` → `docs/en/release_notes/release-3.5.md`).
- The file already exists for active minor versions. A **patch release is a new
  `## X.Y.Z` section added to that file** — never a new file.
- English only. Do **not** create or edit `docs/zh/...` or `docs/ja/...`; the `/translate`
  workflow produces those.

## Frontmatter — do not modify

The file opens with frontmatter that already exists:

```markdown
---
displayed_sidebar: docs
description: "..."
---
```

Leave it exactly as-is. Adding a patch section does **not** change the `description`
(`docs/CLAUDE.md`: do not change existing frontmatter unless explicitly instructed).

## Document structure

```
---  (frontmatter)
# StarRocks version X.Y          <- single H1

:::warning
**Upgrade Notes** / **Downgrade Notes** ...
:::

## X.Y.<newest>                  <- newest patch first
## X.Y.<older>
...
```

## Where to insert a new patch section

Insert the new `## X.Y.Z` section **immediately below the closing `:::` of the
upgrade/downgrade `:::warning` block** and **above the current newest patch section**.
Patch sections are ordered newest-first. Do not touch the `:::warning` block unless the
release introduces a new upgrade/downgrade constraint that belongs there (rare; confirm
with the reviewer rather than inventing one).

## Section template

```markdown
## X.Y.Z

Release date: Month D, YYYY

### Behavior Changes

- <user-facing description of the change>. [#NNNNN](https://github.com/StarRocks/starrocks/pull/NNNNN)

### Improvements

- <user-facing description>. [#NNNNN](https://github.com/StarRocks/starrocks/pull/NNNNN)

### Bug fixes

The following issues have been fixed:

- <user-facing description>. [#NNNNN](https://github.com/StarRocks/starrocks/pull/NNNNN) [#MMMMM](https://github.com/StarRocks/starrocks/pull/MMMMM)
```

Rules:

- Use the heading text exactly: `### Behavior Changes`, `### Improvements`, `### Bug fixes`
  (note the lowercase "fixes" — match the existing file; some older sections use
  `### Bug Fixes`, but follow the most recent section's casing).
- **Omit any subsection that has no entries.** Do not emit an empty heading.
- The `### Bug fixes` section is conventionally preceded by the line
  `The following issues have been fixed:` — keep it if the existing file uses it.
- Each entry is a single `-` bullet, a complete sentence ending in a period **before** the
  PR link(s).
- **PR links**: `[#NNNNN](https://github.com/StarRocks/starrocks/pull/NNNNN)`. When one
  entry covers several related PRs, list every link **space-separated** after the sentence.
- **Release date**: the official date is provided by the user (set by QA/product
  management), not the detected tag/commit date. Format it as `Month D, YYYY`
  (e.g. `June 26, 2026`) on a `Release date:` line. The collector's `release_date_raw` is
  only a suggested default to offer the user.
- Inline code: wrap identifiers, config keys, SQL keywords, and function names in
  backticks (e.g. `mysql_send_packet_timeout_ms`, `SHOW`, `get_json_string`).
- Code fences must declare a language (e.g. ```` ```SQL ````), per docs markdown rules.

## Worked example (modeled on the real `## 3.5.18` section)

```markdown
## 3.5.19

Release date: June 26, 2026

### Behavior Changes

- `SHOW` statements are now allowed inside explicit transactions. [#72954](https://github.com/StarRocks/starrocks/pull/72954)

### Improvements

- Added a configurable FE write timeout `mysql_send_packet_timeout_ms` for the MySQL result send path to prevent indefinitely blocked result sending to slow clients. [#73646](https://github.com/StarRocks/starrocks/pull/73646)
- Reduced metadata and lock overhead in load balancing and compaction scheduling paths. [#73555](https://github.com/StarRocks/starrocks/pull/73555) [#72218](https://github.com/StarRocks/starrocks/pull/72218)

### Bug fixes

The following issues have been fixed:

- NPE in Iceberg `getPartitionLastUpdatedTime` when the snapshot is expired. [#68925](https://github.com/StarRocks/starrocks/pull/68925)
```
