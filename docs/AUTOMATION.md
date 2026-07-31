# Documentation Automation

This file documents the tooling that helps keep StarRocks documentation in sync with the source code. These tools are designed to make the tech writer's workflow proactive — catching gaps when code changes merge rather than waiting for user reports.

---

## Overview

Four sources of truth define documented behavior:

| Source file | What it controls | Doc target |
|---|---|---|
| `fe/fe-core/src/main/java/com/starrocks/common/Config.java` | FE configuration parameters (`@ConfField`) | `docs/en/administration/management/FE_parameters/` |
| `be/src/common/config.h` | BE configuration parameters (`CONF_*` macros) | `docs/en/administration/management/BE_parameters/` |
| `fe/fe-core/src/main/java/com/starrocks/qe/SessionVariable.java` + `GlobalVariable.java` | Session and global variables (`@VarAttr`) | `docs/en/sql-reference/System_variable.md` |
| `gensrc/script/functions.py` | SQL function reference (`function_docs`) | `docs/{en,zh,ja}/sql-reference/sql-functions/` |

The first three are hand-documented Markdown, and the tools below detect when code and
docs have diverged. The fourth is different in kind: those doc sections are **generated**
from the source, so drift is prevented rather than reported.

---

## Tools

### `build-support/extract_and_diff_params.py`

Compares parameters defined in source code against parameters documented in Markdown. Run this to find the full backlog of undocumented parameters or to check a specific area.

**Run from the repo root:**

```bash
# Full gap report across all categories
python3 build-support/extract_and_diff_params.py

# Only params added in the current git branch (same mode used by CI)
python3 build-support/extract_and_diff_params.py --new-params-only

# Machine-readable output (JSON)
python3 build-support/extract_and_diff_params.py --output json

# Markdown formatted for a GitHub PR comment
python3 build-support/extract_and_diff_params.py --new-params-only --output github-comment
```

**Sample text output:**

```
=== FE Configuration  (Config.java → FE_parameters/) ===
MISSING FROM DOCS (304 in code, not documented):
  audit_stmt_before_execute [boolean] default='false' (mutable)
  enable_experimental_feature [boolean] default='false' (mutable)
  ...

STALE IN DOCS (5 in docs, not in code):
  dump_log_dir
  insert_load_default_timeout_second
  ...

Summary: 304 FE config, 373 BE config, 189 session/global var missing from docs.
```

**What it catches:**
- Parameters added to code with no corresponding `###` heading in the doc files
- Parameters removed from code whose doc entries still exist

**What it cannot catch:**
- A parameter present in both code and docs but with the wrong default value or outdated description
- Renamed parameters (a rename looks like one deletion + one addition — requires human review)

**As of the initial run (May 2026):** 304 FE config, 373 BE config, and 189 session/global variable entries exist in code with no documentation. This is the existing backlog to work through. The CI workflows use `--new-params-only` so they do not surface this backlog on every PR.

---

### `docs/scripts/extract_sql_samples.py`

Walks `docs/en/`, finds all ` ```sql ` fenced code blocks, and reports how many are runnable (no placeholder patterns like `<table_name>`) versus illustrative.

**Run from the repo root:**

```bash
# Summary report
python3 docs/scripts/extract_sql_samples.py

# Only show samples that look runnable
python3 docs/scripts/extract_sql_samples.py --filter-runnable

# Only show DDL and SELECT samples
python3 docs/scripts/extract_sql_samples.py --filter-category DDL,SELECT

# Export to CSV for a spreadsheet
python3 docs/scripts/extract_sql_samples.py --format csv --output sql_samples.csv

# Scan a subdirectory only
python3 docs/scripts/extract_sql_samples.py --docs-root docs/en/sql-reference
```

**Sample output:**

```
SQL Sample Extraction Report
============================================================
Total SQL blocks found: 3624

Category      Total  Runnable  Illustrative
--------------------------------------------
SELECT          536       486            50
DDL            1257       885           372
SHOW_ADMIN      435       316           119
SET             127       103            24
DML             235       173            62
OTHER          1034       289           745
```

**As of the initial run (May 2026):** 3,624 SQL blocks in `docs/en/`, of which 2,252 are runnable and 1,372 are illustrative. This is a foundation for future SQL testing — the runnable samples can be executed against a live cluster to verify accuracy.

---

### `gensrc/script/gen_function_docs.py`

Generates the reference sections of SQL function pages from the `function_docs` payload
in `gensrc/script/functions.py`, where the function signatures already live. Unlike the
tools above, this does not report drift — it removes the opportunity for drift by making
the source authoritative.

**Run from `gensrc/script/`:**

```bash
python3 gen_function_docs.py                    # write the generated sections
python3 gen_function_docs.py --check            # exit 1 if any page is out of date
python3 gen_function_docs.py --notes            # advisory: missing translations, frontmatter drift
python3 gen_function_docs.py --function lpad --lang en
```

Generated sections are fenced by per-section MDX markers and everything outside them is
preserved untouched:

~~~markdown
{/* AUTOGENERATED_START:syntax */}
## Syntax

```sql
lpad(str, len [, pad])
```

{/* AUTOGENERATED_END:syntax */}
~~~

Markers are MDX comments (`{/* ... */}`), not HTML comments — Docusaurus parses these
pages as MDX, and `docs/styles/local/HTMLComment.yml` rejects `<!-- ... -->` at error
level.

**Languages.** English pages are generated in full (syntax, parameters, return value,
usage notes, examples). Chinese and Japanese pages get only the language-neutral
sections — syntax and examples — under localized headings; argument and return-value
prose stays with the translation workflow. Example captions already translated on a
localized page are preserved, and examples lacking one are reported by `--notes` rather
than filled in with English.

**Interaction with the SQL sample checker.** Generated examples use ` ```sql ` for the
query and ` ```plaintext ` for the result, with no `MySQL > ` prompt. Since
`extract_sql_samples.py` harvests only ` ```sql ` fences, generating a page brings its
examples under `run_sql_samples.py` for the first time.

Note what that does and does not buy: `run_sql_samples.py` executes each statement and
drains the result set, but never compares the printed table to what the page claims. A
`PASS` means the statement executed. Renamed functions and changed signatures are caught;
a wrong result value is not.

**Scope:** a proof of concept over 12 function names (all 7 `bit-functions`, plus `lpad`,
`locate`, `concat`, `split`, `reverse`). See
`handbook/plans/active/2026-07-29-function-doc-generation.md`.

Unit tests: `python3 -m unittest discover -s gensrc/script -p 'test_gen_function_docs.py'`

---

### `docs/scripts/check_autogenerated_regions.py`

Fails when a generated section does not match the payload — either because a page was
edited by hand inside a region, or because the payload changed and the generator was
never re-run. Also flags unbalanced markers and markers written as HTML comments.

```bash
python3 docs/scripts/check_autogenerated_regions.py
python3 docs/scripts/check_autogenerated_regions.py --base-ref origin/main
```

Adapted from ClickHouse's check of the same name. ClickHouse compares marked regions
between two git revisions because their generator needs a running server; StarRocks'
generator reads a Python table, so this simply re-runs it and asserts nothing changed —
which additionally catches stale generation, and needs no "intentional regeneration"
label.

---

## CI Workflows

### `ci-doc-autogen-guard.yml` — block stale or hand-edited generated docs

**Triggers:** Any PR touching `gensrc/script/functions.py`, the generator, the guard, or
`sql-functions/` in any language.

**What it does:** Runs `check_autogenerated_regions.py` and the generator unit tests, and
fails the build if either fails. Also prints translation gaps as an advisory step that
never fails. Uses `pull_request` rather than `pull_request_target` — it only reads the
tree and sets an exit code, so it needs no write-scoped token.

### `ci-doc-param-drift.yml` — PR comment on config changes

**Triggers:** Any PR that modifies `Config.java`, `SessionVariable.java`, `GlobalVariable.java`, or `config.h`.

**What it does:** Runs `extract_and_diff_params.py --new-params-only --output github-comment` and posts the result as a comment on the PR. If no new undocumented params are detected, it posts a brief "no gaps found" message and clears any previous comment.

**Does not block merge.** The comment is informational — engineers should not be blocked from shipping because docs haven't caught up yet.

**Example comment (when gaps are found):**

> ## New parameters without documentation
>
> This PR introduces **2 parameter(s)** not yet documented.
>
> | Parameter | Type | Default | Mutable | Description | Suggested doc |
> |---|---|---|---|---|---|
> | `enable_skew_join_v2` | boolean | `false` | Yes | — | docs/en/…/FE_parameters/ |

---

### `ci-doc-needs-update.yml` — Issue on behavior-changing merges

**Triggers:** Any PR that merges to `main` with both of these conditions true:
1. The `[x] Yes, this PR will result in a change in behavior.` checkbox is checked.
2. The `[ ] I have added documentation for my new feature or new function` checkbox is **not** checked.

Doc PRs (`[Doc]` prefix in title) are excluded.

**What it does:**
1. Runs `extract_and_diff_params.py --new-params-only --output json` on the merged commit.
2. Opens a GitHub issue with:
   - PR title, number, URL, and author
   - The behavior-change type checkboxes that were checked
   - A table of any newly undocumented parameters (if any were detected)
   - Checklisted action items
3. Labels the issue `documentation` and `docs-maintainer`.

**The `docs-maintainer` label** is picked up by the weekly Sunday triage workflow alongside labeled PRs, so all documentation work appears in one queue.

**Example issue body:**

> ## Documentation needed for merged PR
>
> PR #73891 was merged and declared a behavior change, but documentation was not added.
>
> **PR:** [Feature] Add skew join v2 optimization hint (#73891)
> **Author:** @some-engineer
> **Behavior change types declared:**
> - Policy changes: use new policy to replace old one, functionality automatically enabled
>
> ## New parameters detected with no documentation entry
>
> ### Session / Global Variables
>
> | Parameter | Type | Default | Mutable | Description | Suggested doc |
> |---|---|---|---|---|---|
> | `enable_optimize_skew_join_v2` | boolean | `false` | Yes | — | docs/en/sql-reference/System_variable.md |
>
> ## Action required
> - [ ] Review the PR diff and update the relevant documentation
> - [ ] Close this issue when documentation is published

**Limitation:** This workflow fires only when the PR template is filled out following the convention. If a contributor replaces the template body entirely, the condition won't match and no issue will be opened.

---

## Working Through the Backlog

To generate the full list of all undocumented parameters (not just new ones):

```bash
python3 build-support/extract_and_diff_params.py --output json > backlog.json
```

The JSON structure is:

```json
{
  "fe_config": {
    "missing_from_docs": [
      { "name": "audit_stmt_before_execute", "code_type": "boolean",
        "default": "false", "mutable": true, "description": "",
        "suggested_doc": "docs/en/administration/management/FE_parameters/" }
    ],
    "stale_in_docs": ["dump_log_dir", "insert_load_default_timeout_second"]
  },
  "be_config": { ... },
  "session_var": { ... }
}
```

Each `missing_from_docs` entry has the parameter name, type, default value, mutability, any description extracted from the source annotation, and a suggested doc file to update.

**Stale entries** (`stale_in_docs`) are parameter names that appear in the documentation but no longer exist in the source code. These should be removed or marked as deprecated in the docs.

## Aliases and the missing-from-docs list

Session variables frequently have aliases — shorter or legacy names that resolve to the same underlying variable (e.g. `tx_read_only` is an alias for `transaction_read_only`). The tool flags aliases as missing from docs **intentionally**: aliases need their own entries in `System_variable.md` so that users searching for the name they saw in a blog post, release note, or support article can find it.

A minimal alias entry is sufficient:

```markdown
### tx_read_only

Alias for [transaction_read_only](#transaction_read_only).
```

Do not suppress alias entries or redirect them silently — the alias name is what external content uses, and an undocumented alias generates support tickets.
