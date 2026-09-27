# StarRocks documentation

## How to contribute documentation

Thank you very much for contributing to StarRocks documentation! Your help is important to help improve the docs!

Before contributing, please read this article carefully to quickly understand the tips, writing process, and documentation templates.

### Tips

1. Language: Please use at least one language, Chinese, English, or Japanese. Adding/Updating all three language files is highly preferred.
2. Index: When you add a topic, you also need to add an entry for the topic in the `sidebars.json` file of the `docs/docusaurus` folder. **The path to your topic must be a relative path from the `docs` folder.** This `sidebars.json` file will eventually be rendered as the side navigation bar for documentation on our official website. If you are not sure how to edit this file, you can leave this work to the documentation team.
3. Images: Images must first be put into the **assets** folder. When inserting images into the documentation, please use the relative path, such as `![test image](../../assets/test.png)`.
4. Links: For internal links (links to documentation on our official website), please use the relative path of the document, such as `[test md](./data_source/catalog/hive_catalog.md)`. For external links,  the format must be `[link text](link URL)`.
5. Code blocks: You must add a language identifier for code blocks, for example, `sql`.
6. Currently, special symbols are not supported.

### Writing Process

1. **Writing phase**: Write the topic (in Markdown) according to templates, and add the topic's index to the `sidebars.json` file if the topic is newly added.

    > - *Because the documentation is written in Markdown, we recommend that you use markdown-lint to check whether the documentation conforms to the Markdown syntax.*
    > - *When adding the topic index, please pay attention to* *its category* *in the `sidebars.json` file. For example, the ***Stream Load*** topic belongs to the **Loading** chapter.*

2. **Submission phase**: Create a pull request to submit the documentation changes to our documentation repository on GitHub, English documentation is in the `docs/en` folder of the [StarRocks repository](https://github.com/StarRocks/starrocks), Chinese documentation is in the `docs/zh` folder, and Japanese is in `docs/ja` folder.

   > **Note**
   >
   > All commits in your PR should be signed. To sign a commit you can add the `-s` argument. For example:
   >
   > `commit -s -m "Update the MV doc"`

3. Lists of settings

   Long lists of settings like this do not index well in search, and the reader will not find the information even when they type in the exact name of a setting:

   ```markdown
   - `setting_name_foo`

     Details for foo

   - `setting_name_bar`
     Details for bar
   ...
   ```

   Instead, use a section heading (e.g., `###`) for the setting name and remove the indent for the text:

   ```markdown
   ### `setting_name_foo`

   Details for foo

   ### `setting_name_bar`
   Details for bar
   ...
   ```

   |Search results with a long list:|Search results with H3 headings|
   |--------------------------------|-------------------------------|
   |![image](https://github.com/StarRocks/starrocks/assets/25182304/681580e6-820a-4a5a-8d68-65852687a0df)|![image](https://github.com/StarRocks/starrocks/assets/25182304/8623e005-d6e1-4b73-9270-8bc86a2aa680)|

4. **Review phase**

    The review phase includes automatic checks and manual review.

    - Automatic checks: whether the submitter has signed the Contributor License Agreement (CLA) and whether the documentation conforms to the Markdown syntax.
    - Manual review: Committers will read and communicate with you about the documentation. It will be merged into the StarRocks documentation repository and updated on the official website.

### Documentation template

- [SQL function template](../docs/en/sql-reference/How_to_Write_Functions_Documentation.md)
- [SQL command template](../docs/en/sql-reference//SQL_command_template.md)
- [FE/BE config and variable template](../docs/en/sql-reference/template_for_config.md)
- [Loading data template](../docs/en/loading/Loading_data_template.md)

## Verifying SQL samples

The `sql-reference` docs contain thousands of runnable SQL examples. This procedure
checks that they still run on **every supported StarRocks release** and drafts fixes
for the ones that don't.

**The one rule still holds — each version's docs are tested against a cluster of that
same version** — but the `sql-doc-autofix` skill now does this for the whole supported
set in one run, **one cluster at a time** on `127.0.0.1:9030`. This is what makes
backporting safe: a fix reaches a release branch only if it was verified against that
version's own cluster, and each PR checks only the backport boxes it verified.

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (includes Compose)
- Python 3 and [`uv`](https://docs.astral.sh/uv/)
- [`gh`](https://cli.github.com/), authenticated — used to discover the supported
  versions and to open PRs/issues.
- A clone of this repo (`git clone https://github.com/StarRocks/starrocks.git`) — a
  normal clone has this tooling and knows every release branch via `origin/*`, so
  you never switch its branch.
- [Claude Code](https://docs.claude.com/en/docs/claude-code) with the `starrocks`
  MCP server (from the repo's `.mcp.json`) — used in step 4.

### 0. Overview

The skill runs the full supported set end to end. You mainly confirm the version set
and review the results; the skill drives the clusters:

1. **Confirm the version set** — auto-discovered from GitHub (e.g. `4.1 4.0 3.5`).
2. **Create/refresh a worktree** per version — their docs are what's checked.
3. **Run the `sql-doc-autofix` skill** — for each version it starts a matching
   cluster on `9030`, checks that version's docs, verifies fixes, and stops the cluster
   before the next version.
4. **Review the draft PR(s)** of suggested fixes (backport boxes already match the
   verified versions).
5. **Tear down** anything left over.

> All commands are run from the root of the StarRocks/starrocks repo:
> `cd <wherever you have the repo checked out>`

### 1. Confirm the version set

The skill discovers the supported versions for you and asks you to confirm or edit
them. To preview or pin the set yourself:

```bash
docs/scripts/supported_versions.sh            # e.g. -> "4.1 4.0 3.5"
docs/scripts/supported_versions.sh --max 4.1  # pin the newest, if not the latest
```

It reads GitHub Releases, keeps the newest distinct `major.minor` lines (top 3 by
default), and stays correct as new releases ship — no edits needed when `4.2` lands.
To skip discovery, export an explicit list: `export SR_VERSIONS="4.1 4.0 3.5"`.

### 2. Create a worktree per release branch

One command per version (skip any you already have). Each fetches the branch and
checks it out in its own directory on a branch named exactly `branch-<version>`,
without touching your current branch:

```bash
for v in 4.1 4.0 3.5; do
  git fetch origin branch-$v
  git worktree add ../sr-branch-$v -b branch-$v origin/branch-$v
done
```

Before a later run, refresh each with `git -C ../sr-branch-$v pull`.

### 3. Check the examples and draft fixes

In Claude Code (with the `starrocks` MCP server attached), run the
**`sql-doc-autofix`** skill. For each version in the set it:

- brings up a matching cluster on `127.0.0.1:9030` (user `root`, no password),
- runs the checker against **all three languages** —
  `../sr-branch-<v>/docs/{en,zh,ja}/sql-reference` — against that one cluster, sorting
  every example into **PASS**, **FAIL** (candidate doc bug), **UNRESOLVED** (not
  self-contained), **ENV** (test-cluster limitation), or **SKIP** (not runnable by
  design),
- for the genuinely fixable **FAIL** items, proposes a corrected statement, verifies
  it on that cluster, and applies it to **every language that carries the example** —
  so a fix never lands in `en` alone,
- flags version-gated or illustrative examples instead of "fixing" them, because
  *runs on the cluster* is not the same as *correct documentation*,
- emits a **cross-language parity report** (`docs/scripts/sql_sample_parity.py`) —
  a separate signal that catches examples present in one language and missing from
  the others, which a rot run can never see,
- then stops that cluster and moves to the next version.

That is 3 versions × 3 languages = 9 checker passes, so a full run takes a while. The
triage load does not triple: the en/zh/ja copies of one example share a skeleton hash,
so they are classified and cluster-verified once and edited three times.

```bash
claude
```

If a new MCP server is found for StarRocks, enable it:

```bash
New MCP server found in this project: starrocks
```

Then run the skill (optionally passing a version list):

```bash
/sql-doc-autofix
```

> For a shared-data (cloud-native) cluster, or manual start/stop and health-check
> details, see
> [docs/docker/doc-verification/README.md](docker/doc-verification/README.md).

### 4. Review the PR(s)

Claude will ask how you want to proceed before opening anything. The skill produces:

- **One or more draft `[Doc]` PRs**, grouped so every fix in a PR was verified on every
  version whose **backport** box is checked. A fix that passes on all supported versions
  becomes one PR with all boxes; a fix that fails on an older version becomes a separate
  PR with a narrower box set. Each body is filled from the repo's PR template
  (type, behavior-change, verified backport versions) and adds any **suppressions**
  (a `## Suppressions` section, each with its version scope) for examples that
  legitimately can't run — version-gated, illustrative, or needing external setup.
  Once merged, those stop being reported every run, so the noise shrinks over time.
  Review the edits and suppressions, then un-draft.
- A **tracking issue** holding only the examples that genuinely need a human's judgment,
  so those aren't lost when the run scrolls by. It's updated in place each run (and
  closed when empty). Work through that issue separately.

Full behavior and the report format:
[.claude/skills/sql-doc-autofix/README.md](../.claude/skills/sql-doc-autofix/README.md).

### 5. Tear down

The skill stops each cluster as it finishes, but to be sure nothing is left running,
and to remove the worktrees:

```bash
docker compose -f docs/docker/doc-verification/docker-compose-shared-nothing.yml down
for v in 4.1 4.0 3.5; do
  git worktree remove ../sr-branch-$v   # optional
done
```

