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
checks that they still run on a StarRocks release and drafts fixes for the ones
that don't.

**The one rule: test one version at a time, and match it.** The docs you check and
the cluster you run must be the **same StarRocks version** — otherwise examples for
newer features look "broken" when the release simply doesn't have them yet.

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (includes Compose)
- Python 3 and [`uv`](https://docs.astral.sh/uv/)
- A clone of this repo (`git clone https://github.com/StarRocks/starrocks.git`) — a
  normal clone has this tooling and knows every release branch via `origin/*`, so
  you never switch its branch.
- [Claude Code](https://docs.claude.com/en/docs/claude-code) with the `starrocks`
  MCP server (from the repo's `.mcp.json`) — used in step 5.

### 0. Overview

You verify one release end to end — say **4.1**. `SR_VERSION` is the only thing you
set; every step below uses it:

1. **Set `SR_VERSION`** to the release you're verifying.
2. **Create a worktree** of that release branch — its docs are what you check.
3. **Start a StarRocks cluster** on that version — the examples run there.
4. **Check the cluster is healthy.**
5. **Run the `sql-doc-autofix` skill** — it checks the docs' SQL on the cluster.
6. **Open the draft PR** of suggested fixes (and pick the branches to backport to).
7. **Tear down.**

### 1. Set the version

> Important:
>
> All commands are run from the root of the StarRocks/starrocks repo:
>
> `cd <wherever you have the repo checked out>`

```bash
export SR_VERSION=4.1   # the release you're verifying; every step below uses this
```

### 2. Create a worktree of that release branch

A one-time command per version. It fetches the branch and checks it out in a new
directory, without touching your current branch:

```bash
git fetch origin branch-$SR_VERSION
git worktree add ../sr-branch-$SR_VERSION -b branch-$SR_VERSION origin/branch-$SR_VERSION
```

Before a later run, refresh it with `git -C ../sr-branch-$SR_VERSION pull`.

### 3. Start a matching StarRocks cluster

```bash
docker compose -f docs/docker/doc-verification/docker-compose-shared-nothing.yml up -d --wait
```

The FE is on `127.0.0.1:9030` (user `root`, no password). For the shared-data
(cloud-native) cluster or teardown details, see
[docs/docker/doc-verification/README.md](docker/doc-verification/README.md).

### 4. Check the status of the cluster

```bash
docker compose \
-f docs/docker/doc-verification/docker-compose-shared-nothing.yml \
ps -a --format "table {{.Service}}\t{{.Status}}"
```

Look for both services to be healthy:

```bash
SERVICE        STATUS
starrocks-be   Up 3 minutes (healthy)
starrocks-fe   Up 3 minutes (healthy)
```

### 5. Check the examples and draft fixes

In Claude Code (with the `starrocks` MCP server attached), run the
**`sql-doc-autofix`** skill and have it verify `$SR_VERSION`, whose docs are at
`../sr-branch-$SR_VERSION/docs/en/sql-reference`.

The skill:

- runs the checker against those docs on your cluster and sorts every example into
  **PASS**, **FAIL** (candidate doc bug), **UNRESOLVED** (not self-contained),
  **ENV** (test-cluster limitation), or **SKIP** (not runnable by design);
- for the genuinely fixable **FAIL** items, proposes a corrected statement, verifies
  it on the cluster, and opens a **draft** PR — it never merges;
- flags version-gated or illustrative examples instead of "fixing" them, because
  *runs on the cluster* is not the same as *correct documentation*.

```bash
claude
```

If you see that a new MCP server is found for StarRocks enable it:

```bash
New MCP server found in this project: starrocks
```

After the StarRocks MCP server is allowed for use you can use the `sql-doc-autofix` skill:

```bash
/sql-doc-autofix
```

### 6. Open a PR

Claude will ask how you want to proceed. If you choose to open a `draft PR off main` a PR will be opened.

```bash
How should I deliver the 2 verified fixes?

❯ 1. Open draft PR off main
     Create a worktree off origin/main, apply both edits, open a draft [Doc] PR with Fixes + Not-fixed sections. You review and un-draft.
  2. Apply edits locally only
     Make the two edits in a main-based worktree/branch but do not push or open a PR. You handle the PR.
  3. Just the report
     Stop here with the triage above; make no code changes.
  4. Type something.
```

The skill produces:

- A **draft `[Doc]` PR** with the verified fixes. Its body is filled from the repo's
  PR template — including the type, behavior-change, and version **backport** boxes —
  so it can actually be merged. The same PR also adds any **suppressions** (a
  `## Suppressions` section) for examples that legitimately can't run here
  (version-gated, illustrative, needs external setup). Once merged, those stop being
  reported on every run — so the noise shrinks over time. Review the edits and the
  suppressions, confirm the backport version(s), then un-draft.
- A **tracking issue** holding only the examples that genuinely need a human's
  judgment, so those aren't lost when the run scrolls by. It's updated in place each
  run (and closed when empty). Work through that issue separately.

Full behavior and the report format:
[.claude/skills/sql-doc-autofix/README.md](../.claude/skills/sql-doc-autofix/README.md).

### 7. Tear down

```bash
docker compose -f docs/docker/doc-verification/docker-compose-shared-nothing.yml down
git worktree remove ../sr-branch-$SR_VERSION   # optional
```

