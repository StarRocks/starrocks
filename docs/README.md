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
checks that they still execute on a StarRocks release and, optionally, drafts fixes
for the ones that don't.

**The one rule: test one version at a time, and match it.** The docs you check and
the cluster you run must be the **same StarRocks version** — otherwise examples for
newer features look "broken" when the release simply doesn't have them yet.

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (includes Compose).
- Python 3.
- [`uv`](https://docs.astral.sh/uv/) — only for the optional fix step (step 4).
- A **clone** of this repo (`git clone https://github.com/StarRocks/starrocks.git`).
  A normal clone contains this tooling and already knows every release branch via
  `origin/branch-*` — you don't need a separate copy of the branch you're
  verifying. You run the checker from your clone (on `main`); you do **not** switch
  its branch. To verify a *released* version you create a one-time worktree for that
  branch in Step 2, and point `--docs-root` at it.

### 1. Start a matching StarRocks cluster

Set `SR_VERSION` to the release you're verifying (it must match the docs version
you check in Step 2), then start the shared-nothing (FE + BE) cluster:

```bash
export SR_VERSION=4.1   # e.g. 4.1 when verifying branch-4.1 docs
docker compose -f docs/docker/doc-verification/docker-compose-shared-nothing.yml up -d --wait
```

The FE is reachable on `127.0.0.1:9030` (user `root`, no password). For cluster
details, teardown, or the shared-data (cloud-native) cluster, see
[docs/docker/doc-verification/README.md](docker/doc-verification/README.md).

### 2. Detect broken examples

Point the checker at the docs for the version you're verifying with `--docs-root`
— those docs do not have to live in this checkout.

**Current (`main`) docs** — use this checkout directly:

```bash
python3 docs/scripts/run_sql_samples.py \
    --docs-root docs/en/sql-reference \
    --host 127.0.0.1 --port 9030 --user root \
    --format md > /tmp/sql-report.md
```

**A released version (e.g. 4.1)** — create a worktree of that release branch. This
fetches the branch from `origin` and checks it out in a new directory; it does not
touch your current branch, and you need no prior copy of it. One time:

```bash
git fetch origin branch-4.1
git worktree add ../sr-branch-4.1 -b branch-4.1 origin/branch-4.1
```

Then run the checker against it (repeatable — the worktree persists):

```bash
python3 docs/scripts/run_sql_samples.py \
    --docs-root ../sr-branch-4.1/docs/en/sql-reference \
    --host 127.0.0.1 --port 9030 --user root \
    --format md > /tmp/sql-report.md
```

Refresh the worktree before later runs with `git -C ../sr-branch-4.1 pull`.
Substitute the branch you're verifying (e.g. `branch-3.5`) in all three commands.
(A second full clone works too, but a worktree shares one `.git` and is lighter.)

Open `/tmp/sql-report.md`. Its header confirms the docs and cluster versions are
aligned — if it warns they aren't, fix that before trusting the results. Every
example is sorted into a bucket:

- **PASS** — ran cleanly.
- **FAIL** — errored; this is the work list of candidate doc bugs.
- **UNRESOLVED** — references a table/object defined elsewhere (not self-contained).
- **ENV** — failed because of the small test cluster, not the docs.
- **SKIP** — not meant to run as-is (external systems, syntax notation, transcripts).

Only **FAIL** items are candidate doc bugs. Add `--profile shared-data` if you
started the shared-data cluster instead.

### 3. Review the FAIL list

Work through the **FAIL** entries in the report (each shows `file:line`, the
statement, and the error). Note that "runs on the cluster" is not the same as
"correct documentation": some failures are features newer than this build, or
examples that are illustrative by design — see the classification guidance in
step 4's README before editing.

### 4. (Optional) Generate fix suggestions automatically

The `sql-doc-autofix` Claude Code skill classifies each FAIL item and, only for
genuinely fixable ones, proposes a corrected statement, verifies it against the
running cluster (via the StarRocks MCP server), and opens a **draft** PR for review.
It never merges. Full setup and usage:
[.claude/skills/sql-doc-autofix/README.md](../.claude/skills/sql-doc-autofix/README.md).

### 5. Tear down

```bash
docker compose -f docs/docker/doc-verification/docker-compose-shared-nothing.yml down
```

