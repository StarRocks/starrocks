---
name: project-translation-ci
description: Translation CI workflow bugs, fixes, and tool choices for the doc translation pipeline
metadata:
  type: project
---

## Translation CI uses `git rev-parse HEAD^1` for base SHA (not `pr.base.sha`)

**Why:** GitHub's `pr.base.sha` API field is stale when the PR branch hasn't recently merged from main. It returns the original branch point (e.g., `c63b84be`) rather than the current main tip, causing `tj-actions/changed-files` to detect all main commits since the branch point as "PR changes." This triggers false translation requests for docs not in the PR.

**How to apply:** Never use `pr.base.sha` for `changed-files` base in workflow `ci-doc-translation-check.yml`. The fix uses `git rev-parse HEAD^1` after checking out `refs/pull/{pr}/merge` — the first parent of the synthetic merge commit is always the exact main SHA GitHub used to build it, so it's always in sync.

## Translation runner uses StarRocks/doc-translator at v1.0.1

**Why:** Replaced `StarRocks/markdown-translator@v1.0.6` (Gemini-based) with `StarRocks/doc-translator@v1.0.1` (Anthropic-based). Uses secret `DOCS_ANTHROPIC_API_KEY` exposed as env var `ANTHROPIC_API_KEY`. CLI interface is compatible — same `--input/--language/--source/--output` flags.

**How to apply:** When updating the translation runner workflow (`ci-doc-translater.yml`), keep the tool at `StarRocks/doc-translator` and use `DOCS_ANTHROPIC_API_KEY`, not Gemini credentials.
