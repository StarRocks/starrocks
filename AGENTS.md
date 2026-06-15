# AGENTS.md - StarRocks

Agent entrypoint for this repository. Start with the nearest nested `AGENTS.md`; use this file for repo-wide rules only.

## Read Order

1. Read [`handbook/index.md`](./handbook/index.md) for repo topology, architecture entrypoints, and active engineering plans. If `handbook/plans/local/index.md` exists, treat it as a checkout-local extension to the tracked plan set after reading [`handbook/plans/index.md`](./handbook/plans/index.md).
2. Read the nearest nested `AGENTS.md`.
3. For Backend work, read [`handbook/domains/backend.md`](./handbook/domains/backend.md) and [`be/AGENTS.md`](./be/AGENTS.md).
4. For Frontend work, read [`handbook/domains/frontend.md`](./handbook/domains/frontend.md) and [`fe/AGENTS.md`](./fe/AGENTS.md).
5. For SQL tests, docs, generated code, Java extensions, or CI/tooling, read the matching `handbook/domains/` page before the nested guide.

## Repo Map

- `be/`: C++ backend for execution, storage, services, and data processing.
- `fe/`: Java frontend for parsing, planning, metadata, and coordination.
- `gensrc/`: generated thrift/protobuf code.
- `test/`: SQL integration tests.
- `docs/`: user and admin documentation.
- `java-extensions/`: JNI and external source integrations.

## Quick Commands

```bash
# Build backend or frontend
./build.sh --be
./build.sh --fe

# Run backend or frontend unit tests
./run-be-ut.sh
./run-fe-ut.sh

# Run SQL integration tests
cd test && python3 run.py -v
```

## Repo-Wide Invariants

- Do not hand-edit generated outputs in `gensrc/` unless the generator workflow explicitly requires it.
- Protobuf fields must stay optional/repeated; never add `required` and never reuse ordinals.
- Thrift fields must stay optional/repeated; never add `required` and never reuse ordinals.
- User-facing config or metric changes must update the matching docs in `docs/en/` and `docs/zh/` when applicable.
- When editing any file under `docs/`, read `docs/CLAUDE.md` for documentation-specific rules before making changes.

## PR Contract

- Commit messages: English, imperative, concise.
- PR titles: `[BugFix] ...`, `[Feature] ...`, `[Enhancement] ...`, `[Refactor] ...`, `[UT] ...`, `[Doc] ...`, or `[Tool] ...`.
- Fill the repository PR template completely, including behavior-change classification and test/docs checkboxes.
- Bug-fix PRs intended for branch backports must set the version checkboxes that drive auto-backporting.
- When editing any file under `docs/`, read `docs/CLAUDE.md` for documentation-specific rules before making changes.

## Codex PR Review Context

When performing a PR review (including `@codex review`), first inspect the PR
comments for one beginning with `<!-- module-risk-briefing -->`.

If a current briefing exists, treat it as review focus, not a finding list:
- check the listed modules/files first;
- validate every risk against the actual diff before reporting;
- do not report a finding solely because the briefing cites a historical risk;
- if the embedded `<!-- module-risk-head-sha: X -->` differs from the PR's
  current head commit, the briefing predates the latest push: use it only as
  historical reference, NOT as the primary review focus for the current diff,
  and flag this in the review summary;
- if the briefing is stale or absent, say so briefly — never silently drop the
  historical risks, and never report one as a current finding without checking
  the diff.

## Nested Guides

- [`handbook/index.md`](./handbook/index.md)
- [`handbook/domains/index.md`](./handbook/domains/index.md)
- [`handbook/policies/index.md`](./handbook/policies/index.md)
- [`handbook/quality/index.md`](./handbook/quality/index.md)
- [`be/AGENTS.md`](./be/AGENTS.md)
- [`fe/AGENTS.md`](./fe/AGENTS.md)
- [`docs/AGENTS.md`](./docs/AGENTS.md)
- [`gensrc/AGENTS.md`](./gensrc/AGENTS.md)
- [`java-extensions/AGENTS.md`](./java-extensions/AGENTS.md)
- [`test/AGENTS.md`](./test/AGENTS.md)
