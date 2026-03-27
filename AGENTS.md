# AGENTS.md - StarRocks

Agent entrypoint for this repository. Start with the nearest nested `AGENTS.md`; use this file for repo-wide rules only.

## Read Order

1. Read the nearest nested `AGENTS.md`.
2. For Backend work, read [`be/AGENTS.md`](./be/AGENTS.md).
3. For Frontend work, read [`fe/AGENTS.md`](./fe/AGENTS.md).
4. For SQL tests, docs, generated code, or Java extensions, read the matching nested guide.

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

## PR Contract

- Commit messages: English, imperative, concise.
- PR titles: `[BugFix] ...`, `[Feature] ...`, `[Enhancement] ...`, `[Refactor] ...`, `[UT] ...`, `[Doc] ...`, or `[Tool] ...`.
- Fill the repository PR template completely, including behavior-change classification and test/docs checkboxes.
- Bug-fix PRs intended for branch backports must set the version checkboxes that drive auto-backporting.

## Nested Guides

- [`be/AGENTS.md`](./be/AGENTS.md)
- [`fe/AGENTS.md`](./fe/AGENTS.md)
- [`docs/AGENTS.md`](./docs/AGENTS.md)
- [`gensrc/AGENTS.md`](./gensrc/AGENTS.md)
- [`java-extensions/AGENTS.md`](./java-extensions/AGENTS.md)
- [`test/AGENTS.md`](./test/AGENTS.md)
