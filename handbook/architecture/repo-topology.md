# Repo Topology

## Top-Level Areas

- `be/`: C++ backend for execution, storage, runtime, and services.
- `fe/`: Java frontend for parsing, planning, metadata, and coordination.
- `gensrc/`: generated thrift and protobuf outputs.
- `test/`: SQL integration tests and test harness assets.
- `docs/`: public user and admin documentation.
- `build-support/`: local and CI enforcement tooling.
- `handbook/`: internal contributor and agent-facing engineering knowledge.

## Navigation Rules

- Start at the nearest `AGENTS.md` for task-local workflow rules.
- Use `handbook/` for architecture maps, repo-level operating context, and active execution plans.
- Use public `docs/` only for user-facing behavior, deployment, SQL semantics, or external documentation obligations.

## Current Harnessing Direction

- Phase 1 established a BE module-boundary harness with manifest-driven checks and AGENTS synchronization.
- Phase 2 moves repo-local knowledge into `handbook/` so agents can discover architecture and in-flight work without relying on external context.
