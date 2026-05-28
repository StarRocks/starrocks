# Handbook Architecture

Use this section when you need the repo map or the current internal harness boundaries before touching code.

## Pages

- [Repo Topology](repo-topology.md): top-level map, subsystem entrypoints, and where to look first.
- [BE Boundary Harness](be-boundary-harness.md): phase-1 module-boundary guardrails and the files that enforce them.
- [Schema Compatibility Harness](schema-compatibility-harness.md): diff-aware guardrails for thrift and protobuf evolution in `gensrc/`.
- [PK Tablet Auto-Repair](pk-tablet-auto-repair.md): design and workflow for auto-recovering primary-key tablets whose replica hit init-time corruption.
