# SQL Integration Domain

## Purpose

Map the SQL-tester framework and the end-to-end regression surface used to validate FE/BE behavior against a running StarRocks cluster.

## Entrypoints

- [`test/AGENTS.md`](../../test/AGENTS.md)
- [`test/README.md`](../../test/README.md)
- [`docs/en/developers/mac-compile-run-test.md`](../../docs/en/developers/mac-compile-run-test.md)

## Commands

- `cd test && python3 run.py -v`
- `cd test && python3 run.py -d sql/<suite> -v`
- `cd test && python3 run.py -d sql/<suite>/T/<case> -r`
- `cd test && python3 run.py -l`

## Guardrails

- Use `${uuid0}` for objects that must be unique across runs.
- Clean up created objects in each case.
- Prefer focused tags and filters over whole-tree runs during iteration.

## Test and Validation

- `T/` files define statements; `R/` files define validation expectations.
- Use `[ORDER]`, `[UC]`, shell commands, and helper functions deliberately so expectations stay deterministic.
- Cloud/native mode tags should reflect the actual runtime dependency of a case.

## Open Gaps

- Suite ownership and change-based selection are not encoded mechanically.
- Flake policy and retry evidence are not tracked in repo-local metadata.
- SQL-tester artifacts are not yet normalized into an agent-legible eval registry.
