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
- `cd test && python3 run.py -a sequential -c 1 -C cloud -v`
- `cd test && python3 run.py -a sequential -c 1 -C native -v`

## Guardrails

- Use `${uuid0}` for objects that must be unique across runs.
- Clean up created objects in each case.
- Prefer focused tags and filters over whole-tree runs during iteration.

## Test and Validation

- `T/` files define statements; `R/` files define validation expectations.
- Use `[ORDER]`, `[UC]`, shell commands, and helper functions deliberately so expectations stay deterministic.
- Cloud/native mode tags should reflect the actual runtime dependency of a case.

## Sequential CI Coverage

The [PR pipeline](../../.github/workflows/ci-pipeline.yml) and [reusable inspection workflow](../../.github/workflows/inspection-reusable-pipeline.yml) invoke [ci-tool/bin/run-sql-tester.sh](https://github.com/StarRocks/ci-tool/blob/c466ac932e549bb6f8d77412502f6fc0bfac1d3d/bin/run-sql-tester.sh#L474). Pass selection lives in this external runner, rather than the workflow YAML alone.

- `run_sequential_cases()` selects the positive `sequential` attribute and uses `-c 1`. Normal validation with no positive attribute skips `@sequential` in [choose_cases.py](../../test/lib/choose_cases.py).
- The serial pass is enabled when `GITHUB_REPOSITORY=StarRocks/starrocks`, `IS_INSPECTION=true`, or `RUN_SEQUENTIAL_CASES=true`. ASAN builds skip it. Other repositories' PRs require the opt-in.
- [INSPECTION PIPELINE](../../.github/workflows/inspection-pipeline.yml) sets `IS_INSPECTION=true`; its Release cloud/native SQL-Tester jobs run the serial pass. `-C cloud` excludes `@native`, and `-C native` excludes `@cloud`. Ordinary serial passes exclude `@slow`; eligible inspection slow passes handle slow sequential cases separately. Other filters and disabled markers still apply.

The runner calls serial passes before the normal parallel pass, then eligible slow passes. These calls block and do not overlap. Cases that mutate global FE configuration must restore it even after failure; preserve this isolation when changing pass order.

Reports are saved as `nosetests-sequential.xml`, optionally `nosetests-sequential-system.xml`, and `nosetests-normal.xml`. PR jobs upload XMLs to OSS; inspection also publishes all `test/*.xml` through `Publish SQL-Tester Report`.

## Open Gaps

- Suite ownership and change-based selection are not encoded mechanically.
- Flake policy and retry evidence are not tracked in repo-local metadata.
- SQL-tester artifacts are not yet normalized into an agent-legible eval registry.
