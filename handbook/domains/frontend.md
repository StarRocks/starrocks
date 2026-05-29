# Frontend Domain

## Purpose

Map the FE surface for SQL parsing, optimization, metadata, coordination, and shared Java-side test utilities.

## Entrypoints

- [`fe/AGENTS.md`](../../fe/AGENTS.md)
- [`fe/README.md`](../../fe/README.md)
- [`fe/fe-testing/README.md`](../../fe/fe-testing/README.md)

## Commands

- `./build.sh --fe`
- `./run-fe-ut.sh`
- `./run-fe-ut.sh --test <fully.qualified.TestClass>`
- `cd fe && mvn checkstyle:check -pl fe-core`

## Guardrails

- AST nodes stay immutable after parsing.
- Connectors and plugins should depend on FE SPI boundaries, not FE internals.
- Shared test utilities belong in `fe/fe-testing/` instead of ad hoc local fixtures.

## Test and Validation

- Use focused `run-fe-ut.sh --test ...` loops for planner, analyzer, optimizer, and catalog changes.
- Reuse `PlanTestBase`, `UtFrameUtils`, and `StarRocksAssert` when new FE tests need common setup.
- Run module-level style checks when Java source changes touch FE or Java extensions.

## Open Gaps

- FE package boundaries are documented but not mechanically enforced.
- There is no repo-native registry that maps FE changes to required eval suites.
- Query-profile, log, and trace evidence are not normalized for agent consumption.
