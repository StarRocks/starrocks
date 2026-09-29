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

## Observability Touchpoints

Apply the [Observability Awareness policy](../policies/observability-awareness.md) to the existing FE mechanisms used by the affected path:

- **Query and Load Profiles**: Follow BE profile reports through FE aggregation, finalization, storage, and exposure. Ensure a new path preserves the existing profile lifecycle and does not drop reports, aggregate them incorrectly, or finish the profile prematurely.
- **Metrics**: Inspect the owning component's existing metric registration, update, and cleanup path. Keep those metrics accurate when adding branches, retries, fallbacks, or terminal outcomes; extend the nearby metric set when an important new state or outcome would otherwise be missing, following [Adding/Modifying FE Metrics](../../fe/AGENTS.md#addingmodifying-fe-metrics).

## Test and Validation

- Use focused `run-fe-ut.sh --test ...` loops for planner, analyzer, optimizer, and catalog changes.
- Reuse `PlanTestBase`, `UtFrameUtils`, and `StarRocksAssert` when new FE tests need common setup.
- Run module-level style checks when Java source changes touch FE or Java extensions.

## Open Gaps

- FE package boundaries are documented but not mechanically enforced.
- There is no repo-native registry that maps FE changes to required eval suites.
- Query-profile, log, and trace evidence are not normalized for agent consumption.
