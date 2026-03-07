# Phase 4: Integration Testing - Context

**Gathered:** 2026-03-07
**Status:** Ready for planning

<domain>
## Phase Boundary

Add MockedADBCMetadata for FE plan tests via ConnectorPlanTestBase, write SQL integration test T/R files for test_adbc_catalog, and add sr.conf variables for Flight SQL endpoints. E2E tests run against a manually provisioned DuckDB Flight SQL server; CI integration is a follow-up task.

</domain>

<decisions>
## Implementation Decisions

### SQL Integration Test Scenarios
- **DDL tests:** Full lifecycle — CREATE CATALOG, SHOW DATABASES, SHOW TABLES, DESCRIBE, ALTER CATALOG, DROP CATALOG
- **Query tests:** Core pushdowns plus joins and subqueries — SELECT *, column pruning, WHERE predicate pushdown, LIMIT pushdown, combined pushdown, cross-catalog joins (ADBC + native table), subqueries against ADBC tables. EXPLAIN should show pushed SQL
- **MV tests:** Basic MV cycle — CREATE MV over ADBC table, REFRESH MV, SELECT from MV, verify query rewrite via EXPLAIN, DROP MV
- **File organization:** Multiple test files — test_adbc_ddl, test_adbc_query, test_adbc_mv (separate files per feature area)

### E2E Test Environment
- **Flight SQL server:** DuckDB Flight SQL (`duckdb --flight-sql`) for local verification
- **Skip mode:** sr.conf gating — tests read from `[.flightsql]` section; if empty, tests skip gracefully (standard StarRocks external catalog test pattern)
- **Test data:** Include a seed data setup script (SQL or shell) that creates test tables in DuckDB with known data for deterministic results
- **sr.conf variables:** `external_flightsql_ip` and `external_flightsql_port` only — auth handled by catalog properties in test SQL

### CI Integration
- Skip CI automation for now — document the Docker sidecar approach (DuckDB Flight SQL container) but don't block on CI infra provisioning
- Phase 4 delivers test files and local verification; CI integration is a follow-up task

### Claude's Discretion
- MockedADBCMetadata implementation — mirror MockedJDBCMetadata pattern exactly, register in ConnectorPlanTestBase, test EXPLAIN for ADBC_SCAN_NODE with pushdowns
- Plan test coverage scope and number of test methods
- Exact seed data schema and row counts
- Test cleanup approach and isolation strategy

</decisions>

<specifics>
## Specific Ideas

- "Like JDBC" continues — MockedJDBCMetadata is the direct template for MockedADBCMetadata
- Multiple T/R files (test_adbc_ddl, test_adbc_query, test_adbc_mv) rather than a single monolithic test file
- Cross-catalog joins test ADBC tables alongside native StarRocks tables
- DuckDB Flight SQL is the reference server for all local E2E testing

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `MockedJDBCMetadata.java`: Direct template for MockedADBCMetadata — provides fake table schemas, statistics, partition info for plan tests
- `ConnectorPlanTestBase.java`: Registration point for mocked metadata — add ADBC connector type alongside JDBC
- `test/sql/test_jdbc_catalog/`: Reference T/R structure for external catalog SQL tests (test_binary_type, test_json_type)
- `test/conf/sr.conf`: Config file with existing `[env]` sections for external services (hive, hdfs, oss)

### Established Patterns
- SQL integration tests use `${uuid0}` for unique database/table names and clean up after themselves
- External catalog tests gated on sr.conf variables — empty values mean "skip"
- T files in `test/sql/test_*/T/`, R files in `test/sql/test_*/R/`
- FE plan tests extend `ConnectorPlanTestBase`, call `getFragmentPlan()` and assert EXPLAIN output

### Integration Points
- `ConnectorPlanTestBase.java:78`: Register MockedADBCMetadata alongside existing mocked connectors
- `test/conf/sr.conf`: Add `[.flightsql]` section under `[env]`
- `test/sql/test_adbc_catalog/`: New directory with T/ and R/ subdirectories

</code_context>

<deferred>
## Deferred Ideas

- CI pipeline provisioning of DuckDB Flight SQL Docker container — requires CI infra team coordination
- Performance/load testing against Flight SQL endpoints — out of scope for v1
- Write path tests (INSERT INTO remote) — blocked on v2 write support

</deferred>

---

*Phase: 04-integration-testing*
*Context gathered: 2026-03-07*
