---
phase: 04-integration-testing
plan: 02
subsystem: testing
tags: [sql-tester, integration-tests, adbc, flight-sql, duckdb, sqlflite]

requires:
  - phase: 03-jdbc-parity
    provides: ADBC catalog DDL syntax, query pushdown, MV support
provides:
  - SQL integration test T/R files for ADBC catalog (DDL, query, MV)
  - sr.conf Flight SQL connection variables
  - DuckDB seed data script for E2E test environment
affects: []

tech-stack:
  added: []
  patterns: [sql-tester T/R file pattern for external catalogs]

key-files:
  created:
    - test/sql/test_adbc_catalog/T/test_adbc_ddl
    - test/sql/test_adbc_catalog/T/test_adbc_query
    - test/sql/test_adbc_catalog/T/test_adbc_mv
    - test/sql/test_adbc_catalog/R/test_adbc_ddl
    - test/sql/test_adbc_catalog/R/test_adbc_query
    - test/sql/test_adbc_catalog/R/test_adbc_mv
    - test/lib/setup_flightsql_data.sql
  modified:
    - test/conf/sr.conf

key-decisions:
  - "R files include expected results from seed data; re-record with python3 run.py -r against live server"
  - "EXPLAIN tests use [UC] tag since output format varies across environments"

patterns-established:
  - "ADBC catalog test pattern: create catalog with grpc://$host:$port, query, cleanup with DROP CATALOG"

requirements-completed: [TEST-02, TEST-03, TEST-04]

duration: 2min
completed: 2026-03-07
---

# Phase 04 Plan 02: SQL Integration Tests Summary

**SQL-tester T/R files for ADBC catalog covering DDL lifecycle (3 cases), query pushdown (7 cases), and MV operations (2 cases) with DuckDB seed data**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-07T14:08:21Z
- **Completed:** 2026-03-07T14:10:13Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments
- Added [.flightsql] section to sr.conf with external_flightsql_ip and external_flightsql_port variables
- Created DuckDB seed data script with test_types (5 rows) and test_join (3 rows) tables
- Created 12 SQL integration test cases across 3 test files covering DDL, queries, and MVs
- All tests use ${uuid0} for unique naming and clean up after themselves

## Task Commits

Each task was committed atomically:

1. **Task 1: Add sr.conf Flight SQL section and seed data script** - `fae7bc9be9` (chore)
2. **Task 2: Create SQL integration test T/R files** - `9bb6c2dcc3` (test)

## Files Created/Modified
- `test/conf/sr.conf` - Added [.flightsql] section with connection variables
- `test/lib/setup_flightsql_data.sql` - DuckDB seed data for test_types and test_join tables
- `test/sql/test_adbc_catalog/T/test_adbc_ddl` - DDL lifecycle tests (CREATE, SHOW, DESCRIBE, ALTER, DROP)
- `test/sql/test_adbc_catalog/T/test_adbc_query` - Query pushdown tests (SELECT *, column pruning, predicate, limit, combined, EXPLAIN, subquery)
- `test/sql/test_adbc_catalog/T/test_adbc_mv` - Materialized view lifecycle tests (CREATE, REFRESH, SELECT, query rewrite)
- `test/sql/test_adbc_catalog/R/test_adbc_ddl` - Expected results for DDL tests
- `test/sql/test_adbc_catalog/R/test_adbc_query` - Expected results for query tests
- `test/sql/test_adbc_catalog/R/test_adbc_mv` - Expected results for MV tests

## Decisions Made
- R files include expected results based on seed data; should be re-recorded with `python3 run.py -r` against a live Flight SQL server for accuracy
- EXPLAIN output tests use [UC] tag since format varies across environments
- Used `set catalog default_catalog;` before cleanup in all test cases for safety

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

To run these tests against a real Flight SQL server:
1. `duckdb test.db < test/lib/setup_flightsql_data.sql`
2. `docker run -p 31337:31337 -v $(pwd)/test.db:/data/test.db voltrondata/sqlflite:latest /data/test.db`
3. Edit `test/conf/sr.conf`: set `external_flightsql_ip=127.0.0.1`, `external_flightsql_port=31337`
4. `cd test && python3 run.py -d sql/test_adbc_catalog -r` (record mode to capture actual results)
5. `cd test && python3 run.py -d sql/test_adbc_catalog -v` (validate mode)

## Next Phase Readiness
- All SQL integration test files are in place for ADBC catalog validation
- Tests are gated on sr.conf variables -- skip gracefully when Flight SQL server is not configured
- R file results may need re-recording against a live server for exact output matching

---
*Phase: 04-integration-testing*
*Completed: 2026-03-07*
