---
phase: 02-scanning
plan: 03
subsystem: planner
tags: [adbc, scan-node, sql-pushdown, thrift, explain, plan-fragment, relation-transformer]

# Dependency graph
requires:
  - phase: 02-scanning/02-01
    provides: "LogicalADBCScanOperator, PhysicalADBCScanOperator, TADBCScanNode/TADBCTable Thrift IDL"
provides:
  - "ADBCScanNode with SQL generation (column pruning, predicates, LIMIT pushdown)"
  - "ADBCScanNode.toThrift producing TADBCScanNode for BE"
  - "ADBCTable.toThrift returning TTableDescriptor with ADBC_TABLE type"
  - "PlanFragmentBuilder.visitPhysicalADBCScan creating UNPARTITIONED fragment"
  - "RelationTransformer creating LogicalADBCScanOperator for ADBC tables"
  - "EXPLAIN output showing driver, URI, pushed-down SQL query"
affects: [02-scanning/02-04, 03-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [adbc-scan-node-mirrors-jdbc, sql-string-pushdown, thrift-serialization]

key-files:
  created:
    - fe/fe-core/src/main/java/com/starrocks/planner/ADBCScanNode.java
    - fe/fe-core/src/test/java/com/starrocks/planner/ADBCScanNodeTest.java
  modified:
    - fe/fe-core/src/main/java/com/starrocks/catalog/ADBCTable.java
    - fe/fe-core/src/main/java/com/starrocks/sql/plan/PlanFragmentBuilder.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/transformer/RelationTransformer.java

key-decisions:
  - "ADBCScanNode mirrors JDBCScanNode pattern exactly for SQL generation, filter pushdown, EXPLAIN"
  - "Default identifier quote is double-quote (standard SQL), configurable via adbc.identifier.quote property"
  - "Driver name mapping: flight_sql -> adbc_driver_flightsql; custom names pass through as-is"
  - "EXPLAIN ANALYZE placeholders for ConnectTime/RowsRead/BytesRead from BE runtime profile"

patterns-established:
  - "ADBC scan node SQL generation: SELECT quoted_cols FROM schema.table WHERE filters LIMIT n"
  - "ADBC property lookup: adbc.* keys with fallback to short names (url, username, etc.)"

requirements-completed: [OPT-03, OPT-04, SCAN-04, SCAN-05, SCAN-06]

# Metrics
duration: 8min
completed: 2026-03-06
---

# Phase 2 Plan 3: ADBCScanNode SQL Generation and FE Data Path Wiring Summary

**ADBCScanNode with pushed-down SQL generation (column pruning, predicates, LIMIT), Thrift serialization, EXPLAIN/EXPLAIN ANALYZE output, plus ADBCTable.toThrift, PlanFragmentBuilder, and RelationTransformer wiring**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-05T22:24:41Z
- **Completed:** 2026-03-05T22:33:00Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- ADBCScanNode generates pushed-down SQL with column pruning, predicate pushdown, and LIMIT
- toThrift() produces TADBCScanNode with all fields (driver, URI, credentials, columns, filters, limit)
- EXPLAIN shows TABLE, QUERY, DRIVER, URI; EXPLAIN ANALYZE shows connect_time stats placeholders
- ADBCTable.toThrift() returns valid TTableDescriptor with TTableType.ADBC_TABLE
- PlanFragmentBuilder creates UNPARTITIONED fragment for ADBC scans
- RelationTransformer creates LogicalADBCScanOperator instead of throwing Phase 2 error
- Wave 0 test scaffold with SQL generation, Thrift, and EXPLAIN tests

## Task Commits

Each task was committed atomically:

1. **Task 1: Create ADBCScanNode with SQL generation, toThrift, EXPLAIN, and EXPLAIN ANALYZE** - `dd1d972207` (feat)
2. **Task 2: Wire ADBCTable.toThrift, PlanFragmentBuilder, and RelationTransformer** - `602d5d21df` (feat)

## Files Created/Modified
- `fe/fe-core/src/main/java/com/starrocks/planner/ADBCScanNode.java` - ADBC scan plan node with SQL generation, Thrift serialization, EXPLAIN output
- `fe/fe-core/src/test/java/com/starrocks/planner/ADBCScanNodeTest.java` - Wave 0 test scaffold for SQL generation and Thrift
- `fe/fe-core/src/main/java/com/starrocks/catalog/ADBCTable.java` - Updated toThrift() returning TTableDescriptor with TADBCTable
- `fe/fe-core/src/main/java/com/starrocks/sql/plan/PlanFragmentBuilder.java` - Added visitPhysicalADBCScan handler
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/transformer/RelationTransformer.java` - Replaced Phase 2 error stub with LogicalADBCScanOperator creation

## Decisions Made
- ADBCScanNode mirrors JDBCScanNode pattern exactly for SQL generation, filter pushdown, and EXPLAIN
- Default identifier quote is double-quote (standard SQL), configurable via adbc.identifier.quote property
- Driver name mapping: flight_sql -> adbc_driver_flightsql; custom names pass through as-is
- EXPLAIN ANALYZE shows placeholders for ConnectTime/RowsRead/BytesRead stats from BE runtime profile

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed getTableName() to getName()**
- **Found during:** Task 1 (ADBCScanNode creation)
- **Issue:** Plan referenced adbcTable.getTableName() but Table base class uses getName()
- **Fix:** Changed to adbcTable.getName() in both ADBCScanNode and test
- **Files modified:** ADBCScanNode.java, ADBCScanNodeTest.java
- **Verification:** FE compiles cleanly
- **Committed in:** dd1d972207 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Minor API name correction. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- FE data path complete: SELECT on ADBC table flows through optimizer -> ADBCScanNode -> TADBCScanNode Thrift
- Ready for Plan 04 (end-to-end integration testing)
- BE scanner (Plan 02) provides the runtime execution side

---
## Self-Check: PASSED

All 5 files verified present. Both task commits (dd1d972207, 602d5d21df) verified in git log.

---
*Phase: 02-scanning*
*Completed: 2026-03-06*
