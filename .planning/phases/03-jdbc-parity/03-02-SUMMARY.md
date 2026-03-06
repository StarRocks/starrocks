---
phase: 03-jdbc-parity
plan: 02
subsystem: connector
tags: [adbc, statistics, optimizer, count-query, arrow-flight-sql]

# Dependency graph
requires:
  - phase: 01-foundation
    provides: ADBCMetadata, AdbcDatabase connection management
  - phase: 02-scanning
    provides: Scan path for ANALYZE TABLE (ExternalFullStatisticsCollectJob)
provides:
  - getTableStatistics() with remote row count via COUNT(*) pushdown
  - ColumnStatistic.unknown() for all columns (safe default)
  - Structural proof that ExternalFullStatisticsCollectJob accepts ADBCTable
affects: [03-jdbc-parity, 04-integration-testing]

# Tech tracking
tech-stack:
  added: []
  patterns: [remote-row-count-via-count-star, graceful-stats-fallback]

key-files:
  created: []
  modified:
    - fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java
    - fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetadataTest.java

key-decisions:
  - "Row count fetched via COUNT(*) SQL pushdown to remote, matching JDBC pattern"
  - "All columns default to ColumnStatistic.unknown() -- per-column stats via ANALYZE TABLE"
  - "Fallback rowCount=1 on failure (no exception propagation to optimizer)"

patterns-established:
  - "Stats pattern: getTableStatistics pushes COUNT(*), returns unknown column stats"
  - "Error handling: stats failures return safe defaults, never throw"

requirements-completed: [STAT-01, STAT-02]

# Metrics
duration: 25min
completed: 2026-03-07
---

# Phase 3 Plan 2: Table Statistics Summary

**getTableStatistics() with remote COUNT(*) row count pushdown and ColumnStatistic.unknown() column defaults**

## Performance

- **Duration:** 25 min (effective coding/testing time)
- **Started:** 2026-03-06T21:30:38Z
- **Completed:** 2026-03-06T22:55:00Z
- **Tasks:** 1
- **Files modified:** 2

## Accomplishments
- Implemented getTableStatistics() in ADBCMetadata with remote row count via COUNT(*) SQL pushdown
- All columns return ColumnStatistic.unknown() (per-column stats handled by existing ANALYZE infrastructure)
- Graceful fallback to rowCount=1 on connection or query failure
- Verified ExternalFullStatisticsCollectJob accepts ADBCTable (STAT-01 per-column stats via existing infrastructure)
- All 18 ADBCMetadataTest tests pass (4 new + 14 existing)

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement getTableStatistics() with remote row count via ADBC** - `3b10a63fda` (feat)

**Plan metadata:** [pending]

## Files Created/Modified
- `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java` - Added getTableStatistics() override and getRemoteRowCount() helper
- `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetadataTest.java` - Added 4 test methods, fixed type imports

## Decisions Made
- Row count fetched via COUNT(*) SQL pushdown to remote via ADBC Statement API
- Double-quote identifier quoting for SQL (standard SQL, matching ADBCScanNode pattern)
- ColumnStatistic.unknown() for all columns (per-column stats collected via ANALYZE TABLE using ExternalFullStatisticsCollectJob)
- Fallback rowCount=1 on any failure to prevent optimizer exceptions

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed type references in test file**
- **Found during:** Task 1 (TDD RED phase)
- **Issue:** Tests used `com.starrocks.catalog.Type.INT` which does not exist; correct types are `IntegerType.INT`, `VarcharType.VARCHAR` from `com.starrocks.type` package
- **Fix:** Replaced all type references and added proper imports
- **Files modified:** ADBCMetadataTest.java
- **Verification:** All tests compile and pass
- **Committed in:** 3b10a63fda

**2. [Rule 1 - Bug] Fixed AdbcException constructor in test**
- **Found during:** Task 1 (TDD RED phase)
- **Issue:** Test used `AdbcException.UNKNOWN` but the constant is `AdbcStatusCode.UNKNOWN` (separate enum class)
- **Fix:** Changed to `AdbcStatusCode.UNKNOWN` and added import
- **Files modified:** ADBCMetadataTest.java
- **Verification:** Test compiles and passes
- **Committed in:** 3b10a63fda

**3. [Rule 1 - Bug] Fixed TvrVersionRange import path**
- **Found during:** Task 1 (implementation)
- **Issue:** Plan specified `com.starrocks.sql.optimizer.TvrVersionRange` but actual class is at `com.starrocks.common.tvr.TvrVersionRange`
- **Fix:** Corrected import path
- **Files modified:** ADBCMetadata.java
- **Verification:** Production code compiles
- **Committed in:** 3b10a63fda

---

**Total deviations:** 3 auto-fixed (3 bugs in plan-specified code)
**Impact on plan:** All fixes were necessary for compilation. No scope creep.

## Issues Encountered
- FE build environment had stale JaCoCo-instrumented classes and pre-existing compilation errors in unrelated test files (IcebergCommitQueueManagerTest, spark-dpp tests). Resolved by compiling production code and tests separately, then running surefire directly.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Statistics foundation complete for ADBC tables
- Optimizer can now make cost-based decisions using row count estimates
- Per-column detailed stats available via standard ANALYZE TABLE command
- Ready for Plan 03-03 (remaining JDBC parity features)

## Self-Check: PASSED

- FOUND: ADBCMetadata.java
- FOUND: ADBCMetadataTest.java
- FOUND: commit 3b10a63fda

---
*Phase: 03-jdbc-parity*
*Completed: 2026-03-07*
