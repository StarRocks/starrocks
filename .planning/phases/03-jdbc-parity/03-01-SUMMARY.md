---
phase: 03-jdbc-parity
plan: 01
subsystem: connector
tags: [partition-traits, materialized-view, adbc, external-catalog]

requires:
  - phase: 01-foundation
    provides: ADBCTable, ADBCMetadata, connector registration
  - phase: 02-scanning
    provides: scan operator, scan node, BE scanner

provides:
  - ADBCPartitionTraits registered in TRAITS_TABLE
  - ADBCTable.getCatalogTableName() for MV framework compatibility
  - ADBCPartitionKey implementing NullablePartitionKey
  - PCT explicitly disabled for ADBC tables

affects: [03-jdbc-parity plan 02, 03-jdbc-parity plan 03, materialized-view-support]

tech-stack:
  added: []
  patterns: [partition-traits-registration, getCatalogTableName-override]

key-files:
  created:
    - fe/fe-core/src/main/java/com/starrocks/connector/partitiontraits/ADBCPartitionTraits.java
    - fe/fe-core/src/main/java/com/starrocks/catalog/ADBCPartitionKey.java
  modified:
    - fe/fe-core/src/main/java/com/starrocks/connector/ConnectorPartitionTraits.java
    - fe/fe-core/src/main/java/com/starrocks/catalog/ADBCTable.java
    - fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetadataTest.java

key-decisions:
  - "ADBCPartitionTraits uses ADBCPartitionKey (dedicated class) instead of NullablePartitionKey directly"
  - "isSupportPCTRefresh() returns false -- full refresh only per CONTEXT.md"
  - "getPartitions() returns empty list -- no real partition support initially"

patterns-established:
  - "ADBC partition traits mirror JDBC pattern but simpler (no PCT, no real partitions)"

requirements-completed: [PART-01, PART-02, PART-03, MV-01, MV-03]

duration: 20min
completed: 2026-03-07
---

# Phase 3 Plan 1: Partition Traits Summary

**ADBC partition traits registered in TRAITS_TABLE with getCatalogTableName override enabling MV framework discovery**

## Performance

- **Duration:** 20 min (effective; build environment recovery took additional time)
- **Started:** 2026-03-06T21:30:03Z
- **Completed:** 2026-03-07T00:08:00Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- ADBCPartitionTraits extends DefaultTraits with PCT disabled and empty partition list
- ADBCPartitionKey implements NullablePartitionKey for partition key type safety
- ConnectorPartitionTraits.TRAITS_TABLE now contains Table.TableType.ADBC entry
- ADBCTable.getCatalogTableName() returns catalogName.dbName.name format
- 4 new tests verify traits registration, PCT disabled, catalog table name format, and default partition names

## Task Commits

Each task was committed atomically:

1. **Task 1: Create ADBCPartitionTraits and register in TRAITS_TABLE** - `e6bb67fa7f` (feat)
2. **Task 2: Add getCatalogTableName() to ADBCTable and partition/traits tests** - `0776809130` (feat)

**Plan metadata:** TBD (docs: complete plan)

## Files Created/Modified
- `fe/fe-core/src/main/java/com/starrocks/connector/partitiontraits/ADBCPartitionTraits.java` - Partition traits for ADBC extending DefaultTraits
- `fe/fe-core/src/main/java/com/starrocks/catalog/ADBCPartitionKey.java` - Partition key with nullable support
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorPartitionTraits.java` - Added ADBC to TRAITS_TABLE
- `fe/fe-core/src/main/java/com/starrocks/catalog/ADBCTable.java` - Added getCatalogTableName() override
- `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetadataTest.java` - 4 new partition/traits tests

## Decisions Made
- Used ADBCPartitionKey (dedicated class) instead of NullablePartitionKey -- mirrors JDBC pattern where JDBCPartitionKey exists
- PCT (Partition Change Tracking) disabled per CONTEXT.md -- full refresh only for MV over ADBC tables
- Empty partition list returned from getPartitions() -- minimal partition support sufficient for MV creation

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed checkstyle import ordering in ADBCMetadata.java**
- **Found during:** Task 2 (compilation verification)
- **Issue:** TvrVersionRange import was after OptimizerContext (wrong lexicographic order)
- **Fix:** Reordered imports to satisfy checkstyle CustomImportOrder rule
- **Files modified:** fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java
- **Verification:** mvn compile passes checkstyle
- **Committed in:** 0776809130 (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug fix)
**Impact on plan:** Trivial import ordering fix. No scope creep.

## Issues Encountered
- Build environment corruption after accidental `rm -rf target` required full FE rebuild via `./build.sh --fe` to regenerate thrift/proto/ANTLR sources

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Partition traits registered -- MV framework can now discover ADBC tables
- getCatalogTableName() enables DefaultTraits.getTableName() to work without NotImplementedException
- Ready for Plan 02 (table statistics) and Plan 03 (remaining JDBC parity features)

## Self-Check: PASSED

All 4 key files verified on disk. Both task commits (e6bb67fa7f, 0776809130) verified in git log. 18/18 tests pass.

---
*Phase: 03-jdbc-parity*
*Completed: 2026-03-07*
