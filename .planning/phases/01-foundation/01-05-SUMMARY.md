---
phase: 01-foundation
plan: 05
subsystem: catalog
tags: [adbc, arrow, sql-analysis, type-guards, scan-operator]

# Dependency graph
requires:
  - "01-02: ConnectorType.ADBC, Table.TableType.ADBC, ADBCTable"
  - "01-03: FlightSQLSchemaResolver type mapping"
  - "01-04: ADBCMetadata, ADBCMetaCache"
provides:
  - TableType.ADBC guards in AstToStringBuilder (EXTERNAL TABLE, DDL, partition)
  - MaterializedViewAnalyzer SUPPORTED_TABLE_TYPE includes ADBC
  - DesensitizedSQLBuilder handles ADBC tables without NPE
  - RelationTransformer ADBC scan stub with Phase 2 error message
  - Phase 1 foundation fully certified (66 tests passing, FE compile clean)
affects: [02-query-execution]

# Tech tracking
tech-stack:
  added: []
  patterns: [table-type-guard-pattern, scan-operator-stub]

key-files:
  created: []
  modified:
    - fe/fe-core/src/main/java/com/starrocks/sql/analyzer/AstToStringBuilder.java
    - fe/fe-core/src/main/java/com/starrocks/sql/analyzer/MaterializedViewAnalyzer.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/dump/DesensitizedSQLBuilder.java
    - fe/fe-core/src/main/java/com/starrocks/sql/optimizer/transformer/RelationTransformer.java

key-decisions:
  - "AstToStringBuilder ADBC DDL uses sorted properties output via Joiner to avoid lambda capture issues with StringBuilder"
  - "RelationTransformer throws StarRocksPlannerException with UNSUPPORTED type for ADBC scan attempts, deferring LogicalADBCScanOperator to Phase 2"

patterns-established:
  - "ADBC guards mirror JDBC guard pattern in all SQL analysis switch sites"
  - "Phase 2 scan stub pattern: throw descriptive error rather than fall through to generic 'unsupported type'"

requirements-completed: [BUILD-03, CAT-01, CAT-02, CAT-03, CAT-06, CAT-07, CAT-08]

# Metrics
duration: 13min
completed: 2026-03-05
---

# Phase 1 Plan 5: SQL Analysis Guards Summary

**ADBC type guards added to all four SQL analysis switch sites (AstToStringBuilder, MaterializedViewAnalyzer, DesensitizedSQLBuilder, RelationTransformer) closing NPE traps, with full Phase 1 test suite (66 tests) passing**

## Performance

- **Duration:** 13 min
- **Started:** 2026-03-05T21:01:31Z
- **Completed:** 2026-03-05T21:14:49Z
- **Tasks:** 2 (1 auto + 1 checkpoint verified automatically)
- **Files modified:** 4

## Accomplishments
- All four SQL analysis files now handle TableType.ADBC preventing NPEs on ADBC tables
- AstToStringBuilder generates CREATE EXTERNAL TABLE ... ENGINE=adbc DDL with sorted properties output
- MaterializedViewAnalyzer allows ADBC tables as MV base tables
- RelationTransformer provides clear Phase 2 error message when ADBC scan is attempted
- Full Phase 1 certification: 66 ADBC tests passing, FE compile clean

## Task Commits

Each task was committed atomically:

1. **Task 1: Add ADBC guards to SQL analysis files** - `a1f5fd6662` (feat)

## Verification Results (Task 2 Checkpoint)

The following verification commands were run automatically:

1. **FE compile:** `mvn compile -pl fe-core -am -DskipTests -Dcheckstyle.skip=true -q` -- PASSED (clean compile)
2. **ADBC unit tests:** All 5 test classes run, 66 tests pass (10 ADBCMetadataTest + 7 ADBCConnectorTypeTest + 12 ADBCConnectorTest + 25 FlightSQLSchemaResolverTest + 12 ADBCMetaCacheTest)
3. **TableType.ADBC grep:** Found in AstToStringBuilder.java (3 locations), MaterializedViewAnalyzer.java, DesensitizedSQLBuilder.java (2 locations), RelationTransformer.java, Table.java (2 locations), ADBCTable.java (2 locations)
4. **ADBC connector files:** All 6 source files exist (ADBCConnector, ADBCMetaCache, ADBCMetadata, ADBCSchemaResolver, ADBCTableName, FlightSQLSchemaResolver)

## Files Modified
- `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/AstToStringBuilder.java` - Added EXTERNAL TABLE guard, ADBC DDL branch with ENGINE=adbc, partition check exclusion
- `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/MaterializedViewAnalyzer.java` - Added Table.TableType.ADBC to SUPPORTED_TABLE_TYPE set
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/dump/DesensitizedSQLBuilder.java` - Added ADBC to external table guard and ADBC properties output branch
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/transformer/RelationTransformer.java` - Added ADBC scan stub throwing Phase 2 error

## Decisions Made
- Used `Joiner.on(",\n").join()` pattern instead of `forEach` lambda with `sb.append()` to avoid "effectively final" compilation error (the `sb` StringBuilder is reassigned later in the method)
- RelationTransformer throws `ErrorType.UNSUPPORTED` (not `INTERNAL_ERROR`) since ADBC scanning is a known unimplemented feature, not a bug

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed lambda capture of non-final StringBuilder**
- **Found during:** Task 1 (AstToStringBuilder ADBC branch)
- **Issue:** Plan's template used `forEach(e -> sb.append(...))` but `sb` is not effectively final (reassigned later for partition handling)
- **Fix:** Changed to `map()` + `collect(Collectors.toList())` + `Joiner.on(",\n").join()` pattern
- **Files modified:** AstToStringBuilder.java, DesensitizedSQLBuilder.java
- **Verification:** Compilation passes
- **Committed in:** a1f5fd6662 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Necessary fix for Java compilation. No scope creep.

## Issues Encountered
- Pre-existing checkstyle violations (109 errors) unrelated to ADBC changes; used -Dcheckstyle.skip=true for compilation. These are pre-existing issues in the codebase.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 1 foundation is complete with all 5 plans executed
- All ADBC connector infrastructure is in place: types, connector, metadata, cache, type mapping, SQL analysis guards
- Ready for Phase 2: query execution (LogicalADBCScanOperator, PhysicalADBCScanNode, Arrow data transfer)
- No blockers

## Self-Check: PASSED

All 4 modified files verified on disk. Task commit (a1f5fd6662) verified in git log.

---
*Phase: 01-foundation*
*Completed: 2026-03-05*
