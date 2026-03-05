---
phase: 01-foundation
plan: 04
subsystem: catalog
tags: [adbc, arrow, metadata, cache, caffeine, flightsql, connector-metadata]

# Dependency graph
requires:
  - 01-02
provides:
  - ADBCMetadata ConnectorMetadata implementation using ADBC Java API
  - ADBCMetaCache Caffeine-based TTL cache with adbc_ property keys
  - Config.adbc_meta_default_cache_enable and Config.adbc_meta_default_cache_expire_sec
  - listDbNames, listTableNames, getTable, shutdown implementations
  - FlightSQLSchemaResolver driver factory (adbc.driver=flight_sql)
affects: [01-05]

# Tech tracking
tech-stack:
  added: [org.apache.arrow.adbc.core.AdbcConnection, org.apache.arrow.adbc.core.AdbcDatabase, com.github.benmanes.caffeine.cache.Cache]
  patterns: [adbc-metadata-cache, adbc-connection-per-operation, nested-arrow-vector-parsing]

key-files:
  created:
    - fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetaCache.java
    - fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetaCacheTest.java
    - fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetadataTest.java
  modified:
    - fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java
    - fe/fe-core/src/main/java/com/starrocks/common/Config.java
    - fe/fe-core/src/main/java/com/starrocks/connector/adbc/FlightSQLSchemaResolver.java
    - fe/fe-core/src/test/java/com/starrocks/connector/adbc/FlightSQLSchemaResolverTest.java

key-decisions:
  - "ADBCMetadata opens a new AdbcConnection per operation (listDbNames, listTableNames, getTable) via try-with-resources, not shared across threads"
  - "getTableSchema uses null for catalog parameter, treating dbName as the Arrow Flight SQL schema"
  - "Unknown adbc.driver values fall back to FlightSQLSchemaResolver with a warning log, rather than throwing"
  - "ADBCMetaCache mirrors JDBCMetaCache exactly with adbc_ property keys for consistent configuration"

patterns-established:
  - "ADBC metadata cache pattern: permanent tableIdCache, TTL-based tableInstanceCache/dbNamesCache/tableNamesCache"
  - "Package-private constructor with AdbcDatabase parameter for test injection"
  - "Nested Arrow vector parsing for getObjects results: catalog -> db_schemas -> tables"

requirements-completed: [CAT-03, CAT-04, CAT-05, CAT-06, CAT-07, CAT-08, CAT-09]

# Metrics
duration: 53min
completed: 2026-03-05
---

# Phase 1 Plan 4: ADBCMetadata and Cache Summary

**ADBCMetadata implementation using ADBC Java API with Caffeine-based TTL cache for listDbNames, listTableNames, getTable, and shutdown operations**

## Performance

- **Duration:** 53 min
- **Started:** 2026-03-05T20:04:39Z
- **Completed:** 2026-03-05T20:58:10Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- ADBCMetaCache created as a near-exact copy of JDBCMetaCache with adbc_ property keys, supporting enable/disable, TTL expiry, and permanent cache modes
- ADBCMetadata fully implemented with listDbNames (getObjects DB_SCHEMAS), listTableNames (getObjects TABLES), getTable (getTableSchema + Arrow Schema conversion), and shutdown
- Config.java extended with adbc_meta_default_cache_enable (default: true) and adbc_meta_default_cache_expire_sec (default: 600)
- 22 total tests passing (12 ADBCMetaCacheTest + 10 ADBCMetadataTest)

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement ADBCMetaCache** - `078c8d00bc` (feat)
2. **Task 2: Implement ADBCMetadata with ADBC Java API** - `5addeba68d` (feat)

## Files Created/Modified
- `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetaCache.java` - Caffeine-based cache with adbc_ property keys
- `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java` - Full ConnectorMetadata implementation using ADBC Java API
- `fe/fe-core/src/main/java/com/starrocks/common/Config.java` - Added adbc_meta_default_cache_enable and adbc_meta_default_cache_expire_sec
- `fe/fe-core/src/main/java/com/starrocks/connector/adbc/FlightSQLSchemaResolver.java` - Fixed type references (IntegerType, FloatType, etc.)
- `fe/fe-core/src/test/java/com/starrocks/connector/adbc/FlightSQLSchemaResolverTest.java` - Fixed type references to match corrected implementation
- `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetaCacheTest.java` - 12 tests for cache enable/disable, TTL, permanent cache
- `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetadataTest.java` - 10 tests for metadata operations with mocked ADBC

## Decisions Made
- ADBCMetadata opens a new AdbcConnection per operation via try-with-resources, matching thread-safety requirements
- getTableSchema uses null for catalog parameter, treating dbName as the Arrow Flight SQL schema
- Unknown adbc.driver values fall back to FlightSQLSchemaResolver with a warning rather than throwing
- Package-private constructor added to ADBCMetadata for test injection of mocked AdbcDatabase

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed FlightSQLSchemaResolver type references**
- **Found during:** Task 1 (compilation check)
- **Issue:** FlightSQLSchemaResolver (from Plan 03) used `Type.SMALLINT`, `Type.FLOAT`, `ScalarType.createType()`, etc. which don't exist in StarRocks type system
- **Fix:** Changed to correct constants: `IntegerType.SMALLINT`, `FloatType.FLOAT`, `IntegerType.LARGEINT`, `new DecimalType(...)`, `new VarcharType(...)` etc.
- **Files modified:** FlightSQLSchemaResolver.java, FlightSQLSchemaResolverTest.java
- **Verification:** Compilation passes, FlightSQLSchemaResolverTest passes
- **Committed in:** 078c8d00bc (Task 1 commit)

**2. [Rule 1 - Bug] Fixed checked exception handling in getTable()**
- **Found during:** Task 2 (compilation)
- **Issue:** AdbcConnection.connect() and close() throw checked Exception, not just AdbcException
- **Fix:** Changed catch block from `AdbcException` to `Exception` in getTable() lambda
- **Files modified:** ADBCMetadata.java
- **Verification:** Compilation passes
- **Committed in:** 5addeba68d (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 bug)
**Impact on plan:** Both auto-fixes necessary for compilation. No scope creep.

## Issues Encountered
- Pre-existing build issues (missing protobuf generated code, corrupted checkstyle XML) required using -Dcheckstyle.skip=true and -Djprotobuf.skip=true flags for test execution
- These are pre-existing issues unrelated to ADBC changes

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- ADBCMetadata is fully functional and cached, ready for Plan 05 (scan node and query execution)
- All metadata operations (SHOW DATABASES, SHOW TABLES, DESCRIBE) have working implementations
- No blockers

## Self-Check: PASSED

All 4 created/modified key files verified on disk. Both task commits (078c8d00bc, 5addeba68d) verified in git log.

---
*Phase: 01-foundation*
*Completed: 2026-03-05*
