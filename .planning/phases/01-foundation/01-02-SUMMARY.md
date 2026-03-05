---
phase: 01-foundation
plan: 02
subsystem: catalog
tags: [adbc, arrow, connector, catalog-type, table-type]

# Dependency graph
requires: []
provides:
  - ConnectorType.ADBC enum entry and SUPPORT_TYPE_SET registration
  - Table.TableType.ADBC with @SerializedName for persistence
  - ADBCConnector entry point with property validation (adbc.driver, adbc.url)
  - ADBCSchemaResolver abstract base for Arrow->StarRocks type mapping
  - ADBCTable subclass with TableType.ADBC
  - ADBCTableName cache key type
  - ADBCMetadata stub (Plan 04 replaces)
affects: [01-03, 01-04, 01-05]

# Tech tracking
tech-stack:
  added: [org.apache.arrow.vector.types.pojo.Field, org.apache.arrow.vector.types.pojo.Schema]
  patterns: [connector-registration, table-type-registration, abstract-schema-resolver]

key-files:
  created:
    - fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCConnector.java
    - fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java
    - fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCTableName.java
    - fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCSchemaResolver.java
    - fe/fe-core/src/main/java/com/starrocks/catalog/ADBCTable.java
    - fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCConnectorTypeTest.java
    - fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCConnectorTest.java
  modified:
    - fe/fe-core/src/main/java/com/starrocks/connector/ConnectorType.java
    - fe/fe-core/src/main/java/com/starrocks/catalog/Table.java

key-decisions:
  - "ADBCSchemaResolver uses Arrow vector types (Field, Schema) not Flight SQL types for broader driver compatibility"
  - "ADBCTable.toThrift() returns null stub until TTableType.ADBC_TABLE is added to Thrift IDL"
  - "ADBCMetadata created as minimal stub; Plan 04 replaces with full ADBC API implementation"

patterns-established:
  - "ADBC connector follows same registration pattern as JDBC: ConnectorType enum + SUPPORT_TYPE_SET + Connector class"
  - "ADBCSchemaResolver abstract class with convertArrowFieldToSRType(Field) for pluggable type mapping"
  - "ADBCTable mirrors JDBCTable structure: catalogName, dbName, properties fields"

requirements-completed: [CAT-01, CAT-02, CAT-04, CAT-05, TYPE-02]

# Metrics
duration: 37min
completed: 2026-03-05
---

# Phase 1 Plan 2: Connector Skeleton Summary

**ADBC connector scaffold with ConnectorType/TableType registration, property validation, abstract Arrow schema resolver, and ADBCTable representation**

## Performance

- **Duration:** 37 min
- **Started:** 2026-03-05T19:24:14Z
- **Completed:** 2026-03-05T20:01:12Z
- **Tasks:** 2
- **Files modified:** 9

## Accomplishments
- ConnectorType.ADBC registered in enum and SUPPORT_TYPE_SET so CREATE CATALOG TYPE='adbc' is recognized
- Table.TableType.ADBC added with @SerializedName for correct serialization
- ADBCConnector validates adbc.driver and adbc.url properties, follows JDBC connector pattern
- ADBCSchemaResolver abstract base provides convertArrowFieldToSRType and convertToSRTable contracts
- ADBCTable and ADBCTableName provide table representation and cache key types
- 19 total tests passing (7 type registration + 12 connector/table tests)

## Task Commits

Each task was committed atomically:

1. **Task 1: Register ConnectorType.ADBC and add Table.TableType.ADBC** - `ad2bf60f53` (feat)
2. **Task 2: Create ADBCConnector, ADBCTableName, ADBCSchemaResolver, and ADBCTable** - `e9c03e1e0d` (feat)

## Files Created/Modified
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorType.java` - Added ADBC enum entry and SUPPORT_TYPE_SET registration
- `fe/fe-core/src/main/java/com/starrocks/catalog/Table.java` - Added TableType.ADBC, isADBCTable(), ADBC engine name
- `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCConnector.java` - Connector entry point with property validation
- `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java` - Stub ConnectorMetadata (Plan 04 replaces)
- `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCTableName.java` - Cache key for (catalog, db, table) triple
- `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCSchemaResolver.java` - Abstract base for Arrow type conversion
- `fe/fe-core/src/main/java/com/starrocks/catalog/ADBCTable.java` - Table subclass with ADBC type
- `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCConnectorTypeTest.java` - 7 tests for type registration
- `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCConnectorTest.java` - 12 tests for connector, table, resolver

## Decisions Made
- ADBCSchemaResolver uses Arrow vector types (Field, Schema) rather than Flight SQL types, keeping the abstract class driver-agnostic
- ADBCTable.toThrift() returns null until TTableType.ADBC_TABLE is added to the Thrift IDL in a later plan
- Created ADBCMetadata as a minimal stub implementing ConnectorMetadata; Plan 04 will replace with real ADBC API calls

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed import order in ConnectorType.java**
- **Found during:** Task 1 (ConnectorType registration)
- **Issue:** Import of ADBCConnector was placed after IcebergConnector, violating checkstyle alphabetical order
- **Fix:** Moved import to correct alphabetical position (before benchmark imports)
- **Files modified:** ConnectorType.java
- **Verification:** mvn checkstyle:check passes with 0 violations
- **Committed in:** ad2bf60f53 (Task 1 commit)

**2. [Rule 1 - Bug] Fixed Type.INT reference in tests**
- **Found during:** Task 2 (ADBCConnectorTest)
- **Issue:** Used `Type.INT` but correct static field is `IntegerType.INT`
- **Fix:** Changed import to IntegerType and all references to IntegerType.INT
- **Files modified:** ADBCConnectorTest.java
- **Verification:** All 12 tests compile and pass
- **Committed in:** e9c03e1e0d (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 bug)
**Impact on plan:** Both auto-fixes necessary for correctness. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Connector skeleton complete, ready for Plan 03 (Arrow type mapping in ADBCSchemaResolver)
- Plan 04 can build ADBCMetadata implementation against ADBCConnector entry point
- No blockers

## Self-Check: PASSED

All 7 created files verified on disk. Both task commits (ad2bf60f53, e9c03e1e0d) verified in git log.

---
*Phase: 01-foundation*
*Completed: 2026-03-05*
