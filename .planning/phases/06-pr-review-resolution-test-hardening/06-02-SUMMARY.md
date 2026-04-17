---
phase: 06-pr-review-resolution-test-hardening
plan: 02
subsystem: fe-adbc-connector
tags: [cleanup, rename, test-infrastructure, pr-review]
dependency_graph:
  requires: []
  provides:
    - "Clean ADBCConnector without legacy v1 detection or GSD tags"
    - "Consistent 'username' property key across FE, BE, and verification tests"
    - "Pure mock ADBC test infrastructure in ConnectorPlanTestBase"
    - "Partition TODO comments for future implementation"
  affects:
    - "fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCConnector.java"
    - "fe/fe-core/src/main/java/com/starrocks/catalog/ADBCTable.java"
    - "fe/fe-core/src/test/java/com/starrocks/sql/plan/ConnectorPlanTestBase.java"
    - "fe/fe-core/src/main/java/com/starrocks/catalog/ADBCPartitionKey.java"
    - "fe/fe-core/src/main/java/com/starrocks/connector/partitiontraits/ADBCPartitionTraits.java"
tech_stack:
  added: []
  patterns:
    - "Pure mock pattern for ADBC catalog in ConnectorPlanTestBase (mirrors JDBC)"
key_files:
  created: []
  modified:
    - "fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCConnector.java"
    - "fe/fe-core/src/main/java/com/starrocks/catalog/ADBCTable.java"
    - "fe/fe-core/src/test/java/com/starrocks/sql/plan/ConnectorPlanTestBase.java"
    - "fe/fe-core/src/main/java/com/starrocks/catalog/ADBCPartitionKey.java"
    - "fe/fe-core/src/main/java/com/starrocks/connector/partitiontraits/ADBCPartitionTraits.java"
  deleted:
    - "fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCSQLiteIT.java"
decisions:
  - "D-07/D-08: Renamed 'user' to 'username' in KNOWN_TOP_LEVEL_KEYS, openDatabase(), and ADBCTable.toThrift()"
  - "D-01: Removed legacy v1 catalog detection from constructor and getMetadata()"
  - "D-04/D-06: Rewrote mockADBCCatalogImpl to use dummy driver_url, no System.getProperty"
  - "D-05: Deleted ADBCSQLiteIT.java, integration testing handled by adbc_verification suite"
  - "D-09: Updated all 5 FlightSQL test occurrences of 'user' to 'username'"
metrics:
  duration: "5m 12s"
  completed: "2026-04-17T15:23:03Z"
  tasks_completed: 3
  tasks_total: 3
  files_modified: 5
  files_deleted: 1
---

# Phase 06 Plan 02: FE Cleanup, Property Rename, Test Infrastructure Fix Summary

Clean ADBCConnector of legacy v1 detection and GSD requirement tags, standardize property key from 'user' to 'username' across FE connector/table and verification tests, delete ADBCSQLiteIT integration test, rewrite ConnectorPlanTestBase to use pure mock ADBC registration mirroring the JDBC pattern, and add partition TODO comments.

## Commits

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | Clean ADBCConnector -- legacy removal, GSD refs, user->username | 58e940d41a | ADBCConnector.java, ADBCTable.java |
| 2 | Delete ADBCSQLiteIT, fix ConnectorPlanTestBase, update verification tests | d7c9f9c064 | ADBCSQLiteIT.java (deleted), ConnectorPlanTestBase.java |
| 2 | (adbc_verification repo) Rename user to username in FlightSQL tests | d7bb7d8 | tests/test_flightsql.py |
| 3 | Add partition TODOs, clean GSD refs in partition files | 12f6a0d07a | ADBCPartitionKey.java, ADBCPartitionTraits.java |

## What Changed

### Task 1: ADBCConnector Cleanup
- **Legacy removal:** Deleted the v1 catalog detection block from the constructor (checked for `adbc.driver`/`adbc.url` properties) and the matching guard in `getMetadata()`. These were scaffolding for a migration path that is no longer needed.
- **GSD tag removal:** Stripped all requirement reference tags (PROP-04, D-01, META-05, META-02, PROP-03, PROP-06, VAL-03) from comments, keeping the surrounding rationale text intact.
- **Property rename:** Changed `"user"` to `"username"` in `KNOWN_TOP_LEVEL_KEYS` set and in `openDatabase()` method. Changed `ADBCTable.toThrift()` to read `properties.get("username")` instead of `properties.get("user")`.

### Task 2: Test Infrastructure
- **ADBCSQLiteIT deletion:** Removed the 262-line integration test that required a real SQLite ADBC driver on disk. Integration testing is handled by the external `adbc_verification` suite (24 tests).
- **ConnectorPlanTestBase fix:** Rewrote `mockADBCCatalogImpl()` to use dummy property values (`/mock/path/libadbc_driver.so`, `:memory:`) instead of `System.getProperty("adbc.sqlite.driver.path")` with a hardcoded fallback to `/home/mete/miniconda3/...`. Now mirrors the JDBC mock pattern exactly.
- **adbc_verification update:** Changed all 5 occurrences of `"user": "sqlflite_username"` to `"username": "sqlflite_username"` in `test_flightsql.py` (separate repo commit).

### Task 3: Partition TODOs
- Added `TODO: implement ADBC partition support when drivers expose partitioning metadata` comments above both `ADBCPartitionKey` and `ADBCPartitionTraits` class declarations, referencing their JDBC counterparts.
- Removed `per CONTEXT.md decision` GSD reference from `ADBCPartitionTraits.isSupportPCTRefresh()`.

## Deviations from Plan

None -- plan executed exactly as written.

## Decisions Made

1. **Property key standardization:** `"user"` -> `"username"` aligns with the ADBC specification naming convention. The BE already reads `"username"` from thrift options, so no BE change was needed.
2. **Pure mock pattern:** The dummy `driver_url` of `/mock/path/libadbc_driver.so` is never loaded -- `MockedADBCMetadata` intercepts all metadata calls before any driver interaction.
3. **ADBCSQLiteIT deletion:** Safe because the external `adbc_verification` suite covers the same integration scenarios with more coverage (24 tests vs 5 tests).

## Self-Check: PASSED
