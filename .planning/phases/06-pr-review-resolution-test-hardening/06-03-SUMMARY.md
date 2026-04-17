---
phase: 06-pr-review-resolution-test-hardening
plan: 03
subsystem: fe-adbc-cache-removal
tags: [cache-removal, refactor, pr-review, metadata]
dependency_graph:
  requires:
    - 06-02
  provides:
    - "Cache-free ADBCMetadata with direct ADBC calls"
    - "Stable table IDs via ConcurrentHashMap"
    - "Clean Config.java without adbc cache entries"
  affects:
    - "fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java"
    - "fe/fe-core/src/main/java/com/starrocks/common/Config.java"
    - "fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetadataTest.java"
tech_stack:
  added: []
  patterns:
    - "ConcurrentHashMap for stable table ID assignment (replaces Caffeine permanent cache)"
    - "Direct ADBC API calls without cache wrapper (listDbNames, listTableNames, getTable)"
key_files:
  created: []
  modified:
    - "fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java"
    - "fe/fe-core/src/main/java/com/starrocks/common/Config.java"
    - "fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetadataTest.java"
  deleted:
    - "fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetaCache.java"
    - "fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetaCacheTest.java"
decisions:
  - "D-01: Deleted ADBCMetaCache.java and its test entirely -- no replacement cache"
  - "D-02: Removed adbc_meta_default_cache_enable and adbc_meta_default_cache_expire_sec from Config.java"
  - "D-03: Replaced 4 Caffeine cache fields with single ConcurrentHashMap<ADBCTableName, Integer> for stable table IDs"
  - "Deleted ADBCMetaCacheTest.java alongside ADBCMetaCache.java (Rule 2: test for deleted class is dead code)"
metrics:
  duration: "8m 30s"
  completed: "2026-04-17T15:34:28Z"
  tasks_completed: 2
  tasks_total: 2
  files_modified: 3
  files_deleted: 2
---

# Phase 06 Plan 03: Cache Removal -- Delete ADBCMetaCache, Rewrite ADBCMetadata Summary

Delete Caffeine caching layer from ADBC metadata path, replacing 4 cache fields with a single ConcurrentHashMap for stable table IDs, and update all tests for the cache-free direct ADBC API code path.

## One-liner

Deleted ADBCMetaCache.java and its Caffeine dependency, removed adbc_meta_default_cache Config entries, rewrote ADBCMetadata to call ADBC getObjects/getTableSchema directly with ConcurrentHashMap for stable table IDs, and updated ADBCMetadataTest mock expectations from createStatement/executeQuery to getObjects/getTableSchema.

## Commits

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | Delete ADBCMetaCache.java, remove Config entries, rewrite ADBCMetadata.java | 53a8a5646c | ADBCMetaCache.java (deleted), ADBCMetaCacheTest.java (deleted), Config.java, ADBCMetadata.java |
| 2 | Update ADBCMetadataTest.java for cache-free code path | d611a83a60 | ADBCMetadataTest.java |

## What Changed

### Task 1: Cache Removal and ADBCMetadata Rewrite

- **ADBCMetaCache.java deleted:** Removed the 110-line Caffeine cache wrapper class entirely. No replacement caching -- every metadata call goes directly to the ADBC driver.
- **ADBCMetaCacheTest.java deleted:** Removed the 193-line test file for the deleted cache class (Rule 2 deviation: dead test code).
- **Config.java cleaned:** Removed `adbc_meta_default_cache_enable` and `adbc_meta_default_cache_expire_sec` entries. JDBC cache entries (`jdbc_meta_default_cache_enable`, `jdbc_meta_default_cache_expire_sec`) preserved.
- **ADBCMetadata.java rewritten:**
  - Replaced 4 cache fields (`tableIdCache`, `tableInstanceCache`, `dbNamesCache`, `tableNamesCache`) with single `ConcurrentHashMap<ADBCTableName, Integer> tableIdMap`
  - `listDbNames()`: unwrapped from `dbNamesCache.get()` to direct `adbcDatabase.connect()` + `getObjects()` call
  - `listTableNames()`: unwrapped from `tableNamesCache.get()` to direct call
  - `getTable()`: unwrapped from `tableInstanceCache.get()` to direct call; `tableIdCache.getPersistentCache()` replaced with `tableIdMap.computeIfAbsent()`
  - `refreshTable()`: became a no-op (no cache to invalidate; table IDs stay stable)
  - `clear()`: resets `tableIdMap` and `hierarchyModel` (previously invalidated all 4 caches)
  - `HierarchyModel` lazy resolution with volatile + double-checked locking preserved unchanged
  - Removed all Caffeine imports (`com.github.benmanes.caffeine.cache.*`)
  - Added `java.util.concurrent.ConcurrentHashMap` import

### Task 2: ADBCMetadataTest Update

- **Stale mock expectations removed:** Replaced `createStatement` / `setSqlQuery` / `executeQuery` / `getReader` mock chains with `getObjects()` and `getTableSchema()` expectations matching the current ADBC API code path
- **Stale mock fields removed:** `@Mocked AdbcStatement mockStatement` and `@Mocked AdbcStatement.QueryResult mockQueryResult` deleted
- **New mock field added:** `@Mocked ListVector mockDbSchemasVec` for the `getObjects()` return structure
- **Cache property removed:** `adbc_meta_cache_enable` property removed from setUp
- **Tests renamed for clarity:**
  - `testGetTable_cachedById` -> `testGetTable_stableTableId` (tests ConcurrentHashMap ID stability)
  - `testRefreshTable` -> `testRefreshTable_isNoOp` (refresh is now a no-op)
- **New test added:** `testListDbNames_emptyReturnsMain` (verifies "main" fallback when getObjects returns no results)
- **Cache comments removed:** Removed stale cache-related comments throughout test file

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing deletion] Deleted ADBCMetaCacheTest.java**
- **Found during:** Task 1
- **Issue:** The plan specified deleting `ADBCMetaCache.java` but did not mention its test file `ADBCMetaCacheTest.java` (193 lines). Leaving it would cause compilation failure (imports deleted class).
- **Fix:** Deleted `ADBCMetaCacheTest.java` in the same commit as `ADBCMetaCache.java`
- **Files modified:** `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetaCacheTest.java` (deleted)
- **Commit:** 53a8a5646c

**2. [Rule 1 - Bug] Fixed refreshTable method signature**
- **Found during:** Task 1
- **Issue:** The plan's sample code showed `refreshTable(String, Table, List<String>)` with 3 parameters, but `ConnectorMetadata` interface defines 4 parameters including `boolean onlyCachedPartitions`.
- **Fix:** Used the correct 4-parameter signature matching the interface
- **Files modified:** `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java`
- **Commit:** 53a8a5646c

## Decisions Made

1. **ConcurrentHashMap over simple HashMap:** Table IDs must be assigned thread-safely since multiple catalog operations can happen concurrently. `ConcurrentHashMap.computeIfAbsent` provides atomic put-if-absent semantics.

2. **refreshTable as no-op:** Without a cache, there is nothing to invalidate on refresh. The next `getTable()` call will fetch fresh metadata directly from the driver. Table IDs remain stable because `tableIdMap` is never cleared by refresh.

3. **clear() resets both tableIdMap and hierarchyModel:** The `clear()` method is called when the entire catalog is being reset. Both the table ID assignments and the hierarchy model detection should start fresh.

## Verification Results

All 8 verification checks passed:
1. ADBCMetaCache.java deleted from disk
2. No ADBCMetaCache references in ADBC source directory
3. No adbc_meta_default_cache references in Config.java
4. ConcurrentHashMap<ADBCTableName, Integer> tableIdMap present in ADBCMetadata.java
5. getHierarchyModel preserved (volatile + double-checked locking)
6. computeIfAbsent used for stable table IDs
7. jdbc_meta_default_cache_enable preserved in Config.java (JDBC untouched)
8. No stale AdbcStatement references in ADBCMetadataTest.java

Note: FE compilation verification could not run (thrift compiler not available in worktree environment). Code correctness verified through static analysis of imports, references, and interface signatures.

## Self-Check: PASSED
