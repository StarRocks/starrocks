# Phase 6: PR Review Resolution & Test Hardening - Research

**Researched:** 2026-04-17
**Domain:** Java/C++ codebase cleanup, test refactoring, PR review comment resolution
**Confidence:** HIGH

## Summary

Phase 6 resolves 23 self-review comments on PR #1 (`feature/adbc-catalog-2`) by removing throwaway artifacts (smoke tests, legacy detection code, GSD requirement references, local build workarounds), deleting the `ADBCMetaCache` Caffeine caching layer, standardizing the `user` -> `username` property naming, refactoring FE test infrastructure to use pure mocks (mirroring the JDBC pattern), investigating BE scanner abstractions for simplification, and adding TODOs for future partition support.

This phase is entirely refactoring and deletion work. No new features, no API changes, no dependency bumps. The scope is well-defined by the 23 PR comments catalogued in `.planning/PR-REVIEW-PLAN.md` and the decisions locked in `06-CONTEXT.md`. The primary risk is regression -- each change group must be verified against the 24-test external `adbc_verification` suite at `/home/mete/coding/opensource/adbc_verification`.

**Primary recommendation:** Execute in 3-4 plans organized by dependency order: removals/cleanup first, then cache deletion, then test infrastructure refactoring, then BE investigation + TODOs. The `adbc_verification` suite is the regression gate after each plan.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Delete `ADBCMetaCache.java` entirely. No replacement caching. Every metadata call goes directly to the ADBC driver via native ADBC APIs.
- **D-02:** Remove `adbc_meta_cache_enable` and `adbc_meta_cache_expire_sec` from `Config.java`. No metadata cache configuration surface.
- **D-03:** Update `ADBCMetadata.java` to remove all 4 cache instance fields (`tableIdCache`, `tableInstanceCache`, `dbNamesCache`, `tableNamesCache`) and all cache-related logic.
- **D-04:** Pure mock for plan tests -- mirror JDBC pattern exactly. `MockedADBCMetadata` provides all metadata. `ConnectorPlanTestBase` registers `MockedADBCMetadata` without loading any real ADBC driver. No hardcoded driver paths.
- **D-05:** Delete `ADBCSQLiteIT.java`. Integration testing is handled by the external `adbc_verification` suite. No integration tests requiring a live driver in the FE test suite.
- **D-06:** Remove the hardcoded SQLite driver path from `ConnectorPlanTestBase.java:380-392`. ADBC plan tests should work identically to JDBC plan tests -- purely mocked, no driver `.so` needed.
- **D-07:** Change `"user"` -> `"username"` in `KNOWN_TOP_LEVEL_KEYS` set in `ADBCConnector.java`.
- **D-08:** Update `ADBCTable.java` property extraction to use `"username"` as the source key.
- **D-09:** Update all FE unit tests and `adbc_verification` Python tests to use `"username"` instead of `"user"`.
- **D-10:** The planner receives the 23 PR review comments as input but decides the optimal grouping and execution order. The PR-REVIEW-PLAN's 6-batch / 3-session structure is advisory, not binding.
- **D-11:** ADBC is already at 0.23.0 in `fe/pom.xml`. No version changes in this phase.
- **D-12:** SQL-based metadata is already replaced with native ADBC metadata APIs. Phase 6 only removes the caching layer.

### Claude's Discretion
- BE scanner simplification scope -- investigate `ArrowArrayStreamHolder`, `get_adbc_sql` filter generation, and `ADBCSchemaResolver` type conversion. Keep, simplify, or remove based on investigation findings.
- Commit grouping and message style -- follow the `[Refactor]` / `[BugFix]` / `[Doc]` PR title convention.
- `bin/start_backend.sh` line 109 -- investigate if the LD_LIBRARY_PATH change is necessary for ADBC. Revert if not.

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| GOAL-06 | Resolve all 23 PR #1 review comments, remove throwaway artifacts, delete cache, fix naming, fix test infrastructure, simplify BE, add TODOs, verify with adbc_verification suite | All 6 batches from PR-REVIEW-PLAN.md are covered by this research, with file-level analysis of every modification target |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Commit messages:** English, imperative, concise
- **PR titles:** `[BugFix]`, `[Feature]`, `[Enhancement]`, `[Refactor]`, `[UT]`, `[Doc]`, `[Tool]`
- **Protobuf/Thrift:** Fields must stay optional/repeated; never add `required`, never reuse ordinals
- **Config changes:** Must update matching docs in `docs/en/` and `docs/zh/` (but Phase 4 handles docs for this project)
- **Generated code:** Do not hand-edit `gensrc/` outputs
- **FE code style:** Checkstyle based on Google Java Style, 4-space indent, 130-char line limit
- **FE testing:** JUnit 5 (Jupiter) with JMockit for mocking (jmockit-1.49.4 via surefire)
- **FE imports:** Third-party first, then `java.*`, then static imports
- **BE code style:** `.clang-format`, `#pragma once`, Status/StatusOr for errors
- **License headers:** Apache 2.0 on all new files

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Cache removal | FE (Java) | -- | `ADBCMetaCache.java`, `Config.java`, `ADBCMetadata.java` are all FE code |
| Test infrastructure fix | FE (Java) | External (Python) | `MockedADBCMetadata`, `ConnectorPlanTestBase`, `ADBCSQLiteIT` are FE; `adbc_verification` is external Python |
| Property naming `user`->`username` | FE (Java) | External (Python) | `ADBCConnector`, `ADBCTable` are FE; `test_flightsql.py` is external |
| Smoke test / legacy removal | BE (C++) | FE (Java) | `adbc_smoke_test.cpp` is BE; legacy detection in `ADBCConnector.java` is FE |
| Build workaround revert | Build system | -- | `thirdparty/build-thirdparty.sh`, `.gitignore`, `bin/start_backend.sh` |
| BE scanner investigation | BE (C++) | -- | `ArrowArrayStreamHolder`, `get_adbc_sql` in BE scanner/connector |
| Schema resolver investigation | FE (Java) | -- | `ADBCSchemaResolver.java` type conversion |
| Partition TODOs | FE (Java) | -- | `ADBCPartitionKey.java`, `ADBCPartitionTraits.java` |

## Standard Stack

This phase introduces no new dependencies. All work uses existing project tooling.

### Core (unchanged)
| Library | Version | Purpose | Source |
|---------|---------|---------|--------|
| JMockit | 1.49.4 | FE unit test mocking framework | [VERIFIED: fe-core/pom.xml:1122] |
| JUnit 5 | (project version) | FE test framework | [VERIFIED: ADBCMetadataTest.java imports] |
| Caffeine | (project version) | Cache library being REMOVED | [VERIFIED: ADBCMetaCache.java imports] |
| GTest | (project version) | BE unit test framework being removed for ADBC smoke tests | [VERIFIED: adbc_smoke_test.cpp] |

### External Verification
| Tool | Location | Purpose |
|------|----------|---------|
| adbc_verification | `/home/mete/coding/opensource/adbc_verification` | 24-test Python suite, regression gate | [VERIFIED: exists on filesystem] |

## Architecture Patterns

### System Architecture Diagram

```
PR #1 Review Comments (23 items)
        |
        v
+-------+--------+   +---------+---------+   +--------+--------+
| Batch 1: Delete | | Batch 2: Cache    | | Batch 3: Rename |
| smoke tests,    | | ADBCMetaCache     | | user->username  |
| legacy code,    | | Config.java       | | across FE+Py    |
| GSD refs,       | | ADBCMetadata      | |                 |
| build workarounds| |                   | |                 |
+-----------------+ +-------------------+ +-----------------+
        |                   |                       |
        v                   v                       v
+-----------------+ +-------------------+ +-----------------+
| Batch 4: Test   | | Batch 5: BE       | | Batch 6: TODOs  |
| infrastructure  | | scanner           | | partition files  |
| - delete IT     | | investigation     | |                 |
| - fix mock      | | - ArrowStreamHold.| |                 |
| - pure mock     | | - get_adbc_sql    | |                 |
+-----------------+ | - SchemaResolver  | +-----------------+
                    +-------------------+
        |                   |                       |
        +-------------------+-----------------------+
                            |
                            v
                  adbc_verification suite
                  (24 tests, regression gate)
```

### Recommended Grouping Strategy

The PR-REVIEW-PLAN.md's 6-batch structure maps to 3-4 plans based on file-level dependencies:

**Plan 1: Removals and Cleanup** (Batches 1 + 3 merged)
- Delete: `adbc_smoke_test.cpp`, `ADBC_SMOKE_TEST_README.md`, CMakeLists entries
- Delete: Legacy v1 detection block in `ADBCConnector.java:59-67`
- Remove: `.planning/` from `.gitignore`
- Remove: GSD requirement references from ADBC comments (FE + BE)
- Revert: `thirdparty/build-thirdparty.sh` local `-Wno-*` workarounds
- Investigate + possibly revert: `bin/start_backend.sh:109`
- Rename: `user` -> `username` in `KNOWN_TOP_LEVEL_KEYS`, `ADBCTable`, `ADBCConnector.openDatabase()`, tests, adbc_verification

**Plan 2: Cache Removal** (Batch 2)
- Delete: `ADBCMetaCache.java` entirely
- Remove: `adbc_meta_default_cache_enable` and `adbc_meta_default_cache_expire_sec` from `Config.java`
- Rewrite: `ADBCMetadata.java` to call ADBC APIs directly (remove 4 cache fields, inline direct calls)
- Update: `ADBCMetadataTest.java` to remove cache expectations, update mocked paths for getObjects-based metadata

**Plan 3: Test Infrastructure** (Batch 4)
- Delete: `ADBCSQLiteIT.java`
- Fix: `ConnectorPlanTestBase.mockADBCCatalogImpl()` to remove hardcoded driver path, use pure mock pattern
- Verify: `MockedADBCMetadata` is sufficient as-is (already implements ConnectorMetadata without real driver)

**Plan 4: BE Investigation + TODOs** (Batches 5 + 6)
- Investigate: `ArrowArrayStreamHolder` -- keep (it is RAII-critical per BE-08)
- Investigate: `get_adbc_sql` -- keep (functional, mirrors JDBC pattern)
- Investigate: `ADBCSchemaResolver` duplication -- keep (no duplication found, self-contained)
- Add: TODOs to `ADBCPartitionKey.java` and `ADBCPartitionTraits.java`

### Pattern: JDBC Mock Reference (for Test Infrastructure Fix)

The JDBC connector uses a pure-mock pattern that ADBC must mirror [VERIFIED: ConnectorPlanTestBase.java, MockedJDBCMetadata.java]:

```java
// JDBC mock registration -- NO real JDBC driver loaded
// Source: ConnectorPlanTestBase.java:362-377
private static void mockJDBCCatalogImpl(MockedMetadataMgr metadataMgr) throws DdlException {
    Map<String, String> properties = ImmutableMap.<String, String>builder()
            .put(JDBCResource.TYPE, "jdbc")
            .put(JDBCResource.DRIVER_URL, "xxxx")    // <-- dummy, never loaded
            .put(JDBCResource.CHECK_SUM, "xxxx")      // <-- dummy
            // ...
            .build();
    GlobalStateMgr.getCurrentState().getCatalogMgr().
            createCatalog("jdbc", MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME, "", properties);
    metadataMgr.registerMockedMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME,
            new MockedJDBCMetadata(properties));
}
```

The ADBC version (currently broken at ConnectorPlanTestBase.java:380-392) has a hardcoded path:
```java
// CURRENT (broken) -- hardcoded to developer's machine
String sqliteDriverPath = System.getProperty("adbc.sqlite.driver.path",
        "/home/mete/miniconda3/etc/adbc/drivers/sqlite_linux_amd64_v1.10.0/libadbc_driver_sqlite.so");
```

The fix mirrors JDBC: use dummy property values, never load a real driver:
```java
// FIXED -- pure mock, no real driver
private static void mockADBCCatalogImpl(MockedMetadataMgr metadataMgr) throws DdlException {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("type", "adbc");
    properties.put("driver_url", "/mock/path/libadbc_driver.so");  // dummy, never loaded
    properties.put("uri", ":memory:");
    GlobalStateMgr.getCurrentState().getCatalogMgr().
            createCatalog("adbc", MockedADBCMetadata.MOCKED_ADBC_CATALOG_NAME, "", properties);
    metadataMgr.registerMockedMetadata(MockedADBCMetadata.MOCKED_ADBC_CATALOG_NAME,
            new MockedADBCMetadata(properties));
}
```

[VERIFIED: This pattern is exactly what MockedJDBCMetadata uses -- `ConnectorPlanTestBase.java:362-377`]

### Pattern: Cache Removal from ADBCMetadata

Current `ADBCMetadata` wraps every metadata call in `cache.get(key, lambda)`. After cache removal, calls go directly to ADBC:

```java
// CURRENT (with cache):
public List<String> listDbNames(ConnectContext context) {
    return dbNamesCache.get(catalogName, k -> {
        try (AdbcConnection conn = adbcDatabase.connect()) {
            HierarchyModel model = getHierarchyModel(conn);
            return listDbNamesFromGetObjects(conn, model);
        } catch (Exception e) {
            throw new StarRocksConnectorException(...);
        }
    });
}

// AFTER (direct calls):
public List<String> listDbNames(ConnectContext context) {
    try (AdbcConnection conn = adbcDatabase.connect()) {
        HierarchyModel model = getHierarchyModel(conn);
        return listDbNamesFromGetObjects(conn, model);
    } catch (Exception e) {
        throw new StarRocksConnectorException(...);
    }
}
```

The same unwrapping applies to `listTableNames()` and `getTable()`.

**Important:** The `tableIdCache` is a permanent cache (never expires) used to assign stable IDs to tables. After cache removal, a simple `ConcurrentHashMap<ADBCTableName, Integer>` replaces it, since table IDs must remain stable for the catalog's lifetime but do not need Caffeine's features.

### Pattern: user -> username Rename

Three code locations and one external test file need updating:

1. **`ADBCConnector.java:44`** -- `KNOWN_TOP_LEVEL_KEYS`: change `"user"` to `"username"`
2. **`ADBCConnector.java:154-158`** -- `openDatabase()`: remove `user` -> `username` translation since users now write `"username"` directly. Read `properties.get("username")` and put as `"username"` in params.
3. **`ADBCTable.java:100-103`** -- `toThrift()`: change `properties.get("user")` to `properties.get("username")`, remove the key translation.
4. **`adbc_verification/tests/test_flightsql.py`** -- 5 occurrences of `"user": "flight_username"` -> `"username": "flight_username"`

**BE side is NOT affected.** The BE `adbc_connector.cpp:100` already reads `ctx.adbc_options.find("username")` -- the FE's `ADBCTable.toThrift()` was already translating `user` to `username` in the options map. After this change, the FE stores `username` directly, so no BE change is needed.

### Anti-Patterns to Avoid
- **Leaving orphan imports:** When deleting `ADBCMetaCache.java`, ensure `ADBCMetadata.java` no longer imports Caffeine or the deleted class.
- **Breaking the ADBC connector constructor:** `ADBCConnector` currently calls `new ADBCMetadata(properties, catalogName, allocator, db)`. After cache removal, the `ADBCMetadata` constructor must still work without cache parameters.
- **Removing HierarchyModel during cache removal:** The `HierarchyModel` (CATALOG vs SCHEMA) resolution is NOT cache -- it is a lazy-initialized field with `volatile` + double-checked locking. It MUST be preserved. Only the 4 Caffeine-backed caches are removed.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Table ID stability after cache removal | Custom LRU cache | `ConcurrentHashMap<ADBCTableName, Integer>` | Only need put-if-absent + stable reads; no expiry, no size limit |
| Test metadata mocking | Real ADBC driver in tests | `MockedADBCMetadata` (already exists) | Tests must run in CI without native `.so` files |
| Arrow type conversion | Duplicate converter | Existing `ADBCSchemaResolver` | Already handles all needed types, no duplication found |

## Common Pitfalls

### Pitfall 1: ADBCMetadataTest Uses Stale Mock Expectations
**What goes wrong:** After removing the cache from `ADBCMetadata`, the test's mock expectations for `createStatement/executeQuery` are wrong because `ADBCMetadata` no longer uses SQL-based metadata -- it uses `getObjects()` and `getTableSchema()`.
**Why it happens:** The current `ADBCMetadataTest` was written when `ADBCMetadata` used SQL queries (`SELECT schema_name FROM ...`). But the code was rewritten to use native ADBC `getObjects()` API. The mocks expect `createStatement()` but the code calls `conn.getObjects()`.
**How to avoid:** After cache removal, update mock expectations to match the `getObjects()`-based code path. Mock `AdbcConnection.getObjects()` and `AdbcConnection.getTableSchema()` instead of `createStatement/executeQuery`.
**Warning signs:** Tests fail with "unexpected invocation" or "missing mock definition" for `getObjects()`.

### Pitfall 2: refreshTable and clear Break After Cache Removal
**What goes wrong:** `ADBCMetadata.refreshTable()` calls `tableInstanceCache.invalidate()` and `clear()` calls `invalidateAll()` on all caches. Without the caches, these methods need rewriting.
**Why it happens:** Cache invalidation is the current mechanism for table refresh. Without caches, the concept changes -- "refresh" means nothing when there is no cache.
**How to avoid:** After removing caches, `refreshTable()` becomes a no-op or resets the internal `tableId` map. `clear()` resets `hierarchyModel` to null (already done) and clears the `tableId` map.
**Warning signs:** `NullPointerException` on cache fields, or `refreshTable` tests failing.

### Pitfall 3: GSD Reference Removal Must Not Remove Legitimate Comments
**What goes wrong:** Removing all `D-01`, `META-05`, `PROP-*`, `BE-*`, `VAL-*` references from comments may accidentally remove useful design rationale.
**Why it happens:** Some comments explain WHY code exists (e.g., "per-fragment connection, never cached (per BE-03)"). The requirement ID is the only part to remove; the rationale should stay.
**How to avoid:** For each GSD reference, keep the rationale text and remove only the `(per D-XX)` / `(per BE-XX)` tag. Example: `// per-fragment, never cached (per BE-03)` becomes `// per-fragment, never cached`.
**Warning signs:** Comments become meaningless stubs like `// (per D-01)` removed to just `//`.

### Pitfall 4: build-thirdparty.sh Revert Must Be Surgical
**What goes wrong:** The `-Wno-array-bounds` and `-Wno-stringop-overread` flags in rocksdb (line 780) and the `-Wno-stringop-overflow -Wno-array-bounds` flags in flatbuffers (line 846) may have been added for legitimate build reasons on the developer's toolchain but are not needed for the PR.
**Why it happens:** GCC version differences cause spurious warnings that break `-Werror` builds. The developer added workarounds locally.
**How to avoid:** Compare the flags on line 780 and 846 against the `main` branch. Only revert flags that are not present on `main`. Some flags may exist in upstream already.
**Warning signs:** Build fails after revert on certain GCC versions.

### Pitfall 5: BE Scanner ArrowArrayStreamHolder Is Load-Bearing
**What goes wrong:** Investigation of `ArrowArrayStreamHolder` in `adbc_scanner.h:72` may conclude it should be simplified or removed, but it is the RAII wrapper mandated by BE-08.
**Why it happens:** The name "holder" sounds like unnecessary abstraction, but it owns the C Data Interface `ArrowArrayStream` lifecycle and ensures `stream.release()` is called exactly once.
**How to avoid:** Do NOT remove or simplify `ArrowArrayStreamHolder`. It is defined in `exec/adbc_arrow_raii.h` and is the RAII wrapper required by BE-08. Investigation should confirm it is correct and document that finding.
**Warning signs:** Memory leaks or double-free in Arrow C Data Interface boundary.

### Pitfall 6: Config.java Removal Must Not Break Config Loading Order
**What goes wrong:** Removing `adbc_meta_default_cache_enable` and `adbc_meta_default_cache_expire_sec` from `Config.java` is safe only if no other code references these fields.
**Why it happens:** Caffeine cache initialization in `ADBCMetaCache` reads from `Config.adbc_meta_default_cache_enable`. If `ADBCMetaCache` is deleted first, Config fields become dead code. If Config fields are deleted first while `ADBCMetaCache` still exists, compilation fails.
**How to avoid:** Delete `ADBCMetaCache.java` and the Config fields in the same commit. Also grep for any other references to `adbc_meta_cache` or `adbc_meta_default_cache` across the codebase.
**Warning signs:** Compilation error on `Config.adbc_meta_default_cache_enable`.

## Code Examples

### Cache Removal: ADBCMetadata Constructor After Fix

```java
// Source: Derived from current ADBCMetadata.java + D-01/D-02/D-03
public ADBCMetadata(Map<String, String> properties, String catalogName,
                    BufferAllocator allocator, AdbcDatabase adbcDatabase) {
    this.catalogName = catalogName;
    this.properties = properties;
    this.allocator = allocator;
    this.adbcDatabase = adbcDatabase;
    this.schemaResolver = new ADBCSchemaResolver();
    // tableIdMap replaces the permanent Caffeine cache for stable IDs
    this.tableIdMap = new ConcurrentHashMap<>();
}
```

### ConnectorPlanTestBase Fix: Pure Mock Registration

```java
// Source: Modeled after mockJDBCCatalogImpl at ConnectorPlanTestBase.java:362-377
private static void mockADBCCatalogImpl(MockedMetadataMgr metadataMgr) throws DdlException {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("type", "adbc");
    properties.put("driver_url", "/mock/path/libadbc_driver.so");
    properties.put("uri", ":memory:");
    GlobalStateMgr.getCurrentState().getCatalogMgr().
            createCatalog("adbc", MockedADBCMetadata.MOCKED_ADBC_CATALOG_NAME, "", properties);
    metadataMgr.registerMockedMetadata(MockedADBCMetadata.MOCKED_ADBC_CATALOG_NAME,
            new MockedADBCMetadata(properties));
}
```

### Partition TODO Example

```java
// Source: Modeled on PR-REVIEW-PLAN.md Batch 6
// In ADBCPartitionKey.java:
public class ADBCPartitionKey extends PartitionKey implements NullablePartitionKey {
    // TODO: implement ADBC partition support when drivers expose partitioning metadata.
    //       Reference: JDBC partition support for PostgreSQL in JDBCPartitionKey.java.
    public ADBCPartitionKey() {
        super();
    }
    // ...
}
```

## BE Scanner Investigation Findings

### ArrowArrayStreamHolder (adbc_scanner.h:72)
**Finding: KEEP.** This is the RAII wrapper for the Arrow C Data Interface `ArrowArrayStream`. It is defined in `exec/adbc_arrow_raii.h` and ensures the stream's `release` callback is invoked exactly once on destruction. Removing it would violate BE-08 and introduce memory safety bugs. The wrapper is minimal and correct. [VERIFIED: adbc_scanner.h, adbc_arrow_raii.h]

### get_adbc_sql (adbc_connector.cpp:29-47)
**Finding: KEEP.** This function assembles a SQL query string from the scan node's table name, columns, filters, and limit. It mirrors the JDBC connector's query construction pattern. It is functional, tested implicitly through the `adbc_verification` suite, and is the mechanism by which StarRocks pushes predicates down to ADBC drivers. Removing it would break all ADBC scans. [VERIFIED: adbc_connector.cpp:29-47, used at line 110]

### ADBCSchemaResolver (ADBCSchemaResolver.java)
**Finding: KEEP, no duplication.** The `ADBCSchemaResolver` converts Arrow `Schema` (from ADBC `getTableSchema()`) to StarRocks `Column` definitions. A search for similar Arrow-to-StarRocks type conversion found only `ArrowFlightSqlConnectProcessor.java` -- but that file converts StarRocks types TO Arrow for the outbound Flight SQL server (opposite direction). There is no duplicate inbound Arrow->StarRocks converter. The `ADBCSchemaResolver` is self-contained and correct. [VERIFIED: grep for convertArrow/ArrowType across fe-core, only 3 files found, none duplicate]

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| SQL-based metadata (`information_schema` queries) | Native ADBC `getObjects()` / `getTableSchema()` | Phase 2 rewrite | Cache removal is purely removing Caffeine layer, not changing metadata approach |
| `ADBCMetaCache` with Caffeine | Direct ADBC calls (this phase) | Phase 6 | Simplifies code, removes Config surface, eliminates stale cache bugs |
| `user` property with internal translation | `username` property (ADBC standard) | Phase 6 | Aligns with ADBC spec naming |
| IT tests with real SQLite driver in FE | Pure mocks + external `adbc_verification` | Phase 6 | Tests run in CI without native libraries |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `bin/start_backend.sh:109` is the standard LD_LIBRARY_PATH line (JAVA_HOME/lib/server + STARROCKS_HOME/lib) and is NOT an ADBC-specific change | BE Scanner Investigation | If it IS an ADBC change, reverting it would break ADBC driver loading at runtime. Investigation step verifies this against main branch. |
| A2 | The rocksdb `-Wno-array-bounds -Wno-stringop-overread` and flatbuffers `-Wno-stringop-overflow -Wno-array-bounds` flags at lines 780 and 846 of `build-thirdparty.sh` are local workarounds not present on `main` | Common Pitfalls | If present on `main`, removing them would not change anything. Low risk. |

**Note:** A1 was partially verified -- reading `start_backend.sh:109` shows the standard `LD_LIBRARY_PATH=$STARROCKS_HOME/lib:$JAVA_HOME/lib/server:$JAVA_HOME/lib:$LD_LIBRARY_PATH` line which appears to be the standard path. The investigation step should diff against `main` to confirm.

## Open Questions (RESOLVED)

1. **ADBCMetadataTest mock expectations after cache removal**
   - What we know: Current tests mock `createStatement/executeQuery` for a SQL-based metadata path, but the actual code uses `getObjects()` / `getTableSchema()`. This mismatch means the tests may already be testing stale code paths.
   - What's unclear: Whether the JMockit `@Mocked AdbcConnection` automatically mocks `getObjects()` (JMockit mocks all methods by default) or if explicit Expectations need to be set up for the getObjects-based path.
   - Recommendation: The planner should instruct updating mock expectations to match the `getObjects()`-based code path. Since JMockit `@Mocked` mocks all methods, the key change is setting up `Expectations` for `getObjects()` return values instead of `createStatement()`.
   - **RESOLVED:** Plan 06-03 Task 2 instructs updating mock expectations to use `getObjects()`/`getTableSchema()` instead of `createStatement()`. JMockit `@Mocked` mocks all methods by default, so the key change is setting up new `Expectations` for the getObjects-based path.

2. **fe-core/pom.xml surefire ADBC driver config (Batch 4c)**
   - What we know: The PR-REVIEW-PLAN mentions "move ADBC driver config to `fe.conf`" at `fe-core/pom.xml:1128`, but reading pom.xml shows this is the `arrow.memory.debug.allocator=true` JVM arg, not an ADBC driver config.
   - What's unclear: Whether there is a separate surefire configuration for the ADBC driver path, or if the review comment refers to the `system property` approach in `ADBCSQLiteIT` (`@EnabledIfSystemProperty(named = "adbc.sqlite.driver.path")`).
   - Recommendation: Since `ADBCSQLiteIT.java` is being deleted entirely (D-05), and the pure mock approach (D-04/D-06) eliminates the need for any driver path configuration, Batch 4c may be fully resolved by the delete + mock fix. No `fe.conf` change needed.
   - **RESOLVED:** Plan 06-02 Task 2 deletes `ADBCSQLiteIT.java` entirely (D-05) and rewrites `ConnectorPlanTestBase.mockADBCCatalogImpl()` to use pure mocks with dummy properties (D-04/D-06). This eliminates the need for any driver path configuration in surefire or `fe.conf`. Batch 4c is fully resolved by the delete + mock fix.

## File Inventory

### Files to DELETE (5 files)
| File | Reason |
|------|--------|
| `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetaCache.java` | D-01: cache layer removed |
| `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCSQLiteIT.java` | D-05: replaced by external suite |
| `be/test/exec/adbc_smoke_test.cpp` | Throwaway, replaced by adbc_verification |
| `be/test/exec/ADBC_SMOKE_TEST_README.md` | Documentation for deleted test |
| (CMakeLists entry, not a file) | Lines 46-54 in `be/test/exec/CMakeLists.txt` |

### Files to MODIFY (major changes)
| File | Change | Lines Affected |
|------|--------|----------------|
| `fe/.../ADBCMetadata.java` | Remove 4 cache fields, inline direct calls, keep HierarchyModel | ~40 lines changed |
| `fe/.../ADBCConnector.java` | Remove legacy detection (59-67), GSD refs, `user`->`username` | ~20 lines changed |
| `fe/.../ADBCTable.java` | `user`->`username` in toThrift | ~5 lines changed |
| `fe/.../Config.java` | Remove 2 `adbc_meta_*` entries | 4 lines deleted |
| `fe/.../ADBCMetadataTest.java` | Update mocks for cache-less path, remove cache expectations | ~30 lines changed |
| `fe/.../ConnectorPlanTestBase.java` | Remove hardcoded driver path, use dummy props | ~10 lines changed |
| `be/test/exec/CMakeLists.txt` | Remove lines 46-54 (smoke test target) | 9 lines deleted |
| `.gitignore` | Remove `.planning/` entry (lines 126-127) | 2 lines deleted |

### Files to MODIFY (minor changes)
| File | Change |
|------|--------|
| `thirdparty/build-thirdparty.sh` | Revert `-Wno-*` flags at lines 780, 846 (if not on main) |
| `fe/.../ADBCPartitionKey.java` | Add TODO comment |
| `fe/.../ADBCPartitionTraits.java` | Add TODO comment |
| `adbc_verification/tests/test_flightsql.py` | `"user"` -> `"username"` (5 occurrences) |

### Files to INVESTIGATE
| File | Question | Expected Outcome |
|------|----------|-----------------|
| `bin/start_backend.sh:109` | Is LD_LIBRARY_PATH change ADBC-specific? | Likely standard -- revert if different from main |
| `be/src/exec/adbc_scanner.h:72` | Is ArrowArrayStreamHolder over-abstracted? | KEEP -- RAII is load-bearing |
| `be/src/connector/adbc_connector.cpp:29-47` | Is get_adbc_sql functional? | KEEP -- mirrors JDBC, pushes predicates |
| `fe/.../ADBCSchemaResolver.java` | Duplicate type conversion? | NO duplication found -- KEEP as-is |

### BE files with GSD references to clean
| File | References to Remove |
|------|---------------------|
| `be/src/exec/adbc_scanner.cpp` | BE-05, BE-06, BE-02, BE-03 tags |
| `be/src/exec/adbc_scanner.h` | D-01, D-08, BE-08 tags |
| `be/src/connector/adbc_connector.cpp` | D-04, D-05, D-01 tags |
| `be/src/exec/adbc_driver_registry.h` | BE-05, BE-06 tags |

### FE files with GSD references to clean
| File | References to Remove |
|------|---------------------|
| `fe/.../ADBCConnector.java` | PROP-04, D-01, META-05, D-12, D-03, VAL-01, VAL-02, META-02, PROP-03, PROP-05, PROP-06, VAL-03 tags |

## Verification Protocol

### Regression Gate
After each plan (wave of commits), run:
```bash
cd /home/mete/coding/opensource/adbc_verification
python -m pytest tests/ -v --tb=short
```
All 24 tests must pass. The suite covers:
- SQLite catalog lifecycle (create, list dbs, list tables, schema, data)
- DuckDB catalog operations
- FlightSQL with Docker (if available)
- PostgreSQL with Docker (if available)
- Cross-driver joins
- Negative/error cases

### FE Unit Tests
After FE changes:
```bash
cd /home/mete/coding/opensource/starrocks/fe
mvn test -pl fe-core -Dtest=com.starrocks.connector.adbc.ADBCMetadataTest -am
mvn test -pl fe-core -Dtest=com.starrocks.connector.adbc.ADBCConnectorTest -am
```

### Build Verification
After all changes:
```bash
./build.sh --fe
./build.sh --be
```

## Sources

### Primary (HIGH confidence)
- Source files read directly from the codebase (all file contents verified via Read tool)
- `ADBCConnector.java`, `ADBCMetaCache.java`, `ADBCMetadata.java`, `ADBCTable.java` -- current implementation
- `ADBCSchemaResolver.java` -- type conversion (no duplication confirmed)
- `ConnectorPlanTestBase.java:380-392` -- broken mock registration with hardcoded path
- `MockedJDBCMetadata.java` -- reference pattern for pure mocking
- `ADBCMetadataTest.java` -- current test expectations (JMockit-based)
- `ADBCSQLiteIT.java` -- integration test to delete
- `adbc_smoke_test.cpp` -- C++ smoke test to delete
- `be/test/exec/CMakeLists.txt:46-54` -- smoke test CMake target to delete
- `adbc_connector.cpp` -- `get_adbc_sql` implementation and GSD references
- `adbc_scanner.h` -- `ArrowArrayStreamHolder` usage
- `Config.java:3996-3999` -- `adbc_meta_*` config entries to remove
- `.gitignore:126-127` -- `.planning/` entry to remove
- `thirdparty/build-thirdparty.sh:780,846` -- local `-Wno-*` workarounds
- `bin/start_backend.sh:109` -- LD_LIBRARY_PATH line (standard, not ADBC-specific)
- `ADBCPartitionKey.java`, `ADBCPartitionTraits.java` -- TODO targets
- `adbc_verification/tests/test_flightsql.py` -- 5 `"user"` occurrences to rename

### Secondary (MEDIUM confidence)
- `.planning/PR-REVIEW-PLAN.md` -- 23 review comments organized into 6 batches
- `.planning/phases/06-pr-review-resolution-test-hardening/06-CONTEXT.md` -- locked decisions

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies, purely using existing project tools
- Architecture: HIGH -- all file targets identified and read, modification scope is clear
- Pitfalls: HIGH -- each pitfall derived from actual code inspection, not theoretical

**Research date:** 2026-04-17
**Valid until:** 2026-05-17 (stable -- this is refactoring of existing code, not fast-moving ecosystem)
