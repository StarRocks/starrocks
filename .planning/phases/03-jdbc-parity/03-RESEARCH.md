# Phase 3: JDBC Parity - Research

**Researched:** 2026-03-06
**Domain:** Materialized Views, Column Statistics, Partition Traits for ADBC External Connector
**Confidence:** HIGH

## Summary

Phase 3 delivers three capabilities for the ADBC connector: materialized view support (create, full refresh, query rewrite), column statistics for optimizer cost estimation, and minimal partition traits registration. The good news is that most of the heavy lifting is already done -- ADBC is already in `MaterializedViewAnalyzer.SUPPORTED_TABLE_TYPE`, the scan path from Phase 2 handles data flow, and the statistics/MV infrastructure is well-established with JDBC as a direct template.

The primary work is: (1) registering `ADBCPartitionTraits` in `ConnectorPartitionTraits.TRAITS_TABLE` so the MV framework recognizes ADBC tables, (2) implementing `getTableStatistics()` in `ADBCMetadata` to feed the optimizer, and (3) implementing `listPartitionNames()` and `getPartitions()` for minimal partition support. Statistics collection via `ANALYZE TABLE` should work automatically since it runs SQL queries through the existing ADBC scan path.

**Primary recommendation:** Mirror JDBC partition traits exactly (but with `isSupportPCTRefresh() = false`), implement `getTableStatistics()` by pushing aggregate SQL to remote, and verify MV create/refresh/rewrite works end-to-end via unit tests modeled on `MvRefreshAndRewriteJDBCTest`.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Collect column stats via remote SQL queries pushed through ADBC (COUNT(*), MIN(col), MAX(col), APPROX_COUNT_DISTINCT(col), COUNT(col) for null count)
- ANALYZE TABLE runs full scan -- push complete aggregate queries to remote, no sampling
- Use standard ANSI SQL only for stats queries -- no driver-specific function overrides. If remote doesn't support APPROX_COUNT_DISTINCT, skip NDV
- Cache collected stats locally with configurable TTL (same pattern as JDBC stats caching)
- Statistics feed into optimizer via `getTableStatistics()` in ADBCMetadata
- Full refresh only -- no incremental PCT refresh for ADBC tables
- Both scheduled (REFRESH EVERY interval) and manual (REFRESH MATERIALIZED VIEW) supported
- Automatic query rewrite enabled -- optimizer can rewrite queries against ADBC tables to use MVs
- MV creation, refresh, and rewrite should work out of the box since `Table.TableType.ADBC` is already in `SUPPORTED_TABLE_TYPE`
- Implement `listPartitionNames()` in ADBCMetadata -- discover partition names from remote
- Create `ADBCPartitionTraits` extending `DefaultTraits` and register in `ConnectorPartitionTraits.TRAITS_TABLE`
- No partition pruning logic -- tables remain effectively unpartitioned for query planning
- Foundation for future PCT incremental refresh without the complexity of pruning
- PART-01 and PART-02 addressed minimally; PART-03 (pruning) deferred

### Claude's Discretion
- Stats cache TTL default value and configuration property names
- ADBCPartitionTraits implementation details (which methods to stub vs implement)
- Exact stats SQL query generation approach
- How to handle stats collection failures gracefully
- MV test coverage scope

### Deferred Ideas (OUT OF SCOPE)
- PCT incremental MV refresh -- requires robust partition change detection, pair with full partition pruning
- PART-03 (partition pruning) -- deferred until PCT or explicit need
- Connection pooling for stats queries -- reuse connection-per-operation for now
- Driver-specific stats SQL overrides -- start with standard SQL, add driver hooks if needed
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| MV-01 | User can create materialized view against ADBC external tables | Already enabled: ADBC in SUPPORTED_TABLE_TYPE. Needs ADBCPartitionTraits registration. |
| MV-02 | Async MV refresh fetches data from ADBC tables | MV refresh runs MV query as INSERT OVERWRITE; uses existing ADBC scan path. Needs partition traits. |
| MV-03 | Partition change tracking (PCT) enables incremental MV refresh | **DEFERRED per CONTEXT.md** -- ADBCPartitionTraits returns `isSupportPCTRefresh() = false` |
| MV-04 | Query optimizer rewrites queries to use MVs over ADBC tables | Automatic once MV is created and refreshed; no ADBC-specific code needed. |
| STAT-01 | Column statistics (min, max, ndv, null count) collected from ADBC metadata | Implement `getTableStatistics()` in ADBCMetadata; push aggregate SQL to remote. |
| STAT-02 | Row count estimation available for optimizer cost calculations | Part of `getTableStatistics()` -- COUNT(*) pushed to remote. |
| PART-01 | ADBC connector discovers partitions from remote source metadata | Minimal: `listPartitionNames()` returns empty list (no remote partition discovery protocol in ADBC). |
| PART-02 | Partition names are listed and cached | Minimal: return empty list, cached via existing ADBCMetaCache. |
| PART-03 | Partition pruning filters irrelevant partitions during query planning | **DEFERRED per CONTEXT.md** -- no pruning logic. |
</phase_requirements>

## Architecture Patterns

### Recommended Project Structure

All new files go in existing packages:

```
fe/fe-core/src/main/java/com/starrocks/
├── connector/
│   ├── adbc/
│   │   └── ADBCMetadata.java          # MODIFY: add getTableStatistics(), listPartitionNames(), getPartitions()
│   └── partitiontraits/
│       └── ADBCPartitionTraits.java    # NEW: extends DefaultTraits
│   └── ConnectorPartitionTraits.java   # MODIFY: add ADBC to TRAITS_TABLE
├── catalog/
│   └── ADBCTable.java                 # MODIFY: add getCatalogTableName()
fe/fe-core/src/test/java/com/starrocks/
├── connector/adbc/
│   └── ADBCMetadataTest.java          # MODIFY: add stats tests
```

### Pattern 1: Partition Traits Registration

**What:** Every connector that supports MV must register a PartitionTraits class in `ConnectorPartitionTraits.TRAITS_TABLE`.
**When to use:** Required for MV create/refresh to work.
**Example:**

```java
// In ConnectorPartitionTraits.java TRAITS_TABLE builder:
.put(Table.TableType.ADBC, ADBCPartitionTraits::new)

// ADBCPartitionTraits.java:
public class ADBCPartitionTraits extends DefaultTraits {
    @Override
    public String getTableName() {
        ADBCTable adbcTable = (ADBCTable) table;
        return adbcTable.getCatalogTableName();
    }

    @Override
    public boolean isSupportPCTRefresh() {
        return false;  // Full refresh only
    }

    @Override
    public PartitionKey createEmptyKey() {
        return new NullablePartitionKey();  // Stub -- no real partition keys
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        return Lists.newArrayList();  // No partitions
    }
}
```

### Pattern 2: Statistics via getTableStatistics()

**What:** Override `ConnectorMetadata.getTableStatistics()` to provide optimizer statistics inline. This is separate from `ANALYZE TABLE`.
**When to use:** Every query plan consults this method for cost estimation.
**Example:**

```java
// In ADBCMetadata.java:
@Override
public Statistics getTableStatistics(OptimizerContext session,
                                     Table table,
                                     Map<ColumnRefOperator, Column> columns,
                                     List<PartitionKey> partitionKeys,
                                     ScalarOperator predicate,
                                     long limit,
                                     TvrVersionRange tableVersionRange) {
    Statistics.Builder builder = Statistics.builder();
    // Push COUNT(*) to remote for row count
    long rowCount = getRowCountFromRemote(table);
    builder.setOutputRowCount(rowCount);

    for (Map.Entry<ColumnRefOperator, Column> entry : columns.entrySet()) {
        ColumnStatistic colStat = getColumnStatFromRemote(table, entry.getValue());
        builder.addColumnStatistic(entry.getKey(), colStat);
    }
    return builder.build();
}
```

### Pattern 3: ANALYZE TABLE flow (no ADBC-specific code needed)

**What:** `ANALYZE TABLE my_adbc.db.tbl` runs `ExternalFullStatisticsCollectJob` which executes SQL queries like `SELECT COUNT(1), MIN(col), MAX(col) FROM catalog.db.table`. This SQL flows through the normal ADBC scan path.
**When to use:** Automatic -- the existing ANALYZE infrastructure works for any external catalog.
**Key insight:** ANALYZE TABLE does NOT call `getTableStatistics()`. It runs real SQL queries and stores results in StarRocks' internal statistics tables. The `getTableStatistics()` method is for inline optimizer hints.

### Anti-Patterns to Avoid
- **Implementing PCT refresh prematurely:** CONTEXT explicitly defers this. `isSupportPCTRefresh()` must return `false`.
- **Custom ADBC metadata API for stats:** Don't use Arrow Flight SQL's GetFlightInfo metadata for statistics -- it's unreliable. Push SQL queries instead.
- **Separate stats connection pool:** Reuse the existing connection-per-operation pattern from ADBCMetadata.
- **Modifying MV refresh infrastructure:** MV refresh already works for external tables -- just register the traits and it works.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| MV refresh data flow | Custom ADBC MV refresh logic | Existing MV refresh (INSERT OVERWRITE via scan) | MV refresh runs the MV query through normal scan path |
| Stats storage | Custom stats storage for ADBC | StarRocks internal stats tables via ANALYZE | `ExternalFullStatisticsCollectJob` handles this |
| Partition traits framework | Custom partition detection | `DefaultTraits` base class | All connectors extend it; provides sensible defaults |
| Stats caching | Custom cache for stats | Extend existing `ADBCMetaCache` | Already has TTL, enable/disable via properties |

**Key insight:** Phase 3's scope is primarily "registration and wiring" -- connecting ADBC to existing MV and statistics infrastructure, not building new infrastructure.

## Common Pitfalls

### Pitfall 1: Missing getCatalogTableName() on ADBCTable

**What goes wrong:** `DefaultTraits.getTableName()` calls `table.getCatalogTableName()`, which throws `NotImplementedException` in the base `Table` class.
**Why it happens:** ADBCTable doesn't override `getCatalogTableName()`.
**How to avoid:** Add `getCatalogTableName()` to ADBCTable returning `catalogName + "." + dbName + "." + name`.
**Warning signs:** NPE or NotImplementedException when creating MV over ADBC table.

### Pitfall 2: TRAITS_TABLE is an ImmutableMap

**What goes wrong:** Cannot add ADBC entry at runtime.
**Why it happens:** `ConnectorPartitionTraits.TRAITS_TABLE` uses `ImmutableMap.Builder`.
**How to avoid:** Add the `.put(Table.TableType.ADBC, ADBCPartitionTraits::new)` line to the builder chain at compile time, alongside other external table entries.
**Warning signs:** Compile-time only -- just add the entry.

### Pitfall 3: Statistics query timeout on large remote tables

**What goes wrong:** `getTableStatistics()` pushes aggregate SQL to remote, which may be slow on huge tables.
**Why it happens:** COUNT(*), MIN, MAX over entire table with no sampling.
**How to avoid:** Cache stats results with TTL. Use `ADBCMetaCache` with a configurable TTL (suggest `adbc_stats_cache_expire_sec` defaulting to 600 seconds). On cache miss, run the query; on error, return `ColumnStatistic.unknown()` gracefully.
**Warning signs:** Slow EXPLAIN or query planning times.

### Pitfall 4: APPROX_COUNT_DISTINCT not universally supported

**What goes wrong:** Some remote databases don't support `APPROX_COUNT_DISTINCT`.
**Why it happens:** It's not ANSI SQL; e.g., some Flight SQL servers may reject it.
**How to avoid:** Try the query; if it fails, retry without NDV, setting `distinctValuesCount` to unknown. Or detect failure and fall back.
**Warning signs:** Stats collection errors in logs.

### Pitfall 5: MV with unpartitioned ADBC table must use full refresh

**What goes wrong:** User tries to create partitioned MV over unpartitioned ADBC table.
**Why it happens:** ADBC tables are effectively unpartitioned (`isUnPartitioned()` returns `true`).
**How to avoid:** MV must be unpartitioned or use REFRESH MANUAL/REFRESH EVERY. The MV analyzer already handles this -- unpartitioned base tables create unpartitioned MVs. Full refresh works correctly.
**Warning signs:** "Cannot create partitioned MV" errors are expected and correct behavior.

## Code Examples

### ADBCPartitionTraits (complete implementation)

```java
// Source: Modeled on JDBCPartitionTraits.java
package com.starrocks.connector.partitiontraits;

import com.google.common.collect.Lists;
import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.connector.PartitionInfo;

import java.util.List;

public class ADBCPartitionTraits extends DefaultTraits {
    @Override
    public String getTableName() {
        ADBCTable adbcTable = (ADBCTable) table;
        return adbcTable.getCatalogTableName();
    }

    @Override
    public boolean isSupportPCTRefresh() {
        return false;
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        return Lists.newArrayList();
    }

    @Override
    public PartitionKey createEmptyKey() {
        return new NullablePartitionKey();
    }
}
```

### getTableStatistics() implementation pattern

```java
// Source: Modeled on KuduMetadata.getTableStatistics()
@Override
public Statistics getTableStatistics(OptimizerContext session,
                                     Table table,
                                     Map<ColumnRefOperator, Column> columns,
                                     List<PartitionKey> partitionKeys,
                                     ScalarOperator predicate,
                                     long limit,
                                     TvrVersionRange tableVersionRange) {
    Statistics.Builder builder = Statistics.builder();

    // Default: unknown stats for all columns
    for (ColumnRefOperator colRef : columns.keySet()) {
        builder.addColumnStatistic(colRef, ColumnStatistic.unknown());
    }

    // Try to get row count from cache or remote
    ADBCTable adbcTable = (ADBCTable) table;
    try {
        long rowCount = getRemoteRowCount(adbcTable);
        builder.setOutputRowCount(rowCount);
    } catch (Exception e) {
        LOG.warn("Failed to get row count for {}: {}", adbcTable.getName(), e.getMessage());
        builder.setOutputRowCount(1);  // Fallback
    }

    return builder.build();
}

private long getRemoteRowCount(ADBCTable table) {
    // Push "SELECT COUNT(*) FROM db.table" to remote via ADBC
    String sql = "SELECT COUNT(*) FROM " + quoteIdentifier(table.getDbName()) +
                 "." + quoteIdentifier(table.getName());
    try (AdbcConnection conn = adbcDatabase.connect();
         AdbcStatement stmt = conn.createStatement()) {
        stmt.setSqlQuery(sql);
        ArrowReader reader = stmt.executeQuery().getReader();
        if (reader.loadNextBatch()) {
            return ((BigIntVector) reader.getVectorSchemaRoot().getVector(0)).get(0);
        }
    }
    return 1; // fallback
}
```

### ADBCTable.getCatalogTableName() addition

```java
// Add to ADBCTable.java
@Override
public String getCatalogTableName() {
    return catalogName + "." + dbName + "." + name;
}
```

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | JUnit 5 (Jupiter) + JMockit |
| Config file | `fe/pom.xml` (surefire plugin) |
| Quick run command | `cd fe && mvn test -pl fe-core -Dtest=ADBCMetadataTest -am` |
| Full suite command | `./run-fe-ut.sh` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| MV-01 | CREATE MV over ADBC table | unit (mocked) | `cd fe && mvn test -pl fe-core -Dtest=MvRefreshAndRewriteADBCTest -am` | No -- Wave 0 |
| MV-02 | REFRESH MV fetches ADBC data | unit (mocked) | same as MV-01 | No -- Wave 0 |
| MV-03 | PCT refresh (DEFERRED) | n/a | n/a | n/a |
| MV-04 | Query rewrite to MV | unit (mocked) | same as MV-01 | No -- Wave 0 |
| STAT-01 | getTableStatistics returns col stats | unit | `cd fe && mvn test -pl fe-core -Dtest=ADBCMetadataTest -am` | Partial (extend) |
| STAT-02 | Row count from remote | unit | same as STAT-01 | Partial (extend) |
| PART-01 | listPartitionNames returns empty | unit | same as STAT-01 | No -- Wave 0 |
| PART-02 | Partition names cached | unit | same as STAT-01 | No -- Wave 0 |
| PART-03 | Partition pruning (DEFERRED) | n/a | n/a | n/a |

### Sampling Rate
- **Per task commit:** `cd fe && mvn test -pl fe-core -Dtest=ADBCMetadataTest -am`
- **Per wave merge:** `cd fe && mvn test -pl fe-core -am -Dtest="ADBCMetadataTest,MvRefreshAndRewriteADBCTest"`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `ADBCPartitionTraits.java` -- new file in `connector/partitiontraits/`
- [ ] `MvRefreshAndRewriteADBCTest.java` -- new test modeled on `MvRefreshAndRewriteJDBCTest`
- [ ] `MockedADBCMetadata.java` -- new mocked metadata (if not using existing test mock approach)
- [ ] Extend `ADBCMetadataTest.java` -- add stats and partition test methods

## Sources

### Primary (HIGH confidence)
- Direct codebase inspection of all referenced files
- `ConnectorPartitionTraits.java` lines 61-79 -- TRAITS_TABLE registration pattern
- `MaterializedViewAnalyzer.java` line 178 -- ADBC already in SUPPORTED_TABLE_TYPE
- `JDBCPartitionTraits.java` -- complete reference implementation
- `KuduMetadata.java` lines 305-340 -- simple `getTableStatistics()` pattern
- `ExternalFullStatisticsCollectJob.java` -- ANALYZE TABLE flow (SQL-based, not connector API)
- `ConnectorMetadata.java` lines 225-233 -- `getTableStatistics()` interface
- `ADBCTable.java` -- current state, missing `getCatalogTableName()`
- `Table.java` line 306 -- `getCatalogTableName()` throws NotImplementedException

### Secondary (MEDIUM confidence)
- `MvRefreshAndRewriteJDBCTest.java` -- MV test pattern for external connectors
- `ConnectorPlanTestBase.java` -- mock catalog registration pattern
- `MockedJDBCMetadata.java` -- mocked metadata pattern for tests

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all libraries and patterns are from existing codebase
- Architecture: HIGH - direct mirror of JDBC patterns, all reference code inspected
- Pitfalls: HIGH - identified from code inspection (getCatalogTableName, ImmutableMap, etc.)

**Research date:** 2026-03-06
**Valid until:** 2026-04-06 (stable codebase patterns)
