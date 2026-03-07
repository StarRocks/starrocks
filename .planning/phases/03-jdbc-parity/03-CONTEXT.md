# Phase 3: JDBC Parity - Context

**Gathered:** 2026-03-06
**Status:** Ready for planning

<domain>
## Phase Boundary

Materialized views over ADBC external tables (create, refresh, auto-rewrite), column statistics collection via remote SQL queries, and minimal partition support (discovery and traits registration, no pruning). Full MV refresh only — no incremental PCT refresh.

</domain>

<decisions>
## Implementation Decisions

### Statistics Collection
- Collect column stats via remote SQL queries pushed through ADBC (COUNT(*), MIN(col), MAX(col), APPROX_COUNT_DISTINCT(col), COUNT(col) for null count)
- ANALYZE TABLE runs full scan — push complete aggregate queries to remote, no sampling
- Use standard ANSI SQL only for stats queries — no driver-specific function overrides. If remote doesn't support APPROX_COUNT_DISTINCT, skip NDV
- Cache collected stats locally with configurable TTL (same pattern as JDBC stats caching)
- Statistics feed into optimizer via `getTableStatistics()` in ADBCMetadata

### Materialized View Support
- Full refresh only — no incremental PCT refresh for ADBC tables
- Both scheduled (REFRESH EVERY interval) and manual (REFRESH MATERIALIZED VIEW) supported
- Automatic query rewrite enabled — optimizer can rewrite queries against ADBC tables to use MVs
- MV creation, refresh, and rewrite should work out of the box since `Table.TableType.ADBC` is already in `SUPPORTED_TABLE_TYPE`

### Partition Support (Minimal)
- Implement `listPartitionNames()` in ADBCMetadata — discover partition names from remote
- Create `ADBCPartitionTraits` extending `DefaultTraits` and register in `ConnectorPartitionTraits.TRAITS_TABLE`
- No partition pruning logic — tables remain effectively unpartitioned for query planning
- Foundation for future PCT incremental refresh without the complexity of pruning
- PART-01 and PART-02 addressed minimally; PART-03 (pruning) deferred

### Claude's Discretion
- Stats cache TTL default value and configuration property names
- ADBCPartitionTraits implementation details (which methods to stub vs implement)
- Exact stats SQL query generation approach
- How to handle stats collection failures gracefully
- MV test coverage scope

</decisions>

<specifics>
## Specific Ideas

- "Like JDBC" continues as the guiding principle — mirror JDBC's stats and MV patterns
- Full refresh is intentionally simple — avoid premature complexity of PCT for a new connector
- Stats via SQL queries is the pragmatic choice — Arrow Flight SQL metadata doesn't reliably provide statistics
- Partition support is a foundation layer — enough structure to not block future PCT work

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `JDBCPartitionTraits.java`: Template for ADBCPartitionTraits — extends DefaultTraits, registered in TRAITS_TABLE
- `JDBCMetadata.java`: Reference for listPartitionNames(), getPartitions(), getTableStatistics() implementation
- `HiveStatisticsProvider.java`: Pattern for statistics provider delegation (if needed)
- `MaterializedViewAnalyzer.java`: ADBC already in SUPPORTED_TABLE_TYPE (line 178) — MV creation should work
- `ConnectorPartitionTraits.java`: TRAITS_TABLE registration map (line 76) — add ADBC entry
- `ConnectorMetadata.java`: getTableStatistics() default returns empty — override in ADBCMetadata
- `MvRefreshAndRewriteJDBCTest.java`: Test pattern for MV over external tables

### Established Patterns
- Connector stats: Override `getTableStatistics()` in ConnectorMetadata, return Statistics.Builder with row count + column stats
- Partition traits: Extend DefaultTraits, implement getTableName/isSupportPCTRefresh/getPartitions/createEmptyKey
- MV refresh: Async refresh scheduler runs MV query → data flows through existing scan path → populates MV
- Stats caching: JDBC caches stats in metadata manager with TTL-based expiry

### Integration Points
- `ADBCMetadata.java`: Add getTableStatistics(), listPartitionNames(), getPartitions()
- `ConnectorPartitionTraits.java`: Register Table.TableType.ADBC → ADBCPartitionTraits
- New file: `ADBCPartitionTraits.java` in connector/adbc/ package
- ADBCScanNode: May need stats-aware cost estimation

</code_context>

<deferred>
## Deferred Ideas

- PCT incremental MV refresh — requires robust partition change detection, pair with full partition pruning
- PART-03 (partition pruning) — deferred until PCT or explicit need
- Connection pooling for stats queries — reuse connection-per-operation for now
- Driver-specific stats SQL overrides — start with standard SQL, add driver hooks if needed

</deferred>

---

*Phase: 03-jdbc-parity*
*Context gathered: 2026-03-06*
