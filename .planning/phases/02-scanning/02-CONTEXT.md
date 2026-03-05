# Phase 2: Scanning - Context

**Gathered:** 2026-03-06
**Status:** Ready for planning

<domain>
## Phase Boundary

Full data path from FE query planning through BE native C++ ADBC execution to StarRocks Chunk. Column pruning, predicate pushdown, and limit pushdown. Optimizer integration with LogicalADBCScanOperator → PhysicalADBCScanOperator → ADBCScanNode → Thrift → BE ADBCScanner. No partition discovery/pruning (deferred to Phase 3). No aggregate pushdown.

</domain>

<decisions>
## Implementation Decisions

### SQL Pushdown Dialect
- Driver-configurable identifier wrapping — each ADBC driver specifies its quoting style (like JDBC's per-dialect approach)
- FlightSQL driver determines its own identifier wrapping convention; future drivers specify theirs
- Use schema.table naming in pushed SQL (connection already scoped to catalog)
- Same pushdown capability as JDBC: projection (column pruning), filter predicates, LIMIT
- No aggregate pushdown in Phase 2 — that's a v2 requirement (PUSH-01)
- Predicates converted to SQL strings using AstToStringBuilder (same as JDBC)

### BE Scanner Architecture
- Connection-per-scan lifecycle — each scan opens a new AdbcConnection, executes, reads, closes. No connection pooling.
- Reuse existing `ArrowToStarRocksConverter` directly for RecordBatch → Chunk conversion. No custom conversion code.
- Use `AdbcStatement.SetSqlQuery` with the pushed-down SQL string from FE's ScanNode (same pattern as JDBC's PreparedStatement)
- Re-chunk RecordBatches to respect StarRocks max chunk size configuration (default 4096 rows). Buffer incoming batches and emit fixed-size Chunks.
- Multi-endpoint parallel reading: spawn reader threads per FlightEndpoint, all push RecordBatches into a thread-safe queue. Main thread pulls from queue and converts to Chunks.
- If AdbcReadPartition returns multiple partitions, read them in parallel. If it fails, fall back to single-stream reading.
- Thread safety is critical in C++ — careful with closures and shared state in reader threads.
- LIMIT correctness must be carefully tested: LIMIT 1, LIMIT 5, and large LIMIT values with multi-endpoint reading.

### Partition Model
- FE reports ADBC tables as unpartitioned (like JDBC)
- BE handles FlightEndpoints internally — scanner manages parallelism, not the FE planner
- PART-01, PART-02, PART-03 requirements deferred to Phase 3
- Predicate pushdown to remote handles filtering (no partition pruning needed)

### EXPLAIN Output
- Show pushed-down SQL query string, ADBC driver type, and target URI
- Example: `ADBCScanNode(driver=flight_sql, uri=grpc://host:port, query='SELECT col1 FROM schema.table WHERE x=1 LIMIT 100')`
- SQL string is sufficient for understanding pushdown details — no separate column/predicate breakdown lines
- EXPLAIN ANALYZE shows basic execution stats: rows_read, bytes_read, connect_time_ms

### Claude's Discretion
- Exact thread pool size for parallel endpoint readers
- Error handling and retry logic for connection failures
- Internal queue capacity and backpressure strategy
- Thrift IDL field naming and ordering for TADBCScanNode
- Exact EXPLAIN formatting (indentation, label placement)

</decisions>

<specifics>
## Specific Ideas

- "Like JDBC" — the data path should mirror JDBC's operator → rule → scan node → thrift → BE scanner flow
- User has prior experience implementing ADBC scanning with parallel endpoint reading into queues
- Multi-threading in C++ requires care — closures capturing shared state must be thread-safe
- Connection-per-scan is intentionally simple; pooling deferred to avoid premature complexity

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `ArrowToStarRocksConverter` (be/src/exec/arrow_to_starrocks_converter.h): Handles Arrow → Chunk conversion. Reuse directly.
- `ArrowFlightBatchReader` (be/src/exec/arrow_flight_batch_reader.h): Existing Arrow Flight RecordBatch reader
- `LogicalJDBCScanOperator`: Template for LogicalADBCScanOperator (minimal, inherits LogicalScanOperator)
- `PhysicalJDBCScanOperator`: Template for PhysicalADBCScanOperator (minimal, inherits PhysicalScanOperator)
- `JDBCScanImplementationRule`: Template for ADBCScanImplementationRule (1:1 logical→physical conversion)
- `JDBCScanNode`: Template for ADBCScanNode (SQL generation, toThrift)
- `JDBCConnector/JDBCDataSource/JDBCScanner` (BE): Template for ADBC BE scanner architecture

### Established Patterns
- FE data path: LogicalScanOp → (ImplementationRule) → PhysicalScanOp → (PlanFragmentBuilder) → ScanNode → toThrift → BE
- BE data path: Connector → DataSourceProvider → DataSource → Scanner → ChunkPtr
- TJDBCScanNode thrift struct: tuple_id, table_name, columns, filters, limit — ADBC follows same pattern
- PlanFragmentBuilder.visitPhysicalJDBCScan (line ~2040): Creates JDBCScanNode from physical operator

### Integration Points
- `RelationTransformer.java` (line 759): Currently throws "Phase 2" error for ADBC — replace with LogicalADBCScanOperator creation
- `gensrc/thrift/PlanNodes.thrift`: Add TADBCScanNode struct
- `gensrc/thrift/Types.thrift`: May need TPlanNodeType.ADBC_SCAN_NODE
- `be/src/connector/`: Add adbc_connector.h/.cpp (ADBCConnector, ADBCDataSourceProvider, ADBCDataSource)
- `be/src/exec/`: Add adbc_scanner.h/.cpp (ADBCScanner with parallel endpoint reading)
- `fe/fe-core/.../rule/implementation/`: Add ADBCScanImplementationRule
- `fe/fe-core/.../operator/logical/`: Add LogicalADBCScanOperator
- `fe/fe-core/.../operator/physical/`: Add PhysicalADBCScanOperator
- `fe/fe-core/.../planner/`: Add ADBCScanNode
- `PlanFragmentBuilder.java`: Add visitPhysicalADBCScan handler
- `ADBCTable.java`: Implement toThrift() (currently returns null)

</code_context>

<deferred>
## Deferred Ideas

- PART-01/02/03 (partition discovery, listing, pruning) — moved to Phase 3
- Connection pooling (SCAN-07 partial) — start with connection-per-scan, pool later
- Aggregate pushdown (PUSH-01) — v2 requirement
- TopN pushdown (PUSH-02) — v2 requirement

</deferred>

---

*Phase: 02-scanning*
*Context gathered: 2026-03-06*
