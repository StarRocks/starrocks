# Phase 2: Scanning - Research

**Researched:** 2026-03-06
**Domain:** StarRocks FE optimizer pipeline (LogicalScan -> PhysicalScan -> ScanNode -> Thrift), BE C++ ADBC scanner (connection, query execution, Arrow-to-Chunk conversion), Thrift IDL additions
**Confidence:** HIGH

## Summary

Phase 2 builds the complete data path from FE query planning through BE native C++ ADBC execution. The FE side mirrors the JDBC scan pipeline exactly: LogicalADBCScanOperator, PhysicalADBCScanOperator, ADBCScanImplementationRule, ADBCScanNode, PlanFragmentBuilder handler. The BE side creates a new ADBCConnector/ADBCDataSource/ADBCScanner that uses the C ADBC API (already built as thirdparty in Phase 1) to open connections, set SQL queries, execute, and convert ArrowArrayStream results to StarRocks Chunks via the existing `ArrowToStarRocksConverter`.

The JDBC scanner uses JNI to bridge Java JDBC on the BE side. The ADBC scanner is fundamentally different: it uses **native C++ ADBC** with the C API (`adbc.h`) and the ADBC driver manager. This means no JNI, no Java bridge -- the BE scanner directly calls `AdbcDatabaseNew`, `AdbcConnectionNew`, `AdbcStatementSetSqlQuery`, `AdbcStatementExecuteQuery` which returns an `ArrowArrayStream`. The stream is imported via `arrow::ImportRecordBatchReader` (from `arrow/c/bridge.h`) into C++ `arrow::RecordBatch` objects, which are then converted to StarRocks Chunks using the existing `get_arrow_converter()` / `ConvertFunc` infrastructure.

The FE generates the pushed-down SQL query string in `ADBCScanNode` (column pruning, predicates, LIMIT) and passes it to BE via a new `TADBCScanNode` Thrift struct. The BE scanner reads the SQL and connection parameters from the Thrift message.

**Primary recommendation:** Mirror JDBC's FE pipeline verbatim (operators, rule, scan node, PlanFragmentBuilder handler). On the BE side, use the ADBC C API directly (NOT JNI) -- the `adbc_driver_manager` and `adbc_driver_flightsql` static libraries are already linked.

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Driver-configurable identifier wrapping -- each ADBC driver specifies its quoting style (like JDBC's per-dialect approach)
- FlightSQL driver determines its own identifier wrapping convention; future drivers specify theirs
- Use schema.table naming in pushed SQL (connection already scoped to catalog)
- Same pushdown capability as JDBC: projection (column pruning), filter predicates, LIMIT
- No aggregate pushdown in Phase 2 -- that's a v2 requirement (PUSH-01)
- Predicates converted to SQL strings using AstToStringBuilder (same as JDBC)
- Connection-per-scan lifecycle -- each scan opens a new AdbcConnection, executes, reads, closes. No connection pooling.
- Reuse existing `ArrowToStarRocksConverter` directly for RecordBatch -> Chunk conversion. No custom conversion code.
- Use `AdbcStatementSetSqlQuery` with the pushed-down SQL string from FE's ScanNode (same pattern as JDBC's PreparedStatement)
- Re-chunk RecordBatches to respect StarRocks max chunk size configuration (default 4096 rows). Buffer incoming batches and emit fixed-size Chunks.
- Multi-endpoint parallel reading: spawn reader threads per FlightEndpoint, all push RecordBatches into a thread-safe queue. Main thread pulls from queue and converts to Chunks.
- If AdbcReadPartition returns multiple partitions, read them in parallel. If it fails, fall back to single-stream reading.
- Thread safety is critical in C++ -- careful with closures and shared state in reader threads.
- LIMIT correctness must be carefully tested: LIMIT 1, LIMIT 5, and large LIMIT values with multi-endpoint reading.
- FE reports ADBC tables as unpartitioned (like JDBC)
- BE handles FlightEndpoints internally -- scanner manages parallelism, not the FE planner
- PART-01, PART-02, PART-03 requirements deferred to Phase 3
- Predicate pushdown to remote handles filtering (no partition pruning needed)
- Show pushed-down SQL query string, ADBC driver type, and target URI in EXPLAIN
- Example: `ADBCScanNode(driver=flight_sql, uri=grpc://host:port, query='SELECT col1 FROM schema.table WHERE x=1 LIMIT 100')`
- SQL string is sufficient for understanding pushdown details -- no separate column/predicate breakdown lines
- EXPLAIN ANALYZE shows basic execution stats: rows_read, bytes_read, connect_time_ms

### Claude's Discretion
- Exact thread pool size for parallel endpoint readers
- Error handling and retry logic for connection failures
- Internal queue capacity and backpressure strategy
- Thrift IDL field naming and ordering for TADBCScanNode
- Exact EXPLAIN formatting (indentation, label placement)

### Deferred Ideas (OUT OF SCOPE)
- PART-01/02/03 (partition discovery, listing, pruning) -- moved to Phase 3
- Connection pooling (SCAN-07 partial) -- start with connection-per-scan, pool later
- Aggregate pushdown (PUSH-01) -- v2 requirement
- TopN pushdown (PUSH-02) -- v2 requirement
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| SCAN-01 | FE uses Java ADBC API to fetch metadata (schemas, tables, columns, partitions) | Already implemented in Phase 1 via ADBCMetadata. No additional work needed for this requirement. |
| SCAN-02 | BE uses native C++ ADBC to open connections and execute queries for data scanning | C ADBC API (`adbc.h`) + driver manager already built as thirdparty. ADBCScanner uses AdbcDatabaseNew/AdbcConnectionNew/AdbcStatementSetSqlQuery/AdbcStatementExecuteQuery. |
| SCAN-03 | Arrow RecordBatch data converts directly to StarRocks Chunk/Column format in C++ | ArrowArrayStream -> ImportRecordBatchReader -> RecordBatch -> get_arrow_converter()/ConvertFunc -> Chunk. Existing ArrowToStarRocksConverter handles all supported types. |
| SCAN-04 | Column pruning pushdown -- only requested columns are fetched from remote | ADBCScanNode.createADBCTableColumns() builds column list from materialized slots; only those appear in pushed SQL SELECT. |
| SCAN-05 | Predicate pushdown -- WHERE clause filters are pushed as SQL strings to remote | ADBCScanNode.createADBCTableFilters() converts conjuncts via AstToStringBuilder (same as JDBCScanNode). |
| SCAN-06 | Limit pushdown -- LIMIT N is pushed to remote query | ADBCScanNode.toThrift() passes limit; BE scanner appends LIMIT to pushed SQL. |
| SCAN-07 | Connection pooling and lifecycle management | Connection-per-scan for now (locked decision). Each ADBCScanner::open() creates connection, close() releases it. Pooling deferred. |
| OPT-01 | LogicalADBCScanOperator represents ADBC table scan in logical plan | New class mirroring LogicalJDBCScanOperator with OperatorType.LOGICAL_ADBC_SCAN. Created in RelationTransformer. |
| OPT-02 | PhysicalADBCScanOperator created via implementation rule | New class mirroring PhysicalJDBCScanOperator with OperatorType.PHYSICAL_ADBC_SCAN. ADBCScanImplementationRule transforms logical to physical. |
| OPT-03 | ADBCScanNode generates final SQL query with pushdowns for BE execution | New class mirroring JDBCScanNode. Generates SELECT with column pruning, WHERE predicates, LIMIT. Emits TADBCScanNode in toThrift(). |
| OPT-04 | EXPLAIN shows the pushed-down ADBC query string | ADBCScanNode.getNodeExplainString() outputs driver, URI, and generated SQL query. |
</phase_requirements>

---

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `adbc_driver_manager` (C++) | 19 (ADBC API 1.1.0) | ADBC driver manager for BE scanner | Already in thirdparty, statically linked |
| `adbc_driver_flightsql` (C++) | 19 | FlightSQL ADBC driver for BE | Already in thirdparty, statically linked |
| `arrow/c/bridge.h` (C++) | 19.0.1 | Import ArrowArrayStream to C++ RecordBatch | Already in thirdparty Arrow build |
| `arrow_to_starrocks_converter` (internal) | N/A | Convert Arrow RecordBatch columns to StarRocks Chunk | Existing in `be/src/exec/` |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `arrow::RecordBatchReader` (C++) | 19.0.1 | Iterate RecordBatch stream | After ImportRecordBatchReader from ArrowArrayStream |
| Thrift code-gen | Existing | TADBCScanNode struct | Generated from PlanNodes.thrift |

### No New Dependencies
Phase 2 does not require any new Maven or CMake dependencies. Everything needed is already built from Phase 1.

---

## Architecture Patterns

### FE Data Path (mirrors JDBC exactly)

```
User SQL Query
    |
    v
RelationTransformer.java (line ~759)
    |-- Currently throws "Phase 2" error for ADBC
    |-- Replace with: new LogicalADBCScanOperator(table, colRefMap, ...)
    v
LogicalADBCScanOperator  (new, mirrors LogicalJDBCScanOperator)
    |-- OperatorType.LOGICAL_ADBC_SCAN
    |-- accept() -> visitor.visitLogicalADBCScan()
    v
ADBCScanImplementationRule  (new, mirrors JDBCScanImplementationRule)
    |-- RuleType.IMP_ADBC_LSCAN_TO_PSCAN
    |-- Pattern.create(OperatorType.LOGICAL_ADBC_SCAN)
    |-- transform(): logical -> PhysicalADBCScanOperator
    v
PhysicalADBCScanOperator  (new, mirrors PhysicalJDBCScanOperator)
    |-- OperatorType.PHYSICAL_ADBC_SCAN
    |-- accept() -> visitor.visitPhysicalADBCScan()
    v
PlanFragmentBuilder.visitPhysicalADBCScan()  (new handler, mirrors visitPhysicalJDBCScan at line 2033)
    |-- Creates ADBCScanNode
    |-- Sets predicates, limit, columns
    |-- Creates UNPARTITIONED PlanFragment
    v
ADBCScanNode  (new, mirrors JDBCScanNode)
    |-- Creates column list, filter list, pushed SQL
    |-- toThrift(): TPlanNodeType.ADBC_SCAN_NODE, fills TADBCScanNode
    v
Thrift TADBCScanNode  --over network-->  BE
```

### BE Data Path (native C++ ADBC, NOT JNI)

```
BE receives TPlanNode with adbc_scan_node
    |
    v
exec_factory.cpp: case TPlanNodeType::ADBC_SCAN_NODE
    |-- Creates ConnectorScanNode with connector_name = "adbc"
    v
ADBCConnector::create_data_source_provider(scan_node, plan_node)
    |-- Returns ADBCDataSourceProvider holding TADBCScanNode
    v
ADBCDataSourceProvider::create_data_source(scan_range)
    |-- Returns ADBCDataSource
    v
ADBCDataSource::open(state)
    |-- Extracts SQL, URI, auth from TADBCScanNode
    |-- Creates ADBCScanner
    v
ADBCScanner::open(state)
    |-- AdbcDatabaseNew + AdbcDatabaseSetOption("driver", "adbc_driver_flightsql")
    |-- AdbcDatabaseSetOption("uri", ...)
    |-- AdbcDatabaseInit
    |-- AdbcConnectionNew + AdbcConnectionInit
    |-- AdbcStatementNew
    |-- AdbcStatementSetSqlQuery(pushed_sql)
    |-- AdbcStatementExecuteQuery -> ArrowArrayStream
    |-- arrow::ImportRecordBatchReader(stream) -> RecordBatchReader
    v
ADBCScanner::get_next(state, chunk, eos)
    |-- RecordBatchReader::ReadNext -> RecordBatch
    |-- For each column: get_arrow_converter(arrow_type, sr_type, nullable)
    |-- Convert arrow::Array -> StarRocks Column
    |-- Build Chunk from columns
    |-- Re-chunk to max_chunk_size (4096 default)
    v
ChunkPtr returned to pipeline
```

### Recommended New Files

```
FE (Java):
fe/fe-core/src/main/java/com/starrocks/sql/optimizer/
  operator/logical/LogicalADBCScanOperator.java
  operator/physical/PhysicalADBCScanOperator.java
  rule/implementation/ADBCScanImplementationRule.java
fe/fe-core/src/main/java/com/starrocks/planner/
  ADBCScanNode.java

BE (C++):
be/src/connector/
  adbc_connector.h
  adbc_connector.cpp
be/src/exec/
  adbc_scanner.h
  adbc_scanner.cpp

Thrift:
gensrc/thrift/PlanNodes.thrift  (add TADBCScanNode, ADBC_SCAN_NODE)
gensrc/thrift/Types.thrift      (add ADBC_TABLE to TTableType)
gensrc/thrift/Descriptors.thrift (add TADBCTable)
```

### Files to Modify

```
FE:
- RelationTransformer.java (line ~759): Replace "Phase 2" error with LogicalADBCScanOperator
- OperatorType.java: Add LOGICAL_ADBC_SCAN, PHYSICAL_ADBC_SCAN
- RuleType.java: Add IMP_ADBC_LSCAN_TO_PSCAN
- OperatorVisitor.java: Add visitLogicalADBCScan, visitPhysicalADBCScan
- OptExpressionVisitor.java: Add visitPhysicalADBCScan
- StatisticsCalculator.java: Add visitLogicalADBCScan, visitPhysicalADBCScan
- LogicalPlanPrinter.java: Handle ADBC scan (if needed)
- OutputPropertyDeriver.java: Handle ADBC scan
- RuleSet.java: Register ADBCScanImplementationRule
- PlanFragmentBuilder.java: Add visitPhysicalADBCScan handler
- ADBCTable.java: Implement toThrift() properly (currently returns null)

BE:
- connector/connector.h: Add ADBC ConnectorType enum, string constant
- connector/connector.cpp: Register ADBCConnector, add "adbc" string
- exec/exec_factory.cpp: Add case TPlanNodeType::ADBC_SCAN_NODE
- exec/exec_node.cpp: Add collect_nodes for ADBC_SCAN_NODE
```

### Pattern: TADBCScanNode Thrift Struct

```thrift
// In PlanNodes.thrift
struct TADBCScanNode {
  1: optional Types.TTupleId tuple_id
  2: optional string table_name     // schema.table format
  3: optional list<string> columns  // column list for SELECT
  4: optional list<string> filters  // WHERE clause predicates
  5: optional i64 limit             // LIMIT N (-1 = no limit)
  6: optional string adbc_driver    // "adbc_driver_flightsql"
  7: optional string adbc_uri       // "grpc://host:port"
  8: optional string adbc_username
  9: optional string adbc_password
  10: optional string adbc_token
}
```

### Pattern: ADBCScanNode SQL Generation

```java
// Mirrors JDBCScanNode.getJDBCQueryStr() but simpler (no dialect-specific LIMIT)
private String getADBCQueryStr() {
    StringBuilder sql = new StringBuilder("SELECT ");
    sql.append(Joiner.on(", ").join(columns));
    sql.append(" FROM ").append(tableName);
    if (!filters.isEmpty()) {
        sql.append(" WHERE (");
        sql.append(Joiner.on(") AND (").join(filters));
        sql.append(")");
    }
    if (limit != -1) {
        sql.append(" LIMIT ").append(limit);
    }
    return sql.toString();
}
```

### Pattern: BE ADBCScanner C API Usage

```cpp
// Core execution flow using ADBC C API
#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <arrow/c/bridge.h>

Status ADBCScanner::open(RuntimeState* state) {
    AdbcError error = ADBC_ERROR_INIT;

    // Initialize database
    AdbcDatabaseNew(&_database, &error);
    AdbcDatabaseSetOption(&_database, "driver", _driver_name.c_str(), &error);
    AdbcDatabaseSetOption(&_database, "uri", _uri.c_str(), &error);
    if (!_username.empty()) {
        AdbcDatabaseSetOption(&_database, "username", _username.c_str(), &error);
    }
    if (!_password.empty()) {
        AdbcDatabaseSetOption(&_database, "password", _password.c_str(), &error);
    }
    AdbcDatabaseInit(&_database, &error);

    // Initialize connection
    AdbcConnectionNew(&_connection, &error);
    AdbcConnectionInit(&_connection, &_database, &error);

    // Create and execute statement
    AdbcStatementNew(&_connection, &_statement, &error);
    AdbcStatementSetSqlQuery(&_statement, _sql.c_str(), &error);

    ArrowArrayStream c_stream;
    int64_t rows_affected = -1;
    AdbcStatementExecuteQuery(&_statement, &c_stream, &rows_affected, &error);

    // Import C stream to C++ RecordBatchReader
    auto result = arrow::ImportRecordBatchReader(&c_stream);
    _batch_reader = std::move(result).ValueOrDie();

    return Status::OK();
}

Status ADBCScanner::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    std::shared_ptr<arrow::RecordBatch> batch;
    ARROW_RETURN_NOT_OK(_batch_reader->ReadNext(&batch));
    if (!batch) {
        *eos = true;
        return Status::OK();
    }
    // Convert each column using existing ArrowToStarRocksConverter
    return _convert_batch_to_chunk(batch, chunk);
}
```

### Anti-Patterns to Avoid

- **Don't use JNI for the BE scanner:** The whole point of ADBC is native C++ data path. The JDBC scanner uses JNI because JDBC is Java-only. ADBC has a native C API.
- **Don't build custom Arrow conversion code:** `ArrowToStarRocksConverter` already handles all Arrow type -> StarRocks Column conversions. Use `get_arrow_converter()` directly.
- **Don't forget to register in exec_factory.cpp:** Without the `case TPlanNodeType::ADBC_SCAN_NODE` case, the BE will crash with "unknown plan node type".
- **Don't forget to add ADBC entries to OperatorType enum:** Both LOGICAL_ADBC_SCAN and PHYSICAL_ADBC_SCAN must be added, or the optimizer framework will fail.
- **Don't skip the RuleSet registration:** The ADBCScanImplementationRule must be added to the IMPLEMENTATION_RULES list in RuleSet.java, or logical-to-physical conversion will never happen.
- **Don't forget ADBCTable.toThrift():** Currently returns null. Must return a valid TTableDescriptor with the new ADBC_TABLE TTableType, or descriptor table serialization will fail.
- **Don't mix up C API lifecycle:** AdbcDatabaseRelease, AdbcConnectionRelease, AdbcStatementRelease must all be called in the correct order during cleanup. Use RAII wrappers.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Arrow -> Chunk conversion | Custom column converters | `get_arrow_converter()` from `arrow_to_starrocks_converter.h` | Handles all type combinations, null handling, strict mode |
| ArrowArrayStream -> RecordBatch | Manual C Data Interface parsing | `arrow::ImportRecordBatchReader()` from `arrow/c/bridge.h` | Official Arrow C++ bridge, handles memory ownership |
| SQL predicate string generation | Custom expression printer | `AstToStringBuilder.toString(expr)` (same as JDBC) | Already handles all StarRocks expression types |
| Operator visitor dispatch | Manual type checks | Visitor pattern via OperatorVisitor/OptExpressionVisitor | Framework-standard, all scan types use this |
| Thrift serialization | Manual binary encoding | Thrift code generation from `.thrift` IDL | Standard StarRocks pattern |
| Identifier quoting | Hardcoded quote characters | Per-driver identifier symbol method (like JDBCScanNode.getIdentifierSymbol()) | Different remote databases use different quoting |

**Key insight:** The FE side is pure boilerplate mirroring JDBC. The BE side replaces JNI+Java with direct C API calls. Neither side requires novel algorithms.

---

## Common Pitfalls

### Pitfall 1: ADBC C API Error Handling

**What goes wrong:** ADBC C functions return `AdbcStatusCode` (uint8_t), not exceptions. Ignoring return values causes silent failures -- connection appears open but isn't initialized.

**Why it happens:** C API has no RAII. Every call must check `status != ADBC_STATUS_OK` and read `error.message`.

**How to avoid:** Create a `RETURN_ADBC_NOT_OK(status, error)` macro that converts AdbcStatusCode to StarRocks Status, extracting the error message and releasing the AdbcError. Example:
```cpp
#define RETURN_ADBC_NOT_OK(status_code, adbc_error) \
    do { \
        if ((status_code) != ADBC_STATUS_OK) { \
            std::string msg = (adbc_error).message ? (adbc_error).message : "Unknown ADBC error"; \
            if ((adbc_error).release) (adbc_error).release(&(adbc_error)); \
            return Status::InternalError("ADBC error: " + msg); \
        } \
    } while (0)
```

**Warning signs:** Scanner opens without error but returns 0 rows; cryptic segfaults during query execution.

### Pitfall 2: ArrowArrayStream Ownership and Lifetime

**What goes wrong:** The `ArrowArrayStream` returned by `AdbcStatementExecuteQuery` must outlive the `RecordBatchReader`. If the statement or connection is released before the stream is consumed, the reader crashes.

**Why it happens:** The C Data Interface uses callbacks (function pointers). The stream's private_data may reference state owned by the statement/connection.

**How to avoid:** Hold the `AdbcStatement`, `AdbcConnection`, and `AdbcDatabase` alive for the entire lifetime of the scanner. Release in reverse order: statement first, then connection, then database. Only release after the RecordBatchReader is fully consumed or explicitly closed.

**Warning signs:** Segfault on second `ReadNext()` call, or on scanner close.

### Pitfall 3: Missing OperatorType/RuleType Registration

**What goes wrong:** Adding LogicalADBCScanOperator without the corresponding OperatorType.LOGICAL_ADBC_SCAN enum entry causes NPE in the optimizer framework. Similarly, missing RuleType.IMP_ADBC_LSCAN_TO_PSCAN prevents the implementation rule from being registered.

**Why it happens:** The optimizer uses enum values for type dispatch throughout. Every scan type needs corresponding enum entries.

**How to avoid:** Checklist of enum/registration changes:
1. `OperatorType.java`: LOGICAL_ADBC_SCAN, PHYSICAL_ADBC_SCAN
2. `RuleType.java`: IMP_ADBC_LSCAN_TO_PSCAN
3. `OperatorVisitor.java`: visitLogicalADBCScan, visitPhysicalADBCScan
4. `OptExpressionVisitor.java`: visitPhysicalADBCScan
5. `StatisticsCalculator.java`: visitLogicalADBCScan, visitPhysicalADBCScan
6. `RuleSet.java`: Add ADBCScanImplementationRule to IMPLEMENTATION_RULES
7. `PlanFragmentBuilder.java`: visitPhysicalADBCScan handler

**Warning signs:** "No rule to transform" error; NPE in optimizer.

### Pitfall 4: TTableType/TTableDescriptor Missing ADBC Entry

**What goes wrong:** ADBCTable.toThrift() currently returns null. Without a proper ADBC_TABLE entry in TTableType and TADBCTable struct in Descriptors.thrift, the BE cannot deserialize the table descriptor.

**Why it happens:** Phase 1 deferred this as a stub. Phase 2 needs it working.

**How to avoid:** Add ADBC_TABLE to TTableType enum in Types.thrift. Add TADBCTable struct in Descriptors.thrift (minimal: just the properties needed by BE). Add ADBC field to TTableDescriptor. Implement ADBCTable.toThrift() to populate TADBCTable.

**Warning signs:** BE crashes with "unknown table type" or null pointer during descriptor deserialization.

### Pitfall 5: Re-chunking RecordBatches

**What goes wrong:** Arrow RecordBatches from FlightSQL may have varying row counts (e.g., 65536 rows). StarRocks expects chunks of max_chunk_size (default 4096). Passing oversized chunks causes downstream pipeline issues.

**Why it happens:** Arrow Flight sends data in server-determined batch sizes, not StarRocks chunk sizes.

**How to avoid:** The scanner must buffer incoming RecordBatch rows and emit fixed-size chunks. Track current position in the batch, emit chunk_size rows at a time, carry remainder to next get_next() call.

**Warning signs:** Pipeline buffer overflow; incorrect row counts in EXPLAIN ANALYZE.

### Pitfall 6: ADBC Driver Name for SetOption

**What goes wrong:** Using "flight_sql" (FE property value) as the ADBC driver name in `AdbcDatabaseSetOption("driver", ...)` fails. The C driver manager expects the shared library name or entrypoint.

**Why it happens:** The user-facing property `adbc.driver=flight_sql` is a StarRocks concept. The C API driver manager needs either a library path or the statically linked driver init function.

**How to avoid:** Since `adbc_driver_flightsql` is statically linked, use `AdbcDriverManagerDatabaseSetInitFunc` to directly set the FlightSQL driver init function, or use `AdbcDatabaseSetOption("driver", "adbc_driver_flightsql", ...)` which the driver manager resolves for statically linked drivers.

**Warning signs:** "Driver not found" error from AdbcDatabaseInit.

---

## Code Examples

### LogicalADBCScanOperator (mirrors LogicalJDBCScanOperator exactly)

```java
// Source: LogicalJDBCScanOperator.java (direct template)
package com.starrocks.sql.optimizer.operator.logical;

public class LogicalADBCScanOperator extends LogicalScanOperator {
    public LogicalADBCScanOperator(Table table,
                                    Map<ColumnRefOperator, Column> columnRefOperatorColumnMap,
                                    Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                    long limit, ScalarOperator predicate, Projection projection) {
        super(OperatorType.LOGICAL_ADBC_SCAN, table, columnRefOperatorColumnMap,
              columnMetaToColRefMap, limit, predicate, projection);
        Preconditions.checkState(table instanceof ADBCTable);
    }

    private LogicalADBCScanOperator() {
        super(OperatorType.LOGICAL_ADBC_SCAN);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalADBCScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalADBCScanOperator, Builder> {
        @Override
        protected LogicalADBCScanOperator newInstance() {
            return new LogicalADBCScanOperator();
        }
    }
}
```

### PlanFragmentBuilder.visitPhysicalADBCScan (mirrors visitPhysicalJDBCScan at line 2033)

```java
// Source: PlanFragmentBuilder.java line 2033 (JDBCScan handler)
@Override
public PlanFragment visitPhysicalADBCScan(OptExpression optExpression, ExecPlan context) {
    PhysicalADBCScanOperator node = (PhysicalADBCScanOperator) optExpression.getOp();

    context.getDescTbl().addReferencedTable(node.getTable());
    TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
    tupleDescriptor.setTable(node.getTable());

    for (Map.Entry<ColumnRefOperator, Column> entry : node.getColRefToColumnMetaMap().entrySet()) {
        SlotDescriptor slotDescriptor =
                context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
        slotDescriptor.setColumn(entry.getValue());
        slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
        slotDescriptor.setIsMaterialized(true);
        context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().getName(), slotDescriptor));
    }
    tupleDescriptor.computeMemLayout();

    ADBCScanNode scanNode = new ADBCScanNode(context.getNextNodeId(), tupleDescriptor,
            (ADBCTable) node.getTable());
    currentExecGroup.add(scanNode, true);

    List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
    ScalarOperatorToExpr.FormatterContext formatterContext =
            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
    for (ScalarOperator predicate : predicates) {
        scanNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
    }

    scanNode.setLimit(node.getLimit());
    scanNode.computeColumnsAndFilters();
    scanNode.computeStatistics(optExpression.getStatistics());
    scanNode.setScanOptimizeOption(node.getScanOptimizeOption());
    context.getScanNodes().add(scanNode);
    PlanFragment fragment = new PlanFragment(context.getNextFragmentId(), scanNode, DataPartition.UNPARTITIONED);
    context.getFragments().add(fragment);
    return fragment;
}
```

### BE exec_factory.cpp Addition

```cpp
// Source: exec_factory.cpp line 295 (JDBC_SCAN_NODE pattern)
case TPlanNodeType::ADBC_SCAN_NODE: {
    TPlanNode new_node = tnode;
    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::ADBC;
    new_node.connector_scan_node = connector_scan_node;
    *node = pool->add(new ConnectorScanNode(pool, new_node, descs));
    return Status::OK();
}
```

### BE ADBCConnector/DataSource/Scanner Pattern

```cpp
// Source: jdbc_connector.h pattern
// be/src/connector/adbc_connector.h
class ADBCConnector final : public Connector {
public:
    ~ADBCConnector() override = default;
    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;
    ConnectorType connector_type() const override { return ConnectorType::ADBC; }
};

class ADBCDataSourceProvider final : public DataSourceProvider {
public:
    ADBCDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;
    bool insert_local_exchange_operator() const override { return true; }
    bool accept_empty_scan_ranges() const override { return false; }
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;
private:
    ConnectorScanNode* _scan_node;
    TADBCScanNode _adbc_scan_node;
};

class ADBCDataSource final : public DataSource {
public:
    ADBCDataSource(const ADBCDataSourceProvider* provider, const TScanRange& scan_range);
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;
    // ... stats methods
private:
    const ADBCDataSourceProvider* _provider;
    std::unique_ptr<ADBCScanner> _scanner;
    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
};
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| JDBC uses JNI bridge for BE data | ADBC uses native C API | New for ADBC | No JNI overhead, direct Arrow memory |
| JDBCScanNode generates dialect-specific SQL | ADBCScanNode generates standard SQL | New for ADBC | Simpler -- FlightSQL backends accept standard SQL |
| JDBC scanner fetches rows via Java ResultSet | ADBC scanner reads ArrowArrayStream natively | New for ADBC | Columnar from source to chunk, no row-to-column conversion |

---

## Open Questions

1. **ADBC Driver Manager vs Direct Init Function**
   - What we know: The FlightSQL driver is statically linked. The driver manager can find it by name ("adbc_driver_flightsql") or by init function pointer.
   - What's unclear: Whether `AdbcDatabaseSetOption("driver", "adbc_driver_flightsql")` works for statically linked drivers, or if `AdbcDriverManagerDatabaseSetInitFunc` is required.
   - Recommendation: Try the string name first (simpler). If it fails, use the init function approach. The driver manager header declares `AdbcDriverFlightSQLInit` as an entry point.

2. **Multi-Endpoint Parallel Reading Complexity**
   - What we know: User wants multi-endpoint parallel reading with thread-safe queue.
   - What's unclear: Whether `AdbcStatementExecuteQuery` with FlightSQL already serializes all endpoints into a single ArrowArrayStream, or if we need to use `AdbcStatementExecutePartitions` + `AdbcConnectionReadPartition` to get per-endpoint streams.
   - Recommendation: Start with the simple path (`AdbcStatementExecuteQuery` -> single stream). If performance is insufficient, add `ExecutePartitions` path. The single-stream approach satisfies all functional requirements.

3. **TTableDescriptor for ADBC**
   - What we know: JDBC uses TJDBCTable with driver info (driver_name, driver_url, checksum, class, url, user, password). ADBC needs similar but different fields (driver type, URI, credentials).
   - What's unclear: Whether to embed connection info in TTableDescriptor or only in TADBCScanNode.
   - Recommendation: Put connection info in TADBCScanNode (scan-time data), and minimal info in TADBCTable (just table identification). This matches how JDBC puts the JDBC URL in both TJDBCTable and uses it in the scanner.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework (FE) | JUnit 5 (Jupiter) with Mockito |
| Framework (BE) | Google Test (gtest) |
| FE Quick run | `./run-fe-ut.sh --test com.starrocks.planner.ADBCScanNodeTest` |
| BE Quick run | `./run-be-ut.sh --gtest_filter "ADBCScannerTest*"` |
| Full suite (FE) | `./run-fe-ut.sh` |
| Full suite (BE) | `./run-be-ut.sh` |

### Phase Requirements -> Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SCAN-01 | FE ADBC metadata (already done Phase 1) | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCMetadataTest` | Exists |
| SCAN-02 | BE native C++ ADBC connection + query | unit | `./run-be-ut.sh --gtest_filter "ADBCScannerTest.OpenAndExecute"` | Wave 0 |
| SCAN-03 | Arrow RecordBatch -> Chunk conversion | unit | `./run-be-ut.sh --gtest_filter "ADBCScannerTest.ConvertBatch"` | Wave 0 |
| SCAN-04 | Column pruning in generated SQL | unit | `./run-fe-ut.sh --test com.starrocks.planner.ADBCScanNodeTest` | Wave 0 |
| SCAN-05 | Predicate pushdown in generated SQL | unit | `./run-fe-ut.sh --test com.starrocks.planner.ADBCScanNodeTest` | Wave 0 |
| SCAN-06 | LIMIT in generated SQL | unit | `./run-fe-ut.sh --test com.starrocks.planner.ADBCScanNodeTest` | Wave 0 |
| SCAN-07 | Connection-per-scan lifecycle | unit | `./run-be-ut.sh --gtest_filter "ADBCScannerTest.ConnectionLifecycle"` | Wave 0 |
| OPT-01 | LogicalADBCScanOperator creation | unit | `./run-fe-ut.sh --test com.starrocks.sql.optimizer.operator.ADBCScanOperatorTest` | Wave 0 |
| OPT-02 | Physical operator via implementation rule | unit | `./run-fe-ut.sh --test com.starrocks.sql.optimizer.rule.ADBCScanImplementationRuleTest` | Wave 0 |
| OPT-03 | ADBCScanNode SQL generation | unit | `./run-fe-ut.sh --test com.starrocks.planner.ADBCScanNodeTest` | Wave 0 |
| OPT-04 | EXPLAIN output contains pushed SQL | unit | `./run-fe-ut.sh --test com.starrocks.planner.ADBCScanNodeTest` | Wave 0 |

### Sampling Rate

- **Per task commit:** `cd fe && mvn compile -pl fe-core -am -DskipTests` (FE) or `./build.sh --be` (BE)
- **Per wave merge:** `./run-fe-ut.sh --test com.starrocks.planner.ADBCScanNodeTest && ./run-fe-ut.sh --test com.starrocks.sql.optimizer`
- **Phase gate:** Full FE + BE build + FE UT + BE UT passing

### Wave 0 Gaps

- [ ] `fe/fe-core/src/test/java/com/starrocks/planner/ADBCScanNodeTest.java` -- covers SCAN-04/05/06, OPT-03/04
- [ ] `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/operator/ADBCScanOperatorTest.java` -- covers OPT-01
- [ ] `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/ADBCScanImplementationRuleTest.java` -- covers OPT-02
- [ ] `be/test/connector/adbc_connector_test.cpp` -- covers SCAN-02/03/07
- [ ] Thrift IDL additions (`TADBCScanNode`, `ADBC_SCAN_NODE`, `ADBC_TABLE`, `TADBCTable`) -- prerequisites for all

---

## Sources

### Primary (HIGH confidence)
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/logical/LogicalJDBCScanOperator.java` -- Direct template for LogicalADBCScanOperator
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/physical/PhysicalJDBCScanOperator.java` -- Direct template for PhysicalADBCScanOperator
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/implementation/JDBCScanImplementationRule.java` -- Direct template for ADBCScanImplementationRule
- `fe/fe-core/src/main/java/com/starrocks/planner/JDBCScanNode.java` -- Direct template for ADBCScanNode (SQL generation, toThrift, EXPLAIN)
- `fe/fe-core/src/main/java/com/starrocks/sql/plan/PlanFragmentBuilder.java` (line 2033) -- visitPhysicalJDBCScan handler template
- `be/src/connector/jdbc_connector.h/cpp` -- Direct template for ADBCConnector/DataSource
- `be/src/exec/jdbc_scanner.h/cpp` -- Template for scanner lifecycle (but ADBC uses C API, not JNI)
- `be/src/exec/arrow_to_starrocks_converter.h` -- Existing Arrow -> Column conversion (reuse directly)
- `thirdparty/installed/include/arrow-adbc/adbc.h` -- ADBC C API 1.1.0 function signatures
- `thirdparty/installed/include/arrow-adbc/adbc_driver_manager.h` -- Driver manager API
- `thirdparty/installed/include/arrow/c/bridge.h` -- ImportRecordBatchReader for C->C++ bridge
- `gensrc/thrift/PlanNodes.thrift` -- TJDBCScanNode struct (template for TADBCScanNode)
- `gensrc/thrift/Types.thrift` -- TTableType enum, TPlanNodeType enum
- `be/src/exec/exec_factory.cpp` (line 295) -- JDBC_SCAN_NODE case (template for ADBC_SCAN_NODE)
- `be/src/connector/connector.cpp` -- ConnectorManager registration pattern
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/OperatorType.java` -- LOGICAL_JDBC_SCAN at line 43
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/RuleType.java` -- IMP_JDBC_LSCAN_TO_PSCAN at line 256
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/RuleSet.java` -- Registration at line 202
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/transformer/RelationTransformer.java` (line 759) -- Current ADBC stub

### Secondary (MEDIUM confidence)
- ADBC C API specification 1.1.0 -- lifecycle semantics (New/SetOption/Init/Release ordering)
- Arrow C Data Interface specification -- ArrowArrayStream ownership semantics

### Tertiary (LOW confidence)
- Multi-endpoint parallel reading via ExecutePartitions -- needs runtime validation with actual FlightSQL server to confirm behavior

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already built and linked from Phase 1
- FE architecture: HIGH -- direct mirror of JDBC pattern, all source files inspected
- BE architecture: HIGH -- ADBC C API headers inspected, conversion infrastructure verified
- Thrift additions: HIGH -- TJDBCScanNode pattern directly applicable
- Multi-endpoint reading: MEDIUM -- API exists but runtime behavior needs validation
- Pitfalls: HIGH -- based on direct analysis of C API ownership semantics and existing patterns

**Research date:** 2026-03-06
**Valid until:** 2026-04-06 (30 days; no version changes expected)
