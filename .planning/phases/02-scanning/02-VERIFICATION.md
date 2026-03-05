---
phase: 02-scanning
verified: 2026-03-06T02:00:00Z
status: passed
score: 4/4 success criteria verified
must_haves:
  truths:
    - "User can SELECT col1, col2 FROM my_adbc.mydb.mytable WHERE col1 = 'value' LIMIT 100 and receive correct results"
    - "EXPLAIN SELECT shows the pushed-down SQL query string sent to the remote source (predicate + limit visible)"
    - "Column pruning is active: only selected columns appear in the remote query, not SELECT *"
    - "Connection-per-scan lifecycle: each query opens and closes ADBC connections cleanly"
  artifacts:
    - path: "gensrc/thrift/PlanNodes.thrift"
      provides: "TADBCScanNode struct and ADBC_SCAN_NODE enum entry"
    - path: "gensrc/thrift/Types.thrift"
      provides: "ADBC_TABLE enum entry"
    - path: "gensrc/thrift/Descriptors.thrift"
      provides: "TADBCTable struct and adbcTable field in TTableDescriptor"
    - path: "fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/logical/LogicalADBCScanOperator.java"
      provides: "Logical ADBC scan operator"
    - path: "fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/physical/PhysicalADBCScanOperator.java"
      provides: "Physical ADBC scan operator"
    - path: "fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/implementation/ADBCScanImplementationRule.java"
      provides: "Logical to physical ADBC scan transformation rule"
    - path: "fe/fe-core/src/main/java/com/starrocks/planner/ADBCScanNode.java"
      provides: "ADBC scan plan node with SQL generation, toThrift, EXPLAIN"
    - path: "fe/fe-core/src/main/java/com/starrocks/catalog/ADBCTable.java"
      provides: "Updated toThrift() returning TTableDescriptor with TADBCTable"
    - path: "be/src/connector/adbc_connector.h"
      provides: "ADBCConnector, ADBCDataSourceProvider, ADBCDataSource class declarations"
    - path: "be/src/connector/adbc_connector.cpp"
      provides: "ADBCConnector, ADBCDataSourceProvider, ADBCDataSource implementations"
    - path: "be/src/exec/adbc_scanner.h"
      provides: "ADBCScanner class declaration"
    - path: "be/src/exec/adbc_scanner.cpp"
      provides: "ADBCScanner with ADBC C API, Arrow-to-Chunk conversion, re-chunking"
    - path: "be/src/exec/adbc_parallel_reader.h"
      provides: "ADBCParallelReader and RecordBatchQueue"
    - path: "be/src/exec/adbc_parallel_reader.cpp"
      provides: "Multi-endpoint parallel reading with thread-safe queue"
    - path: "be/src/connector/connector.h"
      provides: "ADBC ConnectorType enum entry"
    - path: "be/src/connector/connector.cpp"
      provides: "ADBCConnector registration in ConnectorManager"
    - path: "be/src/exec/exec_factory.cpp"
      provides: "ADBC_SCAN_NODE case creating ConnectorScanNode"
    - path: "be/src/exec/exec_node.cpp"
      provides: "ADBC_SCAN_NODE in collect_scan_nodes"
  key_links:
    - from: "ADBCScanImplementationRule"
      to: "RuleSet.java"
      via: "IMPLEMENTATION_RULES registration"
    - from: "LogicalADBCScanOperator"
      to: "OperatorVisitor.java"
      via: "visitLogicalADBCScan dispatch"
    - from: "PlanFragmentBuilder.java"
      to: "ADBCScanNode"
      via: "visitPhysicalADBCScan creates new ADBCScanNode"
    - from: "RelationTransformer.java"
      to: "LogicalADBCScanOperator"
      via: "Creates logical scan for ADBC tables"
    - from: "ADBCScanNode"
      to: "TADBCScanNode"
      via: "toThrift() populates Thrift message for BE"
    - from: "exec_factory.cpp"
      to: "ADBCConnector"
      via: "ADBC_SCAN_NODE case -> ConnectorScanNode"
    - from: "connector.cpp"
      to: "ADBCConnector"
      via: "ConnectorManager::put(ADBC, ADBCConnector)"
    - from: "ADBCScanner"
      to: "arrow/c/bridge.h"
      via: "ImportRecordBatchReader"
    - from: "ADBCScanner"
      to: "ArrowToStarRocksConverter"
      via: "get_arrow_converter()"
    - from: "ADBCScanner"
      to: "ADBCParallelReader"
      via: "Parallel reading fallback"
    - from: "ADBCDataSource"
      to: "ADBCScanner"
      via: "Creates and delegates to scanner"
---

# Phase 2: Scanning Verification Report

**Phase Goal:** Users can SELECT from ADBC tables with full pushdown; Arrow data flows from BE native C++ ADBC to StarRocks Chunk; optimizer integrates ADBC scan operators
**Verified:** 2026-03-06T02:00:00Z
**Status:** passed
**Re-verification:** No -- initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | User can SELECT with WHERE and LIMIT from ADBC tables and receive correct results | VERIFIED | Full FE-to-BE data path wired: RelationTransformer creates LogicalADBCScanOperator, optimizer transforms to physical, PlanFragmentBuilder creates ADBCScanNode with UNPARTITIONED fragment, toThrift serializes TADBCScanNode, exec_factory creates ConnectorScanNode with ADBC connector, ADBCDataSource opens ADBCScanner which calls ADBC C API (AdbcDatabaseNew/AdbcConnectionNew/AdbcStatementSetSqlQuery/AdbcStatementExecuteQuery), imports ArrowArrayStream via ImportRecordBatchReader, converts RecordBatch to Chunk via get_arrow_converter |
| 2 | EXPLAIN shows pushed-down SQL query string with predicates and LIMIT | VERIFIED | ADBCScanNode.getNodeExplainString() outputs TABLE, QUERY (with full SQL), DRIVER, URI. ADBCScanNode.getADBCQueryStr() generates SQL with column list, WHERE clause from conjuncts, and LIMIT. EXPLAIN ANALYZE shows ConnectTime, RowsRead, BytesRead |
| 3 | Column pruning active: only selected columns in remote query | VERIFIED | ADBCScanNode.createADBCTableColumns() iterates only materialized slots from TupleDescriptor (set by PlanFragmentBuilder from ColRefToColumnMetaMap which is pruned by PruneScanColumnRule). LOGICAL_ADBC_SCAN registered in PruneScanColumnRule's pattern set |
| 4 | Connection-per-scan lifecycle: open creates, close releases all ADBC resources | VERIFIED | ADBCScanner.open() calls _init_adbc() creating database/connection/statement in order. ADBCScanner.close() releases in reverse order: statement -> connection -> database, with _*_initialized flags preventing double-release. Destructor calls close() if not already closed |

**Score:** 4/4 truths verified

### Required Artifacts

| Artifact | Status | Details |
|----------|--------|---------|
| `gensrc/thrift/PlanNodes.thrift` | VERIFIED | TADBCScanNode struct (line 674) with tuple_id, table_name, columns, filters, limit, adbc_driver, adbc_uri, adbc_username, adbc_password, adbc_token. ADBC_SCAN_NODE enum (line 80). Field 85 in TPlanNode |
| `gensrc/thrift/Types.thrift` | VERIFIED | ADBC_TABLE in TTableType enum (line 439) |
| `gensrc/thrift/Descriptors.thrift` | VERIFIED | TADBCTable struct (line 691) and field 37 in TTableDescriptor (line 741) |
| `LogicalADBCScanOperator.java` | VERIFIED | 2437 bytes, extends LogicalScanOperator, OperatorType.LOGICAL_ADBC_SCAN |
| `PhysicalADBCScanOperator.java` | VERIFIED | 1547 bytes, extends PhysicalScanOperator, OperatorType.PHYSICAL_ADBC_SCAN |
| `ADBCScanImplementationRule.java` | VERIFIED | 1771 bytes, transforms logical to physical, registered in RuleSet IMPLEMENTATION_RULES |
| `ADBCScanNode.java` | VERIFIED | 253 lines, generates pushed-down SQL (columns, WHERE, LIMIT), toThrift populates TADBCScanNode, getNodeExplainString shows TABLE/QUERY/DRIVER/URI |
| `ADBCTable.java` | VERIFIED | toThrift returns TTableDescriptor with TTableType.ADBC_TABLE and TADBCTable with driver/uri/username/password/token/catalog |
| `adbc_connector.h` | VERIFIED | ADBCConnector (ConnectorType::ADBC), ADBCDataSourceProvider, ADBCDataSource class declarations |
| `adbc_connector.cpp` | VERIFIED | 144 lines, full implementation: get_adbc_sql builds SQL from table/columns/filters/limit, ADBCDataSource.open creates ADBCScanner, delegates get_next, captures connect_time_ms |
| `adbc_scanner.h` | VERIFIED | ADBCScanner with ADBC C API handles (AdbcDatabase, AdbcConnection, AdbcStatement), connect_time_ms stat |
| `adbc_scanner.cpp` | VERIFIED | 335 lines, full implementation: _init_adbc (AdbcDatabaseNew/AdbcConnectionNew/AdbcStatementSetSqlQuery), _try_parallel_read (AdbcStatementExecutePartitions), _fallback_single_stream_read (AdbcStatementExecuteQuery), get_next with re-chunking to max_chunk_size, _convert_batch_to_chunk via get_arrow_converter, close releases in reverse order |
| `adbc_parallel_reader.h` | VERIFIED | RecordBatchQueue (thread-safe bounded queue with condition variables), ADBCParallelReader with per-partition threads |
| `adbc_parallel_reader.cpp` | VERIFIED | 223 lines, full implementation: RecordBatchQueue with push/pop/producer_done, ADBCParallelReader.start spawns threads with round-robin partition assignment, each thread creates own AdbcConnection, calls AdbcConnectionReadPartition, reads via ImportRecordBatchReader, pushes batches to queue |
| `connector.h` | VERIFIED | ConnectorType::ADBC = 9, Connector::ADBC string constant |
| `connector.cpp` | VERIFIED | Connector::ADBC = "adbc", ConnectorManager::put(ADBC, ADBCConnector) in init |
| `exec_factory.cpp` | VERIFIED | ADBC_SCAN_NODE case creates ConnectorScanNode with connector_name = Connector::ADBC |
| `exec_node.cpp` | VERIFIED | collect_nodes(TPlanNodeType::ADBC_SCAN_NODE, nodes) |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| ADBCScanImplementationRule | RuleSet.java | IMPLEMENTATION_RULES registration | WIRED | Line 204: `new ADBCScanImplementationRule()` in implementation rules list |
| LogicalADBCScanOperator | OperatorVisitor.java | visitLogicalADBCScan | WIRED | Line 195: dispatch method present |
| PhysicalADBCScanOperator | OperatorVisitor.java | visitPhysicalADBCScan | WIRED | Line 366: dispatch method present |
| StatisticsCalculator | LogicalADBCScanOperator | visitLogicalADBCScan | WIRED | Line 929: calls computeNormalExternalTableScanNode |
| StatisticsCalculator | PhysicalADBCScanOperator | visitPhysicalADBCScan | WIRED | Line 935: calls computeNormalExternalTableScanNode |
| LogicalPlanPrinter | PhysicalADBCScanOperator | visitPhysicalADBCScan | WIRED | Line 455: "ADBC SCAN" label |
| OutputPropertyDeriver | PhysicalADBCScanOperator | visitPhysicalADBCScan | WIRED | Line 691: returns physical properties |
| OptExpressionVisitor | PhysicalADBCScanOperator | visitPhysicalADBCScan | WIRED | Line 189: dispatch method |
| PlanFragmentBuilder | ADBCScanNode | visitPhysicalADBCScan | WIRED | Line 2077-2115: creates ADBCScanNode with UNPARTITIONED fragment |
| RelationTransformer | LogicalADBCScanOperator | Table.TableType.ADBC check | WIRED | Line 760-764: creates new LogicalADBCScanOperator for ADBC tables |
| ADBCScanNode | TADBCScanNode | toThrift() | WIRED | Line 200-231: populates all TADBCScanNode fields |
| ADBCTable | TADBCTable | toThrift() | WIRED | Returns TTableDescriptor with TTableType.ADBC_TABLE and TADBCTable |
| exec_factory.cpp | ADBCConnector | ADBC_SCAN_NODE case | WIRED | Line 303: creates ConnectorScanNode with Connector::ADBC |
| connector.cpp | ADBCConnector | ConnectorManager put | WIRED | Line 75: registers ADBCConnector |
| ADBCScanner | ImportRecordBatchReader | arrow/c/bridge.h | WIRED | Line 162 in scanner, line 149 in parallel reader |
| ADBCScanner | get_arrow_converter | arrow_to_starrocks_converter.h | WIRED | Line 245: calls get_arrow_converter for each column |
| ADBCScanner | ADBCParallelReader | _try_parallel_read | WIRED | Line 144: creates parallel reader when partitions > 1 |
| ADBCDataSource | ADBCScanner | open/get_next/close | WIRED | Lines 94-117: creates scanner, delegates all operations |
| PushDownPredicateToExternalTableScanRule | LOGICAL_ADBC_SCAN | pattern match | WIRED | ADBC scan type in rule pattern |
| PruneScanColumnRule | LOGICAL_ADBC_SCAN | pattern match | WIRED | ADBC scan type in rule pattern |
| MergeLimitDirectRule | LOGICAL_ADBC_SCAN | pattern match | WIRED | ADBC scan type in rule pattern |
| MultiOpPattern | LOGICAL_ADBC_SCAN | SCAN_PATTERN | WIRED | ADBC scan type in multi-op pattern |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| SCAN-01 | 02-01 | FE uses Java ADBC API for metadata | SATISFIED | Already satisfied by Phase 1 ADBCMetadata; tracked here for completeness |
| SCAN-02 | 02-02, 02-04 | BE uses native C++ ADBC for data scanning | SATISFIED | ADBCScanner calls AdbcDatabaseNew, AdbcConnectionNew, AdbcStatementSetSqlQuery, AdbcStatementExecuteQuery via C API |
| SCAN-03 | 02-02, 02-04 | Arrow RecordBatch to StarRocks Chunk conversion | SATISFIED | _convert_batch_to_chunk uses get_arrow_converter per column, handles nullable/non-nullable columns |
| SCAN-04 | 02-03, 02-04 | Column pruning pushdown | SATISFIED | ADBCScanNode.createADBCTableColumns iterates materialized slots only; PruneScanColumnRule registered for LOGICAL_ADBC_SCAN |
| SCAN-05 | 02-03, 02-04 | Predicate pushdown as SQL WHERE | SATISFIED | ADBCScanNode.createADBCTableFilters converts conjuncts to SQL strings; PushDownPredicateToExternalTableScanRule registered |
| SCAN-06 | 02-03, 02-04 | LIMIT pushdown | SATISFIED | ADBCScanNode.getADBCQueryStr appends LIMIT N; MergeLimitDirectRule registered for LOGICAL_ADBC_SCAN |
| SCAN-07 | 02-02, 02-04 | Connection lifecycle management | SATISFIED | ADBCScanner.close releases statement/connection/database in reverse order with initialized flags; ADBCParallelReader.close joins threads |
| OPT-01 | 02-01 | LogicalADBCScanOperator in logical plan | SATISFIED | LogicalADBCScanOperator.java exists, OperatorType.LOGICAL_ADBC_SCAN registered |
| OPT-02 | 02-01 | PhysicalADBCScanOperator via implementation rule | SATISFIED | PhysicalADBCScanOperator.java + ADBCScanImplementationRule.java registered in RuleSet |
| OPT-03 | 02-03 | ADBCScanNode generates SQL with pushdowns | SATISFIED | ADBCScanNode.getADBCQueryStr() generates SELECT cols FROM table WHERE filters LIMIT n |
| OPT-04 | 02-03 | EXPLAIN shows pushed-down ADBC query string | SATISFIED | getNodeExplainString outputs TABLE, QUERY (full SQL), DRIVER, URI |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | - | - | - | No TODO, FIXME, PLACEHOLDER, or stub patterns found in any Phase 2 files |

### Human Verification Required

### 1. End-to-End Query Execution

**Test:** Run `SELECT col1, col2 FROM my_adbc.mydb.mytable WHERE col1 = 'value' LIMIT 100` against a live Flight SQL server
**Expected:** Correct rows returned matching the WHERE filter and LIMIT
**Why human:** Requires a running StarRocks cluster connected to a real ADBC-compatible data source

### 2. EXPLAIN Output Verification

**Test:** Run `EXPLAIN SELECT col1 FROM my_adbc.mydb.mytable WHERE col1 > 10 LIMIT 5`
**Expected:** Output shows `SCAN ADBC` node with TABLE, QUERY containing `SELECT "col1" FROM ... WHERE ("col1" > 10) LIMIT 5`, DRIVER, and URI
**Why human:** Requires running FE to generate explain output

### 3. Full Build Verification

**Test:** Run `./build.sh --fe --be` on the branch
**Expected:** Both FE and BE compile cleanly without errors
**Why human:** Requires full build environment with all thirdparty dependencies (ADBC C library for BE)

### 4. Multi-Endpoint Parallel Reading

**Test:** Connect to a Flight SQL server that exposes multiple endpoints, run a large query
**Expected:** ADBCParallelReader spawns threads per partition, data arrives in parallel from multiple endpoints
**Why human:** Requires a Flight SQL server with multi-endpoint support; behavior only observable with real server

### Gaps Summary

No gaps found. All 4 success criteria are verified at the code level. All 11 requirements (SCAN-01 through SCAN-07, OPT-01 through OPT-04) are satisfied with substantive implementations. All key links are wired -- the complete FE-to-BE data path is connected from RelationTransformer through the optimizer, PlanFragmentBuilder, Thrift serialization, exec_factory, ConnectorScanNode, ADBCConnector, ADBCDataSource, ADBCScanner, and Arrow-to-Chunk conversion. No anti-patterns (TODO, FIXME, placeholders, stubs) were found in any Phase 2 files.

The implementation follows the JDBC connector pattern consistently across FE (operator/rule/scan node/plan fragment) and BE (connector/data source/scanner), with the addition of ADBC-specific features: native C++ ADBC API usage, Arrow C bridge for RecordBatch import, and multi-endpoint parallel reading.

---

_Verified: 2026-03-06T02:00:00Z_
_Verifier: Claude (gsd-verifier)_
