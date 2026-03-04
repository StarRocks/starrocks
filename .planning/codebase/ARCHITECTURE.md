# Architecture

**Analysis Date:** 2026-03-04

## Pattern Overview

**Overall:** Decoupled MPP (Massively Parallel Processing) analytical database with a Java Frontend (FE) for SQL/metadata and a C++ Backend (BE) for execution/storage. Communication uses Thrift RPC, Protobuf, and Apache bRPC.

**Key Characteristics:**
- Strict FE/BE separation: FE never touches storage; BE never parses SQL
- Pipeline-based vectorized execution engine in BE (columnar `Chunk` processing)
- Cost-based query optimizer (CBO) with rule-based transformations and Cascades-style Memo in FE
- Supports two deployment modes: shared-nothing (each BE owns its tablets) and shared-data (Lake mode, using StarOS/S3 remote storage)
- HA via BerkeleyDB JE leader election for FE; horizontal scaling for BE

## Layers

**MySQL Protocol Layer (FE):**
- Purpose: Accept client connections using the MySQL wire protocol
- Location: `fe/fe-core/src/main/java/com/starrocks/mysql/`
- Contains: MySQL packet encoding/decoding, handshake, command handling
- Depends on: Query Execution layer
- Used by: External clients (MySQL-compatible drivers, tools)

**HTTP/REST API Layer (FE):**
- Purpose: Admin actions, stream load, schema change, monitoring
- Location: `fe/fe-core/src/main/java/com/starrocks/http/`
- Contains: `HttpServer`, `BaseAction` handlers, REST endpoints under `/api/v2/`
- Depends on: GlobalStateMgr, ConnectContext
- Used by: Admin scripts, stream load clients, monitoring systems

**SQL Parsing Layer (FE):**
- Purpose: Convert SQL text to typed AST
- Location: `fe/fe-grammar/` (ANTLR grammar), `fe/fe-parser/` (AST nodes), `fe/fe-core/src/main/java/com/starrocks/sql/parser/`
- Contains: `StarRocks.g4`, `StarRocksLex.g4` grammar files; AST node classes in `com.starrocks.sql.ast`
- Depends on: fe-grammar (generated parser), fe-type (type system)
- Used by: Semantic Analyzer

**Semantic Analysis Layer (FE):**
- Purpose: Name resolution, type checking, privilege verification, constant folding
- Location: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/`
- Contains: Per-statement `*Analyzer.java` classes (e.g., `SelectAnalyzer`, `Authorizer`)
- Depends on: Catalog/metadata, fe-parser AST
- Used by: Optimizer

**Query Optimizer (FE):**
- Purpose: Transform logical plan to optimal physical plan using CBO + rule-based rewrites
- Location: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/`
- Contains: `Optimizer.java`, `Memo.java` (Cascades Memo), `Group.java`, `GroupExpression.java`; rules in `rule/transformation/` and `rule/implementation/`; MV rewrite in `MaterializedViewOptimizer.java`
- Depends on: Statistics (`com.starrocks.statistic`), Catalog, Cost model
- Used by: Physical Plan Builder

**Physical Plan Builder (FE):**
- Purpose: Translate optimized logical plan to executable plan fragments
- Location: `fe/fe-core/src/main/java/com/starrocks/sql/plan/`, `fe/fe-core/src/main/java/com/starrocks/planner/`
- Contains: `PlanFragmentBuilder.java`, `StatementPlanner.java`, plan nodes in `com.starrocks.planner` (e.g., `OlapScanNode`, `HashJoinNode`, `AggregationNode`)
- Depends on: Optimizer output, Catalog
- Used by: Query Execution Coordinator

**Query Execution Coordinator (FE):**
- Purpose: Schedule plan fragments to BE nodes, collect results
- Location: `fe/fe-core/src/main/java/com/starrocks/qe/`
- Contains: `DefaultCoordinator.java`, `ConnectContext.java`, `StmtExecutor.java`, `BackendSelector.java`
- Depends on: Physical plan, NodeMgr (BE topology), RPC clients
- Used by: MySQL/HTTP protocol handlers

**Thrift/bRPC Communication:**
- Purpose: FE-to-BE RPC for fragment deployment, heartbeat, agent tasks
- Location: `gensrc/thrift/` (Thrift IDL), `gensrc/proto/` (Protobuf IDL), `fe/fe-core/src/main/java/com/starrocks/rpc/`
- Contains: Key IDLs: `InternalService.thrift`, `FrontendService.thrift`, `BackendService.thrift`, `internal_service.proto`, `lake_service.proto`
- Depends on: Nothing (interface boundary)
- Used by: Both FE and BE

**BE Service Layer (BE):**
- Purpose: Receive fragment execution requests from FE, expose heartbeat and agent services
- Location: `be/src/service/`
- Contains: `starrocks_main.cpp` (process entry), `internal_service.cpp`, `backend_base.cpp`, `daemon.cpp`
- Depends on: Fragment Manager, Agent Server
- Used by: FE coordinator (via Thrift/bRPC)

**Agent/Task Layer (BE):**
- Purpose: Execute management tasks from FE (tablet creation, schema change, compaction triggers)
- Location: `be/src/agent/`
- Contains: `agent_server.cpp`, `heartbeat_server.cpp`, `agent_task.cpp`, `publish_version.cpp`
- Depends on: Storage Engine
- Used by: FE via heartbeat and agent service Thrift

**Pipeline Execution Engine (BE):**
- Purpose: Vectorized pipeline-based query execution
- Location: `be/src/exec/pipeline/`
- Contains: `pipeline_driver.cpp`, `pipeline_driver_executor.cpp`, `operator.h` (base), `fragment_executor.cpp`, workgroup scheduling in `be/src/exec/workgroup/`
- Depends on: Column/Chunk layer, Runtime state, Expression evaluation
- Used by: BE Service layer (fragment execution requests)

**Operators (BE):**
- Purpose: Individual query operators: scan, join, aggregate, sort, exchange
- Location: `be/src/exec/` (scan/join/agg/sort nodes), `be/src/exec/pipeline/` (pipeline operator wrappers), `be/src/exec/aggregate/`, `be/src/exec/join/`, `be/src/exec/sorting/`
- Contains: `hash_join_node.cpp`, `aggregator.cpp`, `chunks_sorter*.cpp`, `connector_scan_node.cpp`, `exchange_node.cpp`
- Depends on: Column layer, Storage layer (for scan), Connector layer (external scans)
- Used by: Pipeline execution engine

**Column/Chunk Layer (BE):**
- Purpose: In-memory columnar data representation and operations
- Location: `be/src/column/`
- Contains: `column.h` (base), `chunk.h`, `binary_column.h`, `array_column.h`, `nullable_column.h`, etc.
- Depends on: base, gutil, TypesCore
- Used by: All execution operators, storage engine read/write paths

**Expression Evaluation (BE):**
- Purpose: Evaluate SQL expressions over columnar data
- Location: `be/src/exprs/`
- Contains: `expr.h` (base), function implementations, predicate evaluation
- Depends on: Column layer, Runtime state
- Used by: Operators (filter, project, aggregate)

**Connector Layer (BE):**
- Purpose: Read data from external sources (Hive, Iceberg, Delta Lake, etc.)
- Location: `be/src/connector/`
- Contains: `connector.h`, `file_connector.cpp`, `es_connector.cpp`, `binlog_connector.cpp`
- Depends on: Formats layer, Filesystem abstraction
- Used by: `connector_scan_node.cpp` in exec

**Format Readers/Writers (BE):**
- Purpose: Read/write columnar file formats
- Location: `be/src/formats/`
- Contains: `parquet/`, `orc/`, `csv/`, `avro/`, `json/`
- Depends on: Column layer, Filesystem abstraction
- Used by: Connector layer, file scan operators

**Storage Engine (BE):**
- Purpose: Manage tablets, rowsets, segments; handle compaction, versioning, transactions
- Location: `be/src/storage/`
- Contains: `tablet.h`, `base_tablet.h`, `rowset/` (segment files), `base_compaction.cpp`, `async_delta_writer.cpp`, `chunk_iterator.h`
- Depends on: Column layer, Filesystem abstraction
- Used by: Scan operators (local OLAP scan), Agent tasks

**Filesystem Abstraction (BE):**
- Purpose: Unified file I/O interface over local POSIX, HDFS, S3, Azure, StarLet (shared-data)
- Location: `be/src/fs/`
- Contains: `fs.h` (interface), `fs_posix.cpp`, `fs_broker.cpp`, `fs_starlet.cpp`, `hdfs/`, `s3/`, `azure/`
- Depends on: Nothing platform-specific at header level
- Used by: Storage engine, format readers/writers

**Metadata Management (FE):**
- Purpose: Catalog, database, table, partition, tablet, function registry
- Location: `fe/fe-core/src/main/java/com/starrocks/catalog/`, `fe/fe-core/src/main/java/com/starrocks/server/`
- Contains: `GlobalStateMgr.java` (singleton God object), `LocalMetastore.java`, `CatalogMgr.java`, `MetadataMgr.java`, `Database.java`, `OlapTable.java`, `MaterializedView.java`
- Depends on: Journal (BDB JE persistence), ConnectorMgr
- Used by: Optimizer, Coordinator, DDL handlers

**Journal/Persistence Layer (FE):**
- Purpose: Write-ahead log for all metadata changes; enables FE HA replay
- Location: `fe/fe-core/src/main/java/com/starrocks/journal/`
- Contains: `Journal.java`, `BDBJEJournal.java`, `JournalWriter.java`, `CheckpointWorker.java`
- Depends on: BerkeleyDB JE
- Used by: GlobalStateMgr, all DDL operations

**External Connector Framework (FE):**
- Purpose: Federate queries to external catalogs (Hive, Iceberg, Delta Lake, JDBC, etc.)
- Location: `fe/fe-core/src/main/java/com/starrocks/connector/`
- Contains: `Connector.java` (interface), `ConnectorMgr.java`, `ConnectorFactory.java`, per-connector implementations (hive/, iceberg/, delta/, jdbc/, etc.)
- Depends on: Catalog, java-extensions JNI bridge
- Used by: Optimizer (external scan rules), Metadata management

**Java Extensions / JNI Bridge:**
- Purpose: JNI-based readers for Hive/Iceberg/Hudi/Paimon/JDBC data sources used by BE
- Location: `java-extensions/`
- Contains: `jdbc-bridge/`, `hive-reader/`, `iceberg-metadata-reader/`, `jni-connector/`, `paimon-reader/`, `hudi-reader/`, `kudu-reader/`, `odps-reader/`
- Depends on: `common-runtime/`, `jni-connector/` (shared JNI interface)
- Used by: BE connector layer via JNI calls

## Data Flow

**Query Execution Flow:**

1. Client connects via MySQL protocol to FE (`MysqlServer` → `ConnectScheduler`)
2. `ConnectProcessor` reads SQL text and dispatches to `StmtExecutor`
3. `StmtExecutor` calls `StatementPlanner.plan()` which invokes parsing → semantic analysis → optimization → physical planning
4. `DefaultCoordinator` serializes plan fragments as Thrift structs and deploys to BE nodes via `BackendServiceClient`
5. BE receives fragment via `internal_service.cpp`, creates `PipelineFragmentContext`, builds pipeline operators
6. Pipeline `PipelineDriverExecutor` runs drivers (operator chains) across threads; scan operators read from storage or external connectors
7. Results stream back through exchange operators; FE coordinator collects and sends to client via MySQL protocol

**Data Ingestion Flow (Stream Load / Insert):**

1. HTTP request hits FE `LoadAction` or INSERT SQL parsed by FE
2. FE creates load job, assigns transaction, sends tablet write plan to BE
3. BE `DeltaWriter` / `AsyncDeltaWriter` receives data, writes to tablet rowsets
4. On commit, FE `GlobalTransactionMgr` coordinates tablet version publication
5. `PublishVersionDaemon` updates visible version across replicas

**Materialized View Refresh Flow:**

1. `TaskManager` / `TaskRunScheduler` triggers MV refresh task
2. `MVTaskRunProcessor` generates INSERT OVERWRITE plan for the MV
3. Plan executes through normal query path against base tables
4. New MV partitions are committed and versions published

**State Management:**
- FE state: single `GlobalStateMgr` singleton; mutations written to BDB JE journal; followers replay journal entries
- BE state: per-tablet `Tablet` objects managed by `StorageEngine` singleton; versioned via rowset visible-version mechanism
- Session state: `ConnectContext` per connection (session variables, transaction state, query ID)

## Key Abstractions

**`Chunk` (BE):**
- Purpose: A batch of rows represented as a collection of `Column` objects (vectorized processing unit)
- Examples: `be/src/column/chunk.h`, `be/src/column/chunk.cpp`
- Pattern: All operators consume and produce `Chunk` objects; size typically 4096 rows (configurable)

**`Column` (BE):**
- Purpose: Type-specific columnar array (Int32Column, BinaryColumn, NullableColumn, ArrayColumn, etc.)
- Examples: `be/src/column/column.h`, `be/src/column/binary_column.h`, `be/src/column/nullable_column.h`
- Pattern: Operators call typed `down_cast<>` to access specific column type; `ColumnPtr = std::shared_ptr<Column>`

**`Status` / `StatusOr<T>` (BE):**
- Purpose: Error-propagating return type throughout BE C++ code
- Examples: `be/src/common/status.h`
- Pattern: `RETURN_IF_ERROR(expr)` macro propagates errors; `StatusOr<T>` carries value or error

**`Operator` (BE Pipeline):**
- Purpose: Base class for all pipeline execution operators (source, transform, sink)
- Examples: `be/src/exec/pipeline/operator.h`
- Pattern: Each operator implements `push_chunk()`, `pull_chunk()`, `has_output()`, `needs_input()` for async scheduling

**`OptExpression` (FE Optimizer):**
- Purpose: Node in the optimizer's logical/physical plan tree
- Examples: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/OptExpression.java`
- Pattern: Wraps an `Operator` (logical or physical) with children; transformed by rules in Cascades Memo

**`GlobalStateMgr` (FE):**
- Purpose: Central singleton owning all FE subsystems (LocalMetastore, ConnectorMgr, AuthorizationMgr, GlobalTransactionMgr, etc.)
- Examples: `fe/fe-core/src/main/java/com/starrocks/server/GlobalStateMgr.java`
- Pattern: `GlobalStateMgr.getCurrentState()` static accessor used everywhere in FE; ~4000 lines

**`ConnectContext` (FE):**
- Purpose: Per-connection context holding session variables, user identity, query ID, transaction state
- Examples: `fe/fe-core/src/main/java/com/starrocks/qe/ConnectContext.java`
- Pattern: Thread-local via `ConnectContext.get()`; passed explicitly through analysis and planning phases

**Tablet (Storage):**
- Purpose: The basic storage unit — a horizontal partition of a table, replicated across BEs
- Examples: `be/src/storage/tablet.h`, `fe/fe-core/src/main/java/com/starrocks/catalog/Tablet.java`
- Pattern: FE tracks tablet metadata (location, replica); BE owns actual data in rowsets/segments

## Entry Points

**FE Process:**
- Location: `fe/fe-core/src/main/java/com/starrocks/StarRocksFEServer.java`
- Triggers: `bin/start_fe.sh` JVM startup
- Responsibilities: Initialize `GlobalStateMgr`, start MySQL server, HTTP server, journal, HA election, heartbeat

**BE Process:**
- Location: `be/src/service/starrocks_main.cpp`
- Triggers: `bin/start_be.sh` process launch
- Responsibilities: Initialize `ExecEnv` (runtime singletons), start Thrift services (heartbeat, backend, agent), start pipeline executor threads

**SQL Query Entry (FE):**
- Location: `fe/fe-core/src/main/java/com/starrocks/qe/ConnectProcessor.java`
- Triggers: MySQL client sends `COM_QUERY` packet
- Responsibilities: Dispatch to `StmtExecutor`, stream results back

**Fragment Execution Entry (BE):**
- Location: `be/src/service/internal_service.cpp` → `FragmentMgr::exec_plan_fragment()`
- Triggers: FE sends `exec_plan_fragment` Thrift/bRPC call
- Responsibilities: Deserialize plan, build pipeline graph, schedule drivers

**HTTP Load Entry (FE):**
- Location: `fe/fe-core/src/main/java/com/starrocks/http/` action classes
- Triggers: HTTP POST to `/api/{db}/{table}/_stream_load` or similar
- Responsibilities: Parse load request, create load job, coordinate with BE

## Error Handling

**Strategy:**
- FE: Java exception hierarchy (`StarRocksException` → `SemanticException`, `DdlException`, `AnalysisException`, etc.); propagated up to `StmtExecutor` which converts to MySQL error packets
- BE: `Status`/`StatusOr<T>` return values; `RETURN_IF_ERROR` macro for propagation; `CHECK`/`DCHECK` for invariants

**Patterns:**
- BE: `StatusOr<T>` used for functions returning a value that may fail; `Status::OK()` indicates success
- BE: `LOG(ERROR)` + return error status (not exceptions) for recoverable errors
- FE: `ErrorReport.reportDdlException()` / `throw new DdlException(msg)` at DDL boundary
- FE: `Preconditions.checkArgument()` / `Preconditions.checkState()` for programmer errors

## Cross-Cutting Concerns

**Logging:**
- BE: Google Logging (glog) — `LOG(INFO)`, `LOG(WARNING)`, `LOG(ERROR)`, `VLOG(n)` in `be/src/common/logging.h`
- FE: Log4j2 — `LogManager.getLogger(MyClass.class)` pattern in every class

**Validation:**
- FE: Semantic analysis in `com.starrocks.sql.analyzer` validates all SQL; privilege checks in `Authorizer.java`
- BE: Input validation at service boundary in `internal_service.cpp`; column type checks in operators

**Authentication:**
- FE: MySQL authentication via `MysqlProto.java`; LDAP/SASL/JWT via `AuthenticationMgr`; RBAC via `AuthorizationMgr`
- Ranger integration: `RangerStarRocksAccessController` for external policy enforcement

**Configuration:**
- FE: `fe/fe-core/src/main/java/com/starrocks/common/Config.java` — `@ConfField` annotated static fields
- BE: `be/src/common/config.h` — `CONF_Int64`, `CONF_Bool`, `CONF_mInt64` macros (prefix `m` = mutable at runtime)

**Memory Management (BE):**
- `MemTracker` hierarchy: `be/src/runtime/mem_tracker.h` — tracks memory per query/fragment/global
- `MemTrackerManager` manages tracker tree; queries are killed if they exceed limits
- Column data uses `ColumnAllocator` for pooled allocation

---

*Architecture analysis: 2026-03-04*
