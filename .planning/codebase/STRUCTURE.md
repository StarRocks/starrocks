# Codebase Structure

**Analysis Date:** 2026-03-04

## Directory Layout

```
starrocks/
├── fe/                        # Frontend (Java) - SQL parsing, optimization, metadata
│   ├── fe-core/               # Core FE logic (optimizer, catalog, qe, planner)
│   ├── fe-parser/             # SQL AST node definitions + parser utilities
│   ├── fe-grammar/            # ANTLR4 grammar files (StarRocks.g4, StarRocksLex.g4)
│   ├── fe-type/               # Type system definitions
│   ├── fe-spi/                # Service Provider Interfaces (connector/plugin contracts)
│   ├── fe-utils/              # General-purpose utilities (no business logic)
│   ├── fe-testing/            # Shared test utilities and mocks for FE tests
│   ├── connector/             # External data source connector README/stubs (see fe-core)
│   ├── hive-udf/              # Hive UDF compatibility layer
│   └── plugin/spark-dpp/      # Spark DPP (bulk load via Spark) application
│
├── be/                        # Backend (C++) - query execution, storage engine
│   ├── src/
│   │   ├── base/              # Minimal base primitives (Status, macros, uid)
│   │   ├── gutil/             # Low-level utilities (no BE module deps)
│   │   ├── common/            # Config, logging, process utilities
│   │   ├── types/             # Type system (TypesCore target)
│   │   ├── column/            # Column/Chunk types (ColumnCore target)
│   │   ├── exprs/             # Expression evaluation (ExprCore + full Exprs)
│   │   ├── runtime/           # Runtime state, MemTracker, ExecEnv, FragmentMgr
│   │   ├── exec/              # Query operators, pipeline engine
│   │   │   ├── pipeline/      # Pipeline execution framework + operators
│   │   │   ├── aggregate/     # Aggregation operator internals
│   │   │   ├── join/          # Join operator internals
│   │   │   ├── sorting/       # Sort operator internals
│   │   │   ├── spill/         # Spill-to-disk for memory-bounded ops
│   │   │   ├── workgroup/     # Resource group scheduling
│   │   │   ├── query_cache/   # Query result caching
│   │   │   └── stream/        # Stream (incremental MV) processing
│   │   ├── storage/           # Storage engine (tablets, rowsets, compaction)
│   │   │   └── rowset/        # Segment file format (pages, indexes, encodings)
│   │   ├── formats/           # File format readers/writers
│   │   │   ├── parquet/       # Apache Parquet
│   │   │   ├── orc/           # Apache ORC
│   │   │   ├── csv/           # CSV
│   │   │   ├── avro/          # Avro
│   │   │   └── json/          # JSON
│   │   ├── connector/         # BE-side external data connectors (file, ES, binlog)
│   │   ├── fs/                # Filesystem abstraction (POSIX, HDFS, S3, Azure, StarLet)
│   │   ├── agent/             # Agent/task handlers (from FE: create tablet, compaction)
│   │   ├── service/           # Thrift/bRPC service implementations + main()
│   │   ├── http/              # BE HTTP endpoints (health, metrics, debug)
│   │   ├── cache/             # DataCache (block cache for shared-data)
│   │   ├── serde/             # Serialization utilities
│   │   ├── io/                # I/O abstractions
│   │   ├── udf/               # UDF runtime infrastructure
│   │   ├── util/              # General utilities
│   │   ├── simd/              # SIMD helpers
│   │   ├── geo/               # Geospatial support
│   │   └── gen_cpp/           # Generated Thrift/Protobuf C++ code
│   └── test/                  # BE unit tests (mirrors be/src/ structure)
│
├── java-extensions/           # JNI-based Java readers used by BE connectors
│   ├── jni-connector/         # Shared JNI interface + off-heap ColumnVector
│   ├── common-runtime/        # Shared runtime for java-extensions
│   ├── jdbc-bridge/           # JDBC data source bridge
│   ├── hive-reader/           # Hive file reader
│   ├── iceberg-metadata-reader/ # Iceberg metadata reader
│   ├── hudi-reader/           # Apache Hudi reader
│   ├── paimon-reader/         # Apache Paimon reader
│   ├── kudu-reader/           # Apache Kudu reader
│   ├── odps-reader/           # Alibaba ODPS reader
│   ├── hadoop-ext/            # Hadoop extensions
│   └── udf-extensions/        # UDF extension support
│
├── gensrc/                    # Interface definitions → generated code
│   ├── thrift/                # Thrift IDL files (FE↔BE RPC contracts)
│   └── proto/                 # Protobuf IDL files (storage, lake service)
│
├── test/                      # SQL integration tests (requires running cluster)
│   ├── sql/                   # SQL test case files
│   ├── run.py                 # Test runner
│   └── common/                # Test framework utilities
│
├── docs/                      # Documentation (Docusaurus)
│   ├── en/                    # English docs
│   └── zh/                    # Chinese docs
│
├── bin/                       # Start/stop scripts (start_fe.sh, start_be.sh)
├── conf/                      # Default configuration files
├── docker/                    # Docker build files
├── build-support/             # Build system helpers, dependency scripts
├── thirdparty/                # Third-party C++ dependency source trees
├── tools/                     # Diagnostic and benchmark tools
├── contrib/                   # External contributions
├── fs_brokers/                # Broker process for HDFS/S3 (legacy)
├── community/                 # Community governance docs
├── extra/                     # Extra scripts and utilities
└── format-sdk/                # StarRocks format SDK for external access
```

## Directory Purposes

**`fe/fe-core/src/main/java/com/starrocks/`:**
- Purpose: The core FE business logic — everything that makes StarRocks a database
- Key sub-packages:
  - `sql/parser/`: SQL text → AST
  - `sql/analyzer/`: AST semantic validation
  - `sql/optimizer/`: Logical → physical plan optimization
  - `sql/plan/`: Physical plan fragment building (`PlanFragmentBuilder.java`, `ExecPlan.java`)
  - `planner/`: Plan node classes (`OlapScanNode.java`, `HashJoinNode.java`, etc.)
  - `catalog/`: Table/database/function metadata models
  - `server/`: `GlobalStateMgr.java` + subsystem managers
  - `qe/`: Query execution coordination (`DefaultCoordinator.java`, `StmtExecutor.java`)
  - `connector/`: External catalog connector framework
  - `transaction/`: MVCC transaction management
  - `scheduler/`: Task scheduler for async jobs (MV refresh, statistics)
  - `statistic/`: Statistics collection and histogram management
  - `authorization/`: RBAC privilege model
  - `lake/`: Shared-data (Lake) mode specific logic
  - `mv/`: Materialized view management and analyzer
  - `load/`: Data loading job management
  - `ha/`: FE leader election and HA state machine
  - `journal/`: BDB JE write-ahead log for metadata persistence
  - `persist/`: Journal log entry types (one class per DDL operation)
  - `mysql/`: MySQL wire protocol implementation
  - `http/`: HTTP server and REST action handlers
  - `rpc/`: bRPC/Thrift client wrappers for FE→BE calls

**`fe/fe-parser/src/main/java/com/starrocks/sql/ast/`:**
- Purpose: All AST node classes generated/maintained alongside the grammar
- Key files: `SelectRelation.java`, `QueryStatement.java`, `CreateTableStmt.java`, etc.
- Note: AST nodes must remain immutable (no setters); parser visitor creates them

**`fe/fe-grammar/src/main/antlr/com/starrocks/grammar/`:**
- Purpose: ANTLR4 grammar defining the StarRocks SQL dialect
- Key files: `StarRocks.g4` (parser rules), `StarRocksLex.g4` (lexer rules)

**`fe/fe-spi/`:**
- Purpose: Stable interfaces that connectors and plugins implement; no internal FE dependencies
- Key files: `AuthenticationProvider.java`, `StarRocksException.java`, `ErrorCode.java`

**`be/src/exec/pipeline/`:**
- Purpose: The core pipeline execution framework
- Key files: `operator.h` (operator base), `pipeline_driver.h` (driver scheduling), `pipeline_driver_executor.h` (thread pool), `fragment_executor.h` (fragment lifecycle), `pipeline_builder.h`

**`be/src/storage/`:**
- Purpose: LSM-tree-inspired tablet storage with columnar segments
- Key files: `tablet.h`, `base_tablet.h`, `tablet_manager.h`, `base_compaction.h`, `async_delta_writer.h`

**`be/src/storage/rowset/`:**
- Purpose: Segment file format (pages, column chunks, indexes, encodings)
- Key files: `segment.h`, `segment_writer.h`, `segment_iterator.h`, `bitmap_index_writer.h`

**`be/src/column/`:**
- Purpose: All in-memory column types and chunk abstraction
- Key files: `column.h`, `chunk.h`, `nullable_column.h`, `binary_column.h`, `array_column.h`

**`gensrc/thrift/`:**
- Purpose: Thrift IDL defining the FE↔BE RPC contract
- Key files: `InternalService.thrift` (fragment exec, tablet ops), `FrontendService.thrift` (FE-facing API), `BackendService.thrift`, `PlanNodes.thrift`, `Descriptors.thrift`

**`gensrc/proto/`:**
- Purpose: Protobuf IDL for storage metadata and lake service
- Key files: `internal_service.proto`, `lake_service.proto`, `olap_file.proto`, `tablet_schema.proto`, `segment.proto`

**`java-extensions/jni-connector/`:**
- Purpose: Shared JNI interface and off-heap `ColumnVector` format for BE↔Java data transfer
- Used by all java-extensions readers as the data exchange contract

**`test/sql/`:**
- Purpose: SQL-driven integration test cases executed against a live cluster
- Run with: `cd test && python3 run.py -v`

## Key File Locations

**Entry Points:**
- `fe/fe-core/src/main/java/com/starrocks/StarRocksFEServer.java`: FE JVM main
- `be/src/service/starrocks_main.cpp`: BE process main
- `fe/fe-core/src/main/java/com/starrocks/qe/ConnectProcessor.java`: SQL query entry
- `be/src/service/internal_service.cpp`: Fragment execution entry

**Configuration:**
- `fe/fe-core/src/main/java/com/starrocks/common/Config.java`: All FE config (`@ConfField`)
- `be/src/common/config.h`: All BE config (`CONF_*` macros)
- `docs/en/administration/management/FE_configuration.md`: FE config documentation
- `docs/en/administration/management/BE_configuration.md`: BE config documentation

**Core Logic:**
- `fe/fe-core/src/main/java/com/starrocks/server/GlobalStateMgr.java`: FE central state (singleton)
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/Optimizer.java`: CBO optimizer entry
- `fe/fe-core/src/main/java/com/starrocks/sql/StatementPlanner.java`: Plan pipeline orchestrator
- `fe/fe-core/src/main/java/com/starrocks/qe/DefaultCoordinator.java`: Fragment scheduling
- `be/src/exec/pipeline/pipeline_driver_executor.h`: Pipeline thread pool scheduler
- `be/src/storage/tablet.h`: Core storage tablet abstraction
- `be/src/column/chunk.h`: Vectorized data batch

**Testing:**
- `fe/fe-core/src/test/java/com/starrocks/`: FE JUnit 5 tests
- `be/test/`: BE Google Test suites (mirrors `be/src/` structure)
- `test/sql/`: SQL integration test cases

**Build:**
- `build.sh`: Top-level build script
- `fe/pom.xml`: Maven multi-module POM
- `be/CMakeLists.txt`: BE CMake build definition

## Naming Conventions

**Files (C++):**
- Source: `snake_case.cpp` / `snake_case.h` — e.g., `tablet_manager.cpp`, `hash_join_node.h`
- Tests: `snake_case_test.cpp` — e.g., `tablet_test.cpp`

**Files (Java):**
- Source: `PascalCase.java` — e.g., `GlobalStateMgr.java`, `HashJoinNode.java`
- Tests: `PascalCaseTest.java` — e.g., `TPCHPlanTest.java`

**Directories:**
- FE packages: `lowercase` (Java convention) — e.g., `com.starrocks.sql.optimizer`
- BE modules: `snake_case` — e.g., `exec/pipeline/`, `storage/rowset/`

**C++ Identifiers:**
- Classes: `PascalCase` — e.g., `TabletManager`, `HashJoinNode`
- Functions/methods: `snake_case` — e.g., `create_tablet()`, `push_chunk()`
- Member variables: `_snake_case` prefix — e.g., `_tablet_id`, `_path`
- Constants: `ALL_CAPS` — e.g., `MAX_SEGMENT_SIZE`
- Namespace: `starrocks`

**Java Identifiers:**
- Classes/interfaces: `PascalCase`
- Methods/variables: `camelCase`
- Constants: `UPPER_SNAKE_CASE`
- Package: `com.starrocks.*`

**Thrift/Proto:**
- Thrift structs: `TPascalCase` — e.g., `TExecPlanFragmentParams`
- Proto messages: `PascalCasePB` — e.g., `TabletSchemaPB`
- All fields: `snake_case`

## Where to Add New Code

**New SQL Feature (FE):**
1. Grammar: `fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocks.g4`
2. AST node: `fe/fe-parser/src/main/java/com/starrocks/sql/ast/`
3. Analyzer: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/`
4. Optimizer rules: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/` or `rule/implementation/`
5. Plan node: `fe/fe-core/src/main/java/com/starrocks/planner/`
6. Tests: `fe/fe-core/src/test/java/com/starrocks/sql/plan/` + `test/sql/`

**New BE Execution Operator:**
1. Operator class: `be/src/exec/pipeline/` (implement `Operator` base from `operator.h`)
2. Supporting node (non-pipeline): `be/src/exec/`
3. Test: `be/test/exec/`

**New External Catalog Connector (FE side):**
1. Connector implementation: `fe/fe-core/src/main/java/com/starrocks/connector/` (sub-package per connector)
2. Metadata class: `fe/fe-core/src/main/java/com/starrocks/catalog/` (table subclass)
3. Scan node: `fe/fe-core/src/main/java/com/starrocks/planner/` + optimizer implementation rule in `rule/implementation/`
4. Register in `ConnectorFactory.java`

**New Java Extension (BE JNI reader):**
1. Module directory: `java-extensions/<name>-reader/`
2. Implement the JNI interface from `java-extensions/jni-connector/`
3. BE connector: `be/src/connector/`

**New FE Configuration Parameter:**
1. Add `@ConfField` annotated field to `fe/fe-core/src/main/java/com/starrocks/common/Config.java`
2. Update `docs/en/administration/management/FE_configuration.md`

**New BE Configuration Parameter:**
1. Add `CONF_*` macro to `be/src/common/config.h`
2. Update `docs/en/administration/management/BE_configuration.md`

**New Thrift RPC (FE↔BE):**
1. Add to appropriate `.thrift` file in `gensrc/thrift/`
2. Run `make` in `gensrc/` to regenerate
3. Implement handler in `be/src/service/internal_service.cpp` (BE side)
4. Add client call in `fe/fe-core/src/main/java/com/starrocks/rpc/` (FE side)

**New Protobuf Message (Storage):**
1. Add to appropriate `.proto` file in `gensrc/proto/`
2. Run `make` in `gensrc/` to regenerate
3. Never use `required` fields; never change existing field ordinals

**Utilities:**
- FE shared helpers: `fe/fe-utils/` (no business logic) or `fe/fe-core/src/main/java/com/starrocks/common/`
- BE shared helpers: `be/src/util/`
- BE base primitives (minimal deps): `be/src/base/`

## Special Directories

**`gensrc/build/`:**
- Purpose: Generated Thrift/Protobuf C++ and Java code
- Generated: Yes (by `make` in `gensrc/`)
- Committed: No (generated at build time)

**`be/src/gen_cpp/`:**
- Purpose: Generated C++ code from Thrift/Protobuf, checked in for build convenience
- Generated: Yes
- Committed: Yes (to avoid requiring Thrift compiler on every dev machine)

**`thirdparty/`:**
- Purpose: Third-party C++ dependency source trees (RocksDB, Arrow, bRPC, etc.)
- Generated: No (source trees)
- Committed: Yes (vendored)

**`.planning/`:**
- Purpose: GSD planning and codebase analysis documents for AI agent context
- Generated: Yes (by GSD map-codebase commands)
- Committed: Developer discretion

**`fe/fe-core/src/test/java/`:**
- Purpose: FE JUnit 5 unit + integration tests
- Generated: No
- Committed: Yes

**`be/test/`:**
- Purpose: BE Google Test unit tests (mirrors `be/src/` structure)
- Generated: No
- Committed: Yes

---

*Structure analysis: 2026-03-04*
