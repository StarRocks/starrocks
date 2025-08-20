# GitHub Copilot Instructions for StarRocks

## Project Overview

StarRocks is a high-performance, cloud-native analytical database system designed for real-time analytics and ad-hoc queries. It features a streamlined architecture with both shared-nothing and shared-data deployment modes, supporting sub-second query performance for complex analytical workloads.

**Key Technologies:**
- **Backend (BE)**: C++ - Core analytical engine, storage layer, and query execution
- **Frontend (FE)**: Java - SQL parsing, query planning, metadata management, and coordination
- **Java Extensions**: Java - External connectors and UDF framework
- **Testing**: Python - Integration tests and SQL test framework

## Architecture Components

### Backend (be/) - C++
The core analytical engine responsible for data storage, processing, and query execution:

**Core Components:**
- `be/src/exec/` - Query execution operators (scan, join, aggregate, etc.)
- `be/src/storage/` - Storage engine (tablets, rowsets, segments, compaction)
- `be/src/exprs/` - Expression evaluation and vectorized computation
- `be/src/formats/` - Data format support (Parquet, ORC, CSV, JSON)
- `be/src/runtime/` - Runtime services (memory management, load balancing, stream processing)
- `be/src/connector/` - External data source connectors (Hive, Iceberg, Delta Lake)
- `be/src/service/` - RPC services and BE coordination
- `be/src/common/` - Shared utilities and common data structures

**Performance Focus:**
- Vectorized query execution
- Columnar storage format
- Memory-efficient algorithms
- SIMD optimizations where applicable

📋 **Note:** See `be/.cursorrules` for detailed backend component breakdown

### Frontend (fe/) - Java
SQL interface and query coordination layer:

**Core Components:**
- `fe/fe-core/src/main/java/com/starrocks/`
  - `sql/` - SQL parser, analyzer, and AST
  - `planner/` - Query planning and optimization (CBO)
  - `catalog/` - Metadata management (tables, partitions, statistics)
  - `scheduler/` - Query scheduling and execution coordination
  - `load/` - Data loading coordination (Broker Load, Stream Load, etc.)
  - `backup/` - Backup and restore functionality
  - `privilege/` - Authentication and authorization
  - `qe/` - Query execution coordination and session management
- `fe/fe-common/` - Common frontend utilities
- `fe/plugin-common/` - Plugin framework common components
- `fe/spark-dpp/` - Spark data preprocessing integration
- `fe/hive-udf/` - Hive UDF compatibility layer

**Key Responsibilities:**
- Parse and validate SQL statements
- Generate optimized query plans using Cost-Based Optimizer (CBO)
- Manage cluster metadata and coordination
- Handle user sessions and security

📋 **Note:** See `fe/.cursorrules` for detailed frontend component breakdown

### Java Extensions (java-extensions/) - Java
External connectivity and extensibility:

**Data Source Connectors:**
- `hive-reader/` - Apache Hive integration
- `iceberg-metadata-reader/` - Apache Iceberg support
- `hudi-reader/` - Apache Hudi integration
- `paimon-reader/` - Apache Paimon support
- `jdbc-bridge/` - JDBC connectivity for external databases
- `odps-reader/` - Alibaba ODPS integration

**Extension Framework:**
- `udf-extensions/` - User-Defined Function framework
- `common-runtime/` - Shared runtime for extensions
- `hadoop-ext/` - Hadoop ecosystem integration

📋 **Note:** See `java-extensions/.cursorrules` for detailed extensions breakdown

### Additional Important Directories

**Generated Sources (gensrc/):**
- `gensrc/proto/` - Protocol buffer definitions
- `gensrc/thrift/` - Thrift interface definitions
- `gensrc/script/` - Code generation scripts

**Testing Framework (test/):**
- `test/sql/` - SQL test cases organized by functionality
- `test/common/` - Common test utilities
- `test/lib/` - Test libraries and helpers

**Tools and Utilities:**
- `tools/` - Diagnostic tools, benchmarks, and utilities
- `bin/` - Binary executables and scripts
- `conf/` - Configuration files and templates
- `build-support/` - Build system support files
- `docker/` - Docker build configurations

**Other Key Directories:**
- `thirdparty/` - External dependencies and patches
- `fs_brokers/` - File system broker implementations
- `webroot/` - Web UI static files
- `format-sdk/` - Format SDK for data interchange

## Coding Guidelines

### C++ (Backend)
```cpp
// Use modern C++ features (C++17/C++20)
// Follow Google C++ Style Guide conventions
// Use RAII for resource management
// Prefer smart pointers over raw pointers
// Use const-correctness

// Example: Vectorized processing pattern
Status ColumnProcessor::process_batch(const ChunkPtr& chunk) {
    const auto& column = chunk->get_column_by_name("column_name");
    auto result_column = std::make_shared<Column>();
    
    // Vectorized operation on entire column
    for (size_t i = 0; i < chunk->num_rows(); ++i) {
        // Process element
    }
    
    return Status::OK();
}
```

### Java (Frontend)
```java
// Follow Java coding conventions
// Use dependency injection where appropriate  
// Implement proper exception handling
// Use builder patterns for complex objects
// Follow existing naming conventions

// Example: Query planning pattern
public class ScanNodePlanner extends PlanFragment {
    @Override
    public PlanFragment visitLogicalScanOperator(
            OptExpression optExpression, ExecPlan context) {
        LogicalScanOperator scanOperator = 
            (LogicalScanOperator) optExpression.getOp();
        
        // Create physical scan node
        ScanNode scanNode = createScanNode(scanOperator);
        return new PlanFragment(scanNode);
    }
}
```


## ⚠️ CRITICAL BUILD SYSTEM WARNING
**DO NOT attempt to build or run unit tests (UT) for this project unless explicitly requested by the user.**

The build system is extremely resource-intensive and time-consuming. Building the full project can take hours and requires significant system resources.

**Specific commands and files to AVOID:**
- `build.sh` - Main build script (extremely resource intensive)
- `build-in-docker.sh` - Docker-based build
- `run-be-ut.sh` / `run-fe-ut.sh` / `run-java-exts-ut.sh` - Unit test runners
- `docker-compose` commands - Heavy resource usage
- `Makefile*` - Make build files
- `pom.xml` - Maven build files (for Java components)

**Focus on code analysis and targeted changes instead of full builds.**

## Important Guidelines

### Pull Request Requirements

**PR Title Format:**
Must include category prefix:
- `[BugFix]` - Bug fixes and error corrections
- `[Feature]` - New features and capabilities  
- `[Enhancement]` - Improvements to existing functionality
- `[Refactor]` - Code refactoring without functional changes
- `[Test]` - Test-related changes
- `[Doc]` - Documentation updates
- `[Build]` - Build system and CI/CD changes
- `[Performance]` - Performance optimizations

**Example:** `[Feature] Add Apache Paimon table format support`


### Code Review Focus Areas

**Performance Considerations:**
- Query execution efficiency
- Memory usage patterns
- Lock contention in concurrent scenarios
- Network I/O optimization

**Correctness Priorities:**
- SQL standard compliance
- Data type handling accuracy
- Transaction consistency
- Error handling completeness

**Security Considerations:**
- Input validation and sanitization
- Authentication and authorization
- Resource usage limits
- Information leak prevention

## Common Development Patterns

### Adding New SQL Functions
1. Define function signature in `fe/fe-core/src/main/java/com/starrocks/catalog/FunctionSet.java`
2. Implement evaluation logic in `be/src/exprs/`
3. Add comprehensive tests in `test/sql/test_functions/`

### Adding New Data Source Connectors
1. Implement connector interface in `java-extensions/`
2. Add metadata reader and schema handling
3. Integrate with query planner in `fe/fe-core/src/main/java/com/starrocks/connector/`
4. Add integration tests

### Query Optimization Improvements
1. Analyze optimizer rules in `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/`
2. Update cost model if needed in `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/cost/`
3. Add test cases in `test/sql/test_optimizer/`

## Documentation References

- **Contributing Guide**: [`CONTRIBUTING.md`](../CONTRIBUTING.md)
- **Development Setup**: [StarRocks Documentation](https://docs.starrocks.io/docs/developers/)
- **Architecture Overview**: [README.md](../README.md#architecture-overview)
- **PR Template**: [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md)

## Quick Reference

**Key File Extensions:**
- `.cpp`, `.h`, `.cc` - C++ backend code
- `.java` - Java frontend/extensions code  
- `.sql` - SQL test cases
- `.py` - Python test scripts
- `.proto` - Protocol buffer definitions
- `.thrift` - Thrift interface definitions

**Important Configuration:**
- `conf/` - Runtime configuration templates
- `gensrc/` - Auto-generated code from IDL definitions
- `thirdparty/` - External dependencies

**Testing Structure:**
- `test/sql/` - SQL correctness tests organized by functionality
- `be/test/` - C++ unit tests
- `fe/fe-core/src/test/` - Java unit tests

**Build System Files to Avoid:**
- `build.sh` - Main build script (very resource intensive)
- `build-in-docker.sh` - Docker-based build
- `run-*-ut.sh` - Unit test runners
- `Makefile*` - Make build files
- `pom.xml` - Maven build files (for Java components)

This project prioritizes **performance**, **correctness**, and **scalability**. When contributing, consider the impact on query performance and ensure changes maintain SQL standard compliance.