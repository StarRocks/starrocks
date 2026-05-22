# Technology Stack

## Overview

StarRocks is a high-performance analytical database built with a decoupled Frontend (FE) and Backend (BE) architecture.

| Component | Technology | Purpose |
|-----------|------------|---------|
| Frontend (FE) | Java 17+ | SQL parsing, query optimization, metadata management |
| Backend (BE) | C++17 | Query execution, storage engine, data processing |
| Build System | CMake (BE), Maven/Gradle (FE) | Compilation and dependency management |
| RPC | Thrift, Protobuf, gRPC | Inter-service communication |

## Frontend (FE) Stack

### Core Technologies
- **Language**: Java 17+
- **Build Tools**: Maven (primary), Gradle Kotlin DSL (`build.gradle.kts`)
- **Framework**: Custom HTTP server based on Apache Thrift
- **SQL Parsing**: ANTLR 4.9.3

### Key Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| Apache Thrift | - | RPC framework for FE-BE communication |
| Apache Arrow | 18.0.0 | Columnar data format |
| gRPC | 1.63.0 | Modern RPC framework |
| Netty | 4.1.128.Final | Network application framework |
| Protobuf Java | 3.25.5 | Protocol buffers |
| Jackson | 2.21.1 | JSON processing |
| Log4j | 2.19.0 | Logging framework |
| JUnit | 5.8.2 | Testing framework |
| Apache Spark | 3.5.5 | Spark DPP plugin |
| Apache Hadoop | 3.4.3 | HDFS integration |
| Apache Hive | 3.1.2-22 | Hive metastore integration |
| Apache Kafka | 3.9.1 | Kafka client |
| Apache Hudi | 1.0.2 | Lakehouse format support |
| Apache Iceberg | 1.10.0 | Table format support |
| Apache Paimon | 1.3.1 | Lakehouse format support |
| Delta Lake | 4.0.0rc1 | Delta format support |
| HikariCP | 7.0.2 | Connection pooling |
| Apache HBase | 2.6.2 | HBase integration |
| Kudu | 1.17.1 | Kudu integration |
| AWS SDK v2 | 2.42.1 | Cloud storage integration |
| Azure SDK | 1.2.34 | Azure cloud integration |
| Apache Parquet | 1.16.0 | Columnar storage format |
| Apache Avro | 1.12.0 | Data serialization |
| Tomcat | 8.5.70 | HTTP server (legacy) |

### FE Module Structure
- `fe-core`: Core FE logic
- `fe-parser`: SQL parser (ANTLR-based)
- `fe-grammar`: ANTLR grammar files
- `fe-type`: Type system
- `fe-spi`: Service Provider Interfaces
- `fe-server`: HTTP server implementation
- `fe-utils`: Utility classes
- `fe-testing`: Testing utilities
- `connector/`: External data source connectors

## Backend (BE) Stack

### Core Technologies
- **Language**: C++17
- **Build System**: CMake 3.16+
- **Compiler**: GCC or Clang (with LLVM support)

### Key Components

| Component | Technology | Purpose |
|-----------|------------|---------|
| Execution Engine | Vectorized C++ | Query execution with SIMD |
| Storage Engine | Custom C++ | Columnar storage with various formats |
| SIMD | AVX2/AVX512 | Vectorized operations |
| Third-party | Static linked | All dependencies statically compiled |

### BE Directory Structure
- `src/agent/`: Agent services
- `src/base/`: Base utilities
- `src/column/`: Columnar data structures
- `src/connector/`: Data source connectors
- `src/exec/`: Query execution operators
- `src/exprs/`: Expression evaluation
- `src/formats/`: File format readers/writers
- `src/fs/`: File system abstractions
- `src/runtime/`: Runtime infrastructure
- `src/storage/`: Storage engine
- `src/types/`: Type system
- `src/util/`: Utilities

## Java Extensions

JNI-based connectors for external data sources:
- `hudi-reader`: Hudi format reader
- `hive-reader`: Hive format reader
- `paimon-reader`: Paimon format reader
- `kudu-reader`: Kudu connector
- `jdbc-bridge`: JDBC connector
- `udf-extensions`: UDF support
- `jni-connector`: JNI framework

## Build Configuration

### Build Scripts
- `build.sh`: Main build script
- `build-in-docker.sh`: Docker-based build
- `env.sh`: Environment setup
- `run-fe-ut.sh`: FE unit test runner
- `run-be-ut.sh`: BE unit test runner

### Build Types
- `Release`: Production builds (default)
- `Debug`: Debug symbols enabled
- `ASAN`: Address sanitizer builds

### Code Generation
- `gensrc/`: Thrift and Protobuf generated code

---
*Mapped: 2026-03-18*
