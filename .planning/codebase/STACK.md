# Technology Stack

**Analysis Date:** 2026-03-04

## Languages

**Primary:**
- Java 11+ (required minimum; JDK 17 used in bundled thirdparty builds) - Frontend (FE): SQL parsing, query planning, metadata management
- C++17 - Backend (BE): Query execution, storage engine, data processing

**Secondary:**
- Python 3 - Build tooling, integration test runner (`test/run.py`)
- SQL - Integration test cases (`test/`)
- Thrift IDL - RPC interface definitions (`gensrc/thrift/*.thrift`)
- Protobuf IDL - Binary serialization definitions (`gensrc/proto/*.proto`)
- ANTLR4 grammar - SQL grammar definition (`fe/fe-grammar/`)

## Runtime

**Frontend Environment:**
- JVM (OpenJDK 17 used in CI; minimum JDK 11 enforced at build time via `env.sh`)
- JVM flags: G1GC, 8GB heap by default (`conf/fe.conf`: `JAVA_OPTS="-Xmx8192m -XX:+UseG1GC"`)

**Backend Environment:**
- Linux (x86_64 or aarch64); macOS supported for development only
- GCC 12.1.0+ required (enforced by `env.sh`)
- Clang/LLVM also supported as alternative compiler (via `STARROCKS_LLVM_HOME`)
- Ninja (preferred) or Make for parallel builds

**Package Manager:**
- Maven (primary FE build tool) - lockfile: none; repositories in `fe/pom.xml`
- Gradle (alternative FE build) - `fe/build.gradle.kts`, `fe/settings.gradle.kts`
- Custom shell scripts for C++ thirdparty (`thirdparty/build-thirdparty.sh`)

## Frameworks

**Core FE:**
- Apache Thrift 0.20.0 - FE/BE RPC protocol
- Netty 4.1.128.Final - Async network I/O for FE HTTP and RPC server
- Log4j2 2.19.0 - Logging framework
- Berkeley DB JE 18.3.20 (`starrocks-bdb-je`) - FE metadata journal and HA replication store
- ANTLR4 4.9.3 - SQL parser generation
- JFlex 1.4.3 + Java-CUP - Lexer/parser for legacy grammar components
- Protobuf Java 3.25.5 - Serialization
- gRPC 1.63.0 - Arrow Flight SQL transport

**Core BE:**
- Apache Thrift 0.20.0 - Thrift C++ server for BE RPCs
- brpc 1.9.0 - High-performance RPC framework (BRPC protocol between FE/BE)
- glog 0.7.1 - Logging
- gflags 2.2.2 - Command-line flags
- Apache Arrow 19.0.1 (BE C++ build) / 18.0.0 (FE Java) - Columnar memory format and Flight SQL
- RocksDB 6.22.1 - BE metadata and persistent index storage
- LevelDB 1.20 - Additional metadata storage
- jemalloc 5.3.0 - Memory allocator (Release/Debug builds)
- fmtlib 8.1.1 - String formatting
- re2 2022-12-01 - Regular expression engine
- simdjson 3.9.4 - Fast JSON parsing
- simdutf - SIMD-accelerated UTF processing

**Storage Compression Libraries (BE):**
- LZ4 1.10.0
- Snappy 1.1.8
- Zstd 1.5.7
- Brotli 1.0.9
- Zlib 1.2.11
- Bzip2 1.0.8

**Data Formats (BE):**
- Apache ORC (native implementation in `be/src/formats/orc/`)
- Apache Parquet (via Arrow 19.0.1)
- Apache Avro (native in `be/src/formats/avro/`)
- CSV, JSON (`be/src/formats/csv/`, `be/src/formats/json/`)
- FlatBuffers 1.10.0

**Vectorization/SIMD:**
- AVX2 enabled by default (`USE_AVX2=ON`)
- AVX512 optional (`USE_AVX512=OFF` by default)
- SSE4.2 enabled by default (`USE_SSE4_2=ON`)
- BMI2 enabled by default (`USE_BMI_2=ON`)
- Intel Hyperscan 5.4.0 (x86_64 only) - Regex acceleration
- CRoaring 4.2.1 - Roaring bitmap operations (BE), RoaringBitmap 0.8.13 (FE Java)
- Bitshuffle 0.5.1 - Column compression

**Testing:**
- JUnit 5.8.2 - FE unit tests (`fe/fe-core/src/test/`)
- Google Test 1.10.0 - BE unit tests (`be/test/`)
- JMockit 1.49.4 - FE mocking
- Mockito 4.11.0 - FE mocking (inline)
- Byteman 4.0.24 - FE bytecode instrumentation for fault injection
- AssertJ 3.24.2 - FE fluent assertions
- Awaitility 4.2.0 - Async test utilities
- JMH 1.37 - FE microbenchmarks
- Python 3 test framework (`test/`) - SQL integration tests against live cluster

**Build/Dev:**
- Docker (`starrocks/dev-env-ubuntu:latest`) for reproducible builds
- `build.sh` orchestrates full FE+BE build
- `run-fe-ut.sh` / `run-be-ut.sh` for unit test execution
- `run-java-exts-ut.sh` for java-extensions unit tests
- clang-format (Google C++ style, 120-char limit) - BE code formatting
- Checkstyle 10.21.1 - FE Java style enforcement (`fe/checkstyle.xml`)
- Maven Surefire 3.2.5 - FE test runner
- Async-profiler 4.0 - Runtime profiling

## Key Dependencies

**Critical FE:**
- `starrocks-bdb-je` 18.3.20 - Metadata persistence and leader election (HA)
- `jprotobuf-starrocks` 1.0.0 - Custom Protobuf RPC (Baidu JProtobuf stack)
- `libthrift` 0.20.0 - All FE/BE Thrift protocol communication
- `grpc-netty-shaded` 1.63.0 - Arrow Flight SQL server transport
- `arrow-vector`, `flight-core`, `flight-sql` 18.0.0 - Arrow data format and Flight SQL protocol
- `nimbus-jose-jwt` 9.37.2 - JWT verification for OAuth2/JWT auth

**Critical BE:**
- `brpc` 1.9.0 - Primary inter-node RPC (FE→BE, BE→BE)
- `arrow` + `arrow_flight` + `arrow_flight_sql` (Apache Arrow 19.0.1) - Columnar exchange and Flight SQL
- `rocksdb` 6.22.1 - Persistent index, meta storage
- `jemalloc` 5.3.0 - Memory management
- `AWS SDK C++` 1.11.267 - S3-compatible object storage access
- `librdkafka` 2.11.0 - Kafka routine load consumer
- Pulsar client C++ 3.3.0 - Pulsar routine load consumer
- `hdfs` (Hadoop JNI) + `jvm` - HDFS access via JNI from BE
- `mariadb-connector-c` 3.1.14 - MySQL external table scan

**Infrastructure:**
- Boost 1.80.0 - Utility libraries (filesystem, date-time, etc.)
- OpenSSL 1.1.1m - TLS/SSL
- libcurl 8.4.0 - HTTP client (BE metrics fetch, broker interaction)
- Breakpad 2024.02.16 - Crash dump (minidump) generation
- S2 Geometry 0.9.0 - Geospatial query support
- CLucene - Full-text inverted index support in BE

**External Format/Table:**
- Apache Iceberg 1.10.0 (`iceberg-api`, `iceberg-core`, `iceberg-hive-metastore`, `iceberg-aws`)
- Apache Hudi 1.0.2 (`hudi-common`, `hudi-hadoop-mr`)
- Delta Lake Kernel 4.0.0rc1 (`delta-kernel-api`, `delta-kernel-defaults`)
- Apache Paimon 1.3.1 (`paimon-bundle`, `paimon-s3`, `paimon-oss`)
- Apache Parquet 1.16.0 (`parquet-hadoop`, `parquet-avro`, `parquet-column`)
- Apache Hive 3.1.2-22 (`hive-apache`)
- Apache Hadoop 3.4.3 (`hadoop-common`, `hadoop-hdfs`, `hadoop-aws`, `hadoop-azure`, `hadoop-aliyun`)

## Configuration

**Frontend:**
- Primary config: `conf/fe.conf` (key/value, read by `bin/start_fe.sh`)
- All parameters documented in `fe/fe-core/src/main/java/com/starrocks/common/Config.java`
- JVM tuning in `JAVA_OPTS` within `conf/fe.conf`
- Ports: HTTP 8030, RPC 9020, MySQL protocol 9030, edit log 9010

**Backend:**
- Primary config: `conf/be.conf` (key/value, read by `bin/start_be.sh`)
- All C++ parameters defined via macros in `be/src/common/config.h`
- JVM options for BE JNI: `JAVA_OPTS` in `conf/be.conf`
- Ports: BE RPC 9060, HTTP 8040, heartbeat 9050, BRPC 8060, StarLet 9070

**Build:**
- `env.sh` - Environment variable setup (JAVA_HOME, GCC_HOME, LLVM_HOME, STARROCKS_THIRDPARTY)
- `thirdparty/vars.sh` + `thirdparty/vars-{arch}.sh` - Thirdparty download URLs and versions
- `be/CMakeLists.txt` - Backend CMake config; build types: Release, Debug, ASAN, UBSAN, TSAN, LSAN
- `BUILD_TYPE` environment variable controls build mode (default: `Release`)

## Platform Requirements

**Development:**
- Linux (Ubuntu 22.04 recommended, CentOS 7 supported)
- GCC 12.1.0+ or Clang/LLVM (from `STARROCKS_LLVM_HOME`)
- Java 11+ (17 recommended)
- Maven (for FE build)
- Python 3 (for test runner and build scripts)
- Docker + Docker Compose (optional, via `docker-compose.dev.yml`)

**Production:**
- Linux x86_64 or aarch64 (ARM)
- Bare metal, VM, or container deployment
- S3-compatible object storage required for shared-data (lake) mode
- HDFS optional (for data lake integration)
- No external coordination service required (BDB JE embedded in FE for HA)

---

*Stack analysis: 2026-03-04*
