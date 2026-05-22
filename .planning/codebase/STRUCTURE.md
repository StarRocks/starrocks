# Directory Structure

## Root Level

```
starrocks/
├── be/                    # Backend (C++) - Query execution & storage
├── fe/                    # Frontend (Java) - SQL & metadata
├── java-extensions/       # JNI connectors for external sources
├── gensrc/               # Generated code (Thrift, Protobuf)
├── test/                 # SQL integration tests
├── docs/                 # Documentation (Docusaurus)
├── thirdparty/           # Third-party dependencies
├── docker/               # Docker build files
├── conf/                 # Configuration templates
├── bin/                  # Runtime scripts
├── tools/                # Utility tools
├── build-support/        # Build helper scripts
└── webroot/              # Static web assets
```

## Backend (`be/`)

```
be/
├── src/                  # Source code
│   ├── agent/           # Agent services (task execution)
│   ├── base/            # Base utilities and common code
│   ├── bench/           # Benchmarks
│   ├── cache/           # Cache implementations
│   ├── column/          # Columnar data structures
│   ├── common/          # Common utilities
│   ├── connector/       # External data source connectors
│   ├── exec/            # Query execution operators
│   ├── exprs/           # Expression evaluation
│   ├── formats/         # File format parsers (Parquet, ORC)
│   ├── fs/              # File system abstractions
│   ├── gen_cpp/         # Generated Thrift/Protobuf code
│   ├── geo/             # Geospatial functions
│   ├── gutil/           # Google utilities
│   ├── http/            # HTTP server
│   ├── io/              # I/O utilities
│   ├── runtime/         # Runtime infrastructure
│   ├── serde/           # Serialization
│   ├── service/         # Backend service implementations
│   ├── storage/         # Storage engine
│   ├── testutil/        # Test utilities
│   ├── types/           # Data types
│   ├── udf/             # User-defined functions
│   └── util/            # General utilities
├── test/                # Unit tests (mirrors src/ structure)
├── CMakeLists.txt       # CMake configuration
└── cmake_modules/       # CMake modules
```

## Frontend (`fe/`)

```
fe/
├── fe-core/             # Core FE logic
│   ├── src/main/java/com/starrocks/
│   │   ├── alter/       # Schema change operations
│   │   ├── analysis/    # SQL analysis (legacy)
│   │   ├── authentication/  # Auth mechanisms
│   │   ├── authorization/   # Privilege management
│   │   ├── backup/      # Backup/restore
│   │   ├── catalog/     # External catalogs
│   │   ├── clone/       # Tablet cloning
│   │   ├── cluster/     # Cluster management
│   │   ├── common/      # Common utilities
│   │   ├── connector/   # Connector framework
│   │   ├── http/        # HTTP REST API
│   │   ├── load/        # Data loading
│   │   ├── meta/        # Metadata management
│   │   ├── metric/      # Metrics collection
│   │   ├── planner/     # Query planning (legacy)
│   │   ├── qe/          # Query execution (coordination)
│   │   ├── server/      # Main server logic
│   │   ├── sql/         # SQL processing
│   │   │   ├── analyzer/    # Semantic analysis
│   │   │   ├── ast/         # AST nodes
│   │   │   ├── optimizer/   # Cost-based optimizer
│   │   │   └── planner/     # Physical plan generation
│   │   ├── statistic/   # Statistics collection
│   │   ├── storagevolume/   # Shared-data volumes
│   │   └── transaction/ # Transaction management
│   └── src/test/java/   # Unit tests
├── fe-parser/           # SQL parser (ANTLR)
├── fe-grammar/          # ANTLR grammar files
├── fe-type/             # Type system
├── fe-spi/              # Service Provider Interfaces
├── fe-server/           # HTTP server
├── fe-utils/            # Utilities
├── fe-testing/          # Test utilities
├── connector/           # Connector implementations
│   ├── hive/            # Hive connector
│   ├── iceberg/         # Iceberg connector
│   ├── hudi/            # Hudi connector
│   ├── deltalake/       # Delta Lake connector
│   ├── jdbc/            # JDBC connector
│   └── paimon/          # Paimon connector
└── plugin/              # Plugins
    ├── spark-dpp/       # Spark data processing
    └── hive-udf/        # Hive UDF bridge
```

## Java Extensions (`java-extensions/`)

```
java-extensions/
├── jni-connector/       # JNI framework
├── hive-reader/         # Hive format reader
├── hudi-reader/         # Hudi format reader
├── paimon-reader/       # Paimon format reader
├── kudu-reader/         # Kudu connector
├── jdbc-bridge/         # JDBC connector
└── udf-extensions/      # UDF support
```

## Tests (`test/`)

```
test/
├── sql/                 # SQL test cases
├── lib/                 # Test utilities
└── run.py               # Test runner
```

## Configuration (`conf/`)

```
conf/
├── fe.conf              # FE configuration
├── be.conf              # BE configuration
└── *.conf               # Other configs
```

## Key File Locations

| Purpose | File Path |
|---------|-----------|
| Main build script | `build.sh` |
| FE build config | `fe/pom.xml`, `fe/build.gradle.kts` |
| BE build config | `be/CMakeLists.txt` |
| Code style (C++) | `.clang-format` |
| Code style (Java) | `fe/checkstyle.xml` |
| Test runner (FE) | `run-fe-ut.sh` |
| Test runner (BE) | `run-be-ut.sh` |
| License header | `fe/checkstyle-apache-header.txt` |

## Generated Code (`gensrc/`)

```
gensrc/
├── thrift/              # Thrift IDL files
├── proto/               # Protobuf definitions
└── scripts/             # Generation scripts
```

## Documentation (`docs/`)

```
docs/
├── en/                  # English docs
├── zh/                  # Chinese docs
└── docusaurus/          # Website config
```

---
*Mapped: 2026-03-18*
