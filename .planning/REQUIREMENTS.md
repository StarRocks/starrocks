# Requirements: ADBC External Catalog

**Defined:** 2026-03-04
**Core Value:** Users can CREATE EXTERNAL CATALOG with ADBC and query remote databases with full JDBC-parity features

## v1 Requirements

### Catalog Infrastructure

- [x] **CAT-01**: User can CREATE CATALOG with type='adbc' and driver/connection properties
- [x] **CAT-02**: User can DROP CATALOG to remove an ADBC catalog
- [x] **CAT-03**: User can ALTER CATALOG to update ADBC catalog properties
- [x] **CAT-04**: ADBC catalog accepts `adbc.driver` property to select driver (flight_sql initially)
- [x] **CAT-05**: ADBC catalog accepts connection properties (uri, user, password, driver-specific options)
- [x] **CAT-06**: User can SHOW DATABASES to list remote schemas/databases
- [x] **CAT-07**: User can SHOW TABLES to list remote tables in a database
- [x] **CAT-08**: User can DESCRIBE table to see column metadata with StarRocks types
- [x] **CAT-09**: Metadata is cached with configurable TTL (database list, table list, table schema, partitions)

### Data Scanning

- [x] **SCAN-01**: FE uses Java ADBC API to fetch metadata (schemas, tables, columns, partitions)
- [x] **SCAN-02**: BE uses native C++ ADBC to open connections and execute queries for data scanning
- [x] **SCAN-03**: Arrow RecordBatch data converts directly to StarRocks Chunk/Column format in C++ (zero-copy where possible)
- [x] **SCAN-04**: Column pruning pushdown — only requested columns are fetched from remote
- [x] **SCAN-05**: Predicate pushdown — WHERE clause filters are pushed as SQL strings to remote
- [x] **SCAN-06**: Limit pushdown — LIMIT N is pushed to remote query
- [x] **SCAN-07**: Connection pooling and lifecycle management for both FE and BE ADBC connections

### Type System

- [x] **TYPE-01**: Arrow types map to StarRocks types (int8-64, float/double, decimal, utf8, binary, date32, timestamp, boolean)
- [x] **TYPE-02**: Schema resolver abstraction supports per-driver type mapping overrides
- [x] **TYPE-03**: FlightSQL schema resolver handles Arrow Flight SQL specific type conventions
- [x] **TYPE-04**: Unsupported Arrow types are gracefully handled (logged warning, column excluded or mapped to VARCHAR)

### Partitioning

- [x] **PART-01**: ADBC connector discovers partitions from remote source metadata
- [x] **PART-02**: Partition names are listed and cached
- [x] **PART-03**: Partition pruning filters irrelevant partitions during query planning

### Materialized Views

- [x] **MV-01**: User can create materialized view against ADBC external tables
- [x] **MV-02**: Async MV refresh fetches data from ADBC tables
- [x] **MV-03**: Partition change tracking (PCT) enables incremental MV refresh
- [x] **MV-04**: Query optimizer rewrites queries to use MVs over ADBC tables

### Statistics

- [x] **STAT-01**: Column statistics (min, max, ndv, null count) collected from ADBC metadata when available
- [x] **STAT-02**: Row count estimation available for query optimizer cost calculations

### Optimizer Integration

- [x] **OPT-01**: LogicalADBCScanOperator represents ADBC table scan in logical plan
- [x] **OPT-02**: PhysicalADBCScanOperator created via implementation rule
- [x] **OPT-03**: ADBCScanNode generates final SQL query with pushdowns for BE execution
- [x] **OPT-04**: EXPLAIN shows the pushed-down ADBC query string

### Testing

- [x] **TEST-01**: MockedADBCMetadata registered in ConnectorPlanTestBase; ADBC plan tests pass
- [x] **TEST-02**: SQL integration test T/R files exist in test/sql/test_adbc_catalog/ with DDL, query, and MV coverage
- [x] **TEST-03**: test/conf/sr.conf has [.flightsql] section with external_flightsql_ip and external_flightsql_port
- [x] **TEST-04**: DuckDB seed data script exists for deterministic E2E test results

### Build & Dependencies

- [x] **BUILD-01**: FE Maven adds `org.apache.arrow.adbc:adbc-core` and `adbc-driver-flight-sql` dependencies
- [x] **BUILD-02**: BE CMake adds C++ ADBC library as thirdparty dependency
- [x] **BUILD-03**: Both FE and BE build cleanly with new dependencies

## v2 Requirements

### Additional Drivers

- **DRV-01**: PostgreSQL ADBC driver support via `adbc.driver = 'postgresql'`
- **DRV-02**: SQLite ADBC driver support via `adbc.driver = 'sqlite'`

### Advanced Pushdown

- **PUSH-01**: Aggregate pushdown (COUNT, SUM, MIN, MAX pushed to remote)
- **PUSH-02**: TopN pushdown (ORDER BY + LIMIT pushed as single remote query)

### Write Path

- **WRITE-01**: INSERT INTO remote ADBC tables
- **WRITE-02**: CTAS (CREATE TABLE AS SELECT) to remote ADBC tables

## Out of Scope

| Feature | Reason |
|---------|--------|
| Substrait-based pushdown | SQL string pushdown matches JDBC pattern, simpler |
| Native ADBC C driver wrappers | Using official apache/arrow-adbc Java and C++ APIs |
| Raw Arrow Flight protocol | ADBC abstracts Flight; use ADBC API only |
| Write path (INSERT) | Read-only for v1; write support in v2 |
| Join pushdown | Not in JDBC either; future enhancement |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| BUILD-01 | Phase 1 | Complete |
| BUILD-02 | Phase 1 | Complete |
| BUILD-03 | Phase 1 | Complete |
| CAT-01 | Phase 1 | Complete |
| CAT-02 | Phase 1 | Complete |
| CAT-03 | Phase 1 | Complete |
| CAT-04 | Phase 1 | Complete |
| CAT-05 | Phase 1 | Complete |
| CAT-06 | Phase 1 | Complete |
| CAT-07 | Phase 1 | Complete |
| CAT-08 | Phase 1 | Complete |
| CAT-09 | Phase 1 | Complete |
| TYPE-01 | Phase 1 | Complete |
| TYPE-02 | Phase 1 | Complete |
| TYPE-03 | Phase 1 | Complete |
| TYPE-04 | Phase 1 | Complete |
| SCAN-01 | Phase 2 | Complete |
| SCAN-02 | Phase 2 | Complete |
| SCAN-03 | Phase 2 | Complete |
| SCAN-04 | Phase 2 | Complete |
| SCAN-05 | Phase 2 | Complete |
| SCAN-06 | Phase 2 | Complete |
| SCAN-07 | Phase 2 | Complete |
| OPT-01 | Phase 2 | Complete |
| OPT-02 | Phase 2 | Complete |
| OPT-03 | Phase 2 | Complete |
| OPT-04 | Phase 2 | Complete |
| PART-01 | Phase 3 | Complete |
| PART-02 | Phase 3 | Complete |
| PART-03 | Phase 3 | Complete |
| MV-01 | Phase 3 | Complete |
| MV-02 | Phase 3 | Complete |
| MV-03 | Phase 3 | Complete |
| MV-04 | Phase 3 | Complete |
| STAT-01 | Phase 3 | Complete |
| STAT-02 | Phase 3 | Complete |

| TEST-01 | Phase 4 | Complete |
| TEST-02 | Phase 4 | Complete |
| TEST-03 | Phase 4 | Complete |
| TEST-04 | Phase 4 | Complete |

**Coverage:**
- v1 requirements: 40 total
- Mapped to phases: 40
- Unmapped: 0

---
*Requirements defined: 2026-03-04*
*Last updated: 2026-03-06 — PART-01/02/03 moved from Phase 2 to Phase 3 per CONTEXT.md deferred decisions*
