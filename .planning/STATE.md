# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-04)

**Core value:** Users can CREATE EXTERNAL CATALOG with ADBC and query remote Arrow Flight SQL databases with full JDBC-parity features
**Current focus:** Phase 2 - Scanning (Phase 1 complete)

## Current Position

Phase: 2 of 3 (Scanning) -- COMPLETE
Plan: 4 of 4 in current phase (all done)
Status: Phase 2 Complete, ready for Phase 3
Last activity: 2026-03-06 — Completed 02-04 (BE wiring + full build verification)

Progress: [██████████] 100% (10/10 plans)

## Performance Metrics

**Velocity:**
- Total plans completed: 10
- Average duration: 20 min
- Total execution time: 3.2 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01-foundation | 5 | 141 min | 28 min |
| 02-scanning | 4 | 50 min | 13 min |

**Recent Trend:**
- Last 5 plans: 02-01 (14 min), 02-02 (5 min), 02-01 (8 min), 02-03 (8 min), 02-04 (15 min)
- Trend: Phase 2 complete; 02-04 slightly longer due to test fixes from prior plans

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Architecture: FE uses Java ADBC for metadata, BE uses native C++ ADBC for data (no JNI in data path)
- Pushdown: SQL string pushdown (not Substrait) — matches JDBC pattern
- Drivers: Pluggable via `adbc.driver` property; FlightSQL is first concrete driver
- Data path: Arrow RecordBatch -> StarRocks Chunk direct conversion in C++ (zero-copy where possible)
- Build: ADBC Java 0.19.0 matches Arrow 18.0.0; ADBC C++ 1.1.0 for BE thirdparty
- Build: CMAKE_INSTALL_LIBDIR=lib64 needed for ADBC C++ to match StarRocks layout
- Connector: ADBCSchemaResolver uses Arrow vector types (Field, Schema) not Flight SQL types for broader driver compatibility
- Connector: ADBCTable.toThrift() returns TTableDescriptor with TTableType.ADBC_TABLE and TADBCTable
- Type mapping: Concrete type classes (IntegerType, FloatType, etc.) used instead of ScalarType factory methods
- Type mapping: Unsigned ints promoted to next-wider signed type (uint8->SMALLINT, uint64->LARGEINT)
- Metadata: ADBCMetadata opens new AdbcConnection per operation via try-with-resources (not shared across threads)
- Metadata: getTableSchema uses null for catalog parameter; dbName is the Arrow Flight SQL schema
- Metadata: Unknown adbc.driver values fall back to FlightSQLSchemaResolver with warning (not error)
- Cache: ADBCMetaCache mirrors JDBCMetaCache with adbc_ property keys (adbc_meta_cache_enable, adbc_meta_cache_expire_sec)
- SQL analysis: ADBC guards mirror JDBC pattern in AstToStringBuilder, MaterializedViewAnalyzer, DesensitizedSQLBuilder, RelationTransformer
- Scan operator: RelationTransformer creates LogicalADBCScanOperator for ADBC tables (Phase 2 stub replaced)
- Thrift IDL: TADBCScanNode field 85 in TPlanNode, TADBCTable field 37 in TTableDescriptor
- Optimizer: ADBC scan operators mirror JDBC pattern exactly for all visitor/rule/pattern registrations
- BE Scanner: RETURN_ADBC_NOT_OK macro for C API error handling; round-robin partition assignment; RecordBatchQueue capacity 16
- BE Scanner: Max 4 parallel reader threads; get_adbc_sql() as free function for testability
- BE Scanner: ADBCConnector/ADBCDataSourceProvider/ADBCDataSource mirror JDBC connector pattern exactly
- Scan Node: ADBCScanNode mirrors JDBCScanNode for SQL generation, filter pushdown, EXPLAIN output
- Scan Node: Default identifier quote is double-quote (standard SQL), configurable via adbc.identifier.quote
- Scan Node: Driver name mapping: flight_sql -> adbc_driver_flightsql; custom names pass through
- BE Wiring: ConnectorType::ADBC = 9; ADBCConnector registered outside __APPLE__ guard
- BE Wiring: ADBC_SCAN_NODE case mirrors JDBC_SCAN_NODE exactly in exec_factory and exec_node

### Pending Todos

None yet.

### Blockers/Concerns

- ~~C++ ADBC thirdparty dependency integration path not yet confirmed~~ (RESOLVED in 01-01)
- Need to verify whether existing JDBC SPI extension points accommodate ADBC without interface changes

## Session Continuity

Last session: 2026-03-06
Stopped at: Completed 02-04-PLAN.md (BE wiring + full build verification). Phase 2 complete.
Resume file: None
