# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-04)

**Core value:** Users can CREATE EXTERNAL CATALOG with ADBC and query remote Arrow Flight SQL databases with full JDBC-parity features
**Current focus:** Phase 2 - Scanning (Phase 1 complete)

## Current Position

Phase: 2 of 3 (Scanning)
Plan: 2 of 5 in current phase
Status: Executing Phase 2
Last activity: 2026-03-06 — Completed 02-02 (BE scanner + connector)

Progress: [███████░░░] 70% (7/10 plans)

## Performance Metrics

**Velocity:**
- Total plans completed: 7
- Average duration: 23 min
- Total execution time: 2.7 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01-foundation | 5 | 141 min | 28 min |
| 02-scanning | 2 | 19 min | 10 min |

**Recent Trend:**
- Last 5 plans: 01-03 (32 min), 01-04 (53 min), 01-05 (13 min), 02-01 (14 min), 02-02 (5 min)
- Trend: Phase 2 executing well; BE C++ plans are fast

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
- Connector: ADBCTable.toThrift() returns null stub until TTableType.ADBC_TABLE added to Thrift IDL
- Type mapping: Concrete type classes (IntegerType, FloatType, etc.) used instead of ScalarType factory methods
- Type mapping: Unsigned ints promoted to next-wider signed type (uint8->SMALLINT, uint64->LARGEINT)
- Metadata: ADBCMetadata opens new AdbcConnection per operation via try-with-resources (not shared across threads)
- Metadata: getTableSchema uses null for catalog parameter; dbName is the Arrow Flight SQL schema
- Metadata: Unknown adbc.driver values fall back to FlightSQLSchemaResolver with warning (not error)
- Cache: ADBCMetaCache mirrors JDBCMetaCache with adbc_ property keys (adbc_meta_cache_enable, adbc_meta_cache_expire_sec)
- SQL analysis: ADBC guards mirror JDBC pattern in AstToStringBuilder, MaterializedViewAnalyzer, DesensitizedSQLBuilder, RelationTransformer
- Scan operator: RelationTransformer throws UNSUPPORTED for ADBC scan attempts; LogicalADBCScanOperator deferred to Phase 2
- BE Scanner: RETURN_ADBC_NOT_OK macro for C API error handling; round-robin partition assignment; RecordBatchQueue capacity 16
- BE Scanner: Max 4 parallel reader threads; get_adbc_sql() as free function for testability
- BE Scanner: ADBCConnector/ADBCDataSourceProvider/ADBCDataSource mirror JDBC connector pattern exactly

### Pending Todos

None yet.

### Blockers/Concerns

- ~~C++ ADBC thirdparty dependency integration path not yet confirmed~~ (RESOLVED in 01-01)
- Need to verify whether existing JDBC SPI extension points accommodate ADBC without interface changes

## Session Continuity

Last session: 2026-03-06
Stopped at: Completed 02-02-PLAN.md (BE scanner + connector)
Resume file: None
