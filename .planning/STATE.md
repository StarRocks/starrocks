---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: in_progress
stopped_at: Completed 04-02 (SQL integration tests)
last_updated: "2026-03-07T14:10:13.000Z"
last_activity: 2026-03-07 — Completed 04-02 (SQL integration tests for ADBC catalog)
progress:
  total_phases: 4
  completed_phases: 3
  total_plans: 14
  completed_plans: 13
  percent: 93
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-04)

**Core value:** Users can CREATE EXTERNAL CATALOG with ADBC and query remote Arrow Flight SQL databases with full JDBC-parity features
**Current focus:** Phase 3 - JDBC Parity (Phases 1-2 complete)

## Current Position

Phase: 4 of 4 (Integration Testing)
Plan: 2 of 2 in current phase (04-02 complete, 04-01 pending)
Status: Phase 4 In Progress
Last activity: 2026-03-07 — Completed 04-02 (SQL integration tests for ADBC catalog)

Progress: [█████████░] 93%

## Performance Metrics

**Velocity:**
- Total plans completed: 13
- Average duration: 19 min
- Total execution time: 4.2 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01-foundation | 5 | 141 min | 28 min |
| 02-scanning | 4 | 50 min | 13 min |
| 03-jdbc-parity | 3 | 61 min | 20 min |

**Recent Trend:**
- Last 5 plans: 02-03 (8 min), 02-04 (15 min), 03-02 (25 min), 03-03 (11 min), 04-02 (2 min)
- Trend: Phase 4 in progress; 04-02 complete, 04-01 pending

*Updated after each plan completion*
| Phase 03 P01 | 20 | 2 tasks | 5 files |
| Phase 03 P03 | 11 | 1 task | 1 file |
| Phase 04 P02 | 2 | 2 tasks | 8 files |

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
- Statistics: getTableStatistics() pushes COUNT(*) to remote for row count; columns default to unknown
- Statistics: Per-column stats via ANALYZE TABLE through existing ExternalFullStatisticsCollectJob (no ADBC-specific code)
- [Phase 03]: ADBCPartitionTraits uses ADBCPartitionKey with PCT disabled; getPartitions returns empty list

### Roadmap Evolution

- Phase 4 added: Integration Testing (MockedADBCMetadata plan tests + SQL test T/R files; E2E blocked on CI infra provisioning Flight SQL server)

### Pending Todos

None yet.

### Blockers/Concerns

- ~~C++ ADBC thirdparty dependency integration path not yet confirmed~~ (RESOLVED in 01-01)
- Need to verify whether existing JDBC SPI extension points accommodate ADBC without interface changes

## Session Continuity

Last session: 2026-03-07T14:10:13.000Z
Stopped at: Completed 04-02 (SQL integration tests)
Resume file: .planning/phases/04-integration-testing/04-02-SUMMARY.md
