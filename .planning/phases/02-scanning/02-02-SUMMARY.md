---
phase: 02-scanning
plan: 02
subsystem: connector
tags: [adbc, arrow, flight-sql, c-api, parallel-reader, scanner]

# Dependency graph
requires:
  - phase: 01-foundation
    provides: ADBC C++ thirdparty libraries (adbc_driver_manager, adbc_driver_flightsql), Arrow headers
provides:
  - ADBCScanner with native C++ ADBC data path (open/get_next/close lifecycle)
  - ADBCParallelReader for multi-endpoint parallel reading with thread-safe queue
  - ADBCConnector/ADBCDataSourceProvider/ADBCDataSource connector infrastructure
  - SQL assembly from TADBCScanNode fields
  - Wave 0 gtest scaffold for connector tests
affects: [02-scanning, 03-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [ADBC C API lifecycle with RAII flags, RecordBatchQueue bounded blocking queue, round-robin partition assignment]

key-files:
  created:
    - be/src/exec/adbc_scanner.h
    - be/src/exec/adbc_scanner.cpp
    - be/src/exec/adbc_parallel_reader.h
    - be/src/exec/adbc_parallel_reader.cpp
    - be/src/connector/adbc_connector.h
    - be/src/connector/adbc_connector.cpp
    - be/test/connector/adbc_connector_test.cpp
  modified: []

key-decisions:
  - "RETURN_ADBC_NOT_OK macro pattern for C API error handling (extract message, release error, return Status)"
  - "Round-robin partition assignment across threads (not work-stealing) for simplicity"
  - "get_adbc_sql() as free function for testability"
  - "RecordBatchQueue capacity 16 batches with bounded blocking"
  - "Max 4 parallel reader threads regardless of partition count"

patterns-established:
  - "ADBC C API lifecycle: New -> SetOption -> Init -> Use -> Release (reverse order)"
  - "Per-connection-per-scan: no connection pooling, each scan opens fresh ADBC resources"
  - "Parallel reader thread owns its own AdbcConnection (connection not thread-safe)"

requirements-completed: [SCAN-02, SCAN-03, SCAN-07]

# Metrics
duration: 5min
completed: 2026-03-06
---

# Phase 2 Plan 02: BE Scanner Summary

**Native C++ ADBC scanner with parallel endpoint reading, Arrow-to-Chunk conversion via ArrowToStarRocksConverter, and JDBC-mirroring connector infrastructure**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-05T22:12:37Z
- **Completed:** 2026-03-05T22:17:40Z
- **Tasks:** 2
- **Files created:** 7

## Accomplishments
- ADBCScanner with full ADBC C API lifecycle using native C headers (no JNI)
- Multi-endpoint parallel reading via ADBCParallelReader with thread-safe RecordBatchQueue
- Single-stream fallback when AdbcStatementExecutePartitions fails or returns 1 partition
- Arrow-to-Chunk conversion reusing existing get_arrow_converter() infrastructure
- Re-chunking logic respecting max_chunk_size (4096 default)
- connect_time_ms stat captured during open() for EXPLAIN ANALYZE
- ADBCConnector/ADBCDataSourceProvider/ADBCDataSource mirroring JDBC connector pattern
- Wave 0 gtest scaffold with SQL assembly tests

## Task Commits

Each task was committed atomically:

1. **Task 1: ADBCScanner and ADBCParallelReader** - `d06036da30` (feat)
2. **Task 2: ADBCConnector, ADBCDataSource, and test scaffold** - `959d15eb21` (feat)

## Files Created/Modified
- `be/src/exec/adbc_scanner.h` - ADBCScanner class with ADBC C API members
- `be/src/exec/adbc_scanner.cpp` - Scanner implementation with open/get_next/close, parallel and single-stream reading, Arrow-to-Chunk conversion
- `be/src/exec/adbc_parallel_reader.h` - ADBCParallelReader and RecordBatchQueue declarations
- `be/src/exec/adbc_parallel_reader.cpp` - Parallel reader with per-endpoint threads and bounded queue
- `be/src/connector/adbc_connector.h` - ADBCConnector, ADBCDataSourceProvider, ADBCDataSource declarations
- `be/src/connector/adbc_connector.cpp` - Connector infrastructure with SQL assembly and scanner delegation
- `be/test/connector/adbc_connector_test.cpp` - gtest scaffold testing connector type, data source name, and SQL assembly

## Decisions Made
- Used RETURN_ADBC_NOT_OK macro for consistent C API error handling (extracts message, releases error struct, returns Status::InternalError)
- Capped parallel reader threads at 4 (configurable in constructor) per CONTEXT.md discretion
- Made get_adbc_sql() a free function in adbc_connector.cpp for independent testability
- RecordBatchQueue uses bounded blocking (capacity 16) to provide backpressure
- Round-robin partition assignment (simpler than work-stealing, sufficient for typical partition counts)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Scanner and connector ready for integration with BE pipeline (exec_factory, connector registration)
- TADBCScanNode Thrift struct already exists in PlanNodes.thrift (added in Phase 1)
- gen_cpp needs regeneration to include TADBCScanNode before compilation
- FE operator/rule/scan node classes needed (Plan 01, 03)
- BE wiring (Plan 04) will register ADBCConnector and add exec_factory case

---
*Phase: 02-scanning*
*Completed: 2026-03-06*
