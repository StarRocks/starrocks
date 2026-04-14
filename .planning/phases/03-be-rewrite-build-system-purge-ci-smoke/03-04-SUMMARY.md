---
plan: 03-04
status: complete
started: 2026-04-14
completed: 2026-04-14
tasks_completed: 2
tasks_total: 2
---

## Summary

Created 4 throwaway GTest smoke tests proving the ADBC scanner stack works E2E against the SQLite ADBC driver, with test target registered in CMake.

## Tasks

| # | Task | Status |
|---|------|--------|
| 1 | Register smoke test target in CMakeLists | ✓ |
| 2 | Write throwaway ADBC smoke tests | ✓ |

## Key Changes

### Task 1: CMake target
- Added `adbc_smoke_test` target in `be/test/exec/CMakeLists.txt` following existing exec_core_test pattern

### Task 2: Smoke tests
- **DriverRegistrySingleton**: get_or_load returns same pointer, loaded_count >= 1
- **ScannerInitViaDriverManager**: database+connection+statement via driver manager protocol
- **EndToEndSelect**: CREATE TABLE, INSERT, SELECT with row count verification
- **ConcurrentScan**: 8 threads with per-thread connections, 100 rows each, atomic success counter

## Key Files

### Created
- `be/test/exec/adbc_smoke_test.cpp` — 4 GTest smoke tests

### Modified
- `be/test/exec/CMakeLists.txt` — adbc_smoke_test target

## Deviations

None.

## Self-Check: PASSED
- [x] 4 test cases exist
- [x] All use GTEST_SKIP if driver not found
- [x] ArrowArrayStreamHolder used (RAII)
- [x] AdbcError re-init before each API call
- [x] ConcurrentScan uses std::atomic
- [x] No dlclose anywhere
