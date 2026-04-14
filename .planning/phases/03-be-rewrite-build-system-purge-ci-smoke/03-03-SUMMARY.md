---
plan: 03-03
status: complete
started: 2026-04-14
completed: 2026-04-14
tasks_completed: 2
tasks_total: 2
---

## Summary

Rewrote the ADBC scanner to use `ADBCScanContext`, implemented process-global driver registry with load-once/never-dlclose semantics, created RAII wrappers for Arrow C Data Interface types, and updated references throughout.

## Tasks

| # | Task | Status |
|---|------|--------|
| 1 | Create driver registry and RAII wrappers | ✓ |
| 2 | Rewrite ADBCScanner to use ADBCScanContext and driver registry | ✓ |

## Key Changes

### Task 1: Driver registry and RAII wrappers
- `adbc_driver_registry.h/cpp`: Process-global singleton, loads driver .so once per realpath via `AdbcLoadDriver(RTLD_NOW|RTLD_LOCAL)`, never dlcloses
- `adbc_arrow_raii.h`: Move-only RAII wrappers for `ArrowArrayStream`, `ArrowSchema`, `ArrowArray`
- Added `adbc_driver_registry.cpp` to exec CMakeLists

### Task 2: Scanner rewrite
- Constructor now takes `ADBCScanContext` struct instead of 11 individual string params
- `_init_adbc()` calls `ADBCDriverRegistry::get_or_load()` first (BE-05/BE-06 enforcement)
- Then uses driver manager protocol: set "driver", "entrypoint", uri/username/password, all adbc_options, then Init
- Replaced raw `ArrowArrayStream` with `ArrowArrayStreamHolder` (RAII)
- Removed all Flight SQL-specific code: TLS certs, token auth, `_read_file_to_string`
- Preserved close ordering: parallel_reader before database release

## Key Files

### Created
- `be/src/exec/adbc_driver_registry.h` — Driver registry singleton
- `be/src/exec/adbc_driver_registry.cpp` — Implementation with realpath + mutex
- `be/src/exec/adbc_arrow_raii.h` — RAII wrappers for Arrow C types

### Modified
- `be/src/exec/adbc_scanner.h` — New constructor, RAII stream holder, no legacy fields
- `be/src/exec/adbc_scanner.cpp` — Full rewrite of _init_adbc(), RAII stream usage
- `be/src/exec/CMakeLists.txt` — Added adbc_driver_registry.cpp

## Deviations

None.

## Self-Check: PASSED
- [x] Scanner takes ADBCScanContext
- [x] _init_adbc() calls get_or_load() first
- [x] Driver manager protocol followed
- [x] ArrowArrayStreamHolder used (no raw release)
- [x] No Flight SQL-specific code remains
- [x] No TLS/token/cert fields
- [x] No _read_file_to_string
- [x] No dlclose anywhere
- [x] Close ordering preserved
