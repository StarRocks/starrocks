---
plan: 03-02
status: complete
started: 2026-04-14
completed: 2026-04-14
tasks_completed: 2
tasks_total: 2
---

## Summary

Added `ADBCTableDescriptor` to the BE descriptor system, wired it into the descriptor factory, defined `ADBCScanContext` struct, and rewrote `ADBCDataSource::open()` to read connection params from the table descriptor instead of directly from `TADBCScanNode` fields.

## Tasks

| # | Task | Status |
|---|------|--------|
| 1 | Add ADBCTableDescriptor and wire descriptor factory | ✓ |
| 2 | Rewrite ADBCDataSource::open() to read from ADBCTableDescriptor | ✓ |

## Key Changes

### Task 1: ADBCTableDescriptor
- Added `ADBCTableDescriptor` class in `descriptors_ext.h` with getters: `adbc_driver_url()`, `adbc_entrypoint()`, `adbc_options()`
- Constructor reads from `TADBCTable` Thrift struct with `__isset` guards
- Wired `TTableType::ADBC_TABLE` case in factory switch using `ALLOC_DESC`

### Task 2: Connector rewrite
- Added `ADBCScanContext` struct in `adbc_connector.h` bundling all connection + query params
- Rewrote `open()` to use `down_cast<const ADBCTableDescriptor*>` from tuple descriptor
- Extracts uri/username/password from `adbc_options` map
- Removed all legacy Flight SQL-specific fields (TLS, token, direct driver refs)
- Scanner constructed with `ADBCScanContext` struct instead of 11 individual parameters

## Key Files

### Modified
- `be/src/runtime/descriptors_ext.h` — ADBCTableDescriptor class added
- `be/src/runtime/descriptors_ext.cpp` — constructor, debug_string, factory switch
- `be/src/connector/adbc_connector.h` — ADBCScanContext struct added
- `be/src/connector/adbc_connector.cpp` — open() rewritten

## Deviations

None.

## Self-Check: PASSED
- [x] ADBCTableDescriptor reads from TADBCTable
- [x] Factory switch wired for ADBC_TABLE
- [x] ADBCScanContext struct defined
- [x] open() uses table descriptor cast
- [x] Legacy TLS/token/driver fields removed
- [x] Scanner takes context struct
