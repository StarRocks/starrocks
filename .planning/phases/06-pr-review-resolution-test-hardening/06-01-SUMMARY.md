---
phase: 06-pr-review-resolution-test-hardening
plan: 01
subsystem: be-cleanup-build-config
tags: [cleanup, build-system, pr-review, be-tests]
dependency_graph:
  requires: []
  provides:
    - clean-be-test-directory
    - reverted-build-workarounds
    - gsd-tag-free-be-sources
  affects:
    - be/test/exec/CMakeLists.txt
    - thirdparty/build-thirdparty.sh
    - bin/start_backend.sh
    - .gitignore
tech_stack:
  added: []
  patterns: []
key_files:
  created: []
  modified:
    - be/test/exec/CMakeLists.txt
    - .gitignore
    - thirdparty/build-thirdparty.sh
    - bin/start_backend.sh
    - be/src/exec/adbc_scanner.cpp
    - be/src/exec/adbc_scanner.h
    - be/src/connector/adbc_connector.cpp
    - be/src/exec/adbc_driver_registry.h
  deleted:
    - be/test/exec/adbc_smoke_test.cpp
    - be/test/exec/ADBC_SMOKE_TEST_README.md
decisions:
  - Kept $STARROCKS_HOME/lib in LD_LIBRARY_PATH (start_backend.sh) because libadbc_driver_manager.so is installed there by BE packaging; added explanatory comment
  - Preserved all design rationale text when removing GSD tags (e.g. "per-fragment, never cached" kept, only "(per BE-03:" removed)
metrics:
  duration: 4m 54s
  completed: 2026-04-17T15:22:38Z
  tasks: 3/3
  files_modified: 10
---

# Phase 06 Plan 01: BE Cleanup, Build Workarounds, and GSD Tag Removal Summary

Delete throwaway BE smoke tests, revert local build workarounds, clean .gitignore, and remove all GSD requirement tags from BE ADBC source comments.

## One-liner

Removed throwaway BE smoke tests and CMakeLists entries, reverted rocksdb/flatbuffers local -Wno-* build flags, cleaned .planning/ from .gitignore, and stripped 10 GSD requirement tags from 4 BE ADBC source files while preserving design rationale.

## Task Results

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Delete BE smoke tests and CMakeLists entries | 69d2eb2492 | be/test/exec/adbc_smoke_test.cpp (deleted), be/test/exec/ADBC_SMOKE_TEST_README.md (deleted), be/test/exec/CMakeLists.txt |
| 2 | Revert build workarounds, clean .gitignore, check start_backend.sh | b267d2dd75 | .gitignore, thirdparty/build-thirdparty.sh, bin/start_backend.sh |
| 3 | Remove GSD requirement tags from BE ADBC source comments | e28a981899 | be/src/exec/adbc_scanner.cpp, be/src/exec/adbc_scanner.h, be/src/connector/adbc_connector.cpp, be/src/exec/adbc_driver_registry.h |

## Decisions Made

1. **start_backend.sh LD_LIBRARY_PATH**: The addition of `$STARROCKS_HOME/lib` to `LD_LIBRARY_PATH` is an intentional ADBC change needed for runtime loading of `libadbc_driver_manager.so`. Kept the change and added an explanatory comment.

2. **GSD tag removal strategy**: Removed only the `(per XX-NN)` tag portion from comments; preserved all surrounding design rationale text. For example, `// 8. Connection (per BE-03: per-fragment, never cached)` became `// 8. Connection (per-fragment, never cached)`.

## Deviations from Plan

None - plan executed exactly as written.

## Verification Results

All automated verification checks passed:
- Smoke test files deleted (adbc_smoke_test.cpp, ADBC_SMOKE_TEST_README.md)
- CMakeLists.txt contains zero adbc_smoke_test references; exec_core_test target intact
- .gitignore does not contain .planning/; handbook/plans/local/ entry preserved
- build-thirdparty.sh has no -Wno-array-bounds, -Wno-stringop-overread, -Wno-stringop-overflow
- build-thirdparty.sh still contains build_adbc() function
- start_backend.sh contains STARROCKS_HOME/lib with explanatory comment
- Zero GSD tags (BE-NN, D-NN, STK-NN, PROP-NN, VAL-NN, META-NN) in all 4 BE ADBC source files
- Rationale text preserved: "per-fragment" in adbc_scanner.cpp, "RAII-managed" in adbc_scanner.h, "ADBCTableDescriptor" in adbc_connector.cpp, "resident until process exit" in adbc_driver_registry.h

## Self-Check: PASSED

All 8 modified/existing files confirmed present on disk. Both deleted files confirmed absent. All 3 commit hashes verified in git log.
