---
plan: 03-01
status: complete
started: 2026-04-14
completed: 2026-04-14
tasks_completed: 2
tasks_total: 2
---

## Summary

Rewrote the thirdparty ADBC build system to produce `libadbc_driver_manager.so` (shared) instead of the static Flight SQL library, purged the Go toolchain from all thirdparty scripts, and swapped the BE CMakeLists link target with runtime .so discoverability.

## Tasks

| # | Task | Status |
|---|------|--------|
| 1 | Rewrite build_adbc(), delete build_go(), purge Go vars | ✓ |
| 2 | Swap CMakeLists link target and ensure runtime .so discoverability | ✓ |

## Key Changes

### Task 1: Build system rewrite
- Deleted `build_go()` function from `build-thirdparty.sh`
- Rewrote `build_adbc()`: `ADBC_BUILD_SHARED=ON`, `ADBC_DRIVER_FLIGHTSQL=OFF`, `ADBC_DRIVER_SQLITE=ON`, removed Go env vars
- Purged `GO_DOWNLOAD`/`GO_NAME`/`GO_SOURCE`/`GO_MD5SUM` from `vars-x86_64.sh` and `vars-aarch64.sh`
- Removed `"go"` from `package-manifest.sh`

### Task 2: CMake link target swap
- Changed `be/CMakeLists.txt` line 638 from `adbc_driver_flightsql` to `adbc_driver_manager`
- Preserved `arrow_flight_sql` on line 637 (unrelated outbound Flight SQL server)
- Registered `adbc_driver_manager` as `SHARED IMPORTED GLOBAL` in `ThirdParty.cmake`
- Added `install(FILES)` to copy `.so` into BE output directory for runtime resolution

## Key Files

### Modified
- `thirdparty/build-thirdparty.sh` — build_adbc() rewritten, build_go() deleted
- `thirdparty/vars-x86_64.sh` — Go vars removed
- `thirdparty/vars-aarch64.sh` — Go vars removed
- `thirdparty/package-manifest.sh` — "go" entry removed
- `be/CMakeLists.txt` — link target swapped to adbc_driver_manager
- `be/cmake_modules/ThirdParty.cmake` — shared lib registration added

## Deviations

None.

## Self-Check: PASSED
- [x] build_go() deleted
- [x] build_adbc() uses ADBC_BUILD_SHARED=ON
- [x] ADBC_DRIVER_FLIGHTSQL=OFF in build_adbc()
- [x] ADBC_DRIVER_SQLITE=ON in build_adbc()
- [x] No GO_SOURCE in vars files
- [x] No "go" in package-manifest.sh
- [x] adbc_driver_manager in CMakeLists.txt
- [x] arrow_flight_sql preserved
- [x] Shared lib registered in ThirdParty.cmake with runtime install
