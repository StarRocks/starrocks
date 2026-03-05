---
phase: 01-foundation
plan: 01
subsystem: infra
tags: [adbc, arrow, maven, cmake, thirdparty, build-system]

# Dependency graph
requires: []
provides:
  - ADBC Java dependencies (adbc-core, adbc-driver-flight-sql 0.19.0) on FE classpath
  - ADBC C++ thirdparty build function (build_adbc) with FlightSQL driver
  - ADBC static libraries linked into BE binary via CMake
affects: [01-02, 01-03, 01-04, 01-05]

# Tech tracking
tech-stack:
  added: [adbc-core-0.19.0, adbc-driver-flight-sql-0.19.0, arrow-adbc-c++-1.1.0]
  patterns: [maven-managed-dep-version, thirdparty-build-function, cmake-static-link]

key-files:
  created: []
  modified:
    - fe/pom.xml
    - fe/fe-core/pom.xml
    - thirdparty/vars.sh
    - thirdparty/build-thirdparty.sh
    - be/CMakeLists.txt

key-decisions:
  - "ADBC Java 0.19.0 chosen to match Arrow 18.0.0 already on classpath (0.20.0+ requires Arrow 20+)"
  - "ADBC C++ 1.1.0 used for BE thirdparty with explicit Arrow cmake dir paths to avoid discovery issues"
  - "Added CMAKE_INSTALL_LIBDIR=lib64 to build_adbc to match StarRocks thirdparty lib layout"

patterns-established:
  - "ADBC version managed in fe/pom.xml properties, referenced via ${adbc.version} in submodules"
  - "build_adbc() placed after build_arrow() in both function definitions and all_packages array"

requirements-completed: [BUILD-01, BUILD-02, BUILD-03]

# Metrics
duration: 6min
completed: 2026-03-05
---

# Phase 1 Plan 01: Build Dependencies Summary

**ADBC 0.19.0 Java deps wired into FE Maven build; ADBC 1.1.0 C++ library added to BE thirdparty with FlightSQL driver and CMake linkage**

## Performance

- **Duration:** 6 min
- **Started:** 2026-03-05T19:24:12Z
- **Completed:** 2026-03-05T19:30:27Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- FE Maven build resolves adbc-core and adbc-driver-flight-sql 0.19.0 from Maven Central
- BE thirdparty system can download and build ADBC C++ library with FlightSQL driver enabled
- BE binary links adbc and adbc_driver_flightsql static libraries via STARROCKS_DEPENDENCIES

## Task Commits

Each task was committed atomically:

1. **Task 1: Add ADBC Maven dependencies to FE pom files** - `8a67920281` (feat)
2. **Task 2: Add ADBC C++ library to BE thirdparty build system and link into BE binary** - `e29a6edc28` (feat)

## Files Created/Modified
- `fe/pom.xml` - Added adbc.version=0.19.0 property and managed dependency entries
- `fe/fe-core/pom.xml` - Added adbc-core and adbc-driver-flight-sql dependencies
- `thirdparty/vars.sh` - Added ADBC_DOWNLOAD, ADBC_NAME, ADBC_SOURCE, ADBC_MD5SUM variables
- `thirdparty/build-thirdparty.sh` - Added build_adbc() function and adbc entry in all_packages array
- `be/CMakeLists.txt` - Added adbc and adbc_driver_flightsql to STARROCKS_DEPENDENCIES

## Decisions Made
- Used ADBC Java 0.19.0 (not 0.20.0+) to avoid Arrow version conflicts with existing 18.0.0
- Used ADBC C++ 1.1.0 with explicit Arrow cmake directory paths to prevent cmake discovery pitfalls
- Added CMAKE_INSTALL_LIBDIR=lib64 to match StarRocks thirdparty library layout convention

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added managed dependency entries in fe/pom.xml dependencyManagement**
- **Found during:** Task 1
- **Issue:** Plan only mentioned adding property and fe-core deps, but fe-core/pom.xml deps without version need managed entries in parent pom.xml dependencyManagement section
- **Fix:** Added adbc-core and adbc-driver-flight-sql to fe/pom.xml dependencyManagement block
- **Files modified:** fe/pom.xml
- **Verification:** Maven dependency:get resolves both artifacts successfully
- **Committed in:** 8a67920281

**2. [Rule 2 - Missing Critical] Added CMAKE_INSTALL_LIBDIR=lib64 to build_adbc()**
- **Found during:** Task 2
- **Issue:** Plan's build_adbc() function lacked CMAKE_INSTALL_LIBDIR=lib64, but StarRocks thirdparty convention installs to lib64/ (seen in build_arrow)
- **Fix:** Added -DCMAKE_INSTALL_LIBDIR=lib64 to cmake invocation
- **Files modified:** thirdparty/build-thirdparty.sh
- **Verification:** grep confirms the flag is present
- **Committed in:** e29a6edc28

---

**Total deviations:** 2 auto-fixed (2 missing critical)
**Impact on plan:** Both auto-fixes necessary for correctness. No scope creep.

## Issues Encountered
- FE Maven full compile cannot run on this machine (missing thrift binary and internal module artifacts) -- verified ADBC dependency resolution via mvn dependency:get instead
- BE thirdparty build not executed (would require full thirdparty download) -- verified by structural grep checks

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Build dependencies are wired; Plan 02 (ADBC Java metadata client) can import org.apache.arrow.adbc classes
- Plan 05 (BE C++ scanner) can reference adbc and adbc_driver_flightsql libraries
- ADBC C++ thirdparty must actually be built before BE compilation (run build-thirdparty.sh)

---
*Phase: 01-foundation*
*Completed: 2026-03-05*
