---
phase: 05-add-tls-support-for-adbc-connector
plan: 01
subsystem: connector
tags: [tls, mtls, grpc, flight-sql, adbc, certificate]

# Dependency graph
requires:
  - phase: 01-foundation
    provides: ADBCConnector, ADBCMetadata, ADBCScanNode, TADBCScanNode
provides:
  - TLS property constants and validation in ADBCConnector
  - TLS options in ADBCMetadata.initDatabase() via FlightSqlConnectionProperties
  - TADBCScanNode Thrift fields 11-14 for TLS file paths
  - ADBCScanNode toThrift() TLS field wiring
  - Unit tests for TLS validation logic
affects: [05-02-be-tls-support]

# Tech tracking
tech-stack:
  added: []
  patterns: [cert-file-validation-at-catalog-creation, uri-scheme-tls-detection, mtls-pair-enforcement]

key-files:
  created: []
  modified:
    - fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCConnector.java
    - fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java
    - gensrc/thrift/PlanNodes.thrift
    - fe/fe-core/src/main/java/com/starrocks/planner/ADBCScanNode.java
    - fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCConnectorTest.java
    - fe/fe-core/src/test/java/com/starrocks/sql/plan/ADBCScanPlanTest.java

key-decisions:
  - "mTLS pair check runs before file existence validation for better error messages"
  - "TLS verify defaults to true in ADBCScanNode.toThrift() when property not set"

patterns-established:
  - "URI scheme detection: case-insensitive grpc+tls:// prefix check via isTlsUri()"
  - "Cert file validation: exists + isFile + canRead at catalog creation time"
  - "mTLS pair enforcement: both client_cert and client_key required together"

requirements-completed: [TLS-01, TLS-02, TLS-03, TLS-04, TLS-05, TLS-06, TLS-07, TLS-08, TLS-10]

# Metrics
duration: 17min
completed: 2026-03-07
---

# Phase 5 Plan 1: FE TLS Support Summary

**FE TLS validation, FlightSql driver TLS options, Thrift IDL fields 11-14, and ADBCScanNode toThrift wiring with 7 unit tests**

## Performance

- **Duration:** 17 min
- **Started:** 2026-03-07T19:59:36Z
- **Completed:** 2026-03-07T20:16:36Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- Added TLS property constants and cert file validation to ADBCConnector with mTLS pair enforcement
- Added TLS options to ADBCMetadata.initDatabase() using FlightSqlConnectionProperties (TLS_ROOT_CERTS, MTLS_CERT_CHAIN, MTLS_PRIVATE_KEY, TLS_SKIP_VERIFY)
- Added TADBCScanNode Thrift fields 11-14 for passing TLS file paths from FE to BE
- Wired TLS fields in ADBCScanNode.toThrift() using ADBCConnector property constants
- Added 6 TLS validation tests and 1 plan test, all passing

## Task Commits

Each task was committed atomically:

1. **Task 1: FE TLS property constants, validation, Thrift IDL, and initDatabase TLS options** - `a2c25c2a8d` (feat)
2. **Task 2: FE unit tests for TLS validation and plan test for Thrift fields** - `0e80f15bc0` (test)

## Files Created/Modified
- `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCConnector.java` - TLS property constants, isTlsUri(), validateCertFile(), hasTlsCertProperties(), mTLS pair enforcement
- `fe/fe-core/src/main/java/com/starrocks/connector/adbc/ADBCMetadata.java` - TLS options in initDatabase() via FlightSqlConnectionProperties
- `gensrc/thrift/PlanNodes.thrift` - TADBCScanNode fields 11-14 (adbc_tls_ca_cert_file, adbc_tls_client_cert_file, adbc_tls_client_key_file, adbc_tls_verify)
- `fe/fe-core/src/main/java/com/starrocks/planner/ADBCScanNode.java` - TLS fields in toThrift()
- `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCConnectorTest.java` - 6 TLS validation tests
- `fe/fe-core/src/test/java/com/starrocks/sql/plan/ADBCScanPlanTest.java` - 1 TLS plan test

## Decisions Made
- mTLS pair check (both cert and key required) runs before file existence validation for better error messages when user provides only one of the pair
- TLS verify defaults to true in ADBCScanNode.toThrift() when property not set (secure by default)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Reordered mTLS pair check before file validation**
- **Found during:** Task 2 (unit tests)
- **Issue:** validateCertFile() ran before mTLS pair check, causing "file does not exist" error instead of the more helpful "must both be provided" error when user provides only client_cert or client_key
- **Fix:** Moved mTLS pair check before file existence validation in ADBCConnector constructor
- **Files modified:** ADBCConnector.java
- **Verification:** testTlsMtlsMissingKeyThrows and testTlsMtlsMissingCertThrows pass
- **Committed in:** 0e80f15bc0 (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug fix)
**Impact on plan:** Better error message ordering for UX. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- FE TLS support complete: property constants, validation, FlightSql driver options, Thrift IDL, ScanNode wiring
- Ready for Plan 02 (BE TLS support): BE can read TLS file paths from TADBCScanNode fields 11-14 and configure C ADBC driver

---
## Self-Check: PASSED

All files and commits verified.

---
*Phase: 05-add-tls-support-for-adbc-connector*
*Completed: 2026-03-07*
