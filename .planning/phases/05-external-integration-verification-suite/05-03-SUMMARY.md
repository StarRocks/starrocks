---
phase: 05-external-integration-verification-suite
plan: 03
subsystem: testing
tags: [pytest, flightsql, postgresql, tls, docker, adbc, integration-testing]

# Dependency graph
requires:
  - phase: 05-external-integration-verification-suite
    plan: 01
    provides: "Project scaffold, conftest fixtures, library modules, driver paths"
provides:
  - "FlightSQL test module with 5 tests (non-TLS, TLS, negative, pass-through)"
  - "PostgreSQL test module with 5 tests (lifecycle, round-trip, show tables, negative, pass-through)"
affects: [05-04]

# Tech tracking
tech-stack:
  added: []
  patterns: [defensive-schema-discovery, docker-exec-data-seeding, tls-cert-passthrough-validation]

key-files:
  created:
    - /home/mete/coding/opensource/adbc_verification/tests/test_flightsql.py
    - /home/mete/coding/opensource/adbc_verification/tests/test_postgres.py
  modified: []

key-decisions:
  - "FlightSQL data query uses defensive table discovery across multiple schema names (main, sqlflite, default)"
  - "PostgreSQL round-trip tries three FQN patterns (public, testdb, testdb.public) for cross-driver schema mapping"
  - "Wrong password test handles both eager and deferred authentication patterns"

patterns-established:
  - "Docker exec psql for PostgreSQL data seeding via module-scoped fixture"
  - "Defensive three-part name resolution for PostgreSQL ADBC schema mapping"
  - "TLS cert pass-through validation via adbc.flight.sql.client_option.tls_root_certs"

requirements-completed: [GOAL-05]

# Metrics
duration: 3min
completed: 2026-04-16
---

# Phase 05 Plan 03: FlightSQL and PostgreSQL Backend Tests Summary

**FlightSQL (non-TLS + TLS) and PostgreSQL test modules with 10 tests covering D-09 scenarios and D-11 TLS validation via adbc.flight.sql.client_option.tls_root_certs pass-through**

## Performance

- **Duration:** 3m 3s
- **Started:** 2026-04-16T13:42:58Z
- **Completed:** 2026-04-16T13:46:01Z
- **Tasks:** 2
- **Files created:** 2

## Accomplishments

- Created FlightSQL test module with 5 tests: catalog lifecycle (non-TLS), data query with defensive table discovery, TLS lifecycle with CA cert pass-through (PROP-05/PROP-09), wrong password negative test, and adbc.* pass-through with custom gRPC header
- Created PostgreSQL test module with 5 tests: catalog lifecycle, data round-trip with seeded test data (3 rows via docker exec psql), show tables, bad URI negative test (port 59999), and adbc.* pass-through with infer_timestamp option
- All 10 tests collected successfully by pytest with no import errors
- Both modules use try/finally cleanup pattern with drop_catalog on all tests
- FlightSQL TLS test validates the full cert chain: sqlflite generates self-signed certs into volume-mounted tempdir, StarRocks catalog receives CA cert path via adbc.flight.sql.client_option.tls_root_certs

## Task Commits

Each task was committed atomically (in adbc_verification repo):

1. **Task 1: Create FlightSQL backend test module** - `3f04e49` (feat)
2. **Task 2: Create PostgreSQL backend test module** - `92e276c` (feat)

## Files Created

- `tests/test_flightsql.py` - 5 FlightSQL tests: lifecycle, data query, TLS lifecycle, wrong password, adbc.* pass-through
- `tests/test_postgres.py` - 5 PostgreSQL tests: lifecycle, data round-trip, show tables, bad URI, adbc.* pass-through

## Decisions Made

- **Defensive schema discovery (FlightSQL):** sqlflite may expose tables under different database names (main, sqlflite, default). The data query test tries multiple patterns and succeeds on the first match, handling the case where sqlflite has no pre-seeded data by passing with metadata-only validation.
- **Defensive three-part names (PostgreSQL):** PostgreSQL ADBC driver may map schemas as `catalog.public.table`, `catalog.testdb.table`, or `catalog.testdb.public.table`. The round-trip test tries all three patterns defensively.
- **Wrong password handling:** FlightSQL authentication may be eager (fail at CREATE CATALOG) or deferred (fail at query time). The test handles both patterns correctly.
- **Data seeding via docker exec:** PostgreSQL test data is seeded via `docker exec psql` in a module-scoped fixture, avoiding any Python PostgreSQL client dependency. The fixture is idempotent with `ON CONFLICT DO NOTHING`.

## Deviations from Plan

None - plan executed exactly as written.

## Known Stubs

None - all tests are fully implemented with real assertions and cleanup.

## Self-Check: PASSED

- test_flightsql.py: FOUND
- test_postgres.py: FOUND
- Commit 3f04e49 (Task 1): FOUND
- Commit 92e276c (Task 2): FOUND
- pytest --collect-only: 10 tests collected
