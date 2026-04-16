---
phase: 05-external-integration-verification-suite
plan: 01
subsystem: testing
tags: [pytest, pymysql, adbc, docker, dbc, toml, tls, integration-testing]

# Dependency graph
requires:
  - phase: 03-be-rewrite-build-system-purge-ci-smoke
    provides: "ADBC scanner, driver registry, build system with libadbc_driver_manager.so"
provides:
  - "Python venv at /home/mete/coding/opensource/adbc_verification with pytest + pymysql + pytest-json-report"
  - "Session-scoped conftest.py fixtures for sr_conn, all driver paths, Docker backends, TLS"
  - "Library modules: starrocks.py, docker_backends.py, driver_registry.py, tls.py, catalog_helpers.py"
  - "All four ADBC drivers installed via dbc (sqlite, flightsql, postgresql, duckdb)"
affects: [05-02, 05-03, 05-04]

# Tech tracking
tech-stack:
  added: [pytest-9.0.3, pymysql-1.1.2, pytest-json-report-1.5.0]
  patterns: [session-scoped-fixtures, dbc-toml-manifest-parsing, docker-subprocess-lifecycle, structured-failure-capture]

key-files:
  created:
    - /home/mete/coding/opensource/adbc_verification/pyproject.toml
    - /home/mete/coding/opensource/adbc_verification/conftest.py
    - /home/mete/coding/opensource/adbc_verification/lib/starrocks.py
    - /home/mete/coding/opensource/adbc_verification/lib/docker_backends.py
    - /home/mete/coding/opensource/adbc_verification/lib/driver_registry.py
    - /home/mete/coding/opensource/adbc_verification/lib/tls.py
    - /home/mete/coding/opensource/adbc_verification/lib/catalog_helpers.py
  modified: []

key-decisions:
  - "Used cursor.description for SHOW BACKENDS Alive column lookup instead of hardcoded index 11"
  - "Mapped platform.machine() x86_64 to dbc TOML key linux_amd64 for cross-platform driver path resolution"

patterns-established:
  - "dbc TOML manifest parsing: read ~/.config/adbc/drivers/{name}.toml, access Driver.shared.linux_amd64"
  - "Docker lifecycle via subprocess: docker run/stop/inspect without docker-py SDK"
  - "pytest_runtest_makereport hook for per-test failure capture with FE/BE log tails"
  - "Idempotent StarRocks startup: check port 9030 before calling start_fe.sh --daemon"

requirements-completed: [GOAL-05]

# Metrics
duration: 5min
completed: 2026-04-16
---

# Phase 05 Plan 01: Project Scaffold Summary

**Python verification suite scaffold with venv, 6 library modules, session fixtures, and 4 ADBC drivers installed via dbc**

## Performance

- **Duration:** 4m 46s
- **Started:** 2026-04-16T13:33:55Z
- **Completed:** 2026-04-16T13:38:41Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments
- Created adbc_verification project at /home/mete/coding/opensource/adbc_verification with venv, pyproject.toml, and directory structure
- All six library modules created: driver_registry, starrocks, docker_backends, tls, catalog_helpers, and lib/__init__.py
- Session-scoped conftest.py with 8 fixtures (sr_conn, 4 driver paths, sqlflite_port, postgres_port, sqlflite_tls) plus structured failure capture
- All four ADBC drivers installed: sqlite 1.11.0, flightsql 1.11.0, postgresql 1.11.0, duckdb 1.5.2

## Task Commits

Each task was committed atomically:

1. **Task 1: Create project scaffold, venv, install dependencies and drivers** - `d8de864` (feat)
2. **Task 2: Create all library modules and session-scoped conftest.py** - `3602658` (feat)

## Files Created/Modified
- `pyproject.toml` - Project metadata, pytest config with markers and JSON report
- `.gitignore` - Excludes venv, pycache, reports, pytest_cache
- `lib/__init__.py` - Package marker
- `lib/driver_registry.py` - Parses dbc TOML manifests to resolve driver .so paths
- `lib/starrocks.py` - FE/BE idempotent startup, MySQL connection, log tail capture
- `lib/docker_backends.py` - Start/stop sqlflite and postgres Docker containers via subprocess
- `lib/tls.py` - sqlflite TLS startup with volume-mounted cert extraction
- `lib/catalog_helpers.py` - CREATE/DROP CATALOG SQL helpers, show_catalogs, execute_sql
- `conftest.py` - Session fixtures and pytest_runtest_makereport failure capture hook
- `tests/__init__.py` - Package marker for test modules

## Decisions Made
- Used `cursor.description` for SHOW BACKENDS Alive column lookup instead of hardcoded index 11 (per Pitfall 1 in RESEARCH.md)
- Mapped `platform.machine()` x86_64 to dbc TOML key `linux_amd64` with an extensible arch map (supports aarch64/arm64 too)
- Set `autocommit=True` on pymysql connection (per Pitfall 4: DDL requires autocommit)
- Postgres startup includes 3-second post-port-open sleep (per Pitfall 6: data directory init)
- sqlflite TLS uses host port 31338 to avoid conflict with non-TLS instance on 31337

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- All infrastructure modules ready for Plans 02/03/04 test modules to import
- `pytest --collect-only` runs cleanly with no import errors
- All four ADBC driver .so paths resolvable via `get_all_driver_paths()`
- Docker container lifecycle tested at import level (actual containers started by downstream tests)

## Self-Check: PASSED

---
*Phase: 05-external-integration-verification-suite*
*Completed: 2026-04-16*
