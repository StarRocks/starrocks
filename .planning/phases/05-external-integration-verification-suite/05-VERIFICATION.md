---
phase: 05-external-integration-verification-suite
verified: 2026-04-16T15:00:00Z
status: human_needed
score: 14/14
overrides_applied: 0
human_verification:
  - test: "Run the full test suite against a live StarRocks instance with ADBC catalog support: STARROCKS_HOME=/path/to/starrocks .venv/bin/pytest tests/ -v"
    expected: "All 29 tests pass — catalog lifecycle, data round-trips, TLS, cross-driver joins, negative cases — with reports/latest.json produced"
    why_human: "Tests require a running StarRocks FE/BE with the Phase 2/3 ADBC scanner stack compiled in, Docker daemon available, and all four ADBC driver .so files loadable by the BE process. Cannot verify correctness without executing against a live stack."
  - test: "Run the two-SQLite join test in isolation: STARROCKS_HOME=... .venv/bin/pytest tests/test_cross_join.py::test_two_sqlite_catalogs_join -v"
    expected: "2 rows returned: Alice/100.0/NYC and Bob/200.0/SF. Validates cross-catalog federation with no Docker dependency."
    why_human: "Requires live StarRocks BE with ADBC scanner. Cannot run without the compiled backend."
  - test: "Run the TLS test: STARROCKS_HOME=... .venv/bin/pytest tests/test_flightsql.py::test_flightsql_tls_lifecycle -v"
    expected: "Docker TLS sqlflite container starts, root-ca.pem is extracted, StarRocks catalog is created with grpc+tls:// URI and adbc.flight.sql.client_option.tls_root_certs property passing to Go driver."
    why_human: "Requires Docker daemon, voltrondata/sqlflite:latest image, and the Go-based FlightSQL driver loaded in the BE process."
---

# Phase 5: External Integration Verification Suite — Verification Report

**Phase Goal:** Create a standalone Python test suite at `/home/mete/coding/opensource/adbc_verification` that exercises the full ADBC catalog stack end-to-end against four backends (SQLite, FlightSQL via sqlflite Docker, PostgreSQL via Docker, DuckDB), with and without TLS, testing catalog operations, schema operations, data read/write, cross-driver joins, error handling, and negative cases. Docker lifecycle management included. Produces agent-native JSON diagnostics for AI-driven fix convergence.

**Verified:** 2026-04-16T15:00:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| #  | Truth | Status | Evidence |
|----|-------|--------|----------|
| 1  | Python venv exists with pytest, pymysql, and pytest-json-report installed | VERIFIED | `.venv/bin/pytest` exists; `import pytest, pymysql, pytest_jsonreport` all succeed in venv |
| 2  | All four ADBC drivers (sqlite, flightsql, postgresql, duckdb) installed via dbc with resolvable `.so` paths | VERIFIED | All four `.toml` manifests present in `~/.config/adbc/drivers/`; all four `.so` files exist on disk per `get_all_driver_paths()` |
| 3  | `lib/starrocks.py` provides FE/BE startup via `ensure_starrocks_running()` with `cursor.description` column lookup | VERIFIED | Function exists at line 57; `STARROCKS_HOME` constant at line 13; `FE_MYSQL_PORT = 9030` at line 15; `cursor.description` used in `_find_column_index` at line 43; `start_fe.sh` and `start_be.sh` invocations present |
| 4  | `lib/docker_backends.py` can start/stop sqlflite and postgres Docker containers | VERIFIED | `start_sqlflite_no_tls`, `start_postgres`, `ensure_sqlflite_running`, `ensure_postgres_running`, `stop_container` all implemented with `SQLFLITE_PASSWORD=sqlflite_password` and `POSTGRES_PASSWORD=testpass` |
| 5  | `lib/driver_registry.py` parses dbc TOML manifests and returns driver `.so` paths | VERIFIED | `get_driver_path` and `get_all_driver_paths` implemented using `tomllib`, accessing `manifest["Driver"]["shared"][arch_key]` |
| 6  | `conftest.py` session fixtures are wired and importable by downstream test modules | VERIFIED | All imports present (`lib.starrocks`, `lib.docker_backends`, `lib.driver_registry`, `lib.tls`); 8 session fixtures defined (`sr_conn`, 4 driver paths, `sqlflite_port`, `postgres_port`, `sqlflite_tls`); `pytest_runtest_makereport` hook and `capture_on_failure` autouse fixture both present |
| 7  | SQLite test module provides 6 tests covering all D-09 scenarios | VERIFIED | 6 test functions: `test_sqlite_catalog_lifecycle`, `test_sqlite_data_roundtrip`, `test_sqlite_show_tables`, `test_sqlite_bad_driver_url`, `test_sqlite_unknown_top_level_key`, `test_sqlite_adbc_passthrough`; all use `try/finally` with `drop_catalog` |
| 8  | DuckDB test module provides 4 tests with `duckdb_adbc_init` entrypoint | VERIFIED | 4 test functions including `test_duckdb_missing_entrypoint` (PROP-03 negative case); all DuckDB tests pass `entrypoint="duckdb_adbc_init"` |
| 9  | FlightSQL test module provides 5 tests including TLS (non-TLS + TLS + negative + pass-through) | VERIFIED | 5 test functions; `grpc+tls://` URI and `adbc.flight.sql.client_option.tls_root_certs` pass-through present; `@pytest.mark.tls` on TLS test |
| 10 | PostgreSQL test module provides 5 tests with docker exec data seeding | VERIFIED | 5 test functions; `_PG_URI = "postgresql://testuser:testpass@127.0.0.1:5432/testdb"` present; `docker exec` data seeding in `postgres_test_data` fixture |
| 11 | Cross-driver join module provides 2 tests (SQLite-PostgreSQL join, two-SQLite join) | VERIFIED | `test_sqlite_postgres_join` and `test_two_sqlite_catalogs_join`; both contain SQL `JOIN` with three-part fully-qualified table names; both use `try/finally` dropping multiple catalogs |
| 12 | Negative test module provides 7 tests covering all VAL/PROP error classes | VERIFIED | 7 tests: PROP-02 mutual exclusion, PROP-02 missing both, VAL-04 unknown key (names `weird_unknown_key`), VAL-03 file not found (`/does/not/exist/`), VAL-03 bad entrypoint (`NonExistentInitFunction`), PROP-05 pass-through acceptance, duplicate catalog rejection |
| 13 | Full 29-test suite collects cleanly with zero import errors | VERIFIED | `pytest --collect-only` produces "29 tests collected in 0.01s" with `STARROCKS_HOME` set; per-module: 6+4+5+5+2+7=29 |
| 14 | JSON report output configured in `pyproject.toml` addopts | VERIFIED | `pyproject.toml` `addopts` contains `--json-report`, `--json-report-file=reports/latest.json`, `--json-report-indent=2`; `reports/latest.json` produced from collect-only run |

**Score:** 14/14 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `/home/mete/coding/opensource/adbc_verification/pyproject.toml` | Project metadata, pytest config, dependency list | VERIFIED | Contains `pytest-json-report`, `testpaths = ["tests"]`, all 7 markers |
| `/home/mete/coding/opensource/adbc_verification/conftest.py` | Session-scoped fixtures for StarRocks, Docker, driver paths | VERIFIED | `sr_conn` and all 7 other session fixtures defined; failure capture hook wired |
| `/home/mete/coding/opensource/adbc_verification/lib/starrocks.py` | FE/BE startup, SQL helpers, log capture | VERIFIED | `ensure_starrocks_running`, `tail_log`, `FE_MYSQL_PORT = 9030`, `cursor.description` column lookup, `start_fe.sh` + `start_be.sh` invocations |
| `/home/mete/coding/opensource/adbc_verification/lib/docker_backends.py` | Docker container lifecycle (sqlflite, postgres) | VERIFIED | `start_sqlflite_no_tls`, `start_postgres`, `ensure_*_running`, `stop_container` all present with correct env vars |
| `/home/mete/coding/opensource/adbc_verification/lib/driver_registry.py` | dbc TOML manifest parser for driver `.so` paths | VERIFIED | `get_driver_path` using `tomllib` and `Driver"]["shared"]` key path |
| `/home/mete/coding/opensource/adbc_verification/lib/tls.py` | sqlflite TLS startup with volume-mounted cert extraction | VERIFIED | `start_sqlflite_tls`, `root-ca.pem` polling, host port `31338` |
| `/home/mete/coding/opensource/adbc_verification/lib/catalog_helpers.py` | CREATE/DROP CATALOG SQL helpers | VERIFIED | `create_adbc_catalog`, `drop_catalog`, `show_catalogs`, `execute_sql` |
| `/home/mete/coding/opensource/adbc_verification/tests/test_sqlite.py` | SQLite backend test module (6 tests) | VERIFIED | All 6 D-09 scenario tests present |
| `/home/mete/coding/opensource/adbc_verification/tests/test_duckdb.py` | DuckDB backend test module (4 tests) | VERIFIED | All 4 tests with `duckdb_adbc_init` entrypoint |
| `/home/mete/coding/opensource/adbc_verification/tests/test_flightsql.py` | FlightSQL backend test module (5 tests) | VERIFIED | Non-TLS, TLS, negative, pass-through all present |
| `/home/mete/coding/opensource/adbc_verification/tests/test_postgres.py` | PostgreSQL backend test module (5 tests) | VERIFIED | Lifecycle, round-trip, show tables, bad URI, pass-through |
| `/home/mete/coding/opensource/adbc_verification/tests/test_cross_join.py` | Cross-driver join tests (2 tests) | VERIFIED | SQLite-PostgreSQL join and two-SQLite join |
| `/home/mete/coding/opensource/adbc_verification/tests/test_negative.py` | Negative/error validation tests (7 tests) | VERIFIED | All VAL/PROP error classes covered |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `conftest.py` | `lib/starrocks.py` | `from lib.starrocks import ensure_starrocks_running, tail_log` | WIRED | Line 7 of conftest.py |
| `conftest.py` | `lib/docker_backends.py` | `from lib.docker_backends import ensure_sqlflite_running, ensure_postgres_running, stop_container` | WIRED | Lines 8-12 of conftest.py |
| `conftest.py` | `lib/driver_registry.py` | `from lib.driver_registry import get_driver_path` | WIRED | Line 13 of conftest.py |
| `conftest.py` | `lib/tls.py` | `from lib.tls import start_sqlflite_tls` | WIRED | Line 14 of conftest.py |
| `tests/test_sqlite.py` | `conftest.py` | Fixture injection (`sr_conn`, `sqlite_driver_path`) | WIRED | Function signatures use both fixtures |
| `tests/test_duckdb.py` | `conftest.py` | Fixture injection (`sr_conn`, `duckdb_driver_path`) | WIRED | Function signatures use both fixtures |
| `tests/test_flightsql.py` | `conftest.py` | Fixture injection (`sr_conn`, `flightsql_driver_path`, `sqlflite_port`, `sqlflite_tls`) | WIRED | All four fixtures used across 5 tests |
| `tests/test_postgres.py` | `conftest.py` | Fixture injection (`sr_conn`, `postgres_driver_path`, `postgres_port`) | WIRED | All three fixtures used across 5 tests |
| `tests/test_cross_join.py` | `conftest.py` | Fixture injection (`sr_conn`, `sqlite_driver_path`, `postgres_driver_path`, `postgres_port`) | WIRED | All four fixtures used |
| `tests/test_negative.py` | `conftest.py` | Fixture injection (`sr_conn`, `sqlite_driver_path`) | WIRED | Both fixtures used |
| `lib/tls.py` | `lib/docker_backends.py` | `from lib.docker_backends import _wait_for_port` | WIRED | Line 10 of tls.py |

### Data-Flow Trace (Level 4)

This phase produces a test infrastructure suite, not a rendering component. Artifacts are Python modules and test files, not data-rendering components with state variables. Level 4 data-flow tracing does not apply. The "data flows" in this context are verified at the structural level: fixtures return live values (driver paths, Docker ports, DB connections), and test functions pass them to `create_adbc_catalog` / `execute_sql`. The full data chain can only be confirmed with a live StarRocks instance (see Human Verification Required).

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All 29 tests collect without import errors | `STARROCKS_HOME=... .venv/bin/pytest --collect-only` | "29 tests collected in 0.01s" | PASS |
| All library modules import cleanly | `STARROCKS_HOME=... .venv/bin/python -c "from lib.X import ..."` (all 5 modules) | All imports succeed | PASS |
| All four driver `.so` files resolvable via dbc manifests | `get_all_driver_paths()` | 4 paths returned, all `os.path.exists()` True | PASS |
| JSON report structure produced | `cat reports/latest.json` | Valid JSON with 8 top-level keys including `tests`, `summary`, `collectors` | PASS |
| Full 29-test suite end-to-end execution | Requires live StarRocks BE + Docker | N/A — cannot run without compiled backend | SKIP |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| GOAL-05 | 05-01, 05-02, 05-03, 05-04 | Standalone Python test suite at `/home/mete/coding/opensource/adbc_verification` exercising four backends end-to-end with TLS, cross-driver joins, error handling, JSON diagnostics | VERIFIED (structurally) | 29-test suite collects cleanly; all infrastructure and test modules exist with correct content; live execution pending Phase 2/3 completion |

Note: `GOAL-05` is a phase-level goal reference, not one of the 51 numbered v1 requirements in REQUIREMENTS.md (STK/PROP/META/VAL/PLAN/BE/TEST/DOC). The REQUIREMENTS.md traceability table maps no v1 requirements to Phase 5 by design — Phase 5 is the external verification suite, not an implementation requirement. No orphaned requirements were found.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None | — | No TODO/FIXME/placeholder, no empty handlers, no hardcoded empty returns in rendering paths | — | — |

No anti-patterns found. All "return null / return []" occurrences are in test assertion branches (empty result detection) or are correct behavior (e.g., `_mysql_connect` returning None before a connection is made). The `duckdb_test_db` fixture returns a path string and correctly removes stale DB files before creating fresh ones — this is correct state management, not a stub.

### Human Verification Required

#### 1. Full Integration Test Run Against Live StarRocks

**Test:** With the Phase 2 (FE rewrite) and Phase 3 (BE scanner rewrite) ADBC catalog stack compiled and running:
```bash
cd /home/mete/coding/opensource/adbc_verification
STARROCKS_HOME=/home/mete/coding/opensource/starrocks .venv/bin/pytest tests/ -v
```
**Expected:** All 29 tests pass. `reports/latest.json` is produced with per-test pass/fail and `user_properties` containing `fe_log_tail`/`be_log_tail` entries for any failures.

**Why human:** Requires a compiled StarRocks BE with the ADBC driver manager stack, Docker daemon, and all four ADBC `.so` files accessible to the BE process. The test suite is structurally verified but its functional correctness depends on the Phase 2/3 implementation being complete.

#### 2. Cross-Driver Federation Join (No Docker)

**Test:**
```bash
STARROCKS_HOME=... .venv/bin/pytest tests/test_cross_join.py::test_two_sqlite_catalogs_join -v
```
**Expected:** StarRocks executes `SELECT o.customer, o.amount, c.city FROM cross_a_cat.main.orders o JOIN cross_b_cat.main.customers c ON o.customer = c.name ORDER BY o.customer` returning exactly 2 rows: `('Alice', 100.0, 'NYC')` and `('Bob', 200.0, 'SF')`.

**Why human:** Requires live StarRocks BE with ADBC catalog scanner. This test has no Docker dependency (both backends are local SQLite files) and is the simplest end-to-end federation validation.

#### 3. TLS FlightSQL Connection

**Test:**
```bash
STARROCKS_HOME=... .venv/bin/pytest tests/test_flightsql.py::test_flightsql_tls_lifecycle -v
```
**Expected:** `voltrondata/sqlflite:latest` container starts with `TLS_ENABLED=1`, writes `root-ca.pem` to the volume-mounted temp dir, and StarRocks successfully connects via `grpc+tls://127.0.0.1:31338` using `adbc.flight.sql.client_option.tls_root_certs` pass-through.

**Why human:** Requires Docker daemon, the sqlflite image, and the Go-based FlightSQL driver loaded in the BE process via `dlopen`.

### Gaps Summary

No structural gaps. All 29 tests exist, collect cleanly, and implement the specified scenarios. The only remaining work is executing the suite against the live Phase 2+3 implementation — this is a dependency on earlier phases, not a gap in this phase's deliverables.

---

_Verified: 2026-04-16T15:00:00Z_
_Verifier: Claude (gsd-verifier)_
