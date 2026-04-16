---
phase: 05-external-integration-verification-suite
plan: 04
subsystem: testing
tags: [cross-join, negative-tests, federation, validation, json-report]

# Dependency graph
requires:
  - phase: 05-02
    provides: "SQLite and DuckDB test modules with data fixtures"
  - phase: 05-03
    provides: "FlightSQL and PostgreSQL test modules with Docker fixtures"
provides:
  - "Cross-driver join tests validating federated queries across SQLite and PostgreSQL catalogs"
  - "Negative test module covering all VAL-03/VAL-04/PROP-02/PROP-05 error classes"
  - "Full 29-test suite collectable with zero import errors"
  - "JSON report output via pytest-json-report at reports/latest.json"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns: [cross-catalog-federation-joins, three-part-table-names, negative-error-class-testing, structured-json-diagnostics]

key-files:
  created:
    - /home/mete/coding/opensource/adbc_verification/tests/test_cross_join.py
    - /home/mete/coding/opensource/adbc_verification/tests/test_negative.py
  modified: []

# Metrics
metrics:
  files_created: 2
  files_modified: 0
  tests_added: 9
  total_suite_tests: 29

---

## What was built

### Cross-driver join test module (test_cross_join.py)
Two tests validating the core ADBC catalog federation value proposition:
1. `test_sqlite_postgres_join` — Creates SQLite and PostgreSQL catalogs simultaneously, executes a cross-catalog JOIN using three-part fully-qualified table names (`cross_sqlite_cat.main.employees JOIN cross_pg_cat.public.departments`), asserts correct merged result set.
2. `test_two_sqlite_catalogs_join` — Simpler variant joining two separate SQLite catalogs without Docker dependency, validates the join mechanics work even within the same driver type.

### Negative test module (test_negative.py)
Seven tests covering all externally-triggerable validation error classes:
1. `test_both_driver_url_and_name_rejected` — PROP-02 mutual exclusion
2. `test_neither_driver_url_nor_name_rejected` — PROP-02 missing both identifiers
3. `test_unknown_top_level_key_names_the_key` — VAL-04 (error names `weird_unknown_key`)
4. `test_nonexistent_driver_url_path` — VAL-03 file not found (`/does/not/exist/`)
5. `test_bad_entrypoint_symbol` — VAL-03 entrypoint missing (`NonExistentInitFunction`)
6. `test_adbc_prefixed_key_not_rejected` — PROP-05 pass-through acceptance
7. `test_duplicate_catalog_name_rejected` — Duplicate catalog name rejection

### JSON report verification
Full suite configured via pyproject.toml `addopts` to produce `reports/latest.json` with per-test pass/fail status and `user_properties` for failure diagnostics (FE/BE log tails via the `capture_on_failure` hook from conftest.py).

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | `e2343f6` | test(05-04): add cross-driver join test module (D-10) |
| 2 | `7c6b0c4` | test(05-04): add negative test module for all VAL/PROP error classes |

## Deviations

None.

## Self-Check: PASSED

- [x] test_cross_join.py created with 2 cross-driver join tests
- [x] test_negative.py created with 7 negative/error validation tests
- [x] All tests use `@pytest.mark.cross_join` / `@pytest.mark.negative` markers
- [x] All tests use `try/finally` with `drop_catalog` cleanup
- [x] Full suite (29 tests across 6 modules) collects with zero import errors
- [x] JSON report configuration in pyproject.toml addopts
