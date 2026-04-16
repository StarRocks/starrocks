---
status: partial
phase: 05-external-integration-verification-suite
source: [05-VERIFICATION.md]
started: 2026-04-16
updated: 2026-04-16
---

## Current Test

[awaiting human testing]

## Tests

### 1. Full integration run — all 29 tests pass against live StarRocks with Docker
expected: `STARROCKS_HOME=/home/mete/coding/opensource/starrocks /home/mete/coding/opensource/adbc_verification/.venv/bin/pytest /home/mete/coding/opensource/adbc_verification/tests/ -v` exits 0 with 29 tests passing
result: [pending]

### 2. Cross-driver federation join (no Docker) — test_two_sqlite_catalogs_join
expected: `pytest tests/test_cross_join.py::test_two_sqlite_catalogs_join -v` passes, validating federation mechanics with only SQLite (no Docker dependency)
result: [pending]

### 3. TLS FlightSQL connection — test_flightsql_tls_lifecycle
expected: `pytest tests/test_flightsql.py::test_flightsql_tls_lifecycle -v` passes, validating cert pass-through with Go driver via `adbc.flight.sql.client_option.tls_root_certs`
result: [pending]

## Summary

total: 3
passed: 0
issues: 0
pending: 3
skipped: 0
blocked: 0

## Gaps
