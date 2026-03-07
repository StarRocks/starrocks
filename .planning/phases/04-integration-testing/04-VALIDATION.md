---
phase: 4
slug: integration-testing
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-07
---

# Phase 4 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit 5 (FE plan tests) + StarRocks SQL-tester (T/R integration tests) |
| **Config file** | `fe/pom.xml` (JUnit), `test/conf/sr.conf` (SQL-tester) |
| **Quick run command** | `cd fe && mvn test -Dtest=ADBCScanPlanTest -pl fe-core` |
| **Full suite command** | `cd test && python3 run.py -d sql/test_adbc_catalog -v` |
| **Estimated runtime** | ~60 seconds (plan tests); E2E requires manual Flight SQL server |

---

## Sampling Rate

- **After every task commit:** Run `cd fe && mvn test -Dtest=ADBCScanPlanTest -pl fe-core`
- **After every plan wave:** Run full plan test suite + verify E2E locally
- **Before `/gsd:verify-work`:** All plan tests green; E2E verified against sqlflite
- **Max feedback latency:** 60 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 04-01-01 | 01 | 1 | TEST-01 | unit (plan test) | `cd fe && mvn test -Dtest=ADBCScanPlanTest -pl fe-core` | ❌ W0 | ⬜ pending |
| 04-02-01 | 02 | 1 | TEST-02, TEST-03 | integration | `cd test && python3 run.py -d sql/test_adbc_catalog -v` | ❌ W0 | ⬜ pending |
| 04-02-02 | 02 | 1 | TEST-04 | manual | Manual: start sqlflite, run tests | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `fe/fe-core/src/test/java/com/starrocks/connector/adbc/MockedADBCMetadata.java` — stubs for TEST-01
- [ ] `fe/fe-core/src/test/java/com/starrocks/sql/plan/ADBCScanPlanTest.java` — plan test class for TEST-01
- [ ] `test/sql/test_adbc_catalog/T/test_adbc_ddl` — T file for TEST-02
- [ ] `test/sql/test_adbc_catalog/T/test_adbc_query` — T file for TEST-02
- [ ] `test/sql/test_adbc_catalog/T/test_adbc_mv` — T file for TEST-02
- [ ] `test/conf/sr.conf` update with `[.flightsql]` section — for TEST-03

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Tests pass against Flight SQL server | TEST-04 | Requires external DuckDB Flight SQL process | 1. Start sqlflite Docker container; 2. Set sr.conf variables; 3. Run `cd test && python3 run.py -d sql/test_adbc_catalog -v` |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
